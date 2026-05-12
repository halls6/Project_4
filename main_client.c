#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <libssh/libssh.h>


// ports
#define SSH_PORT      2222   // EXTRA 1: connect SSH here
#define INTERNAL_PORT 8081   // EXTRA 1: forwarded-to port on server side

// limits
#define MAX_USERS 32


void error(const char *msg) { perror(msg); exit(0); }


// ANSI color codes (Req 5)
const char *COLORS[] = {
   "\033[31m", "\033[32m", "\033[33m", "\033[34m",
   "\033[35m", "\033[36m", "\033[91m", "\033[92m",
   "\033[93m", "\033[94m", "\033[95m", "\033[96m",
};
#define NUM_COLORS 12
#define RESET "\033[0m"


char color_names[MAX_USERS][64];
int  color_assigned[MAX_USERS];
int  num_colors_used = 0;
pthread_mutex_t color_lock = PTHREAD_MUTEX_INITIALIZER;


const char *get_color(const char *name) {
   pthread_mutex_lock(&color_lock);
   for (int i = 0; i < num_colors_used; i++) {
       if (strcmp(color_names[i], name) == 0) {
           const char *c = COLORS[color_assigned[i]];
           pthread_mutex_unlock(&color_lock);
           return c;
       }
   }
   if (num_colors_used < MAX_USERS) {
       strncpy(color_names[num_colors_used], name, 64);
       color_assigned[num_colors_used] = num_colors_used % NUM_COLORS;
       const char *c = COLORS[num_colors_used % NUM_COLORS];
       num_colors_used++;
       pthread_mutex_unlock(&color_lock);
       return c;
   }
   pthread_mutex_unlock(&color_lock);
   return RESET;
}


// ── EXTRA 1: SSH channel as the transport ────────────────────────────────────
// g_channel is set once after the tunnel is established.
// ch_send / ch_recv replace every send() / recv() call in the client.
// This avoids the socketpair bridge which caused a race on the shared fd.

static ssh_channel g_channel = NULL;
static ssh_session g_session = NULL;
static pthread_mutex_t g_ch_write_lock = PTHREAD_MUTEX_INITIALIZER;

// EXTRA 1: replaces send() — writes bytes into the SSH channel
int ch_send(const void *buf, int len) {
   pthread_mutex_lock(&g_ch_write_lock);
   int w = ssh_channel_write(g_channel, buf, len);
   pthread_mutex_unlock(&g_ch_write_lock);
   return (w == SSH_ERROR) ? -1 : w;
}

// EXTRA 1: replaces recv() — reads bytes from the SSH channel
int ch_recv(void *buf, int len) {
   while (1) {
       if (ssh_channel_is_eof(g_channel)) return 0;
       int n = ssh_channel_read_timeout(g_channel, buf, len, 0, 200);
       if (n == SSH_ERROR) return -1;
       if (n > 0) return n;
       // n == 0 means timeout; loop and retry
   }
}

// EXTRA 1: connect via SSH and open a direct-tcpip channel to the server's
// internal chat port. Sets g_session and g_channel; returns 0 or -1.
int ssh_connect_and_forward(const char *server_ip) {
   g_session = ssh_new();
   if (!g_session) return -1;

   ssh_options_set(g_session, SSH_OPTIONS_HOST, server_ip);
   int port = SSH_PORT;
   ssh_options_set(g_session, SSH_OPTIONS_PORT, &port);
   // disable strict host-key checking for ease of use
   int strict = 0;
   ssh_options_set(g_session, SSH_OPTIONS_STRICTHOSTKEYCHECK, &strict);

   if (ssh_connect(g_session) != SSH_OK) {
       fprintf(stderr, "SSH connect error: %s\n", ssh_get_error(g_session));
       ssh_free(g_session); return -1;
   }

   // EXTRA 1: try none auth first; fall back to password if needed
   if (ssh_userauth_none(g_session, NULL) != SSH_AUTH_SUCCESS) {
       if (ssh_userauth_password(g_session, NULL, "chat") != SSH_AUTH_SUCCESS) {
           fprintf(stderr, "SSH auth failed: %s\n", ssh_get_error(g_session));
           ssh_disconnect(g_session); ssh_free(g_session); return -1;
       }
   }

   // EXTRA 1: open direct-tcpip channel to server's internal chat port
   g_channel = ssh_channel_new(g_session);
   if (!g_channel) { ssh_disconnect(g_session); ssh_free(g_session); return -1; }

   if (ssh_channel_open_forward(g_channel,
           "127.0.0.1", INTERNAL_PORT,
           "127.0.0.1", 0) != SSH_OK) {
       fprintf(stderr, "SSH channel forward error: %s\n", ssh_get_error(g_session));
       ssh_channel_free(g_channel);
       ssh_disconnect(g_session); ssh_free(g_session); return -1;
   }

   return 0;
}


// ── EXTRA 2: file transfer state ─────────────────────────────────────────────

// server IP needed to open the raw data connection for file bytes
char g_server_ip[64] = {0};

// pending incoming offer (set when FILE_OFFER arrives, cleared after Y/N)
typedef struct {
   int  active;
   char sender[64];
   char filename[256];
   long filesize;
} PendingReceive;

PendingReceive pending_recv = {0};
pthread_mutex_t pending_recv_lock = PTHREAD_MUTEX_INITIALIZER;

// set when we sent FILE_SEND and are waiting for FILE_PORT from the server
int  g_expecting_file_port_send = 0;
char g_pending_filepath[512]    = {0};
long g_pending_filesize         = 0;

// EXTRA 2: receive thread — connects to the relay port and writes file to disk
typedef struct { int ft_port; char filename[256]; long filesize; } FTRecvArgs;

void *ft_receive_thread(void *arg) {
   pthread_detach(pthread_self());
   FTRecvArgs *a = (FTRecvArgs *)arg;

   // EXTRA 2: prefix output filename with "received_" to avoid overwriting
   // the source file when sender and receiver are in the same directory
   char outname[300];
   snprintf(outname, sizeof(outname), "received_%s", a->filename);

   int sock = socket(AF_INET, SOCK_STREAM, 0);
   if (sock < 0) { free(a); return NULL; }

   struct sockaddr_in sa;
   memset(&sa, 0, sizeof(sa));
   sa.sin_family      = AF_INET;
   sa.sin_addr.s_addr = inet_addr(g_server_ip);
   sa.sin_port        = htons(a->ft_port);

   // retry up to 5 s for the server relay thread to be ready
   int retries = 0;
   while (connect(sock, (struct sockaddr *)&sa, sizeof(sa)) < 0) {
       if (++retries > 10) {
           printf("\n[FT] Could not connect to transfer port.\n");
           close(sock); free(a); return NULL;
       }
       usleep(500000);
   }

   FILE *fp = fopen(outname, "wb");
   if (!fp) {
       printf("\n[FT] Cannot create output file '%s'.\n", outname);
       close(sock); free(a); return NULL;
   }

   char buf[8192];
   long received = 0;
   int n;
   while (received < a->filesize) {
       n = recv(sock, buf, sizeof(buf), 0);
       if (n <= 0) break;
       fwrite(buf, 1, n, fp);
       received += n;
   }
   fclose(fp);
   close(sock);

   printf("\n[FT] Received '%s' -> saved as '%s' (%ld bytes).\n",
          a->filename, outname, received);
   fflush(stdout);
   free(a);
   return NULL;
}

// EXTRA 2: send thread — connects to the relay port and streams the file
typedef struct { int ft_port; char filepath[512]; long filesize; } FTSendArgs;

void *ft_send_thread(void *arg) {
   pthread_detach(pthread_self());
   FTSendArgs *a = (FTSendArgs *)arg;

   int sock = socket(AF_INET, SOCK_STREAM, 0);
   if (sock < 0) { free(a); return NULL; }

   struct sockaddr_in sa;
   memset(&sa, 0, sizeof(sa));
   sa.sin_family      = AF_INET;
   sa.sin_addr.s_addr = inet_addr(g_server_ip);
   sa.sin_port        = htons(a->ft_port);

   int retries = 0;
   while (connect(sock, (struct sockaddr *)&sa, sizeof(sa)) < 0) {
       if (++retries > 10) {
           printf("\n[FT] Could not connect to transfer port for sending.\n");
           close(sock); free(a); return NULL;
       }
       usleep(500000);
   }

   FILE *fp = fopen(a->filepath, "rb");
   if (!fp) {
       printf("\n[FT] Cannot open '%s' for reading.\n", a->filepath);
       close(sock); free(a); return NULL;
   }

   char buf[8192];
   long sent_total = 0;
   int n;
   while ((n = fread(buf, 1, sizeof(buf), fp)) > 0) {
       int off = 0;
       while (off < n) {
           int w = send(sock, buf + off, n - off, 0);
           if (w <= 0) goto send_done;
           off += w;
       }
       sent_total += n;
   }
send_done:
   fclose(fp);
   close(sock);
   printf("\n[FT] Sent %ld bytes.\n", sent_total);
   fflush(stdout);
   free(a);
   return NULL;
}


// ── chat threads ─────────────────────────────────────────────────────────────

void *thread_main_receive(void *args) {
   pthread_detach(pthread_self());
   (void)args;

   char buffer[1024];
   int nrcv;

   while (1) {
       memset(buffer, 0, sizeof(buffer));
       nrcv = ch_recv(buffer, sizeof(buffer) - 1);
       if (nrcv <= 0) break;
       buffer[nrcv] = '\0';

       // EXTRA 2: FILE_OFFER — incoming transfer offer from another user
       if (strncmp(buffer, "FILE_OFFER ", 11) == 0) {
           char sender[64] = {0}, fname[256] = {0};
           long fsize = 0;
           sscanf(buffer + 11, "%63s %255s %ld", sender, fname, &fsize);

           pthread_mutex_lock(&pending_recv_lock);
           pending_recv.active = 1;
           strncpy(pending_recv.sender,   sender, 64);
           strncpy(pending_recv.filename, fname,  256);
           pending_recv.filesize = fsize;
           pthread_mutex_unlock(&pending_recv_lock);

           printf("\n[FT] %s wants to send you '%s' (%ld bytes). Accept? (Y/N): ",
                  sender, fname, fsize);
           fflush(stdout);
           continue;
       }

       // EXTRA 2: FILE_PORT — server assigned a relay port; start data thread
       if (strncmp(buffer, "FILE_PORT ", 10) == 0) {
           int ft_port = atoi(buffer + 10);

           pthread_mutex_lock(&pending_recv_lock);
           int is_recv = pending_recv.active;
           pthread_mutex_unlock(&pending_recv_lock);

           if (is_recv) {
               // we are the receiver
               pthread_mutex_lock(&pending_recv_lock);
               FTRecvArgs *ra = malloc(sizeof(FTRecvArgs));
               ra->ft_port = ft_port;
               strncpy(ra->filename, pending_recv.filename, 256);
               ra->filesize = pending_recv.filesize;
               pending_recv.active = 0;
               pthread_mutex_unlock(&pending_recv_lock);
               pthread_t tid;
               pthread_create(&tid, NULL, ft_receive_thread, ra);
           } else if (g_expecting_file_port_send) {
               // we are the sender
               g_expecting_file_port_send = 0;
               FTSendArgs *sa = malloc(sizeof(FTSendArgs));
               sa->ft_port = ft_port;
               strncpy(sa->filepath, g_pending_filepath, 512);
               sa->filesize = g_pending_filesize;
               pthread_t tid;
               pthread_create(&tid, NULL, ft_send_thread, sa);
           }
           continue;
       }

       // EXTRA 2: status messages only for sender/receiver — print and continue
       if (strncmp(buffer, "FILE_WAIT",     9) == 0 ||
           strncmp(buffer, "FILE_DONE",     9) == 0 ||
           strncmp(buffer, "FILE_ERROR",   10) == 0 ||
           strncmp(buffer, "FILE_ABORT",   10) == 0 ||
           strncmp(buffer, "FILE_REJECTED",13) == 0) {
           printf("\n%s\n", buffer);
           fflush(stdout);
           continue;
       }

       // Req 5: normal chat message — parse sender name and apply unique color
       if (buffer[0] == '[') {
           char name[64] = {0};
           char *start = buffer + 1;
           char *end   = strstr(start, " (");
           if (end) {
               int len = end - start;
               if (len > 63) len = 63;
               strncpy(name, start, len);
               const char *color = get_color(name);
               printf("\n%s%s%s\n", color, buffer, RESET);
           } else {
               printf("\n%s\n", buffer);
           }
       } else {
           printf("\n%s\n", buffer);
       }
       fflush(stdout);
   }
   return NULL;
}


void *thread_main_send(void *args) {
   (void)args;

   char buffer[512];

   while (1) {
       memset(buffer, 0, sizeof(buffer));
       fgets(buffer, sizeof(buffer) - 1, stdin);

       if (strlen(buffer) == 1 && buffer[0] == '\n') break;
       buffer[strcspn(buffer, "\n")] = '\0';

       // EXTRA 2: handle Y/N response to a FILE_OFFER
       pthread_mutex_lock(&pending_recv_lock);
       int has_offer = pending_recv.active;
       pthread_mutex_unlock(&pending_recv_lock);

       if (has_offer) {
           if (buffer[0] == 'Y' || buffer[0] == 'y') {
               ch_send("FILE_ACCEPT", 11);
           } else {
               ch_send("FILE_REJECT", 11);
               pthread_mutex_lock(&pending_recv_lock);
               pending_recv.active = 0;
               pthread_mutex_unlock(&pending_recv_lock);
           }
           continue;
       }

       // EXTRA 2: handle "SEND <receiver> <filename>" typed by the user
       if (strncmp(buffer, "SEND ", 5) == 0) {
           char receiver[64] = {0}, filepath[256] = {0};
           if (sscanf(buffer + 5, "%63s %255s", receiver, filepath) != 2) {
               printf("[FT] Usage: SEND <username> <filename>\n");
               continue;
           }

           FILE *fp = fopen(filepath, "rb");
           if (!fp) { printf("[FT] Cannot open '%s'.\n", filepath); continue; }
           fseek(fp, 0, SEEK_END);
           long fsize = ftell(fp);
           fclose(fp);

           // store for the send thread (triggered when FILE_PORT arrives)
           strncpy(g_pending_filepath, filepath, 512);
           g_pending_filesize         = fsize;
           g_expecting_file_port_send = 1;

           // use just the basename in the header sent to the server
           char *basename = strrchr(filepath, '/');
           if (basename) basename++; else basename = filepath;

           char header[400];
           snprintf(header, sizeof(header), "FILE_SEND %s %s %ld", receiver, basename, fsize);
           ch_send(header, strlen(header));
           continue;
       }

       // normal chat message
       if (ch_send(buffer, strlen(buffer)) < 0) break;
   }
   return NULL;
}


// ── main ─────────────────────────────────────────────────────────────────────

int main(int argc, char *argv[]) {
   if (argc < 2) error("Please specify hostname");  // C3: updating argc check

   strncpy(g_server_ip, argv[1], 63);  // EXTRA 2: save for file transfer threads

   // EXTRA 1: establish SSH tunnel; all I/O goes through ch_send/ch_recv
   printf("Connecting to %s via SSH tunnel...\n", argv[1]);
   if (ssh_connect_and_forward(argv[1]) < 0) {
       fprintf(stderr, "Failed to establish SSH tunnel to %s\n", argv[1]);
       return 1;
   }
   printf("SSH tunnel established.\n");

   // C2/C3: send room request
   if (argc == 3) {
       ch_send(argv[2], strlen(argv[2]));
   } else {
       ch_send("list", 4);
   }

   char response[1024] = {0};
   int n = ch_recv(response, 1023);
   if (n <= 0) { fprintf(stderr, "ERROR receiving room response\n"); return 1; }
   response[n] = '\0';

   if (strcmp(response, "ERROR") == 0) {
       printf("Room does not exist.\n");
       return 0;
   }

   if (strncmp(response, "LIST", 4) == 0) {
       // C3: display list and prompt
       printf("Server says the following options are available:\n");
       printf("%s", response + 5);
       printf("Choose the room number or type [new] to create a new room: ");
       fflush(stdout);

       char choice[16] = {0};
       fgets(choice, 15, stdin);
       choice[strcspn(choice, "\n")] = '\0';
       ch_send(choice, strlen(choice));

       memset(response, 0, sizeof(response));
       n = ch_recv(response, 1023);
       if (n <= 0) { fprintf(stderr, "ERROR receiving room number\n"); return 1; }
       response[n] = '\0';

       if (strcmp(response, "ERROR") == 0) {
           printf("Room does not exist.\n");
           return 0;
       }
       printf("Connected to room %s\n", response);

   } else if (strncmp(response, "NEW", 3) == 0) {
       printf("Connected to %s with new room number %s\n", argv[1], response + 4);
   } else {
       if (argc == 3 && strcmp(argv[2], "new") == 0)
           printf("Connected to %s with new room number %s\n", argv[1], response);
       else
           printf("Connected to room %s\n", response);
   }

   // Req 3: prompt for username and send to server
   char username[64];
   printf("Type your user name: ");
   fflush(stdout);
   fgets(username, 63, stdin);
   username[strcspn(username, "\n")] = '\0';
   ch_send(username, strlen(username));

   // Req 3/4: print own join message locally
   printf("%s (SSH) joined the chat room!\n", username);
   printf("[FT] To send a file: SEND <username> <filename>\n");
   fflush(stdout);

   pthread_t tid1, tid2;
   pthread_create(&tid1, NULL, thread_main_send,    NULL);
   pthread_create(&tid2, NULL, thread_main_receive, NULL);

   pthread_join(tid1, NULL);

   // cleanup SSH session on exit
   ssh_channel_send_eof(g_channel);
   ssh_channel_close(g_channel);
   ssh_channel_free(g_channel);
   ssh_disconnect(g_session);
   ssh_free(g_session);
   return 0;
}