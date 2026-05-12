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
#include <libssh/server.h>


// ports
#define SSH_PORT       2222   // EXTRA 1: clients SSH into this port
#define INTERNAL_PORT  8081   // chat logic; bound to 127.0.0.1 only
#define FILE_BASE_PORT 9000   // EXTRA 2: ephemeral ports for file byte relay

// limits
#define MAX_ROOMS      10
#define MAX_PENDING_FT 16     // EXTRA 2: max concurrent in-flight file transfers

// SSH host key path (generated automatically on first run)
#define HOSTKEY_PATH "/tmp/chat_hostkey_rsa"


void error(const char *msg) { perror(msg); exit(1); }


// one connected chat user
typedef struct _USR {
    int          clisockfd;
    char         user[64];
    char         ip[32];
    struct _USR *next;
} USR;

// one chat room
typedef struct _ROOM {
    int             room_number;
    int             active;
    USR            *members;
    pthread_mutex_t lock;
} ROOM;

ROOM rooms[MAX_ROOMS];
int  next_room_number = 1;
pthread_mutex_t rooms_lock = PTHREAD_MUTEX_INITIALIZER;

// EXTRA 2: one pending file transfer slot
typedef struct _FT {
    int  in_use;
    int  ft_port;
    char sender[64];
    char receiver[64];
    char filename[256];
    long filesize;
    int  sender_fd;
    int  receiver_fd;
    int  room_idx;
} FT;

FT ft_slots[MAX_PENDING_FT];
pthread_mutex_t ft_lock = PTHREAD_MUTEX_INITIALIZER;
int ft_next_port = FILE_BASE_PORT;


// ── room management ──────────────────────────────────────────────────────────

// C2: initialise rooms array
void init_rooms(void) {
    for (int i = 0; i < MAX_ROOMS; i++) {
        rooms[i].room_number = 0;
        rooms[i].active      = 0;
        rooms[i].members     = NULL;
        pthread_mutex_init(&rooms[i].lock, NULL);
    }
}

// C2: find a room by room_number; returns slot index or -1
int find_room(int room_number) {
    for (int i = 0; i < MAX_ROOMS; i++)
        if (rooms[i].active && rooms[i].room_number == room_number)
            return i;
    return -1;
}

// C2: mark first inactive slot as a new room
int create_room(void) {
    pthread_mutex_lock(&rooms_lock);
    for (int i = 0; i < MAX_ROOMS; i++) {
        if (!rooms[i].active) {
            rooms[i].active      = 1;
            rooms[i].room_number = next_room_number++;
            rooms[i].members     = NULL;
            pthread_mutex_unlock(&rooms_lock);
            return i;
        }
    }
    pthread_mutex_unlock(&rooms_lock);
    return -1;
}

// C2: add a new client to a room
void room_add_client(int room_idx, int clisockfd, char *ip) {
    pthread_mutex_lock(&rooms[room_idx].lock);
    USR *u = malloc(sizeof(USR));
    u->clisockfd = clisockfd;
    strncpy(u->ip, ip, 32);
    strcpy(u->user, "unknown");
    u->next = NULL;
    if (!rooms[room_idx].members) {
        rooms[room_idx].members = u;
    } else {
        USR *cur = rooms[room_idx].members;
        while (cur->next) cur = cur->next;
        cur->next = u;
    }
    pthread_mutex_unlock(&rooms[room_idx].lock);
}

// C2: remove a client from a room; close room if empty
void room_remove_client(int room_idx, int clisockfd) {
    pthread_mutex_lock(&rooms[room_idx].lock);
    USR *cur = rooms[room_idx].members, *prev = NULL;
    while (cur) {
        if (cur->clisockfd == clisockfd) {
            if (!prev) rooms[room_idx].members = cur->next;
            else       prev->next = cur->next;
            free(cur);
            break;
        }
        prev = cur; cur = cur->next;
    }
    if (!rooms[room_idx].members) {
        rooms[room_idx].active = 0;
        printf("Room %d is now empty and closed.\n", rooms[room_idx].room_number);
    }
    pthread_mutex_unlock(&rooms[room_idx].lock);
}

// print all connected clients to the server terminal
void print_client_list(void) {
    printf("Connected Clients:\n");
    int found = 0;
    for (int i = 0; i < MAX_ROOMS; i++) {
        if (rooms[i].active) {
            for (USR *cur = rooms[i].members; cur; cur = cur->next) {
                printf(" [Room %d] %s (%s)\n", rooms[i].room_number, cur->user, cur->ip);
                found = 1;
            }
        }
    }
    if (!found) printf("No clients connected\n");
}

// C3: build a human-readable room list into buf
void build_room_list(char *buf, int bufsize) {
    memset(buf, 0, bufsize);
    int offset = 0;
    for (int i = 0; i < MAX_ROOMS; i++) {
        if (rooms[i].active) {
            int count = 0;
            for (USR *c = rooms[i].members; c; c = c->next) count++;
            char line[64];
            if (count == 1)
                snprintf(line, sizeof(line), "Room %d: 1 person\n",  rooms[i].room_number);
            else
                snprintf(line, sizeof(line), "Room %d: %d people\n", rooms[i].room_number, count);
            strncat(buf, line, bufsize - offset - 1);
            offset += strlen(line);
        }
    }
}

// C2: broadcast a message to all room members except the sender
void room_broadcast(int room_idx, int fromfd, char *message) {
    pthread_mutex_lock(&rooms[room_idx].lock);
    for (USR *cur = rooms[room_idx].members; cur; cur = cur->next)
        if (cur->clisockfd != fromfd)
            send(cur->clisockfd, message, strlen(message), 0);
    pthread_mutex_unlock(&rooms[room_idx].lock);
}

// EXTRA 2: send a message directly to one named user in the room
void send_to_user(int room_idx, const char *username, const char *msg) {
    pthread_mutex_lock(&rooms[room_idx].lock);
    for (USR *cur = rooms[room_idx].members; cur; cur = cur->next)
        if (strcmp(cur->user, username) == 0) {
            send(cur->clisockfd, msg, strlen(msg), 0);
            break;
        }
    pthread_mutex_unlock(&rooms[room_idx].lock);
}

// EXTRA 2: return the socket fd for a named user in the room, or -1
int fd_for_user(int room_idx, const char *username) {
    pthread_mutex_lock(&rooms[room_idx].lock);
    int fd = -1;
    for (USR *cur = rooms[room_idx].members; cur; cur = cur->next)
        if (strcmp(cur->user, username) == 0) { fd = cur->clisockfd; break; }
    pthread_mutex_unlock(&rooms[room_idx].lock);
    return fd;
}


// ── EXTRA 2: file transfer ───────────────────────────────────────────────────

// allocate a transfer slot; returns index or -1 if full
int ft_alloc(void) {
    pthread_mutex_lock(&ft_lock);
    for (int i = 0; i < MAX_PENDING_FT; i++) {
        if (!ft_slots[i].in_use) {
            ft_slots[i].in_use  = 1;
            ft_slots[i].ft_port = ft_next_port++;
            pthread_mutex_unlock(&ft_lock);
            return i;
        }
    }
    pthread_mutex_unlock(&ft_lock);
    return -1;
}

void ft_free(int idx) {
    pthread_mutex_lock(&ft_lock);
    ft_slots[idx].in_use = 0;
    pthread_mutex_unlock(&ft_lock);
}

// EXTRA 2: async relay thread — opens an ephemeral TCP port, accepts sender
// and receiver, pipes bytes between them, then notifies both when done.
// Runs in its own detached thread so chat is never blocked.
typedef struct { int ft_idx; } FTRelayArgs;

void *ft_relay_thread(void *arg) {
    pthread_detach(pthread_self());
    int idx = ((FTRelayArgs *)arg)->ft_idx;
    free(arg);

    FT *ft   = &ft_slots[idx];
    int port = ft->ft_port;

    // bind a temporary listen socket for the raw file bytes
    int lsock = socket(AF_INET, SOCK_STREAM, 0);
    if (lsock < 0) { ft_free(idx); return NULL; }
    int opt = 1;
    setsockopt(lsock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in la;
    memset(&la, 0, sizeof(la));
    la.sin_family      = AF_INET;
    la.sin_addr.s_addr = INADDR_ANY;
    la.sin_port        = htons(port);

    if (bind(lsock, (struct sockaddr *)&la, sizeof(la)) < 0 || listen(lsock, 2) < 0) {
        close(lsock); ft_free(idx); return NULL;
    }

    // EXTRA 2: tell both parties which port to connect to for the raw transfer
    char port_msg[64];
    snprintf(port_msg, sizeof(port_msg), "FILE_PORT %d\n", port);
    send(ft->sender_fd,   port_msg, strlen(port_msg), 0);
    send(ft->receiver_fd, port_msg, strlen(port_msg), 0);

    // accept sender and receiver connections (30 s timeout each)
    int conn[2] = {-1, -1};
    for (int i = 0; i < 2; i++) {
        fd_set rfds; FD_ZERO(&rfds); FD_SET(lsock, &rfds);
        struct timeval tv = {30, 0};
        if (select(lsock + 1, &rfds, NULL, NULL, &tv) <= 0) break;
        struct sockaddr_in ca; socklen_t cl = sizeof(ca);
        conn[i] = accept(lsock, (struct sockaddr *)&ca, &cl);
    }
    close(lsock);

    // EXTRA 2: abort gracefully if one party never connected
    if (conn[0] < 0 || conn[1] < 0) {
        char ab[] = "FILE_ABORT Transfer aborted: peer did not connect.\n";
        if (conn[0] >= 0) { send(ft->sender_fd,   ab, strlen(ab), 0); close(conn[0]); }
        if (conn[1] >= 0) { send(ft->receiver_fd, ab, strlen(ab), 0); close(conn[1]); }
        ft_free(idx); return NULL;
    }

    // relay bytes: conn[0] = sender, conn[1] = receiver
    char buf[8192];
    long total = ft->filesize, received = 0;
    int n;
    while (received < total) {
        n = recv(conn[0], buf, sizeof(buf), 0);
        if (n <= 0) break;
        int sent = 0;
        while (sent < n) {
            int w = send(conn[1], buf + sent, n - sent, 0);
            if (w <= 0) goto relay_done;
            sent += w;
        }
        received += n;
    }
relay_done:
    close(conn[0]);
    close(conn[1]);

    // EXTRA 2: notify only sender and receiver — rest of room never sees this
    char done[512];
    snprintf(done, sizeof(done),
             "FILE_DONE Transfer of '%s' complete (%ld/%ld bytes).\n",
             ft->filename, received, total);
    send(ft->sender_fd,   done, strlen(done), 0);
    send(ft->receiver_fd, done, strlen(done), 0);

    ft_free(idx);
    return NULL;
}

// EXTRA 2: handle "FILE_SEND <receiver> <filename> <size>" from a sender
void handle_file_send(int room_idx, int sender_fd, const char *sender_name, char *payload) {
    char receiver[64] = {0}, filename[256] = {0};
    long filesize = 0;

    if (sscanf(payload, "%63s %255s %ld", receiver, filename, &filesize) != 3) {
        char e[] = "FILE_ERROR Bad FILE_SEND format.\n";
        send(sender_fd, e, strlen(e), 0); return;
    }
    if (strcmp(receiver, sender_name) == 0) {
        char e[] = "FILE_ERROR Cannot send a file to yourself.\n";
        send(sender_fd, e, strlen(e), 0); return;
    }

    int rfd = fd_for_user(room_idx, receiver);
    if (rfd < 0) {
        char e[] = "FILE_ERROR Receiver not found in this room.\n";
        send(sender_fd, e, strlen(e), 0); return;
    }

    int idx = ft_alloc();
    if (idx < 0) {
        char e[] = "FILE_ERROR Server busy; try again later.\n";
        send(sender_fd, e, strlen(e), 0); return;
    }

    strncpy(ft_slots[idx].sender,   sender_name, 64);
    strncpy(ft_slots[idx].receiver, receiver,    64);
    strncpy(ft_slots[idx].filename, filename,    256);
    ft_slots[idx].filesize    = filesize;
    ft_slots[idx].sender_fd   = sender_fd;
    ft_slots[idx].receiver_fd = rfd;
    ft_slots[idx].room_idx    = room_idx;

    // EXTRA 2: offer goes only to the receiver — room-isolated
    char offer[400];
    snprintf(offer, sizeof(offer), "FILE_OFFER %s %s %ld\n", sender_name, filename, filesize);
    send(rfd, offer, strlen(offer), 0);

    char wait[] = "FILE_WAIT Waiting for receiver to accept...\n";
    send(sender_fd, wait, strlen(wait), 0);
}

// EXTRA 2: handle FILE_ACCEPT or FILE_REJECT from the receiver
void handle_file_response(int room_idx, int receiver_fd, int accepted) {
    pthread_mutex_lock(&ft_lock);
    int idx = -1;
    for (int i = 0; i < MAX_PENDING_FT; i++) {
        if (ft_slots[i].in_use &&
            ft_slots[i].receiver_fd == receiver_fd &&
            ft_slots[i].room_idx   == room_idx) {
            idx = i; break;
        }
    }
    pthread_mutex_unlock(&ft_lock);
    if (idx < 0) return;

    if (!accepted) {
        char rej[] = "FILE_REJECTED Receiver declined the transfer.\n";
        send(ft_slots[idx].sender_fd, rej, strlen(rej), 0);
        ft_free(idx); return;
    }

    // EXTRA 2: start the async relay thread so chat is not blocked
    FTRelayArgs *a = malloc(sizeof(FTRelayArgs));
    a->ft_idx = idx;
    pthread_t tid;
    pthread_create(&tid, NULL, ft_relay_thread, a);
}


// ── per-client chat thread ───────────────────────────────────────────────────

typedef struct _ThreadArgs {
    int clisockfd;
    int room_idx;   // C2
} ThreadArgs;

void *thread_main(void *args) {
    pthread_detach(pthread_self());

    int clisockfd = ((ThreadArgs *)args)->clisockfd;
    int room_idx  = ((ThreadArgs *)args)->room_idx;
    free(args);

    // get client IP
    struct sockaddr_in cliaddr;
    socklen_t clen = sizeof(cliaddr);
    getpeername(clisockfd, (struct sockaddr *)&cliaddr, &clen);
    char ip[32];
    strncpy(ip, inet_ntoa(cliaddr.sin_addr), 32);

    room_add_client(room_idx, clisockfd, ip);  // C2

    // Req 3: first recv is the username
    char username[64] = {0};
    int nrcv;
    do { nrcv = recv(clisockfd, username, 63, 0); } while (nrcv == 0);
    if (nrcv > 0) {
        username[nrcv] = '\0';
        pthread_mutex_lock(&rooms[room_idx].lock);
        for (USR *c = rooms[room_idx].members; c; c = c->next)
            if (c->clisockfd == clisockfd) { strncpy(c->user, username, 64); break; }
        pthread_mutex_unlock(&rooms[room_idx].lock);
    }

    print_client_list();

    // Req 4: broadcast join notification
    char join_msg[128];
    snprintf(join_msg, sizeof(join_msg), "%s (%s) joined the chat room!\n", username, ip);
    room_broadcast(room_idx, clisockfd, join_msg);

    // main recv / dispatch loop
    char buffer[512];
    char formatted[700];

    while ((nrcv = recv(clisockfd, buffer, 511, 0)) > 0) {
        buffer[nrcv] = '\0';
        int blen = strlen(buffer);
        if (blen > 0 && buffer[blen-1] == '\n') buffer[--blen] = '\0';

        // EXTRA 2: intercept file-transfer protocol messages
        if (strncmp(buffer, "FILE_SEND ", 10) == 0) {
            handle_file_send(room_idx, clisockfd, username, buffer + 10);
        } else if (strcmp(buffer, "FILE_ACCEPT") == 0) {
            handle_file_response(room_idx, clisockfd, 1);
        } else if (strcmp(buffer, "FILE_REJECT") == 0) {
            handle_file_response(room_idx, clisockfd, 0);
        } else {
            // normal chat message
            snprintf(formatted, sizeof(formatted), "[%s (%s)] %s\n", username, ip, buffer);
            room_broadcast(room_idx, clisockfd, formatted);
        }
        memset(buffer, 0, sizeof(buffer));
    }

    // Req 4: broadcast leave notification
    char leave_msg[128];
    snprintf(leave_msg, sizeof(leave_msg), "%s (%s) left the room!\n", username, ip);
    room_broadcast(room_idx, clisockfd, leave_msg);

    room_remove_client(room_idx, clisockfd);
    print_client_list();
    close(clisockfd);
    return NULL;
}


// ── internal chat listener (127.0.0.1:INTERNAL_PORT) ────────────────────────
// accepts TCP connections forwarded from the SSH channel bridge

// C2/C3: room negotiation on a freshly-accepted internal socket
void handle_room_negotiation(int newsockfd) {
    char room_req[16] = {0}, response[1024] = {0};
    int n = recv(newsockfd, room_req, 15, 0);
    if (n <= 0) { close(newsockfd); return; }
    room_req[n] = '\0';

    int room_idx = -1;

    if (strcmp(room_req, "new") == 0) {
        // C2: create new room
        room_idx = create_room();
        if (room_idx < 0) { send(newsockfd, "ERROR", 5, 0); close(newsockfd); return; }
        snprintf(response, sizeof(response), "%d", rooms[room_idx].room_number);
        send(newsockfd, response, strlen(response), 0);
        printf("Created room %d\n", rooms[room_idx].room_number);

    } else if (strcmp(room_req, "list") == 0) {
        // C3: send room list or auto-create if none exist
        int any = 0;
        for (int i = 0; i < MAX_ROOMS; i++) if (rooms[i].active) { any = 1; break; }

        if (!any) {
            room_idx = create_room();
            char auto_msg[32];
            snprintf(auto_msg, sizeof(auto_msg), "NEW %d", rooms[room_idx].room_number);
            send(newsockfd, auto_msg, strlen(auto_msg), 0);
            printf("Auto created room %d\n", rooms[room_idx].room_number);
        } else {
            char list_buf[1024] = "LIST\n", room_list[900] = {0};
            build_room_list(room_list, 900);
            strncat(list_buf, room_list, sizeof(list_buf) - 6);
            send(newsockfd, list_buf, strlen(list_buf), 0);

            char choice[16] = {0};
            int cn = recv(newsockfd, choice, 15, 0);
            if (cn <= 0) { close(newsockfd); return; }
            choice[cn] = '\0';

            if (strcmp(choice, "new") == 0) {
                room_idx = create_room();
                if (room_idx < 0) { send(newsockfd, "ERROR", 5, 0); close(newsockfd); return; }
                snprintf(response, sizeof(response), "%d", rooms[room_idx].room_number);
                send(newsockfd, response, strlen(response), 0);
                printf("Created room %d\n", rooms[room_idx].room_number);
            } else {
                int rn = atoi(choice);
                room_idx = find_room(rn);
                if (room_idx < 0) { send(newsockfd, "ERROR", 5, 0); close(newsockfd); return; }
                snprintf(response, sizeof(response), "%d", rn);
                send(newsockfd, response, strlen(response), 0);
                printf("Joined room %d\n", rn);
            }
        }
    } else {
        // C2: join by room number
        int rn = atoi(room_req);
        room_idx = find_room(rn);
        if (room_idx < 0) { send(newsockfd, "ERROR", 5, 0); close(newsockfd); return; }
        snprintf(response, sizeof(response), "%d", rn);
        send(newsockfd, response, strlen(response), 0);
        printf("Joined room %d\n", rn);
    }

    ThreadArgs *ta = malloc(sizeof(ThreadArgs));
    if (!ta) error("ERROR malloc thread args");
    ta->clisockfd = newsockfd;
    ta->room_idx  = room_idx;
    pthread_t tid;
    if (pthread_create(&tid, NULL, thread_main, ta) != 0)
        error("ERROR creating per-client thread");
}

// EXTRA 1: background thread running the internal TCP listener
void *internal_listener_thread(void *arg) {
    (void)arg;
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) error("ERROR opening internal socket");
    int opt = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");   // loopback only
    addr.sin_port        = htons(INTERNAL_PORT);

    if (bind(sockfd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
        error("ERROR binding internal socket");
    listen(sockfd, 10);
    printf("Internal chat listener on 127.0.0.1:%d\n", INTERNAL_PORT);

    while (1) {
        struct sockaddr_in cli; socklen_t cl = sizeof(cli);
        int newsock = accept(sockfd, (struct sockaddr *)&cli, &cl);
        if (newsock < 0) continue;
        printf("Connected (internal): %s\n", inet_ntoa(cli.sin_addr));
        handle_room_negotiation(newsock);
    }
    return NULL;
}


// ── EXTRA 1: libssh server bridge ───────────────────────────────────────────
// each SSH client opens a direct-tcpip channel; we splice it to a real TCP
// socket on 127.0.0.1:INTERNAL_PORT using two threads:
//   bridge_ch_to_sock: SSH channel → socket
//   bridge_sock_to_ch: socket → SSH channel

typedef struct {
    ssh_channel      channel;
    int              sockfd;
    int             *done;
    pthread_mutex_t *done_lock;
} BridgeArgs;

// EXTRA 1: forward bytes from SSH channel into the internal socket
void *bridge_ch_to_sock(void *arg) {
    BridgeArgs *a = (BridgeArgs *)arg;
    char buf[4096];
    while (1) {
        pthread_mutex_lock(a->done_lock);
        int d = *a->done; pthread_mutex_unlock(a->done_lock);
        if (d) break;

        int n = ssh_channel_read_timeout(a->channel, buf, sizeof(buf), 0, 200);
        if (n == SSH_ERROR) break;
        if (n == 0) { if (ssh_channel_is_eof(a->channel)) break; continue; }

        int sent = 0;
        while (sent < n) {
            int w = send(a->sockfd, buf + sent, n - sent, 0);
            if (w <= 0) goto done_ch;
            sent += w;
        }
    }
done_ch:
    pthread_mutex_lock(a->done_lock); *a->done = 1; pthread_mutex_unlock(a->done_lock);
    free(a); return NULL;
}

// EXTRA 1: forward bytes from the internal socket back into the SSH channel
void *bridge_sock_to_ch(void *arg) {
    BridgeArgs *a = (BridgeArgs *)arg;
    char buf[4096];
    while (1) {
        pthread_mutex_lock(a->done_lock);
        int d = *a->done; pthread_mutex_unlock(a->done_lock);
        if (d) break;

        fd_set rfds; FD_ZERO(&rfds); FD_SET(a->sockfd, &rfds);
        struct timeval tv = {0, 200000};
        if (select(a->sockfd + 1, &rfds, NULL, NULL, &tv) <= 0) continue;

        int n = recv(a->sockfd, buf, sizeof(buf), 0);
        if (n <= 0) break;
        if (ssh_channel_write(a->channel, buf, n) == SSH_ERROR) break;
    }
    pthread_mutex_lock(a->done_lock); *a->done = 1; pthread_mutex_unlock(a->done_lock);
    free(a); return NULL;
}

// EXTRA 1: handle one SSH session: key exchange → auth → channel open → bridge
typedef struct { ssh_session session; } SSHClientArgs;

void *ssh_client_handler(void *arg) {
    pthread_detach(pthread_self());
    ssh_session session = ((SSHClientArgs *)arg)->session;
    free(arg);

    // EXTRA 1: SSH handshake
    if (ssh_handle_key_exchange(session) != SSH_OK) {
        fprintf(stderr, "SSH key exchange error: %s\n", ssh_get_error(session));
        ssh_disconnect(session); ssh_free(session); return NULL;
    }

    // EXTRA 1: accept none or password auth (any password accepted)
    ssh_set_auth_methods(session, SSH_AUTH_METHOD_NONE | SSH_AUTH_METHOD_PASSWORD);

    ssh_channel channel = NULL;
    int auth_ok = 0;

    while (!channel) {
        ssh_message msg = ssh_message_get(session);
        if (!msg) break;

        int mtype = ssh_message_type(msg);
        int msub  = ssh_message_subtype(msg);

        if (mtype == SSH_REQUEST_AUTH) {
            if (msub == SSH_AUTH_METHOD_NONE || msub == SSH_AUTH_METHOD_PASSWORD) {
                ssh_message_auth_reply_success(msg, 0);
                auth_ok = 1;
            } else {
                ssh_message_reply_default(msg);
            }
        } else if (mtype == SSH_REQUEST_CHANNEL_OPEN && auth_ok) {
            // EXTRA 1: accept direct-tcpip channel for port forwarding
            if (msub == SSH_CHANNEL_DIRECT_TCPIP) {
                channel = ssh_message_channel_request_open_reply_accept(msg);
            } else {
                ssh_message_reply_default(msg);
            }
        } else {
            ssh_message_reply_default(msg);
        }
        ssh_message_free(msg);
    }

    if (!channel) {
        ssh_disconnect(session); ssh_free(session); return NULL;
    }

    // EXTRA 1: connect to the internal chat listener and bridge
    int isock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in ia;
    memset(&ia, 0, sizeof(ia));
    ia.sin_family      = AF_INET;
    ia.sin_addr.s_addr = inet_addr("127.0.0.1");
    ia.sin_port        = htons(INTERNAL_PORT);

    if (connect(isock, (struct sockaddr *)&ia, sizeof(ia)) < 0) {
        ssh_channel_close(channel); ssh_channel_free(channel);
        ssh_disconnect(session); ssh_free(session); close(isock); return NULL;
    }

    int done = 0;
    pthread_mutex_t done_lock = PTHREAD_MUTEX_INITIALIZER;

    BridgeArgs *a1 = malloc(sizeof(BridgeArgs));
    a1->channel = channel; a1->sockfd = isock;
    a1->done = &done; a1->done_lock = &done_lock;

    BridgeArgs *a2 = malloc(sizeof(BridgeArgs));
    a2->channel = channel; a2->sockfd = isock;
    a2->done = &done; a2->done_lock = &done_lock;

    pthread_t t1, t2;
    pthread_create(&t1, NULL, bridge_ch_to_sock, a1);
    pthread_create(&t2, NULL, bridge_sock_to_ch, a2);
    pthread_join(t1, NULL);
    pthread_join(t2, NULL);

    close(isock);
    ssh_channel_send_eof(channel);
    ssh_channel_close(channel);
    ssh_channel_free(channel);
    ssh_disconnect(session);
    ssh_free(session);
    return NULL;
}

// EXTRA 1: generate RSA host key if not present
void ensure_hostkey(void) {
    if (access(HOSTKEY_PATH, F_OK) == 0) return;
    printf("Generating SSH host key at %s ...\n", HOSTKEY_PATH);
    char cmd[300];
    snprintf(cmd, sizeof(cmd),
             "ssh-keygen -t rsa -b 2048 -N '' -f %s >/dev/null 2>&1", HOSTKEY_PATH);
    if (system(cmd) != 0)
        fprintf(stderr, "Warning: ssh-keygen failed; SSH tunnel may not work.\n");
}


// ── main ─────────────────────────────────────────────────────────────────────

int main(int argc, char *argv[]) {
    (void)argc; (void)argv;

    init_rooms();
    memset(ft_slots, 0, sizeof(ft_slots));  // EXTRA 2: clear transfer slots

    // EXTRA 1: run the internal chat listener on a background thread
    pthread_t internal_tid;
    pthread_create(&internal_tid, NULL, internal_listener_thread, NULL);
    pthread_detach(internal_tid);

    // EXTRA 1: ensure RSA host key exists
    ensure_hostkey();

    // EXTRA 1: create and configure libssh bind
    ssh_bind sshbind = ssh_bind_new();
    int ssh_port = SSH_PORT;
    ssh_bind_options_set(sshbind, SSH_BIND_OPTIONS_BINDPORT, &ssh_port);
    ssh_bind_options_set(sshbind, SSH_BIND_OPTIONS_RSAKEY,   HOSTKEY_PATH);

    if (ssh_bind_listen(sshbind) < 0) {
        fprintf(stderr, "ssh_bind_listen failed: %s\n", ssh_get_error(sshbind));
        return 1;
    }

    printf("SSH chat server listening on port %d\n", SSH_PORT);
    printf("Clients connect with: ./main_client <server-ip>\n");

    // EXTRA 1: accept SSH connections, one thread per client
    while (1) {
        ssh_session session = ssh_new();
        if (!session) { fprintf(stderr, "ssh_new failed\n"); continue; }

        if (ssh_bind_accept(sshbind, session) != SSH_OK) {
            fprintf(stderr, "ssh_bind_accept: %s\n", ssh_get_error(sshbind));
            ssh_free(session); continue;
        }

        SSHClientArgs *a = malloc(sizeof(SSHClientArgs));
        a->session = session;
        pthread_t tid;
        pthread_create(&tid, NULL, ssh_client_handler, a);
    }

    ssh_bind_free(sshbind);
    return 0;
}