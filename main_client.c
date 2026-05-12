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

// EXTRA 1: global SSH channel used by ch_send / ch_recv wrappers.
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
        // n == 0 means timeout, loop and try again
    }
}

// EXTRA 1: connect to server via SSH and open a direct-tcpip forwarding channel.
// Sets g_session and g_channel; returns 0 on success, -1 on error.
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

    // EXTRA 1: open direct-tcpip channel → server's internal chat port
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

// Chat threads — send/recv replaced with ch_send/ch_recv
typedef struct { int clisockfd; } ThreadArgs;

void *thread_main_receive(void *args) {
    pthread_detach(pthread_self());
    (void)args;

    char buffer[512];
    int nrcv;

    while (1) {
        memset(buffer, 0, sizeof(buffer));
        nrcv = ch_recv(buffer, sizeof(buffer) - 1);
        if (nrcv <= 0) break;
        buffer[nrcv] = '\0';

        // Req 5: parse sender name and apply unique color
        if (buffer[0] == '[') {
            char name[64] = {0};
            char *start = buffer + 1;
            har *end   = strstr(start, " (");
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
    char buffer[256];

    while (1) {
        memset(buffer, 0, sizeof(buffer));
        fgets(buffer, sizeof(buffer) - 1, stdin);

        if (strlen(buffer) == 1 && buffer[0] == '\n') break;
        buffer[strcspn(buffer, "\n")] = '\0';

        if (ch_send(buffer, strlen(buffer)) < 0) break;
    }
    return NULL;
}

// main
int main(int argc, char *argv[]) {
    if (argc < 2) error("Please specify hostname");  // C3: updating argc check

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