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

/* EXTRA 1 – libssh client: establish tunnel, return a normal socket fd */

typedef struct {
    ssh_channel      channel;
    int              local_fd;
    int             *done;
    pthread_mutex_t *done_lock;
} SSHBridgeArgs;

// EXTRA 1: forward bytes from SSH channel into the local socketpair end
void *ssh_bridge_ch_to_local(void *arg) {
    SSHBridgeArgs *a = (SSHBridgeArgs *)arg;
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
            int w = send(a->local_fd, buf + sent, n - sent, 0);
            if (w <= 0) goto done_c2l;
            sent += w;
        }
    }
done_c2l:
    pthread_mutex_lock(a->done_lock); *a->done = 1; pthread_mutex_unlock(a->done_lock);
    return NULL;
}

// EXTRA 1: forward bytes from the local socketpair end into the SSH channel
void *ssh_bridge_local_to_ch(void *arg) {
    SSHBridgeArgs *a = (SSHBridgeArgs *)arg;
    char buf[4096];
    while (1) {
        pthread_mutex_lock(a->done_lock);
        int d = *a->done; pthread_mutex_unlock(a->done_lock);
        if (d) break;

        fd_set rfds; FD_ZERO(&rfds); FD_SET(a->local_fd, &rfds);
        struct timeval tv = {0, 200000};
        if (select(a->local_fd + 1, &rfds, NULL, NULL, &tv) <= 0) continue;

        int n = recv(a->local_fd, buf, sizeof(buf), 0);
        if (n <= 0) break;
        if (ssh_channel_write(a->channel, buf, n) == SSH_ERROR) break;
    }
    pthread_mutex_lock(a->done_lock); *a->done = 1; pthread_mutex_unlock(a->done_lock);
    return NULL;
}

/* EXTRA 1: connect to server via SSH and open a direct-tcpip forwarding channel. */
// Returns a file descriptor that behaves like a plain TCP socket, or -1 on error.
int ssh_connect_and_forward(const char *server_ip) {
    ssh_session session = ssh_new();
    if (!session) return -1;

    ssh_options_set(session, SSH_OPTIONS_HOST, server_ip);
    int port = SSH_PORT;
    ssh_options_set(session, SSH_OPTIONS_PORT, &port);
    // disable strict host-key checking for ease of use
    int strict = 0;
    ssh_options_set(session, SSH_OPTIONS_STRICTHOSTKEYCHECK, &strict);

    if (ssh_connect(session) != SSH_OK) {
        fprintf(stderr, "SSH connect error: %s\n", ssh_get_error(session));
        ssh_free(session); return -1;
    }

    // EXTRA 1: try none auth first; fall back to password if needed
    if (ssh_userauth_none(session, NULL) == SSH_AUTH_ERROR) {
        if (ssh_userauth_password(session, NULL, "chat") != SSH_AUTH_SUCCESS) {
            fprintf(stderr, "SSH auth failed: %s\n", ssh_get_error(session));
            ssh_disconnect(session); ssh_free(session); return -1;
        }
    }

    // EXTRA 1: open direct-tcpip channel → server's internal chat port
    ssh_channel channel = ssh_channel_new(session);
    if (!channel) { ssh_disconnect(session); ssh_free(session); return -1; }

    if (ssh_channel_open_forward(channel,
            "127.0.0.1", INTERNAL_PORT,
            "127.0.0.1", 0) != SSH_OK) {
        fprintf(stderr, "SSH channel forward error: %s\n", ssh_get_error(session));
        ssh_channel_free(channel);
        ssh_disconnect(session); ssh_free(session); return -1;
    }

    // create a local socketpair; bridge threads own sv[0], caller gets sv[1]
    int sv[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) {
        ssh_channel_close(channel); ssh_channel_free(channel);
        ssh_disconnect(session); ssh_free(session); return -1;
    }

    int *done = malloc(sizeof(int)); *done = 0;
    pthread_mutex_t *done_lock = malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(done_lock, NULL);

    SSHBridgeArgs *a1 = malloc(sizeof(SSHBridgeArgs));
    a1->channel = channel; a1->local_fd = sv[0];
    a1->done = done; a1->done_lock = done_lock;

    SSHBridgeArgs *a2 = malloc(sizeof(SSHBridgeArgs));
    a2->channel = channel; a2->local_fd = sv[0];
    a2->done = done; a2->done_lock = done_lock;

    pthread_t t1, t2;
    pthread_create(&t1, NULL, ssh_bridge_ch_to_local, a1);
    pthread_create(&t2, NULL, ssh_bridge_local_to_ch, a2);
    pthread_detach(t1);
    pthread_detach(t2);

    close(sv[0]);   // bridge owns this end
    return sv[1];   // caller uses this end just like a TCP socket
}

// Chat threads (unchanged from CP3)
typedef struct { int clisockfd; } ThreadArgs;

void *thread_main_receive(void *args) {
    pthread_detach(pthread_self());
    int sockfd = ((ThreadArgs *)args)->clisockfd;
    free(args);

    char buffer[512];
    int nrcv;

    while (1) {
        memset(buffer, 0, sizeof(buffer));
        nrcv = recv(sockfd, buffer, sizeof(buffer) - 1, 0);
        if (nrcv <= 0) break;
        buffer[nrcv] = '\0';

        // Req 5: parse sender name and apply unique color
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
    int sockfd = ((ThreadArgs *)args)->clisockfd;
    free(args);

    char buffer[256];
    int nsen;

    while (1) {
        memset(buffer, 0, sizeof(buffer));
        fgets(buffer, sizeof(buffer) - 1, stdin);

        if (strlen(buffer) == 1 && buffer[0] == '\n') break;
        buffer[strcspn(buffer, "\n")] = '\0';

        nsen = send(sockfd, buffer, strlen(buffer), 0);
        if (nsen < 0) error("ERROR writing to socket");
    }
    return NULL;
}

/* main */

int main(int argc, char *argv[]) {
    if (argc < 2) error("Please specify hostname");  // C3: updating argc check

    // EXTRA 1: establish SSH tunnel; sockfd behaves like a plain TCP socket
    printf("Connecting to %s via SSH tunnel...\n", argv[1]);
    int sockfd = ssh_connect_and_forward(argv[1]);
    if (sockfd < 0) {
        fprintf(stderr, "Failed to establish SSH tunnel to %s\n", argv[1]);
        return 1;
    }
    printf("SSH tunnel established.\n");

    // C2/C3: send room request
    if (argc == 3) {
        send(sockfd, argv[2], strlen(argv[2]), 0);
    } else {
        send(sockfd, "list", 4, 0);
    }

    char response[1024] = {0};
    int n = recv(sockfd, response, 1023, 0);
    if (n <= 0) error("ERROR receiving room response");
    response[n] = '\0';

    if (strcmp(response, "ERROR") == 0) {
        printf("Room does not exist.\n");
        close(sockfd); return 0;
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
        send(sockfd, choice, strlen(choice), 0);

        memset(response, 0, sizeof(response));
        n = recv(sockfd, response, 1023, 0);
        if (n <= 0) error("ERROR receiving room number");
        response[n] = '\0';

        if (strcmp(response, "ERROR") == 0) {
            printf("Room does not exist.\n");
            close(sockfd); return 0;
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
    send(sockfd, username, strlen(username), 0);

    // Req 3/4: print own join message locally
    struct sockaddr_in own_addr;
    socklen_t alen = sizeof(own_addr);
    getsockname(sockfd, (struct sockaddr *)&own_addr, &alen);
    char own_ip[32];
    strncpy(own_ip, inet_ntoa(own_addr.sin_addr), 32);
    printf("%s (%s) joined the chat room!\n", username, own_ip);
    fflush(stdout);

    pthread_t tid1, tid2;
    ThreadArgs *a;

    a = malloc(sizeof(ThreadArgs)); a->clisockfd = sockfd;
    pthread_create(&tid1, NULL, thread_main_send, a);

    a = malloc(sizeof(ThreadArgs)); a->clisockfd = sockfd;
    pthread_create(&tid2, NULL, thread_main_receive, a);

    pthread_join(tid1, NULL);
    close(sockfd);
    return 0;
}