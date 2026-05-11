#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <pthread.h>

#define PORT_NUM 8080
#define MAX_USERS 32

void error(const char *msg)
{
    perror(msg);
    exit(0);
}

/* Req 5: color codes */
const char* COLORS[] = {
    "\033[31m",
    "\033[32m",
    "\033[33m",
    "\033[34m",
    "\033[35m",
    "\033[36m",
    "\033[91m",
    "\033[92m",
    "\033[93m",
    "\033[94m",
    "\033[95m",
    "\033[96m",
};

#define NUM_COLORS 12
#define RESET "\033[0m"

char color_names[MAX_USERS][64];
int color_assigned[MAX_USERS];
int num_colors_used = 0;
pthread_mutex_t color_lock = PTHREAD_MUTEX_INITIALIZER;

const char* get_color(const char* name) {
    pthread_mutex_lock(&color_lock);

    for (int i = 0; i < num_colors_used; i++) {
        if (strcmp(color_names[i], name) == 0) {
            int idx = color_assigned[i];
            pthread_mutex_unlock(&color_lock);
            return COLORS[idx];
        }
    }

    if (num_colors_used < MAX_USERS) {
        strncpy(color_names[num_colors_used], name, 64);
        color_assigned[num_colors_used] = num_colors_used % NUM_COLORS;
        const char* c = COLORS[num_colors_used % NUM_COLORS];

        num_colors_used++;
        pthread_mutex_unlock(&color_lock);
        return c;
    }

    pthread_mutex_unlock(&color_lock);
    return RESET;
}

typedef struct _ThreadArgs {
    int clisockfd;
} ThreadArgs;

void* thread_main_receive(void* args)
{
    pthread_detach(pthread_self());

    int sockfd = ((ThreadArgs*) args)->clisockfd;
    free(args);

    char buffer[512];
    int nrcv;

    while (1) {
        memset(buffer, 0, 512);
        nrcv = recv(sockfd, buffer, 511, 0);
        if (nrcv <= 0) break;
        buffer[nrcv] = '\0';

        /* Req 5: parse sender name and colorize */
        if (buffer[0] == '[') {
            char name[64] = {0};
            char* start = buffer + 1;
            char* end = strstr(start, " (");
            if (end != NULL) {
                int len = end - start;
                if (len > 63) len = 63;
                strncpy(name, start, len);
                const char* color = get_color(name);
                printf("\n%s%s%s\n", color, buffer, RESET);
            } else {
                printf("\n%s\n", buffer);
            }
        } else {
            /* system messages in default color */
            printf("\n%s\n", buffer);
        }
        fflush(stdout);
    }

    return NULL;
}

void* thread_main_send(void* args) {

    int sockfd = ((ThreadArgs*) args)->clisockfd;
    free(args);

    char buffer[256];
    int nsen;

    while (1) {
        memset(buffer, 0, 256);
        fgets(buffer, 255, stdin);

        if (strlen(buffer) == 1 && buffer[0] == '\n') break;
        buffer[strcspn(buffer, "\n")] = '\0';

        nsen = send(sockfd, buffer, strlen(buffer), 0);
        if (nsen < 0) {
            error ("ERROR writing to socket");
        }
    }

    return NULL;
}

int main(int argc, char *argv[])
{
    if (argc < 3) error("Please specify hostname and [new] or [room number]");

    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) error("ERROR opening socket");

    struct sockaddr_in serv_addr;
    socklen_t slen = sizeof(serv_addr);
    memset((char*) &serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = inet_addr(argv[1]);
    serv_addr.sin_port = htons(PORT_NUM);

    printf("Trying connecting to %s\n", inet_ntoa(serv_addr.sin_addr));

    if (connect(sockfd, (struct sockaddr*) &serv_addr, slen) < 0)
        error("ERROR connecting");

    send(sockfd, argv[2], strlen(argv[2]), 0);

    char response[32] = {0};
    int n = recv(sockfd, response, 31, 0);
    if (n <= 0) {
        error("ERROR receiving room response");
    }

    response[n] = '\0';

    if (strcmp(response, "ERROR") == 0) {
        printf("Room does not exist. \n");
        close(sockfd);
        return 0;
    }

    if (strcmp(argv[2], "new") == 0) {
        printf("Connected to %s with new room number %s\n", argv[1], response);
    } else {
    printf("Connected to room %s\n", response);
    }

    /* Req 3: prompt for username and send to server */
    char username[64];
    printf("Type your user name: ");
    fflush(stdout);
    /* scanf reads the name and stops before the newline */
   fgets(username, 63, stdin);
   username[strcspn(username, "\n")] = '\0';
   send(sockfd, username, strlen(username), 0);

    pthread_t tid1;
    pthread_t tid2;
    ThreadArgs* args;

    args = (ThreadArgs*) malloc(sizeof(ThreadArgs));
    args->clisockfd = sockfd;
    pthread_create(&tid1, NULL, thread_main_send, (void*) args);

    args = (ThreadArgs*) malloc(sizeof(ThreadArgs));
    args->clisockfd = sockfd;
    pthread_create(&tid2, NULL, thread_main_receive, (void*) args);

    pthread_join(tid1, NULL);

    close(sockfd);
    return 0;
}