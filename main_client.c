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

/* Req 5: ANSI color codes */
const char* COLORS[] = {
    "\033[31m",   /* red */
    "\033[32m",   /* green */
    "\033[33m",   /* yellow */
    "\033[34m",   /* blue */
    "\033[35m",   /* magenta */
    "\033[36m",   /* cyan */
    "\033[91m",   /* bright red */
    "\033[92m",   /* bright green */
    "\033[93m",   /* bright yellow */
    "\033[94m",   /* bright blue */
    "\033[95m",   /* bright magenta */
    "\033[96m",   /* bright cyan */
};
#define NUM_COLORS 12
#define RESET "\033[0m"

/* Req 5: map usernames to colors */
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

        /* Req 5: parse sender name from "[Name (IP)] message" and colorize */
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
            /* system messages (join/leave) in default color */
            printf("\n%s\n", buffer);
        }
        fflush(stdout);
    }

    return NULL;
}

void* thread_main_send(void* args) {
    pthread_detach(pthread_self());

    int sockfd = ((ThreadArgs*) args)->clisockfd;
    free(args);

    char buffer[256];
    int nsen;

    while (1) {
        memset(buffer, 0, 256);
        fgets(buffer, 255, stdin);

        if (strlen(buffer) == 1) break;
        buffer[strcspn(buffer, "\n")] = '\0';

        nsen = send(sockfd, buffer, strlen(buffer), 0);
        if (nsen < 0) error("ERROR writing to socket");
    }

    return NULL;
}

int main(int argc, char *argv[])
{
    if (argc < 2) error("Please specify hostname");

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

    printf("Connected to server\n");

    /* Req 3: prompt for username and send to server */
    char username[64];
    printf("Type your user name: ");
    fflush(stdout);
    /* scanf reads the name and stops before the newline */
    scanf("%63s", username);
    /* fix: drain the leftover newline so thread_main_send doesn't read it */
    int c;
    while ((c = getchar()) != '\n' && c != EOF);
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