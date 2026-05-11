#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>

#define PORT_NUM 8080

/* checkpoint 2: define rooms */
#define MAX_ROOMS 10

void error(const char *msg)
{
    perror(msg);
    exit(1);
}

/* represent each connected client */
typedef struct _USR {
    int clisockfd;
    char user[64];
    char ip[32];
    struct _USR* next;
} USR;

typedef struct _ROOM {
    int room_number;
    int active;
    USR* members;
    pthread_mutex_t lock;
} ROOM;

ROOM rooms[MAX_ROOMS];
int next_room_number = 1;
pthread_mutex_t rooms_lock = PTHREAD_MUTEX_INITIALIZER;

void init_rooms() { /* C2: sets up rooms */
    for (int i = 0; i < MAX_ROOMS; i++) {
        rooms[i].room_number = 0;
        rooms[i].active = 0;
        rooms[i].members = NULL;
        pthread_mutex_init(&rooms[i].lock, NULL);
    }
}

/* C2: searches rooms and returns index of room with given num*/
int find_room(int room_number) {
    for (int i = 0; i < MAX_ROOMS; i++) {
        if (rooms[i].active && rooms[i].room_number == room_number) {
            return i;
        }
    }
        return -1;
 }

 int create_room() { /* C2: marks first inactive room as active */
    pthread_mutex_lock(&rooms_lock);
    for (int i = 0; i < MAX_ROOMS; i++) {
        if (!rooms[i].active) {
            rooms[i].active = 1;
            rooms[i].room_number = next_room_number++;
            rooms[i].members = NULL;
            pthread_mutex_unlock(&rooms_lock);
            return i;
        }
    }

    pthread_mutex_unlock(&rooms_lock);
    return -1;

 }

 /* C2: replaces add_client function */
 void room_add_client(int room_idx, int clisockfd, char* ip) {
    pthread_mutex_lock(&rooms[room_idx].lock);
    USR* newuser = (USR*) malloc(sizeof(USR));
    newuser->clisockfd = clisockfd;
    strncpy(newuser->ip, ip, 32);
    strcpy(newuser->user, "unknown");
    newuser->next = NULL;

    if (rooms[room_idx].members == NULL) {
        rooms[room_idx].members = newuser;
    } else {
        USR* cur = rooms[room_idx].members;
        while (cur->next != NULL) cur = cur->next;
        cur->next = newuser;
    }
    pthread_mutex_unlock(&rooms[room_idx].lock);
}

/* C2: replaces remove_client function*/
void room_remove_client(int room_idx, int clisockfd) {
    pthread_mutex_lock(&rooms[room_idx].lock);
    USR* cur = rooms[room_idx].members;
    USR* prev = NULL;

    while (cur != NULL) {
        if (cur->clisockfd == clisockfd) {
            if (prev == NULL) rooms[room_idx].members = cur->next;
            else prev->next = cur->next;
            free(cur);
            break;
        }
        prev = cur;
        cur = cur->next;
    }

    if (rooms[room_idx].members == NULL) {
        rooms[room_idx].active = 0;
        printf("Room %d is now empty and closed.\n", rooms[room_idx].room_number);
    }
    pthread_mutex_unlock(&rooms[room_idx].lock);
}

void print_client_list() {
    printf("Connected Clients:\n");
    int found = 0;
    for (int i = 0; i < MAX_ROOMS; i++) {
        if (rooms[i].active) {
            USR* cur = rooms[i].members;
            while (cur != NULL) {
            printf(" [Room %d] %s (%s)\n", rooms[i].room_number, cur->user, cur->ip);
            cur = cur->next;
            found = 1;
            }
        }
    }

    if (!found) {
        printf("No clients connected\n");
    }
}

/* C3: builds string listing of all active rooms and members */
void build_room_list(char* buf, int bufsize) {
    memset(buf, 0, bufsize);
    int offset = 0;

    for (int i = 0; i < MAX_ROOMS; i++) {
        if (rooms[i].active) {

            /* counts members */
            int count = 0;
            USR* cur = rooms[i].members;

            while (cur != NULL) {
                count++;
                cur = cur->next;
            }

            char line[64];
            if (count == 1) {
                snprintf(line, sizeof(line), "Room %d: 1 person\n", rooms[i].room_number);
            } else {
                snprintf(line, sizeof(line), "Room %d: %d people\n", rooms[i].room_number, count);
            }

            strncat(buf, line, bufsize - offset - 1);
        }
    }
}

/* C2: replaces broadcast function*/
void room_broadcast(int room_idx, int fromfd, char* message) {
    pthread_mutex_lock(&rooms[room_idx].lock);
    USR* cur = rooms[room_idx].members;
    while (cur != NULL) {
        if (cur->clisockfd != fromfd) {
            int nsen = send(cur->clisockfd, message, strlen(message), 0);
            if (nsen < 0) error("ERROR send() failed");
        }
        cur = cur->next;
    }
    pthread_mutex_unlock(&rooms[room_idx].lock);
}

typedef struct _ThreadArgs {
    int clisockfd;
    int room_idx; /* C2: add room_idx into thread args*/
} ThreadArgs;

void* thread_main(void* args)
{
    pthread_detach(pthread_self());

    int clisockfd = ((ThreadArgs*) args)->clisockfd;
    /* C2: add room_idx into thread_main */
    int room_idx = ((ThreadArgs*) args)->room_idx;
    free(args);

    /* get client IP address */
    struct sockaddr_in cliaddr;
    socklen_t clen = sizeof(cliaddr);
    getpeername(clisockfd, (struct sockaddr*)&cliaddr, &clen);
    char ip[32];
    strncpy(ip, inet_ntoa(cliaddr.sin_addr), 32);

    room_add_client(room_idx, clisockfd, ip); /* C2: change name */

    /* Req 3: first message from client is their username */
    char username[64] = {0};
    int nrcv;
    do {
        nrcv = recv(clisockfd, username, 63, 0);
    } while (nrcv == 0);

    if (nrcv > 0) {
        username[nrcv] = '\0';
        /* store in list */
        pthread_mutex_lock(&rooms[room_idx].lock);
        USR* cur = rooms[room_idx].members;
        while (cur != NULL) {
            if (cur->clisockfd == clisockfd) {
                strncpy(cur->user, username, 64);
                break;
            }
            cur = cur->next;
        }
        pthread_mutex_unlock(&rooms[room_idx].lock);
    }

    print_client_list();

    /* Req 4: broadcast join notification */
    char join_msg[128];
    snprintf(join_msg, sizeof(join_msg), "%s (%s) joined the chat room!\n", username, ip);
    room_broadcast(room_idx, clisockfd, join_msg);

    /* receive and broadcast messages */
    char buffer[256];
    char formatted[400];

    while ((nrcv = recv(clisockfd, buffer, 255, 0)) > 0) {
        buffer[nrcv] = '\0';
        snprintf(formatted, sizeof(formatted), "[%s (%s)] %s\n", username, ip, buffer);
        room_broadcast(room_idx, clisockfd, formatted);
        memset(buffer, 0, 256);
    }

    /* Req 4: broadcast leave notification */
    char leave_msg[128];
    snprintf(leave_msg, sizeof(leave_msg), "%s (%s) left the room!\n", username, ip);
    room_broadcast(room_idx, clisockfd, leave_msg);

    room_remove_client(room_idx, clisockfd);
    print_client_list();
    close(clisockfd);

    return NULL;
}

int main(int argc, char *argv[])
{
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) error("ERROR opening socket");

    /* allows reuse of port immediately */
    int opt = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in serv_addr;
    socklen_t slen = sizeof(serv_addr);
    memset((char*) &serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(PORT_NUM);

    if (bind(sockfd, (struct sockaddr*) &serv_addr, slen) < 0)
        error("ERROR on binding");

    listen(sockfd, 5);
    init_rooms();
    printf("Server started on port %d\n", PORT_NUM);

    while (1) {
        struct sockaddr_in cli_addr;
        socklen_t clen = sizeof(cli_addr);
        int newsockfd = accept(sockfd, (struct sockaddr *) &cli_addr, &clen);
        if (newsockfd < 0) error("ERROR on accept");

        printf("Connected: %s\n", inet_ntoa(cli_addr.sin_addr));

        /* C2: */
        char room_req[16] = {0};
        char response[1024] = {0};

        int n = recv(newsockfd, room_req, 15, 0);
        if(n <= 0) {
            close(newsockfd);
            continue;
        }

        room_req[n] = '\0';

int room_idx = -1;

        if (strcmp(room_req, "new") == 0) {
            room_idx = create_room();
            if (room_idx < 0) {
                send(newsockfd, "ERROR", 5, 0);
                close(newsockfd);
                continue;
            }
            snprintf(response, sizeof(response), "%d", rooms[room_idx].room_number);
            send(newsockfd, response, strlen(response), 0);
            printf("Created room %d\n", rooms[room_idx].room_number);

        } else if (strcmp(room_req, "list") == 0) {
            /* C3: check if any rooms exist */
            int any_active = 0;
            for (int i = 0; i < MAX_ROOMS; i++) {
                if (rooms[i].active) { any_active = 1; break; }
            }

            if (!any_active) {
                /* no rooms so create one */
                room_idx = create_room();
                char auto_msg[32];
                snprintf(auto_msg, sizeof(auto_msg), "NEW %d", rooms[room_idx].room_number);
                send(newsockfd, auto_msg, strlen(auto_msg), 0);
                printf("Auto created room %d\n", rooms[room_idx].room_number);
            } else {
                /* send list to client */
                char list_buf[1024] = "LIST\n";
                char room_list[900] = {0};
                build_room_list(room_list, 900);
                strncat(list_buf, room_list, sizeof(list_buf) - 6);
                send(newsockfd, list_buf, strlen(list_buf), 0);

                /* wait for clients choice */
                char choice[16] = {0};
                int cn = recv(newsockfd, choice, 15, 0);
                if (cn <= 0) { close(newsockfd); continue; }
                choice[cn] = '\0';

                if (strcmp(choice, "new") == 0) {
                    room_idx = create_room();
                    if (room_idx < 0) {
                        send(newsockfd, "ERROR", 5, 0);
                        close(newsockfd);
                        continue;
                    }
                    snprintf(response, sizeof(response), "%d", rooms[room_idx].room_number);
                    send(newsockfd, response, strlen(response), 0);
                    printf("Created room %d\n", rooms[room_idx].room_number);
                } else {
                    int room_number = atoi(choice);
                    room_idx = find_room(room_number);
                    if (room_idx < 0) {
                        send(newsockfd, "ERROR", 5, 0);
                        close(newsockfd);
                        continue;
                    }
                    snprintf(response, sizeof(response), "%d", room_number);
                    send(newsockfd, response, strlen(response), 0);
                    printf("Joined room %d\n", room_number);
                }
            }

        } else {
            int room_number = atoi(room_req);
            room_idx = find_room(room_number);
            if (room_idx < 0) {
                send(newsockfd, "ERROR", 5, 0);
                close(newsockfd);
                continue;
            }
            snprintf(response, sizeof(response), "%d", room_number);
            send(newsockfd, response, strlen(response), 0);
            printf("Joined room %d\n", room_number);
        }
        ThreadArgs* args = (ThreadArgs*) malloc(sizeof(ThreadArgs));
        if (args == NULL) error("ERROR creating thread argument");
        args->clisockfd = newsockfd;
        args->room_idx = room_idx;

        pthread_t tid;
        if (pthread_create(&tid, NULL, thread_main, (void*) args) != 0)
            error("ERROR creating a new thread");
    }

    return 0;
}