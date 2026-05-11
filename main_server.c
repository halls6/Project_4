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

USR* head = NULL;
pthread_mutex_t list_lock = PTHREAD_MUTEX_INITIALIZER;

/* add a new client to the list */
void add_client(int clisockfd, char* ip) {
    pthread_mutex_lock(&list_lock);
    USR* newuser = (USR*) malloc(sizeof(USR));
    newuser->clisockfd = clisockfd;
    strncpy(newuser->ip, ip, 32);
    strcpy(newuser->user, "unknown");
    newuser->next = NULL;

    if (head == NULL) {
        head = newuser;
    } else {
        USR* cur = head;
        while (cur->next != NULL) {
            cur = cur->next;
        }
        cur->next = newuser;
    }

    pthread_mutex_unlock(&list_lock);
}

void remove_client(int clisockfd) {
    pthread_mutex_lock(&list_lock);

    USR* cur = head;
    USR* prev = NULL;

    while (cur != NULL) {
        if (cur->clisockfd == clisockfd) {
            if (prev == NULL) {
                head = cur->next;
            } else {
                prev->next = cur->next;
            }
            free(cur);
            break;
        }
        prev = cur;
        cur = cur->next;
    }

    pthread_mutex_unlock(&list_lock);
}

/* print the current list of clients */
void print_client_list() {
    printf("Connected Clients:\n");
    USR* cur = head;

    if (cur == NULL) {
        printf("No clients connected\n");
    }

    while (cur != NULL) {
        printf(" %s (%s)\n", cur->user, cur->ip);
        cur = cur->next;
    }
}

/* send a message to all connected clients except sender */
void broadcast(int fromfd, char* message) {
    pthread_mutex_lock(&list_lock);

    USR* cur = head;
    while (cur != NULL) {
        if (cur->clisockfd != fromfd) {
            int nsen = send(cur->clisockfd, message, strlen(message), 0);
            if (nsen < 0) {
                error("ERROR send() failed");
            }
        }
        cur = cur->next;
    }
    pthread_mutex_unlock(&list_lock);
}

typedef struct _ThreadArgs {
	int clisockfd;
} ThreadArgs;

void* thread_main(void* args)
{
	pthread_detach(pthread_self());

	int clisockfd = ((ThreadArgs*) args)->clisockfd;
	free(args);

    /* get client IP address */
    struct sockaddr_in cliaddr;
    socklen_t clen = sizeof(cliaddr);
    getpeername(clisockfd, (struct sockaddr*)&cliaddr, &clen);
    char ip[32];
    strncpy(ip, inet_ntoa(cliaddr.sin_addr), 32);

    /* add client to list */
    add_client(clisockfd, ip);

    /* Req 3: first message from client is their username */
    char username[64] = "unknown";
    int nrcv = recv(clisockfd, username, 63, 0);
    if (nrcv > 0) {
        username[nrcv] = '\0';

        /* store username in the client list */
        pthread_mutex_lock(&list_lock);
        USR* cur = head;
        while (cur != NULL) {
            if (cur->clisockfd == clisockfd) {
                strncpy(cur->user, username, 64);
                break;
            }
            cur = cur->next;
        }
        pthread_mutex_unlock(&list_lock);
    }

    print_client_list();

    /* Req 4: broadcast join notification */
    char join_msg[128];
    snprintf(join_msg, sizeof(join_msg), "%s (%s) joined the chat room!", username, ip);
    broadcast(clisockfd, join_msg);

    /* receive and broadcast messages */
	char buffer[256];

    while ((nrcv = recv(clisockfd, buffer, 255, 0)) > 0) {
        buffer[nrcv] = '\0';

        /* format message with sender name and IP */
        char formatted[320];
        snprintf(formatted, sizeof(formatted), "[%s (%s)] %s", username, ip, buffer);
        broadcast(clisockfd, formatted);
        memset(buffer, 0, 256);
	}

    /* Req 4: broadcast leave notification before removing */
    char leave_msg[128];
    snprintf(leave_msg, sizeof(leave_msg), "%s (%s) left the room!", username, ip);
    broadcast(clisockfd, leave_msg);

    /* client disconnect */
    remove_client(clisockfd);
    print_client_list();
    close(clisockfd);

    return NULL;
}

int main(int argc, char *argv[])
{
	int sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd < 0) error("ERROR opening socket");

	struct sockaddr_in serv_addr;
	socklen_t slen = sizeof(serv_addr);
	memset((char*) &serv_addr, 0, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_addr.s_addr = INADDR_ANY;	
	serv_addr.sin_port = htons(PORT_NUM);

    if (bind(sockfd, (struct sockaddr*) &serv_addr, slen) < 0) {
        error("ERROR on binding");
    }

    listen(sockfd, 5);
    printf("Server started on port %d\n", PORT_NUM);

    while (1) {
        struct sockaddr_in cli_addr;
		socklen_t clen = sizeof(cli_addr);
		int newsockfd = accept(sockfd, (struct sockaddr *) &cli_addr, &clen);
		if (newsockfd < 0) error("ERROR on accept");

		printf("Connected: %s\n", inet_ntoa(cli_addr.sin_addr));

		ThreadArgs* args = (ThreadArgs*) malloc(sizeof(ThreadArgs));
		if (args == NULL) error("ERROR creating thread argument");
		args->clisockfd = newsockfd;

		pthread_t tid;
		if (pthread_create(&tid, NULL, thread_main, (void*) args) != 0)
            error("ERROR creating a new thread");
	}

	return 0;
}