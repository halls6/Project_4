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

void error(const char *msg)
{
	perror(msg);
	exit(0);
}

typedef struct _ThreadArgs {
	int clisockfd;
} ThreadArgs;

void* thread_main_receive(void* args)
{
	// make sure thread resources are deallocated upon return
	pthread_detach(pthread_self());

	// get socket descriptor from argument
	int sockfd = ((ThreadArgs*) args)->clisockfd;
	free(args);

    /* keep receiving and showing messages from server */
    char buffer[512];
    int nrcv;

    while (1) {
        memset(buffer, 0, 512);
        nrcv = recv(sockfd, buffer, 511, 0);
        if (nrcv <= 0) { break; }
        buffer[nrcv] = '\0';
        printf("\n%s\n", buffer);
	}

    return NULL;
}

void* thread_main_send(void* args) {
    // make sure thread resources are deallocated upon return
	pthread_detach(pthread_self());

	// get socket descriptor from argument
	int sockfd = ((ThreadArgs*) args)->clisockfd;
	free(args);

    /* keep receiving and showing messages from server */
    char buffer[256];
    int nsen;

    while (1) {
        memset(buffer, 0, 256);
        fgets(buffer, 255, stdin);

        if (strlen(buffer) == 1) { break; }
        buffer[strcspn(buffer, "\n")] = '\0';

        nsen = send(sockfd, buffer, strlen(buffer), 0);
        if (nsen < 0) {
            error("ERROR writing to socket");
        }
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

    if (connect(sockfd, (struct sockaddr*) &serv_addr, slen) < 0) {
        error("ERROR connecting");
    }

    printf("Connected to server\n");

   pthread_t tid1;
   pthread_t tid2;
   ThreadArgs* args;

   args = (ThreadArgs*) malloc(sizeof(ThreadArgs));
   args->clisockfd = sockfd;
   pthread_create(&tid1, NULL, thread_main_send, (void*) args);

   args = (ThreadArgs*) malloc(sizeof(ThreadArgs));
   args->clisockfd = sockfd;
   pthread_create(&tid2, NULL, thread_main_receive, (void*) args);

    /* wait for send thread to finish */
    pthread_join(tid1, NULL);

    close(sockfd);
	return 0; 

    }