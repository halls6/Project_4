all:
	gcc -o main_server main_server.c -lpthread -lssh 
	gcc -o main_client main_client.c -lpthread -lssh 

clean:
	rm -f main_server main_client