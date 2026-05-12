all:
	gcc -o main_server main_server.c -lpthread -lssh -I/opt/homebrew/include -L/opt/homebrew/lib
	gcc -o main_client main_client.c -lpthread -lssh -I/opt/homebrew/include -L/opt/homebrew/lib

clean:
	rm -f main_server main_client