CC = gcc
CFLAGS = -Wall -lm --std c17

all: src/main.c
	$(CC) $(CFLAGS) src/main.c -o bin/main
	chmod +x bin/main

clean:
	rm -rf bin/*