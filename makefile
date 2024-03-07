
all : db

db:
	gcc -O2 db.c -o db

db.o : db.c
		gcc -c db.c
     
clean:
	 rm db.o db 

