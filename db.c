//
// Created by SAIF KAIF on 2023-06-23.
//


#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


typedef struct{
    char* buffer;
    size_t bufferLength;
    ssize_t inputLength;
}InputBuffer;

InputBuffer* new_input_buffer(){
    InputBuffer* input_buffer = malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->bufferLength = 0;
    input_buffer->inputLength = 0;

    return input_buffer;
}

void printPrompt(){printf("db >");}

size_t getLine(char **lineptr, size_t *n, FILE *stream) {
    char *bufptr = NULL;
    char *p = bufptr;
    size_t size;
    int c;

    if (lineptr == NULL) {
        return -1;
    }
    if (stream == NULL) {
        return -1;
    }
    if (n == NULL) {
        return -1;
    }
    bufptr = *lineptr;
    size = *n;

    c = fgetc(stream);
    if (c == EOF) {
        return -1;
    }
    if (bufptr == NULL) {
        bufptr = malloc(128);
        if (bufptr == NULL) {
            return -1;
        }
        size = 128;
    }
    p = bufptr;
    while(c != EOF) {
        if ((p - bufptr) > (size - 1)) {
            size = size + 128;
            bufptr = realloc(bufptr, size);
            if (bufptr == NULL) {
                return -1;
            }
        }
        *p++ = c;
        if (c == '\n') {
            break;
        }
        c = fgetc(stream);
    }

    *p++ = '\0';
    *lineptr = bufptr;
    *n = size;

    return p - bufptr - 1;
}

void readInput(InputBuffer* inputBuffer){
    ssize_t bytesRead = getLine(&(inputBuffer->buffer),&(inputBuffer->bufferLength),stdin);

    if(bytesRead<=0){
        printf("Error Reading Input \n");
        exit(EXIT_FAILURE);
    }

    inputBuffer->inputLength = bytesRead - 1;
    inputBuffer->buffer[bytesRead - 1] = 0;
}

void closeInputBuffer(InputBuffer* inputBuffer){
    free(inputBuffer->buffer);
    free(inputBuffer);
}

int main(){
    InputBuffer* inputBuffer = new_input_buffer();
    while(true){
        printPrompt();
        readInput(inputBuffer);

        if(strcmp(inputBuffer->buffer,".exit")==0){
            closeInputBuffer(inputBuffer);
            exit(EXIT_SUCCESS);
        }else{
            printf("Unrecognized Command %s \n",inputBuffer->buffer);
        }
    }
}
