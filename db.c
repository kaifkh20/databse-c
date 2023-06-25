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

typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum { PREPARE_SUCCESS, PREPARE_UNRECOGNIZED_STATEMENT } PrepareResult;

typedef enum { STATEMENT_INSERT, STATEMENT_SELECT } StatementType;

typedef struct {
    StatementType type;
} Statement;

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

MetaCommandResult doMetaCommand(InputBuffer* inputBuffer){
    if(strcmp(inputBuffer->buffer,".exit")==0){
        exit(EXIT_SUCCESS);
    }else{
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult prepareStatement(InputBuffer* inputBuffer,Statement* statement){
    if(strncmp(inputBuffer->buffer,"insert",6)==0){
        statement->type = STATEMENT_INSERT;
        return  PREPARE_SUCCESS;
    }
    if(strncmp(inputBuffer->buffer,"select",6)==0){
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;

}

void executeStatement(Statement* statement){
    switch(statement->type){
        case(STATEMENT_INSERT):
            printf("This is where we would do Insert \n");
            break;
        case (STATEMENT_SELECT):
            printf("This is where we would do Select \n");
            break;

    }
}

int main(){
    InputBuffer* inputBuffer = new_input_buffer();
    while(true){
        printPrompt();
        readInput(inputBuffer);

//        if(strcmp(inputBuffer->buffer,".exit")==0){
//            closeInputBuffer(inputBuffer);
//            exit(EXIT_SUCCESS);
//        }else{
//            printf("Unrecognized Command %s \n",inputBuffer->buffer);
//        }

        if(inputBuffer->buffer[0]=='.'){
            switch(doMetaCommand(inputBuffer)){
                case(META_COMMAND_SUCCESS):
                    continue;

                case(META_COMMAND_UNRECOGNIZED_COMMAND):
                    printf("Unrecognized Command '%s'\n",inputBuffer->buffer);
                    continue;

            }
        }

        Statement statement;
        switch(prepareStatement(inputBuffer,&statement)){
            case(PREPARE_SUCCESS):break;
            case(PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized keyword at start of '%s'.\n",
                       inputBuffer->buffer);
                continue;
        }



        executeStatement(&statement);
        printf("Executed \n");
    }


}
