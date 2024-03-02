//
// Created by SAIF KAIF on 2023-06-23.
//

#include <stdint.h>

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
#define sizeOfAttribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)
#define TABLE_MAX_PAGES 100



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

typedef enum { 
    PREPARE_SUCCESS, 
    PREPARE_UNRECOGNIZED_STATEMENT, 
    PREPARE_SYNTAX_ERROR,
    PREPARE_STRING_TOO_LONG,
    PREPARE_NEGATIVE_ID 
} 

PrepareResult;

typedef enum { STATEMENT_INSERT, STATEMENT_SELECT } StatementType;

typedef enum{
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL
} ExecuteResult;

typedef struct{
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE+1];
    char email[COLUMN_EMAIL_SIZE+1];
}Row;

typedef struct {
    StatementType type;
    Row rowToInsert;
} Statement;



const uint32_t ID_SIZE = sizeOfAttribute(Row, id);
const uint32_t USERNAME_SIZE = sizeOfAttribute(Row, username);
const uint32_t EMAIL_SIZE = sizeOfAttribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
const uint32_t PAGE_SIZE = 4096;

const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

typedef struct {
      uint32_t num_rows;
      void* pages[TABLE_MAX_PAGES];
    } Table;

InputBuffer* new_input_buffer(){
    InputBuffer* input_buffer = malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->bufferLength = 0;
    input_buffer->inputLength = 0;

    return input_buffer;
}

Table* newTable(){
    Table* table = malloc(sizeof (Table));
    table->num_rows = 0;
    for(uint32_t i=0;i<TABLE_MAX_PAGES;i++){
        table->pages[i] = NULL;
    }
    return table;
}


void serializeRow(Row* source,void* dest){
    memcpy(dest+ID_OFFSET,&(source->id),ID_SIZE);
    memcpy(dest+USERNAME_OFFSET,&(source->username),USERNAME_SIZE);
    memcpy(dest+EMAIL_OFFSET,&(source->email),EMAIL_SIZE);
}

void deserializeRow(void* source,Row* dest){
    memcpy(&(dest->id),source+ID_OFFSET,ID_SIZE);
    memcpy(&(dest->username),source+USERNAME_OFFSET,USERNAME_SIZE);
    memcpy(&(dest->email),source+EMAIL_OFFSET,EMAIL_SIZE);
}

void* rowSlot(Table* table,uint32_t rowNum){
    uint32_t pageNum = rowNum/ROWS_PER_PAGE;
    void* page = table->pages[pageNum];

    if(page==NULL){
        page = table->pages[pageNum] = malloc(PAGE_SIZE);
    }

    uint32_t rowOffset = rowNum%ROWS_PER_PAGE;
    uint32_t byteOffset = rowOffset * ROW_SIZE;
    return page+byteOffset;
}

void printPrompt(){printf("db >");}
void printRow(Row* row){
    printf("(%d, %s, %s)\n",row->id,row->username,row->email);
}

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

void freeTable(Table* table){
    for(uint32_t i=0;i<TABLE_MAX_PAGES;i++){
        free(table->pages[i]);
    }
    free(table);
}

MetaCommandResult doMetaCommand(InputBuffer* inputBuffer){
    if(strcmp(inputBuffer->buffer,".exit")==0){
        exit(EXIT_SUCCESS);
    }else{
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}


PrepareResult prepareInsert(InputBuffer* inputBuffer,Statement* statement){
    char* keyword = strtok(inputBuffer->buffer," ");
    char* id_string = strtok(NULL," ");
    char* username = strtok(NULL," ");
    char* email = strtok(NULL," ");

    if(id_string==NULL || id_string==NULL || username==NULL || email==NULL){
        return PREPARE_SYNTAX_ERROR;
    }

    int id = atoi(id_string);

    if(id<0){
        return PREPARE_NEGATIVE_ID;
    }

    if(strlen(username)>COLUMN_USERNAME_SIZE){
        return PREPARE_STRING_TOO_LONG;
    }

    if(strlen(email)>COLUMN_EMAIL_SIZE){
        return PREPARE_STRING_TOO_LONG;
    }

    statement->rowToInsert.id = id;
    strcpy(statement->rowToInsert.username,username);
    strcpy(statement->rowToInsert.email,email);

    return PREPARE_SUCCESS;
}

PrepareResult prepareStatement(InputBuffer* inputBuffer,Statement* statement){
    if(strncmp(inputBuffer->buffer,"insert",6)==0){
        return prepareInsert(inputBuffer,statement);
    }
    if(strncmp(inputBuffer->buffer,"select",6)==0){
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;

}


ExecuteResult executeInsert(Statement* statement,Table* table){
    if(table->num_rows>= TABLE_MAX_ROWS){
        return EXECUTE_TABLE_FULL;
    }
    Row* rowToInsert = &(statement->rowToInsert);
    serializeRow(rowToInsert, rowSlot(table,table->num_rows));
    table->num_rows++;
    return EXECUTE_SUCCESS;
}

ExecuteResult executeSelect(Statement* statement,Table* table){
    Row row;
    for(uint32_t i=0;i<table->num_rows;i++){
        deserializeRow(rowSlot(table,i),&row);
        printRow(&row);
    }
    return EXECUTE_SUCCESS;
}



ExecuteResult executeStatement(Statement* statement,Table* table){
    switch(statement->type){
        case(STATEMENT_INSERT):
            return executeInsert(statement,table);
        case (STATEMENT_SELECT):
            return executeSelect(statement,table);
    }
}

int main(){
    InputBuffer* inputBuffer = new_input_buffer();
    Table* table = newTable();
    while(true){
        printPrompt();
        readInput(inputBuffer);


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
            case(PREPARE_SYNTAX_ERROR):
                printf("Syntax Error");
                continue;
            case(PREPARE_STRING_TOO_LONG):
                printf("String too long.\n");
                continue;
            case (PREPARE_NEGATIVE_ID):
                printf("ID must be positive.\n");
        }



        switch(executeStatement(&statement,table)){
            case(EXECUTE_SUCCESS):
                printf("Executed.\n");
                break;
            case(EXECUTE_TABLE_FULL):
                printf("Error :Table FUll \n");
                break;
        }
    }


}
