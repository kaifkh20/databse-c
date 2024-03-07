//
// Created by SAIF KAIF on 2023-06-23.
//


#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
#define sizeOfAttribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)
#define TABLE_MAX_PAGES 100


#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>


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

typedef enum{
    NODE_INTERNAL,
    NODE_LEAF
} NodeType;

typedef struct{
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE+1];
    char email[COLUMN_EMAIL_SIZE+1];
}Row;

typedef struct {
    StatementType type;
    Row rowToInsert;
} Statement;

typedef struct{
    int fileDescriptor;
    uint32_t fileSize;
    void *pages[TABLE_MAX_PAGES];
}Pager;

typedef struct {
      uint32_t num_rows;
      Pager* pager;
    //   void* pages[TABLE_MAX_PAGES];
} Table;

typedef struct{
    Table* table;
    uint32_t row_num;
    bool endOfTable;
} Cursor;


// Table Design

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


// Common Node Layout

const uint32_t NODE_TYPE_SIZE  = sizeof(uint8_t);
const uint32_t NODE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint8_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE; 

// Leaf Node Header Layout

const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

// Body of LeafNode

const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;


InputBuffer* new_input_buffer(){
    InputBuffer* input_buffer = malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->bufferLength = 0;
    input_buffer->inputLength = 0;

    return input_buffer;
}

Pager* pagerOpen(const char* filename){
    int fd = open(filename,O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);

    if(fd==-1){
        printf("Unable to open file\n");
        exit(EXIT_FAILURE);
    }
    off_t fileLength = lseek(fd,0,SEEK_END);
    Pager* pager = malloc(sizeof(Pager));
    pager->fileDescriptor = fd;
    pager->fileSize = fileLength;

    for(uint32_t i=0;i<TABLE_MAX_PAGES;i++){
        pager->pages[i] = NULL;
    }

    return pager;
}

void pagerFlush(Pager* pager,uint32_t pageNum,uint32_t size){
    if(pager->pages[pageNum]==NULL){
        printf("Tried to flush null page.\n");
        exit(EXIT_FAILURE);
    }
    
    off_t offset = lseek(pager->fileDescriptor,pageNum*PAGE_SIZE,SEEK_SET);

    if(offset == -1){
        printf("Error seeking.\n");
        exit(EXIT_FAILURE);
    }

    __ssize_t bytesWritten = write(pager->fileDescriptor,pager->pages[pageNum],size);

    if(bytesWritten==-1){
        printf("Error writing into file.\n");
        exit(EXIT_FAILURE);
    }


}

Table* dbOpen(const char* filename){

    Pager* pager = pagerOpen(filename);
    uint32_t numRows = pager->fileSize/ROW_SIZE;

    Table* table = malloc(sizeof (Table));

    table->pager = pager;
    table->num_rows = numRows;

    return table;
}



void dbClose(Table* table){
    Pager* pager = table->pager;
    uint32_t num_of_full_pages =  table->num_rows/ROWS_PER_PAGE;
    for(uint32_t i = 0;i<num_of_full_pages;++i){
        if(pager->pages[i]==NULL) continue;

        pagerFlush(pager,i,PAGE_SIZE);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }
    // Temporarily to be changed shortly

    uint32_t num_of_additional_rows = table->num_rows % ROWS_PER_PAGE;
    if(num_of_additional_rows>0){
        uint32_t pageNum = num_of_full_pages;
        if(pager->pages[pageNum]!=NULL){
            pagerFlush(pager,pageNum,num_of_additional_rows*ROW_SIZE);
            free(pager->pages[pageNum]);
            pager->pages[pageNum] = NULL;
        }
    }

    int result = close(pager->fileDescriptor);
    if(result==-1){
        printf("Error closing db file.\n");
        exit(EXIT_FAILURE);
    }

    for(uint32_t i=0;i<TABLE_MAX_PAGES;++i){
        void* page = pager->pages[i];
        if(page){
            free(page);
            pager->pages[i] = NULL;
        }
    }

    free(pager);
    free(table);

}

Cursor* tableStart(Table* table){
    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->row_num = 0;
    cursor->endOfTable = (table->num_rows==0);

    return cursor;
}

Cursor* tableEnd(Table* table){
    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->row_num = table->num_rows;
    cursor->endOfTable = true;

    return cursor;
}


void serializeRow(Row* source,void* dest){
    memcpy(dest+ID_OFFSET,&(source->id),ID_SIZE);
    // memcpy(dest+USERNAME_OFFSET,&(source->username),USERNAME_SIZE);
    // memcpy(dest+EMAIL_OFFSET,&(source->email),EMAIL_SIZE);

    // For all bytes initialized
    strncpy(dest+USERNAME_OFFSET,&(source->username),USERNAME_SIZE);
    strncpy(dest+EMAIL_OFFSET,&(source->email),EMAIL_SIZE);

}

void deserializeRow(void* source,Row* dest){
    memcpy(&(dest->id),source+ID_OFFSET,ID_SIZE);
    memcpy(&(dest->username),source+USERNAME_OFFSET,USERNAME_SIZE);
    memcpy(&(dest->email),source+EMAIL_OFFSET,EMAIL_SIZE);
}

void* getPage(Pager* pager,uint32_t pageNum){
    if(pageNum>TABLE_MAX_PAGES){
        printf("Tried to fetch number out of bounds %d\n",TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }
    if(pager->pages[pageNum]==NULL){
        // Cache Miss Allocate memory and load from file.
        void* page = malloc(PAGE_SIZE);
        uint32_t num_pages = pager->fileSize/PAGE_SIZE;

        // We might save a partial page at the end of the file
        if(pager->fileSize % PAGE_SIZE){
            num_pages+=1;
        }

        if(pageNum<=num_pages){
            lseek(pager->fileDescriptor,pageNum*PAGE_SIZE,SEEK_SET);
            ssize_t bytesRead = read(pager->fileDescriptor,page,PAGE_SIZE);
            if(bytesRead==-1){
                printf("Error reading file:%d\n",errno);
                exit(EXIT_FAILURE);
            }
        }
       pager->pages[pageNum] = page;
    }

    return pager->pages[pageNum];
}

void* cursorValue(Cursor* cursor){
    uint32_t rowNum = cursor->row_num;
    uint32_t pageNum = rowNum/ROWS_PER_PAGE;
    // void* page = table->pages[pageNum];

    // if(page==NULL){
    //     page = table->pages[pageNum] = malloc(PAGE_SIZE);
    // }
    void* page = getPage(cursor->table->pager,pageNum);

    uint32_t rowOffset = rowNum%ROWS_PER_PAGE;
    uint32_t byteOffset = rowOffset * ROW_SIZE;
    return page+byteOffset;
}

void cursorAdvance(Cursor* cursor){
    cursor->row_num+=1;
    if(cursor->row_num>=cursor->table->num_rows){
        cursor->endOfTable = true;
    }
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


MetaCommandResult doMetaCommand(InputBuffer* inputBuffer,Table* table){
    if(strcmp(inputBuffer->buffer,".exit")==0){
        dbClose(table);
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
        statement->type = STATEMENT_INSERT; 
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
    Cursor* cursor = tableEnd(table);
    serializeRow(rowToInsert, cursorValue(cursor));
    table->num_rows++;

    free(cursor);

    return EXECUTE_SUCCESS;
}

ExecuteResult executeSelect(Statement* statement,Table* table){
    Row row;
    Cursor* cursor = tableStart(table);
    // for(uint32_t i=0;i<table->num_rows;i++){
    //     deserializeRow(rowSlot(table,i),&row);
    //     printRow(&row);
    // }

    while(!(cursor->endOfTable)){
        deserializeRow(cursorValue(cursor),&row);
        printRow(&row);
        cursorAdvance(cursor);
    }

    free(cursor);

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

int main(int argc, char* argv[]){
    if(argc<2){
            printf("Must provide a database name.\n");
            exit(EXIT_FAILURE);
    }
    char* filename = argv[1];
    Table* table = dbOpen(filename);
    InputBuffer* inputBuffer = new_input_buffer();
    while(true){
        printPrompt();
        readInput(inputBuffer);


        if(inputBuffer->buffer[0]=='.'){
            switch(doMetaCommand(inputBuffer,table)){
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
                printf("Syntax Error.\n");
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
