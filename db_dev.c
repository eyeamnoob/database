/*
a simple database system

Todo:
	1. spliting internal nodes:
		1.1. spliting child/key pairs (done)
		1.2. strugling with the most right child (done)
		1.2. we just need to merge the new internal node with the parent (done)
		1.3. consider the case when the parent is the root (done)
		1.4. consider the case when the parent is full (done)
		1.6. define a new function that get internal node rightmost child max key (done)
		1.7. just some more tests needed
	2. merging leaf node(delete operation)
*/


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdbool.h>


// my defines
#define INPUT_BUFFER_MAX_SIZE 4 + 1 + 32 + 1 + 255 + 1
#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
#define TABLE_MAX_PAGES	100
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)


// custom datatypes
typedef enum
{
	META_COMMAND_SUCCESS,
	META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum
{
	PREPARE_SUCCESS,
	PREPARE_SYNTAX_ERROR,
	PREPARE_STRING_TOO_LONG,
	PREPARE_NEGATIVE_ID,
	PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

typedef enum
{
	STATEMENT_INSERT,
	STATEMENT_SELECT,
	STATEMENT_UPDATE,
	STATEMENT_DELETE
} StatementType;

typedef enum {
	EXECUTE_SUCCESS,
	EXECUTE_TABLE_FULL,
	EXECUTE_DUPLICATE_KEY,
	EXECUTE_RECORD_NOT_FOUND
} ExecuteResult;

typedef enum{
	NODE_INTERNAL,
	NODE_LEAF
} NodeType;

typedef struct
{
	char* buffer;
	size_t buffer_length;
	size_t input_length;
} InputBuffer;

typedef struct
{
	uint32_t id;
	char username[COLUMN_USERNAME_SIZE + 1]; // + 1 for '\0'
	char email[COLUMN_EMAIL_SIZE + 1]; // + 1 for '\0'
} Row;

typedef struct
{
	StatementType type;
	Row row_to_insert; // used by insert and update statement
} Statement;

typedef struct
{
	FILE* file_desc;
	uint32_t file_lenght;
	uint32_t num_pages;
	void* pages[TABLE_MAX_PAGES];
} Pager;

typedef struct
{
	uint32_t root_page_num;
	Pager* pager;
} Table;

typedef struct
{
	Table* table;
	uint32_t page_num;
	uint32_t cell_num;
	bool end_of_table;
} Cursor;

typedef struct
{
	bool is_root;
	uint32_t type;
	uint32_t page_num;
	uint32_t parent_page_num;
	uint32_t num_keys;
} NodeInJson;


// constants

// size and offset of each attributes
const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;

const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE; // size of each row
const uint32_t PAGE_SIZE = 4096; // size of each page

// common node header(metadata)
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint32_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

// leaf node header(metadata)
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_NEXT_LEAF_NODE_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NEXT_LEAF_NODE_OFFSET = LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NUM_CELLS_OFFSET;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_NODE_SIZE;

//leaf node body
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
// const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = 3; // for testing

//spliting a leaf node
const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT = LEAF_NODE_MAX_CELLS + 1 - LEAF_NODE_RIGHT_SPLIT_COUNT;


// internal node header(metadata)
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t INTERNAL_NODE_RIGHTMOST_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_RIGHTMOST_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const uint32_t INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHTMOST_CHILD_SIZE;

//internal node body
const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CELL_SIZE = INTERNAL_NODE_KEY_SIZE + INTERNAL_NODE_CHILD_SIZE;
const uint32_t INTERNAL_NODE_SPACE_FOR_CELLS = PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE;
// const uint32_t INTERNAL_NODE_MAX_CELLS = INTERNAL_NODE_SPACE_FOR_CELLS / INTERNAL_NODE_CELL_SIZE;
const uint32_t INTERNAL_NODE_MAX_CELLS = 3; // for testing

//spliting a internal node
const uint32_t INTERNAL_NODE_RIGHT_SPLIT_COUNT = (INTERNAL_NODE_MAX_CELLS) / 2;
const uint32_t INTERNAL_NODE_LEFT_SPLIT_COUNT = INTERNAL_NODE_MAX_CELLS - INTERNAL_NODE_RIGHT_SPLIT_COUNT;


// my functions

/**
 * @brief create and return a new input buffer
 * 
 * @return InputBuffer* 
 */
InputBuffer* new_input_buffer();

/**
 * @brief show a line to users so they know what should input
 * 
 */
void print_prompt();

/**
 * @brief read a line of input and store it in given InputBuffer structure
 * 
 * @param buffer InputBuffer*
 */
void read_input(InputBuffer* buffer);

/**
 * @brief close and free the given InputBuffer structure pointer
 * 
 * @param buffer InputBuffer*
 */
void close_input_buffer(InputBuffer* buffer);

/**
 * @brief read meta command from buffer and execute it
 * 
 * @param buffer  InputBuffer*
 * @param table   Table*
 * @return MetaCommandResult 
 */
MetaCommandResult do_meta_command(InputBuffer* buffer, Table* table);

/**
 * @brief read the buffer and make a statement
 * 
 * @param buffer     InputBuffer*
 * @param statement  Statement*
 * @return PrepareResult 
 */
PrepareResult prepare_statement(InputBuffer* buffer, Statement* statement);

/**
 * @brief prepare a insert operation
 * 
 * @param buffer      InputBuffer*
 * @param statement   Statement*
 * @return PrepareResult 
 */
PrepareResult prepare_insert(InputBuffer* buffer, Statement* statement);

/**
 * @brief prepare an udpate operation.
 * 
 * @param buffer    InputBuffer*
 * @param statement Statement*
 * @return PrepareResult 
 */
PrepareResult prepare_update(InputBuffer* buffer, Statement* statement);

/**
 * @brief prepare a delete operation.
 * 
 * @param buffer     Buffer*
 * @param statement  Statement*  
 * @return PrepareResult 
 */
PrepareResult prepare_delete(InputBuffer* buffer, Statement* statement);

/**
 * @brief execute the prepared statement on given table
 * 
 * @param statement Statement*
 * @param table     Table*
 * @return ExecuteResult 
 */
ExecuteResult execute_statement(Statement* statement, Table* table);

/**
 * @brief execute the insert operation on given table
 * 
 * @param statement   Statement*
 * @param table 	  Table*
 * @return ExecuteResult 
 */
ExecuteResult execute_insert(Statement* statement, Table* table);

/**
 * @brief execute the select operation on given table
 * 
 * @param statement   Statement*
 * @param table       Table*
 * @return ExecuteResult 
 */
ExecuteResult execute_select(Statement* statement, Table* table);

/**
 * @brief compress a row to a block of memory to write on the disk
 * 
 * @param source   Row*
 * @param dest     void*
 */
void serialize_row(Row* source, void* dest);

/**
 * @brief decompress the given block of memory to a row
 * 
 * @param source   void*
 * @param dest     Row*
 */
void deserialize_row(void* source, Row* dest);

/**
 * @brief returns a pointer to the position described by the cursor
 * 
 * @param cursor  Cursor*
 * @return void* 
 */
void* cursor_value(Cursor* cursor);

/**
 * @brief print a row
 * 
 * @param row Row*
 */
void print_row(Row* row);

/**
 * @brief open a connection to a database file and return the table
 * 
 * @param filename  const char*
 * @return Table* 
 */
Table* db_open(const char* filename);

/**
 * @brief free the table, its pager, pages and close the database file
 * 
 * @param table Table*
 */
void db_close(Table* table);


/**
 * @brief return the pager of a table
 * 
 * @param filename   char*
 * @return Pager* 
 */
Pager* pager_open(const char* filename);

/**
 * @brief Get the page object from a table pager
 * 
 * @param pager     Pager*
 * @param page_num  uint32_t
 * @return void* 
 */
void* get_node(Pager* pager, uint32_t page_num);

/**
 * @brief flush a page of a pager to the database file
 * 
 * @param pager     Pager*
 * @param page_num  uint32_t
 * @param size      uint32_t
 */
void pager_flush(Pager* pager, uint32_t page_num);

/**
 * @brief create a cursor at start of the given table
 * 
 * @param table   Table*
 * @return Cursor* 
 */
Cursor* table_start(Table* table);

/**
 * @brief reate a cursor at end of the given table
 * 
 * @param table   Table*
 * @return Cursor* 
 */
Cursor* table_end(Table* table);

/**
 * @brief advances the cursor on row
 * 
 * @param cursor  Cursor*
 */
void cursor_advance(Cursor* cursor);

/**
 * @brief return number of cells that the node is holding
 * 
 * @param node  void*
 * @return uint32_t* 
 */
uint32_t* leaf_node_num_cells(void* node);

/**
 * @brief return a cell
 * 
 * @param node      void*
 * @param cell_num  uint32_t
 * @return void* 
 */
void* leaf_node_cell(void* node, uint32_t cell_num);

/**
 * @brief return a key of the leaf node cell
 * 
 * @param node     void*
 * @param cell_num uint32_t
 * @return uint32_t* 
 */
uint32_t* leaf_node_key(void* node, uint32_t cell_num);

/**
 * @brief return a value of the leaf node
 * 
 * @param ndoe     void*
 * @param cell_num uint32_t
 * @return void* 
 */
void* leaf_node_value(void* ndoe, uint32_t cell_num);

/**
 * @brief initialize a leaf node
 * 
 * @param node void*
 */
void initialize_leaf_node(void* node);

/**
 * @brief insert key-value pair into a leaf node
 * 
 * @param cursor Cursor*
 * @param key    uint32_t
 * @param value  Row*
 */
void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value);

/**
 * @brief print all constants
 * 
 */
void print_constants();

/**
 * @brief Returns the position of the given key. if the key is not present, returns the position where it should be inserted
 * 
 * @param table  Table*
 * @param key    uint32_t
 * @return Cursor* 
 */
Cursor* table_find(Table* table, uint32_t key);

/**
 * @brief Get the node type
 * 
 * @param node   void*
 * @return NodeType 
 */
NodeType get_node_type(void* node);

/**
 * @brief  finds and returns the place of the given key in the node.
 * 
 * @param table      Table*
 * @param page_num   uint32_t
 * @param key 		 uint32_t
 * @return Cursor* 
 */
Cursor* leaf_node_find(Table* table, uint32_t page_num, uint32_t key);

/**
 * @brief Set the node type
 * 
 * @param node  void*
 * @param type  NodeType
 */
void set_node_type(void* node, NodeType type);

/**
 * @brief it's called when a node is full. it split a leaf node and insert new value
 * 
 * @param cursor  Cursorr*
 * @param key     uint32_t
 * @param value   Row*
 */
void leaf_node_split_then_insert(Cursor* cursor, uint32_t key, Row* value);

/**
 * @brief Get the page number that is not being used
 * 
 * @param pager 
 * @return uint32_t 
 */
uint32_t get_unused_page_num(Pager* pager);

/**
 * @brief return the number of keys that the node is holding
 * 
 * @param node   void*
 * @return uint32_t 
 */
uint32_t* internal_node_num_keys(void* node);

/**
 * @brief return the most right child of the node
 * 
 * @param node   void*
 * @return uint32_t* 
 */
uint32_t* internal_node_rightmost_child(void* node);

/**
 * @brief return a cell
 * 
 * @param node      void*
 * @param cell_num  uint32_t
 * @return uint32_t 
 */
uint32_t* internal_node_cell(void* node, uint32_t cell_num);

/**
 * @brief return a child of the internal node
 * 
 * @param node       void*
 * @param child_num  uint32_t
 * @return uint32_t* 
 */
uint32_t* internal_node_child(void* node, uint32_t child_num);

/**
 * @brief return a key of a cell
 * 
 * @param node 
 * @param key_num 
 * @return uint32_t* 
 */
uint32_t* internal_node_key(void* node, uint32_t key_num);

/**
 * @brief return max key of a node (not including the rightmost child of the internal node)
 * 
 * @param node   void*
 * @return uint32_t 
 */
uint32_t get_node_max_key(void* node);

/**
 * @brief return true if a node is root, otherwise return false
 * 
 * @param node   void*
 * @return true 
 * @return false 
 */
bool is_node_root(void* node);

/**
 * @brief set a node root or not
 * 
 * @param node    void*
 * @param is_root bool
 */
void set_node_root(void* node, bool is_root);

/**
 * @brief initialize a internal node
 * 
 * @param node 
 */
void initialize_internal_node(void* node);

/**
 * @brief print some indent in stdout
 * 
 * @param level uint32_t
 */
void indent(uint32_t level);

/**
 * @brief print a Btree in stdout
 * 
 * @param pager               Pager*
 * @param page_num 			  uint32_t
 * @param indentation_level   uint32_t
 */
void print_tree(Pager* pager, uint32_t page_num, uint32_t indentation_level);

/**
 * @brief create a new root after spliting
 * 
 * @param table                 Table*
 * @param right_child_page_num  uint32_t
 */
void create_new_root(Table* table, uint32_t right_child_page_num);

/**
 * @brief searchs the internal node and returns the key position.
 * 
 * @param table     Table*
 * @param page_num  uint32_t
 * @param key 		uint32_t
 * @return Cursor* 
 */
Cursor* internal_node_find(Table* table, uint32_t page_num, uint32_t key);

/**
 * @brief return the number of next sibing
 * 
 * @param node   void*
 * @return uint32_t* 
 */
uint32_t* leaf_node_next_leaf_node(void* node);

/**
 * @brief return the parent node number
 * 
 * @param node  void*
 * @return uint32_t* 
 */
uint32_t* node_parent(void* node);

/**
 * @brief set a new key
 * 
 * @param node    void*
 * @param old_key uint32_t
 * @param new_key uint32_t
 */
void update_internal_node_key(void* node, uint32_t old_key, uint32_t new_key);

/**
 * @brief return the index of the child which should contain the given key
 * 
 * @param node  void*
 * @param key   uint32_t
 * @return uint32_t 
 */
uint32_t internal_node_find_child(void* node, uint32_t key);

/**
 * @brief  add a new child/key pair to parent that corresponds to child
 * 
 * @param table   			Table*
 * @param parent_page_num   uint32_t
 * @param child_page_num 	uint32_t
 */
void internal_node_insert(Table* table, uint32_t parent_page_num, uint32_t child_page_num);

/**
 * @brief execute an update operation.
 * 
 * @param statement    Statement*
 * @param table        Table*
 * @return ExecuteResult 
 */
ExecuteResult execute_update(Statement* statement, Table* table);

/**
 * @brief execute a delete operation.
 * 
 * @param statement  Statement*
 * @param table 	 Table*
 * @return ExecuteResult 
 */
ExecuteResult execute_delete(Statement* statement, Table* table);

/**
 * @brief just a utility to remove whitespaces after a word.
 * 
 * @param str  char*
 * @return char* 
 */
char* remove_whitespaces(char* str);

/**
 * @brief split an internal node then insert a child/key pair to it
 * 
 * @param table 					Table*
 * @param internal_parent_page_num  uint32_t
 * @param leaf_child_page_num       uint32_t
 */
void internal_node_split_then_insert(Table* table, uint32_t internal_parent_page_num, uint32_t leaf_child_page_num);

/**
 * @brief get internal node max key (including rightmost child)
 * 
 * @param pager  Pager*
 * @param node   void*
 * @return uint32_t 
 */
uint32_t internal_node_max_key(Pager* pager, void* node);

/**
 * @brief create a file and write every node to it
 * 
 * @param table 
 */
void export_to_json(Table* table);


int main(int argc, char const *argv[])
{
	if (argc < 2)
	{
		printf("Must supply a database filename.\n");
	   exit(EXIT_FAILURE);
	}

	const char* filename = argv[1];
	Table* table = db_open(filename);

	InputBuffer* input_buffer = new_input_buffer();
	while(1)
	{
		print_prompt();
		read_input(input_buffer);
		
		if (input_buffer->buffer[0] == '.')
		{
			switch (do_meta_command(input_buffer, table))
			{
				case META_COMMAND_SUCCESS:
					continue;
				case META_COMMAND_UNRECOGNIZED_COMMAND:
					printf("Unrecognized command '%s'\n", input_buffer->buffer);
					continue;
			}
		}
		
		Statement statement = {0};
		switch (prepare_statement(input_buffer, &statement))
		{
			case PREPARE_SUCCESS:
				break;
			case PREPARE_SYNTAX_ERROR:
				printf("Syntax error. Could not parse statement.\n");
				continue;
			case PREPARE_STRING_TOO_LONG:
				printf("String is too long.\n");
				continue;
			case PREPARE_NEGATIVE_ID:
				printf("ID must be positive.\n");
				continue;
			case PREPARE_UNRECOGNIZED_STATEMENT:
				printf("Unrecognized keyword at start of '%s'.\n", input_buffer->buffer);
       			continue;
		}

		switch(execute_statement(&statement, table))
		{
			case EXECUTE_SUCCESS:
				printf("executed.\n");
				break;
			case EXECUTE_TABLE_FULL:
				printf("Error: Table is full.\n");
				break;
			case EXECUTE_DUPLICATE_KEY:
				printf("Error: duplicate key.\n");
				break;
			case EXECUTE_RECORD_NOT_FOUND:
				printf("Error: record not found.\n");
				break;
		}
	}

	return 0;
}


InputBuffer* new_input_buffer()
{
	InputBuffer* temp_buffer = (InputBuffer*) malloc(sizeof(InputBuffer));

	temp_buffer->buffer_length = INPUT_BUFFER_MAX_SIZE;
	temp_buffer->input_length = 0;
	temp_buffer->buffer = (char*) calloc(temp_buffer->buffer_length, sizeof(char));

	return temp_buffer;
}


void print_prompt()
{ 
	printf("db -> ");
}


void read_input(InputBuffer* buffer)
{
	fgets(buffer->buffer, buffer->buffer_length, stdin);
	buffer->input_length = strlen(buffer->buffer);

	//ignore '\n'
	buffer->input_length = buffer->input_length - 1;
	buffer->buffer[buffer->input_length] = 0;
}


void close_input_buffer(InputBuffer* buffer)
{
    free(buffer->buffer);
    free(buffer);
}


MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* table)
{
	if(strcmp(input_buffer->buffer, ".exit") == 0)
	{
		db_close(table);
		exit(EXIT_SUCCESS);
	}
	else if (strcmp(input_buffer->buffer, ".constants") == 0)
	{
		printf("constants:\n");
		print_constants();
		return META_COMMAND_SUCCESS;
	}
	else if (strcmp(input_buffer->buffer, ".btree") == 0)
	{
		printf("tree:\n");
		print_tree(table->pager, table->root_page_num, 0);
		return META_COMMAND_SUCCESS;
	}
	else
	{
		return META_COMMAND_UNRECOGNIZED_COMMAND;
	}
}


PrepareResult prepare_insert(InputBuffer* buffer, Statement* statement)
{
	statement->type = STATEMENT_INSERT;

	char* keyword = strtok(buffer->buffer, " ");
	char* id_string = strtok(NULL, " ");
	char* username = strtok(NULL, " ");
	char* email = strtok(NULL, " ");

	if (id_string == NULL || username == NULL || email == NULL)
	{
		return PREPARE_SYNTAX_ERROR;
	}
	
	int id = atoi(id_string);
	if (id < 0)
	{
		return PREPARE_NEGATIVE_ID;
	}
	
	if (strlen(username) > COLUMN_USERNAME_SIZE || strlen(email) > COLUMN_EMAIL_SIZE)
	{
		return PREPARE_STRING_TOO_LONG;
	}
	
	statement->row_to_insert.id = id;
	strcpy(statement->row_to_insert.username, username);
	strcpy(statement->row_to_insert.email, email);

	return PREPARE_SUCCESS;
}


PrepareResult prepare_statement(InputBuffer* buffer, Statement* statement)
{
	buffer->buffer = remove_whitespaces(buffer->buffer);
	if (strncmp(buffer->buffer, "insert", 6) == 0)
	{
		return prepare_insert(buffer, statement);
	}
	if (strcmp(buffer->buffer, "select") == 0)
	{
		statement->type = STATEMENT_SELECT;
		return PREPARE_SUCCESS;
	}
	if (strncmp(buffer->buffer, "update", 6) == 0)
	{
		return prepare_update(buffer, statement);
	}
	if (strncmp(buffer->buffer, "delete", 6) == 0)
	{
		return prepare_delete(buffer, statement);
	}

	return PREPARE_UNRECOGNIZED_STATEMENT;
}


ExecuteResult execute_insert(Statement* statement, Table* table)
{
	Row* row_to_insert = &(statement->row_to_insert);
	uint32_t key_to_insert = row_to_insert->id;
	Cursor* cursor = table_find(table, key_to_insert);

	void* node = get_node(table->pager, cursor->page_num);
	uint32_t num_cells = (*leaf_node_num_cells(node));

	if (cursor->cell_num < num_cells)
	{
		/* Debug
		printf("node type: %d\n", get_node_type(node));
		Debug */
		uint32_t key_at_index = *leaf_node_key(node, cursor->cell_num);
		if (key_at_index == key_to_insert)
		{
			return EXECUTE_DUPLICATE_KEY;
		}
	}
	
	leaf_node_insert(cursor, row_to_insert->id, row_to_insert);

	free(cursor);

	return EXECUTE_SUCCESS;
}


ExecuteResult execute_select(Statement* statement, Table* table)
{
	Row row;
	Cursor* cursor = table_start(table);

	while (!cursor->end_of_table)
	{
		deserialize_row(cursor_value(cursor), &row);
		print_row(&row);
		cursor_advance(cursor);
	}
	
	free(cursor);
	
	return EXECUTE_SUCCESS;
}


ExecuteResult execute_statement(Statement* statement, Table* table)
{
	switch (statement->type)
	{
		case STATEMENT_INSERT:
			return execute_insert(statement, table);
		case STATEMENT_SELECT:
			return execute_select(statement, table);
		case STATEMENT_UPDATE:
			return execute_update(statement, table);
		case STATEMENT_DELETE:
			return execute_delete(statement, table);
	}
}


void serialize_row(Row* source, void* dest)
{
	memcpy(dest + ID_OFFSET, &(source->id), ID_SIZE);
	memcpy(dest + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
	memcpy(dest + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}


void deserialize_row(void* source, Row* dest)
{
	memcpy(&(dest->id), source + ID_OFFSET, ID_SIZE);
	memcpy(&(dest->username), source + USERNAME_OFFSET, USERNAME_SIZE);
	memcpy(&(dest->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}


void* get_node(Pager* pager, uint32_t page_num)
{
	if (page_num > TABLE_MAX_PAGES)
	{
		printf("Error: Tried to fetch page number out of bounds. %d > %d\n",
			page_num,
        	TABLE_MAX_PAGES
		);
    	exit(EXIT_FAILURE);
	}
	
	if (pager->pages[page_num] == NULL) // we have not this page on memory or it's empty
	{
		void* page = calloc(PAGE_SIZE, 1);
		uint32_t num_pages = pager->file_lenght / PAGE_SIZE;

		if (pager->file_lenght % PAGE_SIZE)
		{
			num_pages++;
		}
		
		if (page_num <= num_pages) // the page is present on disk so we read the page from database
		{
			fseek(pager->file_desc, page_num * PAGE_SIZE, SEEK_SET);
			ssize_t bytes_read = fread(page, 1, PAGE_SIZE, pager->file_desc);
			if (bytes_read == -1)
			{
				printf("Error reading file: %d\n", errno);
        		exit(EXIT_FAILURE);
			}
		}

		pager->pages[page_num] = page;

		if (page_num >= pager->num_pages)
		{
			pager->num_pages = page_num + 1;
		}
		
	}

	return pager->pages[page_num];
}


void* cursor_value(Cursor* cursor)
{
	uint32_t page_num = cursor->page_num;

	void* page = get_node(cursor->table->pager, page_num);

	return leaf_node_value(page, cursor->cell_num);
}


Pager* pager_open(const char* filename)
{
	FILE* fd;
	fd = fopen(filename, "rb+");

	if (!fd) // new database file
	{
		printf("the new database file is fed\n");
		fd = fopen(filename, "wb+");

		if (!fd)
		{
			printf("cannot open file %d", errno);
			exit(EXIT_FAILURE);
		}
	}

	int seek_result;
	uint32_t begin, end, file_lenght;

	seek_result = fseek(fd, 0, SEEK_SET);
	if (seek_result != 0)
	{
		printf("seeking error %d\n", errno);
		exit(EXIT_FAILURE);
	}

	begin = ftell(fd);

	seek_result = fseek(fd, 0, SEEK_END);
	if (seek_result != 0)
	{
		printf("seeking error %d\n", errno);
		exit(EXIT_FAILURE);
	}

	end = ftell(fd);

	file_lenght = end - begin;

	Pager* pager = (Pager*) malloc(sizeof(Pager));
	pager->file_desc = fd;
	pager->file_lenght = file_lenght;
	pager->num_pages = file_lenght / PAGE_SIZE;

	if (file_lenght % PAGE_SIZE != 0)
	{
		printf("DB file is not a whole number of pages. the file maybe damaged\n");
		exit(EXIT_FAILURE);
	}
	
	for (size_t i = 0; i < TABLE_MAX_PAGES; i++)
	{
		pager->pages[i] = NULL;
	}
	
	return pager;
}


void pager_flush(Pager* pager, uint32_t page_num)
{
	if (pager->pages[page_num] == NULL)
	{
		printf("Tried to flush null page\n");
 	   exit(EXIT_FAILURE);
	}
	
	off_t offset = fseek(pager->file_desc, page_num * PAGE_SIZE, SEEK_SET);
	if (offset == -1)
	{
		printf("Error seeking: %d\n", errno);
	    exit(EXIT_FAILURE);
	}

	/* Debug
	if (page_num == 0)
	{
		unsigned char* iterator = pager->pages[page_num] + LEAF_NODE_HEADER_SIZE + LEAF_NODE_VALUE_OFFSET;
		for (int i = 0; i < LEAF_NODE_VALUE_SIZE; i++)
		{
			printf("%p: %.2x\n", iterator + i, *(iterator + i));
		}
	}
	Debug */

	ssize_t bytes_written = fwrite(pager->pages[page_num], 1, PAGE_SIZE, pager->file_desc);
	if (bytes_written == -1)
	{
		printf("Error writing: %d\n", errno);
	    exit(EXIT_FAILURE);
	}
}


Table* db_open(const char* filename)
{
	Pager* pager = pager_open(filename);

	Table* table = (Table*) malloc(sizeof(Table));
	table->pager = pager;
	table->root_page_num = 0;

	if (pager->num_pages == 0) // new database file
	{
		void* root_node = get_node(pager, 0);
		initialize_leaf_node(root_node);
		set_node_root(root_node, true);
	}

	return table;
}


void db_close(Table* table)
{
	Pager* pager = table->pager;

	for (size_t i = 0; i < pager->num_pages; i++)
	{
		if (pager->pages[i] == NULL)
		{
			continue;
		}
		
		pager_flush(pager, i);
		free(pager->pages[i]);
		pager->pages[i] = NULL;
	}

	int result = fclose(pager->file_desc);
	if (result == -1)
	{
		printf("Error closing db file.\n");
	    exit(EXIT_FAILURE);
	}
	
	for (size_t i = 0; i < TABLE_MAX_PAGES; i++)
	{
		void* page = pager->pages[i];
		if (page)
		{
			free(page);
			pager->pages[i] = NULL;
		}
	}
	free(pager);
	pager = NULL;
}


void print_row(Row* row)
{
	printf("(%d %s %s)\n", row->id, row->username, row->email);
}


Cursor* table_start(Table* table)
{
	Cursor* cursor = table_find(table, 0);

	void* node = get_node(table->pager, cursor->page_num);
	uint32_t num_cells = *leaf_node_num_cells(node);
	cursor->end_of_table = num_cells == 0;

	return cursor;
}


Cursor* table_end(Table* table)
{
	Cursor* cursor = (Cursor*) malloc(sizeof(Cursor));

	cursor->table = table;
	cursor->page_num = table->root_page_num;
	cursor->end_of_table = true;

	void* root_node = get_node(table->pager, table->root_page_num);
	uint32_t num_cells = *leaf_node_num_cells(root_node);
	cursor->cell_num = num_cells;

	return cursor;
}


void cursor_advance(Cursor* cursor)
{
	uint32_t page_num = cursor->page_num;
	void* node = get_node(cursor->table->pager, page_num);

	cursor->cell_num++;
	if (cursor->cell_num >= *leaf_node_num_cells(node))
	{
		uint32_t next_page_num = *leaf_node_next_leaf_node(node);
		if (next_page_num == 0)
		{
			cursor->end_of_table = true;
		}
		else
		{
			cursor->page_num = next_page_num;
			cursor->cell_num = 0;
		}
	}
}


uint32_t* leaf_node_num_cells(void* node)
{
	return node + LEAF_NODE_NUM_CELLS_OFFSET;
}


void* leaf_node_cell(void* node, uint32_t cell_num)
{
	return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}


uint32_t* leaf_node_key(void* node, uint32_t cell_num)
{
	return leaf_node_cell(node, cell_num);
}


void* leaf_node_value(void* node, uint32_t cell_num)
{
	return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}


void initialize_leaf_node(void* node)
{
	set_node_type(node, NODE_LEAF);
	set_node_root(node, false);
	*leaf_node_num_cells(node) = 0;
	*leaf_node_next_leaf_node(node) = 0;
}


void initialize_internal_node(void* node)
{
	set_node_type(node, NODE_INTERNAL);
	set_node_root(node, false);
	*internal_node_num_keys(node) = 0;
}


void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value)
{
	void* node = get_node(cursor->table->pager, cursor->page_num);

	uint32_t num_cells = *leaf_node_num_cells(node);
	if (num_cells >= LEAF_NODE_MAX_CELLS)
	{
		leaf_node_split_then_insert(cursor, key, value);
		return;
	}

	if (cursor->cell_num < num_cells)
	{
		for (uint32_t i = num_cells; i > cursor->cell_num; i--)
		{
			memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1), LEAF_NODE_CELL_SIZE);
		}
	}
	
	*(leaf_node_num_cells(node)) += 1;
	*(leaf_node_key(node, cursor->cell_num)) = key;
	serialize_row(value, leaf_node_value(node, cursor->cell_num));
}


void print_constants()
{
	printf("\trow_size: %d\n", ROW_SIZE);
	printf("\tcommon_node_header_size: %d\n", COMMON_NODE_HEADER_SIZE);
	printf("\tleaf_node_header_size: %d\n", LEAF_NODE_HEADER_SIZE);
	printf("\tleaf_node_cell_size: %d\n", LEAF_NODE_CELL_SIZE);
	printf("\tleaf_node_space_for_cells: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
	printf("\tleaf_node_max_cells: %d\n", LEAF_NODE_MAX_CELLS);

	printf("\tinternal_node_header_size: %d\n", INTERNAL_NODE_HEADER_SIZE);
	printf("\tinternal_node_cell_size: %d\n", INTERNAL_NODE_CELL_SIZE);
	printf("\tinternal_node_space_for_cells: %d\n", INTERNAL_NODE_SPACE_FOR_CELLS);
	printf("\tinternal_node_max_cells: %d\n", INTERNAL_NODE_MAX_CELLS);
}


Cursor* table_find(Table* table, uint32_t key)
{
	uint32_t root_page_num = table->root_page_num;
	void* root_node = get_node(table->pager, root_page_num);

	if (get_node_type(root_node) == NODE_LEAF)
	{
		return leaf_node_find(table, root_page_num, key);
	}
	else
	{
		return internal_node_find(table, root_page_num, key);
	}
}


NodeType get_node_type(void* node)
{
	return *((uint8_t*)node + NODE_TYPE_OFFSET);
}

Cursor* leaf_node_find(Table* table, uint32_t page_num, uint32_t key)
{
	void* node = get_node(table->pager, page_num);
	uint32_t num_cells = *leaf_node_num_cells(node);

	Cursor* cursor = (Cursor*) malloc(sizeof(Cursor));
	cursor->table = table;
	cursor->page_num = page_num;

	// binary search
	uint32_t min_index = 0;
	uint32_t one_past_max_index = num_cells;

	while (one_past_max_index != min_index)
	{
		uint32_t index = (min_index + one_past_max_index) / 2;
		uint32_t key_at_index = *(leaf_node_key(node, index));
		if (key == key_at_index)
		{
			cursor->cell_num = index;
			return cursor;
		}
		if (key < key_at_index)
		{
			one_past_max_index = index;
		}
		else
		{
			min_index = index + 1;
		}
	}

	cursor->cell_num = min_index;
	return cursor;
}


void set_node_type(void* node, NodeType type)
{
	*((uint8_t*)node + NODE_TYPE_OFFSET) = (uint8_t)type;
}


void leaf_node_split_then_insert(Cursor* cursor, uint32_t key, Row* value)
{
	void* old_node = get_node(cursor->table->pager, cursor->page_num);
	uint32_t old_max = get_node_max_key(old_node);
	uint32_t new_page_num = get_unused_page_num(cursor->table->pager);
	void* new_node = get_node(cursor->table->pager, new_page_num);
	initialize_leaf_node(new_node);
	*node_parent(new_node) = *node_parent(old_node);
	*leaf_node_next_leaf_node(new_node) = *leaf_node_next_leaf_node(old_node);
	*leaf_node_next_leaf_node(old_node) = new_page_num;
	
	for (int32_t i = LEAF_NODE_MAX_CELLS; i >= 0; i--)
	{
		void* dest_node;
		dest_node = (i >= LEAF_NODE_LEFT_SPLIT_COUNT) ? new_node : old_node;

		uint32_t index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
		void* dest = leaf_node_cell(dest_node, index_within_node);

		if (i == cursor->cell_num)
		{
			// printf("moving: %d %s %s\n", value->id, value->username, value->email);
			serialize_row(value, leaf_node_value(dest_node, index_within_node));
			*leaf_node_key(dest_node, index_within_node) = key;
		}
		else if (i > cursor->cell_num)
		{
			/* Debug
			Row row;
			deserialize_row(leaf_node_value(old_node, i - 1), &row);
			printf("moving: %d %s %s\n", row.id, row.username, row.email);
			Debug */
			memcpy(dest, leaf_node_cell(old_node, i - 1), LEAF_NODE_CELL_SIZE);
		}
		else
		{
			/* Debug
			Row row;
			deserialize_row(leaf_node_value(old_node, i), &row);
			printf("moving: %d %s %s\n", row.id, row.username, row.email);
			Debug */
			memcpy(dest, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE);
		}
	}
	
	*(leaf_node_num_cells(old_node)) = LEAF_NODE_LEFT_SPLIT_COUNT;
	*(leaf_node_num_cells(new_node)) = LEAF_NODE_RIGHT_SPLIT_COUNT;

	if (is_node_root(old_node))
	{
		return create_new_root(cursor->table, new_page_num);
	}
	else
	{
		uint32_t parent_page_num = *node_parent(old_node);
		uint32_t new_max = get_node_max_key(old_node);
		void* parent = get_node(cursor->table->pager, parent_page_num);
		update_internal_node_key(parent, old_max, new_max);
		internal_node_insert(cursor->table, parent_page_num, new_page_num);
	}
}


uint32_t get_unused_page_num(Pager* pager)
{
	return pager->num_pages;
}


void create_new_root(Table* table, uint32_t right_child_page_num)
{
	void* root = get_node(table->pager, table->root_page_num);
	void* right_child = get_node(table->pager, right_child_page_num);
	uint32_t left_child_page_num = get_unused_page_num(table->pager);
	void* left_child = get_node(table->pager, left_child_page_num);

	memcpy(left_child, root, PAGE_SIZE);
	set_node_root(left_child, false);

	initialize_internal_node(root);
	set_node_root(root, true);
	*internal_node_num_keys(root) = 1;
	*internal_node_child(root, 0) = left_child_page_num;
	uint32_t left_child_max_key = get_node_max_key(left_child);
	*internal_node_key(root, 0) = left_child_max_key;
	*internal_node_rightmost_child(root) = right_child_page_num;
	*node_parent(left_child) = table->root_page_num;
	*node_parent(right_child) = table->root_page_num;
}


uint32_t* internal_node_num_keys(void* node)
{
	return node + INTERNAL_NODE_NUM_KEYS_OFFSET;
}


uint32_t* internal_node_rightmost_child(void* node)
{
	return node + INTERNAL_NODE_RIGHTMOST_CHILD_OFFSET;
}


uint32_t* internal_node_cell(void* node, uint32_t cell_num)
{
	return node + INTERNAL_NODE_HEADER_SIZE + INTERNAL_NODE_CELL_SIZE * cell_num;
}


uint32_t* internal_node_child(void* node, uint32_t child_num)
{
	uint32_t num_keys = *internal_node_num_keys(node);
	if (child_num > num_keys)
	{
		printf("tried to access a child that does not exist! child_num: %d > num_keys: %d", child_num, num_keys);
		exit(EXIT_FAILURE);
	}
	else if (child_num == num_keys)
	{
		return internal_node_rightmost_child(node);
	}
	else
	{
		return internal_node_cell(node, child_num);
	}
}


uint32_t* internal_node_key(void* node, uint32_t key_num)
{
	return (void*)internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
}


uint32_t get_node_max_key(void* node)
{
	switch (get_node_type(node))
	{
	case NODE_INTERNAL:
		return *internal_node_key(node, *(internal_node_num_keys(node)) - 1);
		break;
	case NODE_LEAF:
		return *leaf_node_key(node, *(leaf_node_num_cells(node)) - 1);
	default:
		break;
	}
}


bool is_node_root(void* node)
{
	 return (bool)*((uint8_t*)(node + IS_ROOT_OFFSET));
}


void set_node_root(void* node, bool is_root)
{
	*((uint8_t*)(node + IS_ROOT_OFFSET)) = (uint8_t)is_root;
}


void indent(uint32_t level)
{
	for (size_t i = 0; i < level; i++)
	{
		printf(" ");
	}
	if (level == 0)
	{
		printf("┌── ");
	}
}


void print_tree(Pager* pager, uint32_t page_num, uint32_t indentation_level)
{
	void* node = get_node(pager, page_num);
	uint32_t num_keys, child, parent;
	parent = *node_parent(node);

	switch (get_node_type(node))
	{
	case NODE_LEAF:
		num_keys = *leaf_node_num_cells(node);
		indent(indentation_level);
		printf("└┬── leaf (size %d), page num: %d, parent: %d\n", num_keys, page_num, parent);
		for (size_t i = 0; i < num_keys; i++)
		{
			indent(indentation_level + 1);
			printf("│");
			printf("\n");
			indent(indentation_level + 1);
			if (i == num_keys - 1)
			{
				printf("└── ");
			}
			else
			{
				printf("├── ");
			}
			printf("%d\n", *leaf_node_key(node, i));
		}
		break;
	
	case NODE_INTERNAL:
		num_keys = *internal_node_num_keys(node);
		indent(indentation_level);
		if (!is_node_root(node))
		{
			printf("└┬── ");
		}
		printf("internal (size %d), page num: %d, parent: %d\n", num_keys, page_num, parent);
		for (size_t i = 0; i < num_keys; i++)
		{
			child = *internal_node_child(node, i);
			print_tree(pager, child, indentation_level + 1);
			indent(indentation_level + 1);
			if (i == num_keys - 1)
			{
				printf("└── ");
			}
			else
			{
				printf("├── ");
			}
			printf("key %d page num: %d\n", *internal_node_key(node, i), page_num);
		}
		child = *internal_node_rightmost_child(node);
		print_tree(pager, child, indentation_level + 1);
		break;
	}
}


Cursor* internal_node_find(Table* table, uint32_t page_num, uint32_t key)
{
	void* node = get_node(table->pager, page_num);
	
	uint32_t child_index = internal_node_find_child(node, key);
	uint32_t child_num = *internal_node_child(node, child_index);
	void* child = get_node(table->pager, child_num);

	switch (get_node_type(child))
	{
		case NODE_LEAF:
			return leaf_node_find(table, child_num, key);
			break;
			
		case NODE_INTERNAL:
			internal_node_find(table, child_num, key);
			break;
	}
}


uint32_t* leaf_node_next_leaf_node(void* node)
{
	return node + LEAF_NODE_NEXT_LEAF_NODE_OFFSET;
}


uint32_t* node_parent(void* node)
{
	return node + PARENT_POINTER_OFFSET;
}


void update_internal_node_key(void* node, uint32_t old_key, uint32_t new_key)
{
	uint32_t old_child_index = internal_node_find_child(node, old_key);
	*internal_node_key(node, old_child_index) = new_key;
}


uint32_t internal_node_find_child(void* node, uint32_t key)
{
	uint32_t num_keys = *internal_node_num_keys(node);

	// binary search
	uint32_t min_index = 0;
	uint32_t max_index = num_keys;

	while (min_index != max_index)
	{
		uint32_t index = (min_index + max_index) / 2;
		uint32_t key_to_right = *internal_node_key(node, index);
		if (key_to_right >= key)
		{
			max_index = index;
		}
		else
		{
			min_index = index + 1;
		}
	}

	return min_index;
}


void internal_node_insert(Table* table, uint32_t parent_page_num, uint32_t child_page_num)
{
	void* parent = get_node(table->pager, parent_page_num);
	void* child = get_node(table->pager, child_page_num);
	uint32_t child_max_key = get_node_max_key(child);
	uint32_t index = internal_node_find_child(parent, child_max_key);

	uint32_t original_num_keys = *internal_node_num_keys(parent);

	if (original_num_keys >= INTERNAL_NODE_MAX_CELLS)
	{
		*internal_node_num_keys(parent) = original_num_keys + 1;
		return internal_node_split_then_insert(table, parent_page_num, child_page_num);
	}

	*internal_node_num_keys(parent) = original_num_keys + 1;
	uint32_t rightmost_child_page_num = *internal_node_rightmost_child(parent);
	void* rightmost_child = get_node(table->pager, rightmost_child_page_num);

	if (child_max_key > get_node_max_key(rightmost_child)) // replace right child
	{
		*internal_node_child(parent, original_num_keys) = rightmost_child_page_num;
		*internal_node_key(parent, original_num_keys) = get_node_max_key(rightmost_child);
		*internal_node_rightmost_child(parent) = child_page_num;
	}
	else // make new room for the new cell
	{
		for (uint32_t i = original_num_keys; i > index; i--)
		{
			void* dest = internal_node_cell(parent, i);
			void* source = internal_node_cell(parent, i-1);
			memcpy(dest, source, INTERNAL_NODE_CELL_SIZE);
		}
		*internal_node_child(parent, index) = child_page_num;
		*internal_node_key(parent, index) = child_max_key;
	}

	*node_parent(child) = parent_page_num;
}


PrepareResult prepare_update(InputBuffer* buffer, Statement* statement)
{
	statement->type = STATEMENT_UPDATE;

	buffer->buffer = buffer->buffer + 6;

	uint32_t id;
	char email[COLUMN_EMAIL_SIZE + 1] = {0};
	char username[COLUMN_USERNAME_SIZE + 1] = {0};
	char column[100];
	int column_index = 0;
	bool possible_whitespaces = true;
	
	for (char* i = buffer->buffer; *i != 0; i++)
	{
		if (possible_whitespaces)
		{
			i = remove_whitespaces(i);
			possible_whitespaces = false;
		}

		if (!('a' <= *i && *i <= 'z'))
		{
			i = remove_whitespaces(i);// username = test, email = test where id = 1

			if (*i == '=')
			{
				i++;
				i = remove_whitespaces(i);
				if (strncmp(column, "username", 8) == 0)
				{
					int username_len = 0;
					for (char* j = username; *i != ',' && *i != ' '; i++, j++)
					{
						if (username_len > COLUMN_USERNAME_SIZE)
						{
							return PREPARE_STRING_TOO_LONG;
						}
						
						*j = *i;
						username_len++;
					}
					column_index = 0;
					possible_whitespaces = true;
					continue;
				}
				else if (strncmp(column, "email", 5) == 0)
				{
					int email_len = 0;
					for (char* j = email; *i != ',' && *i != ' '; i++, j++)
					{
						if (email_len > COLUMN_EMAIL_SIZE)
						{
							return PREPARE_STRING_TOO_LONG;
						}

						*j = *i;
						email_len++;
					}
					column_index = 0;
					possible_whitespaces = true;
					continue;
				}
				else
				{
					return PREPARE_SYNTAX_ERROR;
				}
			}
			else if (strncmp(column, "where", 5) == 0)
				{
					if (strncmp(i, "id", 2) == 0)
					{
						i = remove_whitespaces(i + 2);
					}
					if (*i == '=')
					{
						i = remove_whitespaces(i + 1);
					}
					sscanf(i, "%d", &id);
					if (id < 0)
					{
						return PREPARE_NEGATIVE_ID;
					}

					statement->row_to_insert.id = id;
					strcpy(statement->row_to_insert.username, username);
					strcpy(statement->row_to_insert.email, email);

					return PREPARE_SUCCESS;
				}
		}

		column[column_index++] = *i;
		column[column_index] = 0;
	}

	return PREPARE_SYNTAX_ERROR;
}


ExecuteResult execute_update(Statement* statement, Table* table)
{
	uint32_t id = statement->row_to_insert.id;
	char* username = statement->row_to_insert.username;
	char* email = statement->row_to_insert.email;

	Cursor* cursor = table_find(table, id);

	void* node = get_node(table->pager, cursor->page_num);
	uint32_t key = *leaf_node_key(node, cursor->cell_num);
	if (key != id)
	{
		return EXECUTE_RECORD_NOT_FOUND;
	}

	void* cell_value = leaf_node_value(node, cursor->cell_num);

	Row row;
	deserialize_row(cell_value, &row);

	if (*username)
	{
		strcpy(row.username, username);
	}
	if (*email)
	{
		strcpy(row.email, email);
	}
	
	serialize_row(&row, cell_value);

	return EXECUTE_SUCCESS;
}


char* remove_whitespaces(char* str)
{
	for (; *str == ' '; str++);
	return str;
}


PrepareResult prepare_delete(InputBuffer* buffer, Statement* statement)
{
	statement->type = STATEMENT_DELETE;

	buffer->buffer = buffer->buffer + 6;
	
	buffer->buffer = remove_whitespaces(buffer->buffer);

	uint32_t id;
	sscanf(buffer->buffer, "%d", &id);
	if (id < 0)
	{
		return PREPARE_NEGATIVE_ID;
	}

	statement->row_to_insert.id = id;

	return PREPARE_SUCCESS;
}


ExecuteResult execute_delete(Statement* statement, Table* table)
{
	uint32_t id = statement->row_to_insert.id;

	Cursor* cursor = table_find(table, id);

	void* node = get_node(table->pager, cursor->page_num);
	uint32_t key = *leaf_node_key(node, cursor->cell_num);
	uint32_t old_max_key = get_node_max_key(node);
	if (key != id)
	{
		return EXECUTE_RECORD_NOT_FOUND;
	}

	uint32_t num_cells = *leaf_node_num_cells(node);
	if (cursor->cell_num < num_cells - 1)
	{
		for (uint32_t i = cursor->cell_num; i < num_cells; i++)
		{
			memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i + 1), LEAF_NODE_CELL_SIZE);
		}
	}

	num_cells--;
	*leaf_node_num_cells(node) = num_cells;

	if (num_cells != 0)
	{
		uint32_t new_max_key = get_node_max_key(node);
		if (new_max_key != old_max_key)
		{
			uint32_t parent_page_num = *node_parent(node);
			void* parent = get_node(cursor->table->pager, parent_page_num);
			update_internal_node_key(parent, old_max_key, new_max_key);
		}
	}
	else
	{
		printf("Error: restructure the Tree needed\n");
		exit(EXIT_FAILURE);
	}

	return EXECUTE_SUCCESS;
}


void internal_node_split_then_insert(Table* table, uint32_t internal_parent_page_num, uint32_t leaf_child_page_num)
{
	/* split the internal node and distribute child/key pairs */
	void* old_internal = get_node(table->pager, internal_parent_page_num);
	if (!is_node_root(old_internal))
	{
		uint32_t old_internal_parent_page_num = *node_parent(old_internal);
		void* old_internal_parent = get_node(table->pager, old_internal_parent_page_num);

		if (*internal_node_num_keys(old_internal_parent) >= INTERNAL_NODE_MAX_CELLS)
		{
			printf("Error: need to recusrsively split the tree\n");
			exit(EXIT_FAILURE);
		}
	}

	uint32_t old_internal_max_key = internal_node_max_key(table->pager, old_internal);
	uint32_t new_internal_page_num = table->pager->num_pages;
	void* new_internal = get_node(table->pager, new_internal_page_num);

	initialize_internal_node(new_internal);

	void* leaf_child = get_node(table->pager, leaf_child_page_num);
	uint32_t leaf_child_max_key = get_node_max_key(leaf_child);

	/* Debug
	printf("old node: (size %u) \n", *internal_node_num_keys(old_internal));
	for (int32_t i = 0; i < *internal_node_num_keys(old_internal); i++)
	{
		printf("  %u: %u\n", i, *internal_node_key(old_internal, i));
	}
	printf("  most right child: %u\n", get_node_max_key(get_node(table->pager, *internal_node_rightmost_child(old_internal))));

	printf("new node: (size %u)\n", *internal_node_num_keys(new_internal));
	for (int32_t i = 0; i < *internal_node_num_keys(new_internal); i++)
	{
		printf("  %u: %u\n", i, *internal_node_key(new_internal, i));
	}
	Debug */

	uint32_t old_internal_rightmost_node_page_num = *internal_node_rightmost_child(old_internal);
	void* old_internal_rightmost_node = get_node(table->pager, old_internal_rightmost_node_page_num);
	*internal_node_rightmost_child(new_internal) = old_internal_rightmost_node_page_num;
	*node_parent(old_internal_rightmost_node) = new_internal_page_num;

	uint32_t index_within_old_internal = internal_node_find_child(old_internal, leaf_child_max_key);

	for (int32_t i = INTERNAL_NODE_MAX_CELLS - 1; i >= 0; i--)
	{
		void* dest_node;
		dest_node = (i >= INTERNAL_NODE_LEFT_SPLIT_COUNT) ? new_internal : old_internal;

		uint32_t index_within_dest_node = i % INTERNAL_NODE_LEFT_SPLIT_COUNT;
		void* dest = (void*) internal_node_cell(dest_node, index_within_dest_node);

		/* Debug

		printf("%u: %u, index: %u\n", get_node_type(dest_node), *internal_node_key(dest_node, index_within_dest_node), index_within_dest_node);

		Debug */
		
		void* cell = (void*) internal_node_cell(old_internal, i);
		// printf("moving internal: %u\n", *(uint32_t*)cell + INTERNAL_NODE_CHILD_SIZE);
		memcpy(dest, cell, INTERNAL_NODE_CELL_SIZE);
		*node_parent(get_node(table->pager, *(uint32_t*)cell)) = (i >= INTERNAL_NODE_LEFT_SPLIT_COUNT) ? new_internal_page_num : internal_parent_page_num;
	}
	/* distribute child/key pairs end here */

	*internal_node_num_keys(old_internal) = INTERNAL_NODE_LEFT_SPLIT_COUNT;
	*internal_node_num_keys(new_internal) = INTERNAL_NODE_RIGHT_SPLIT_COUNT;

	/* change most right child */
	uint32_t last_child_page_num = *internal_node_child(old_internal, INTERNAL_NODE_LEFT_SPLIT_COUNT - 1);
	*internal_node_rightmost_child(old_internal) = last_child_page_num;
	*internal_node_num_keys(old_internal) = INTERNAL_NODE_LEFT_SPLIT_COUNT - 1;

	internal_node_insert(table, new_internal_page_num, leaf_child_page_num);

	if (is_node_root(old_internal))
	{
		uint32_t new_root_page_num = get_unused_page_num(table->pager);
		void* new_root = get_node(table->pager, new_root_page_num);

		initialize_internal_node(new_root);
		set_node_root(new_root, true);
		set_node_root(old_internal, false);

		*internal_node_num_keys(new_root) = 1;
		*internal_node_child(new_root, 0) = internal_parent_page_num;
		uint32_t old_internal_max_key = internal_node_max_key(table->pager, old_internal);
		*internal_node_key(new_root, 0) = old_internal_max_key;
		*internal_node_rightmost_child(new_root) = new_internal_page_num;

		table->root_page_num = new_root_page_num;
		*node_parent(old_internal) = table->root_page_num;
		*node_parent(new_internal) = table->root_page_num;
	}
	else
	{
		uint32_t parent_page_num = *node_parent(old_internal);
		void* parent = get_node(table->pager, parent_page_num);
		update_internal_node_key(parent, old_internal_max_key, internal_node_max_key(table->pager, old_internal));
		internal_node_insert(table, parent_page_num, new_internal_page_num);
	}

	/* Debug
	printf("old node: (size %u) \n", *internal_node_num_keys(old_internal));
	for (int32_t i = 0; i < *internal_node_num_keys(old_internal); i++)
	{
		printf("  %u: %u\n", i, *internal_node_key(old_internal, i));
	}
	printf("  most right child: %u\n", get_node_max_key(get_node(table->pager, *internal_node_rightmost_child(old_internal))));

	printf("new node: (size %u)\n", *internal_node_num_keys(new_internal));
	for (int32_t i = 0; i < *internal_node_num_keys(new_internal); i++)
	{
		printf("  %u: %u\n", i, *internal_node_key(new_internal, i));
	}
	printf("  most right child: %u\n", get_node_max_key(get_node(table->pager, *internal_node_rightmost_child(new_internal))));
	Debug */
}


uint32_t internal_node_max_key(Pager* pager, void* node)
{
	return get_node_max_key(get_node(pager, *internal_node_rightmost_child(node)));
}


void export_to_json(Table* table)
{
	FILE* file = fopen("table.json", "w");
	if (file == NULL)
	{
		printf("Could not open file\n");
		exit(EXIT_FAILURE);
	}

	size_t offset = 0;

	NodeInJson* node_json = (NodeInJson*) malloc(sizeof(NodeInJson));

	void* root = get_node(table->pager, table->root_page_num);

	node_json->type = get_node_type(root);
	node_json->is_root = is_node_root(root);
	node_json->page_num = table->root_page_num;
	node_json->parent_page_num = node_json->page_num;
	node_json->num_keys = (node_json->type == NODE_LEAF) ? *leaf_node_num_cells(root) : *internal_node_num_keys(root);

	fseek(file, offset, SEEK_SET);
	offset += fprintf(file,
		"{'type': %d,\
		'is_root': %d,\
		'page_num': %u,\
		'parent_page_num': %u,\
		'num_keys': %u,\
		'body': {",
		node_json->type,
		node_json->is_root,
		node_json->page_num,
		node_json->parent_page_num,
		node_json->num_keys
	);

	for (uint32_t i = 0; i < node_json->num_keys; i++)
	{
		uint32_t key, child;
		switch (node_json->type)
		{
		case NODE_LEAF:
			key = *leaf_node_key(root, i);
			fseek(file, offset, SEEK_SET);
			offset += fprintf(file, "%u,", key);
			break;
		
		case NODE_INTERNAL:
			key = *internal_node_key(root, i);
			child = *internal_node_child(root, i);
			fseek(file, offset, SEEK_SET);
			offset += fprintf(file, "'%u': %u,", child, key);
			break;
		}
	}
	fseek(file, --offset, SEEK_SET);
	fprintf(file, "]},");

	fclose(file);
}