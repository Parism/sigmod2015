#include <stdint.h>

#define INITIAL_CAPACITY 3   //initial capacity of the journal 
#define INCREASE_VALUE 20

typedef struct JournalRecord
{
	int transaction_id;
	int num_of_lines;

	uint64_t** columns_array;//holds all records of the transaction and the last column is the dirty bit

}JournalRecord;

typedef struct Journal
{
	JournalRecord** transactions_array;
	int records_counter;		//the number of full positions of the transactions_array
	int journal_capacity;		//the capacity of the transactions_array

}Journal;

typedef struct Records_Node
{
		
	JournalRecord* journal_record;
	struct Records_Node* next;

}Records_Node;


Journal* createJournal();
int insertJournalRecord(Journal* , JournalRecord* , int , int  );
int increase_Journal(Journal* );
Records_Node* getJournalRecords(Journal* , int , int );
int Journal_Binary_Search(Journal* , int , int );
int destroyJournal(Journal*);