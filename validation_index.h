#include "structs.h"


#define VALIDATION_BUCKET_SIZE 10
#define VALIDATION_INDEX_GLOBAL_DEPTH 15

typedef struct Predicate_Key
{
	int range_start;
	int range_end;
	int column;
	int op;
	uint64_t value;
} Predicate_Key;

typedef struct Validation_Pr_Node
{
	struct my_Validation* validation;
	struct Validation_Pr_Node* next;
	int query_number;

} Validation_Pr_Node;

typedef struct Validation_Predicate
{
	Predicate_Key key;
	int conflict_size;
	char* conflict;
	Validation_Pr_Node* list;
	Validation_Pr_Node* last;
} Validation_Predicate;


typedef struct predicateNode
{
	Validation_Predicate* predicate;
	int query_number;
	struct predicateNode* next;
} predicateNode;

typedef struct my_Validation
{
	/// The validation id. Monotonic increasing
	 uint64_t validationId;
	 /// The transaction range
	 uint64_t from,to;
	 /// The query count
	 uint32_t queryCount;
	 /// The queries
	 my_Query* queries;

	 predicateNode* predicate_list_start;
	 predicateNode* predicate_list_last;

	 int conflict;
} my_Validation;



//---------------------------------------------------------------------------

typedef struct ValidationNode
{
		my_Validation* validation;
		struct ValidationNode* next;

} ValidationNode;



typedef struct Validation_Bucket
{
	int local_depth;
	int keys_counter;
	int del_start;
	int ptr_counter;
	Validation_Predicate key_array[VALIDATION_BUCKET_SIZE];
} Validation_Bucket;

typedef struct ValidationIndex
{
	int global_depth;
	Validation_Bucket** bucket_array;
} ValidationIndex;



ValidationIndex* createValidationIndex();

void initialiseBucket(Validation_Bucket* bucket, int global_depth);

int insertValidationIndexRecord(ValidationIndex* index, Predicate_Key key, my_Validation* val, char* conflict, int query_number, int old_conflict_size);

int predicateHashFun(Predicate_Key key, int global_depth);

int compareKeys(Predicate_Key key1, Predicate_Key key2);

int copyPredicateBucket(Validation_Bucket* old_bucket, Validation_Bucket* new_bucket);

int double_predicate_index(Validation_Bucket*** bucket_array, int global_depth);

int split_double_predicate(ValidationIndex* index,int hash_pos, Validation_Bucket* new_bucket,Validation_Bucket* temp_bucket, Predicate_Key key, my_Validation* new_validation, int query_number );

int split_predicate(ValidationIndex* index,int hash_pos, Validation_Bucket* new_bucket,Validation_Bucket* temp_bucket, Predicate_Key key, my_Validation* new_validation,int step, int query_number );

int freeValidationBucket(Validation_Bucket* bucket);

int createValidationPointer(my_Validation* val, Validation_Predicate* predicate,int query_number);

int deleteValidationPointer(my_Validation* val, Validation_Predicate* predicate);
