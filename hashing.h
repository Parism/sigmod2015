
#include <stdint.h>


#define GLOBAL_DEPTH 1 		//The initial global depth of our hash table
#define C 4			//The initial size of the range_array table
#define BUCKET_SIZE 1		//The size of the bucket


typedef struct Transaction{
	int trans_id; 		//the id of the transaction
	//int dirty;		//it shows if our record was "deleted"
	int case_record[2];	//points to a deleted record in case a deletion and an insertion of the same key happen at the same transaction
	int record[2];		//points to a record of the journals,the first position is the position in the journal's index(which is made of transaction ids)
				//and the second one is the position of the record in the array that the index position points to
}Transaction;


typedef struct Key{
	uint64_t key;		//the key
	int trans_counter;	//counts the transactions that the key has
	int capacity;		//counts the number of positions the range_array has in total
	Transaction* range_array;	//an array of transactions
}Key;


typedef struct Bucket{
	int local_depth;	//the local depth
	int keys_counter;	//counts the keys that the bucket has
	int del_start;		//a flag variable that shows if the deletion process has started for this bucket,it is used in the destroyHash function
	int ptr_counter;	//counts the number of pointers that point to a bucket,it is used in the destroyHash function
	Key key_array[BUCKET_SIZE];		//an array of keys
}Bucket;


typedef struct Hash{
	int global_depth;	//the global depth
	Bucket** hash_array;	// the index,an array of pointers to buckets
}Hash;

typedef struct Hash_Record_Node
{
	Transaction* transaction;
	struct Hash_Record_Node* next;
} Hash_Record_Node;


Hash* createHash();

Bucket* createBucket(int, int, int);

int copyBucket(Bucket*, Bucket*);

void freeBucket(Bucket*, int);

int copyTransaction(Transaction*, Transaction);

int stepCalculate(int, int);

int doubleIndex(int, Bucket***);

int split(Hash*, int, Bucket**, int, int, Bucket*, Bucket*, uint64_t, Transaction);

int split_double(Hash*, Bucket*, int, Bucket*, uint64_t,Transaction);

int Hash_Fun(uint64_t, int);

int insertHashRecord(Hash*, uint64_t , Transaction);

int destroyHash(Hash*);

Transaction* getHashRecord(Hash*, int, int);

Hash_Record_Node* getHashRecords(Hash* , uint64_t , int , int  );