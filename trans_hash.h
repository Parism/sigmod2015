#include <stdint.h>
#include "journal.h"

#define INDEX_GLOBAL_DEPTH 1 		//The initial global depth of our hash table
#define INDEX_BUCKET_SIZE 1		//The size of the bucket



typedef struct TransactionIndexKey{
	int trans_id;	//the id of the transaction
	int offset;	//the offset of the journal where this transaction id can be found
}TransactionIndexKey;


typedef struct TransactionIndexBucket{
	int local_depth;	//the local depth
	int trans_counter;	//counts the transaction ids that the bucket has
	int del_start;		//a flag variable that shows if the deletion process has started for this bucket,it is used in the destroyHash function
	int ptr_counter;	//counts the number of pointers that point to a bucket,it is used in the destroyHash function
	TransactionIndexKey trans_array[INDEX_BUCKET_SIZE];//an array of transactions
}TransactionIndexBucket;


typedef struct TransactionIndex{
	int global_depth;	//the global depth
	TransactionIndexBucket** hash_array;	//an array of pointers to buckets
}TransactionIndex;


TransactionIndex* createTransactionIndex();


TransactionIndexBucket* IndexCreateBucket(int);

int IndexCopyBucket(TransactionIndexBucket*,TransactionIndexBucket*);

int insertTransactionIndexRecord(TransactionIndex* , int, int);

int Split(TransactionIndex*,int,int,int,TransactionIndexBucket*);

int getTransactionIndexRecord(TransactionIndex*,int);

int destroyTransactionIndex(TransactionIndex*);

Records_Node* getTransactionIndexRecords(TransactionIndex* index, int range_start, int range_end,Journal* jour_ptr, int* start_position);

