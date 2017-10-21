#include <stdio.h>
#include "trans_hash.h"
#include <math.h>
#include <stdlib.h>
#include "hashing.h"


extern int level2;

TransactionIndex* createTransactionIndex(){//Creates the transaction index 

	int i=0;
	int index_size=pow(2,INDEX_GLOBAL_DEPTH);//The initial size of the hash table

	TransactionIndex* index=malloc(sizeof(TransactionIndex));//Creation of an index struct

	index->global_depth=INDEX_GLOBAL_DEPTH;//Initialisation of the global depth 

	index->hash_array=malloc(index_size*sizeof(TransactionIndexBucket*));//Creation of the hash table

	for(i=0; i < index_size; i++){

		index->hash_array[i]=IndexCreateBucket(INDEX_GLOBAL_DEPTH);//Make each pointer of the hash table point to a new empty bucket
	}
	return index;//Return a pointer to the index
}


TransactionIndexBucket* IndexCreateBucket(int local_depth){//Creates a new empty bucket

	int i=0;
	TransactionIndexBucket* bucket=malloc(sizeof(TransactionIndexBucket));//Creation of a new bucket	

	bucket->local_depth=local_depth;//Initialisation of the bucket's contents
	bucket->trans_counter=0;
	bucket->del_start=0;
	bucket->ptr_counter=0;

	for(i=0; i < INDEX_BUCKET_SIZE; i++){//For every transaction of the bucket

		bucket->trans_array[i].trans_id=-1;//We initialise the transaction's contents
		bucket->trans_array[i].offset=-1;
	}
	return bucket;
}



int IndexCopyBucket(TransactionIndexBucket* new_bucket,TransactionIndexBucket* old_bucket){//Copies the contents of the old bucket to the new bucket

	int i=0;
	new_bucket->local_depth=old_bucket->local_depth;
	new_bucket->trans_counter=old_bucket->trans_counter;
	new_bucket->del_start=old_bucket->del_start;
	new_bucket->ptr_counter=old_bucket->ptr_counter;

	for(i=0; i < new_bucket->trans_counter; i++){//for every transaction of the bucket
		new_bucket->trans_array[i].trans_id=old_bucket->trans_array[i].trans_id;//Copy the transaction id
		new_bucket->trans_array[i].offset=old_bucket->trans_array[i].offset;//Copy the offset 
	}
	return 0;
}



int insertTransactionIndexRecord(TransactionIndex* index, int key, int offset){//Inserts a key(transaction id) to the index

	TransactionIndexBucket* temp=NULL;
	TransactionIndexBucket* new_bucket=NULL;
	int old_local=0;
	int i=0,k=0;
	int step=0;
	TransactionIndexBucket* temp_ptr=NULL;
	int first_ptr=0;

	int hash_pos=Hash_Fun(key,index->global_depth);//We take the index position where the given key must be inserted 

	int trans_counter=index->hash_array[hash_pos]->trans_counter;//the number of transaction ids the appropriate bucket has

	for(i=0; i < trans_counter; i++){

		if(index->hash_array[hash_pos]->trans_array[i].trans_id == key){//If the key for insertion is already in the bucket
			return 0;//return without doing anything
		}
	}

	if(trans_counter < INDEX_BUCKET_SIZE){	//If there is space in the bucket

		//fprintf(stderr,"EXW XWRO!!\n");
		index->hash_array[hash_pos]->trans_array[trans_counter].trans_id=key;	//Insert the given transaction id
		index->hash_array[hash_pos]->trans_array[trans_counter].offset=offset;	//and the offset
		index->hash_array[hash_pos]->trans_counter++;	//Increase the transactions counter
		return 0;
	}

	//If there is no space in the bucket
	//fprintf(stderr, "DEN EXW XWRO!!\n");
	temp=IndexCreateBucket(0);//We create a temporary bucket

	IndexCopyBucket(temp,index->hash_array[hash_pos]);//We copy to temp bucket the contents of the old full bucket
	

	old_local=index->hash_array[hash_pos]->local_depth;//the local depth of the old full bucket

	new_bucket=IndexCreateBucket(old_local+1);//We create a new bucket,with the right local depth

	index->hash_array[hash_pos]->local_depth++;//We increase the local depth of the old full bucket


	//If LOCAL DEPTH==GLOBAL DEPTH
	//DOUBLE INDEX CASE
	if(old_local == index->global_depth){

	//	fprintf(stderr, "DOUBLEEE!!!\n");
		index->global_depth++;//Increase the global depth of the index
		
		//We double the size of the index array
		//index->hash_array = realloc(index->hash_array, pow(2,index->global_depth)*sizeof(TransactionIndexBucket*));
		
		doubleTransactionIndex(index->global_depth, &(index->hash_array));


		//We arrange the new pointers of the index
		//for every pointer of the index from the middle till the end of it
		for(i=pow(2,(index->global_depth)-1),k=0; i < pow(2,index->global_depth); i++,k++){

			if(k==hash_pos){//if the pointer points to the hash position the hash function returned
				index->hash_array[i]=new_bucket;//make it point to the new bucket
			}
			else{//else make each other pointer point to the correct bucket
				index->hash_array[i]=index->hash_array[k];
			}
		}
		//We share the contents of the old full bucket,between the new bucket and the old one
		if(Split(index,hash_pos,key,offset,temp)!=0){
			fprintf(stderr, "Error in Split function of double case!!\n");
			return 1;
		}
		free(temp);
		//fprintf(stderr, "telos double\n" );
		return 0;
	}
	//If LOCAL DEPTH < GLOBAL DEPTH
	//SPLIT CASE
	
	//fprintf(stderr, "SPLITTT\n");
	step=pow(2,old_local);//this is the distance in the index between two pointers that point to the same bucket

	first_ptr=hash_pos%step;//we find the first pointer of the index that points to the bucket where the given key must be inserted

	temp_ptr=index->hash_array[hash_pos];//this pointer points to the bucket where an insertion must be made

	for(i=first_ptr; i < pow(2,index->global_depth); i+=(2*step)){//for every pointer that points to the bucket where we must insert the key

		index->hash_array[i]=temp_ptr;//we alternatively place the pointers of the full bucket
		index->hash_array[i+step]=new_bucket;//one to the old and one to the new bucket
	}
	//We share the contents of the old full bucket,between the new bucket and the old one
	if(Split(index,hash_pos,key,offset,temp)!=0){
		fprintf(stderr, "Error in Split function of split case!!\n");
		return 1;
	}
	free(temp);
	return 0;
}

int doubleTransactionIndex(int global_depth,TransactionIndexBucket*** trans_index){

	*trans_index = realloc((*trans_index), pow(2,global_depth)*sizeof(TransactionIndexBucket*)); //double the index's size
	return 0;
}


int Split(TransactionIndex* index,int hash_pos,int key,int offset,TransactionIndexBucket* temp){
//Splits the contents of a full bucket between a new bucket and the old one

	int trans_counter=index->hash_array[hash_pos]->trans_counter;
	int i=0,j=0;
	int trans_id=0,trans_offset=0;

	//We empty the old full bucket
	for(i=0; i < trans_counter; i++){//for very transaction of the bucket
		index->hash_array[hash_pos]->trans_array[i].trans_id=-1;//we empty each position of the transaction array
		index->hash_array[hash_pos]->trans_array[i].offset=-1;
	}
	index->hash_array[hash_pos]->trans_counter=0;//we make the transactions counter zero

	//We insert each transaction of the temporary bucket
	for(j=0; j < INDEX_BUCKET_SIZE; j++){//for every transaction of the temp bucket

		trans_id=temp->trans_array[j].trans_id;
		trans_offset=temp->trans_array[j].offset;
		insertTransactionIndexRecord(index,trans_id,trans_offset);//we insert the transaction
	}
	//We insert the transaction we wanted to insert in the beginning
	insertTransactionIndexRecord(index,key,offset);
	return 0;
}


int getTransactionIndexRecord(TransactionIndex* index,int key){//Returns the offset of a given key

	int hash_pos=Hash_Fun(key,index->global_depth);//We take the index position where the given key has been inserted 

	int trans_counter=index->hash_array[hash_pos]->trans_counter;

	int i=0;
	for(i=0; i < trans_counter; i++){//for every transaction of the bucket
		if(index->hash_array[hash_pos]->trans_array[i].trans_id == key){//if we find the given key in the bucket
			
			return index->hash_array[hash_pos]->trans_array[i].offset;//we return the offset
		}
	}
	//fprintf(stderr,"The given transaction id has not been found!!\n");
	return -1;
}

Records_Node* getTransactionIndexRecords(TransactionIndex* index, int range_start, int range_end,Journal* jour_ptr,int* start_position){
	int start_pos, end_pos, j, i, journal_pos;
	Records_Node* return_list;
	Records_Node* temp;

	return_list = malloc(sizeof(Records_Node));
	return_list->next = NULL;
	temp = return_list;

	int last_tran = jour_ptr->records_counter-1;

	if(last_tran < 0){
		return NULL;
	}

	if(range_start > jour_ptr->transactions_array[last_tran]->transaction_id || range_end < jour_ptr->transactions_array[0]->transaction_id){
		return NULL;
	}

	for(i = range_start; i <= range_end; i++ ){
		if((journal_pos=getTransactionIndexRecord(index,i)) != -1){
			(*start_position) = journal_pos;
			temp->journal_record = jour_ptr->transactions_array[journal_pos];
			journal_pos++;
			break;
		}
	}

	if(journal_pos == -1){
		return NULL;
	}

	while(journal_pos < jour_ptr->records_counter){

		if(jour_ptr->transactions_array[journal_pos]->transaction_id <= range_end){
			temp->next = malloc(sizeof(Records_Node));
			temp->next->next = NULL;
			temp->next->journal_record = jour_ptr->transactions_array[journal_pos];
			temp = temp->next;
			journal_pos++;
		}

		else{
			break;
		}
	}

	return return_list;
}


int destroyTransactionIndex(TransactionIndex* index){

	int i=0;
	for(i=0; i < pow(2,index->global_depth); i++){//For each position of the index

		if(index->hash_array[i]->del_start == 0){//if the deletion process hasn't started for this bucket

			index->hash_array[i]->del_start = 1;//start the process by marking this flag of the bucket

			//we initialise the counter of the pointers that point to this bucket
			index->hash_array[i]->ptr_counter = pow(2,(index->global_depth)-(index->hash_array[i]->local_depth));
		}
		else if(index->hash_array[i]->del_start == 1){//If the deletion process has already started for this bucket

			index->hash_array[i]->ptr_counter--;//Decrease the number of pointers to this bucket
		}
		if(index->hash_array[i]->ptr_counter == 1){//If there is only one pointer left pointing to this bucket

			free(index->hash_array[i]);//Free the bucket
		}
	}
	free(index->hash_array);
	free(index);
	return 0;
}
