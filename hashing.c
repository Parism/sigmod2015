#include <stdio.h>
#include "hashing.h"
#include <math.h>
#include <stdlib.h>


Hash* createHash(){

	int table_size=pow(2,GLOBAL_DEPTH);	//The initial size of the hash table

	Hash* hash=malloc(sizeof(Hash));//Allocation of a Hash struct
	hash->global_depth=GLOBAL_DEPTH;//Initialise the global depth

	hash->hash_array=malloc(table_size*sizeof(Bucket*));//Creating the hash table

	int i=0;
	
	for(i=0; i < table_size; i++){		//For each position of the hash table

		hash->hash_array[i]=createBucket(GLOBAL_DEPTH,BUCKET_SIZE,C);//Create an empty bucket
	}
	return hash;
}


Bucket* createBucket(int local_dep,int bucket_size,int transactions_capacity){
	Bucket* bucket=NULL;
	int i=0;
	int j=0;
	Transaction init;
	init.trans_id = -1;
	init.case_record[0]=-1;
	init.case_record[1]=-1;
	init.record[0]=-1;
	init.record[1]=-1;

	bucket=malloc(sizeof(Bucket));	//Create a bucket
	bucket->local_depth=local_dep;	//Initialisation of Bucket struct's variables
	bucket->keys_counter=0;
	bucket->del_start=0;
	bucket->ptr_counter=0;

	for(i=0; i < bucket_size; i++){	//For every key of the bucket
		bucket->key_array[i].key=0;
		bucket->key_array[i].trans_counter=0;
		bucket->key_array[i].capacity=transactions_capacity;
		bucket->key_array[i].range_array=malloc(transactions_capacity*sizeof(Transaction));
	

		for(j=0; j < transactions_capacity; j++){		//For every transaction of the key

			copyTransaction(&bucket->key_array[i].range_array[j], init);
		}
	}
	return bucket;
}


void freeBucket(Bucket* bucket,int bucket_size){
	int i=0;
	for(i=0; i < bucket_size; i++){		//For every key in the bucket
		free(bucket->key_array[i].range_array);//Free the array of Transactions
	}
	free(bucket);//Free the bucket
}



int copyBucket(Bucket* new_bucket,Bucket* old_bucket){

	int i=0;
	int j=0;
	new_bucket->keys_counter=old_bucket->keys_counter;
	new_bucket->local_depth=old_bucket->local_depth;

	for(i=0; i < old_bucket->keys_counter;i++){	//For every key of the old bucket

		new_bucket->key_array[i].key=old_bucket->key_array[i].key;//Copy the contents of each key
		new_bucket->key_array[i].trans_counter=old_bucket->key_array[i].trans_counter;

		if(old_bucket->key_array[i].trans_counter > new_bucket->key_array[i].capacity){//If the transactions of the key of the old bucket 
											//do not fit in the range array of the key of the new bucket
			new_bucket->key_array[i].range_array=realloc(new_bucket->key_array[i].range_array, (old_bucket->key_array[i].trans_counter)*sizeof(Transaction));
			new_bucket->key_array[i].capacity=old_bucket->key_array[i].trans_counter;
		}
		

		for(j=0; j < new_bucket->key_array[i].trans_counter; j++){//For every transaction of the key

			new_bucket->key_array[i].range_array[j].trans_id=old_bucket->key_array[i].range_array[j].trans_id;
			new_bucket->key_array[i].range_array[j].record[0]=old_bucket->key_array[i].range_array[j].record[0];
			new_bucket->key_array[i].range_array[j].record[1]=old_bucket->key_array[i].range_array[j].record[1];
			new_bucket->key_array[i].range_array[j].case_record[0]=old_bucket->key_array[i].range_array[j].case_record[0];
			new_bucket->key_array[i].range_array[j].case_record[1]=old_bucket->key_array[i].range_array[j].case_record[1];
		}
	}
	return 0;
}





int Hash_Fun(uint64_t hash_key,int global_depth){	//The hash function

	return hash_key%(int)(pow(2,global_depth));	
}

int copyTransaction(Transaction* range_array,Transaction tr){

	range_array->trans_id=tr.trans_id;
	range_array->record[0]=tr.record[0];
	range_array->record[1]=tr.record[1];
	range_array->case_record[0]=tr.case_record[0];
	range_array->case_record[1]=tr.case_record[1];
	return 0;
}




int insertHashRecord(Hash* hash,uint64_t key,Transaction trans){
	int i=0;
	int trans_counter=0;
	int capacity=0;
	int keys_counter=0;
	Bucket* temp=NULL;
	Bucket* new_bucket=NULL;
	int old_local=0;
	int step=0;
	int j=0;
	int k=0;
	int l=0;
	int p = 0;
	Transaction init;
	init.trans_id = -1;
	init.case_record[0]=-1;
	init.case_record[1]=-1;
	init.record[0]=-1;
	init.record[1]=-1;

	int hash_pos=Hash_Fun(key,hash->global_depth);//We use the hash function with the given key


	if(hash->hash_array[hash_pos]->key_array[0].range_array == NULL){//If there is no transaction array in the bucket

		

		for(j=0; j < BUCKET_SIZE;j++){//For every key position of the key array

			hash->hash_array[hash_pos]->key_array[j].range_array=malloc(C*sizeof(Transaction));
			hash->hash_array[hash_pos]->key_array[j].capacity=C;
			hash->hash_array[hash_pos]->key_array[j].trans_counter=0;

			for(l=0; l < C; l++){		//For every transaction of the key

				copyTransaction(&hash->hash_array[hash_pos]->key_array[j].range_array[l], init);
			}
		}
	}

	for(i=0; i < hash->hash_array[hash_pos]->keys_counter; i++){	//For every key of the bucket

		trans_counter=hash->hash_array[hash_pos]->key_array[i].trans_counter;
		capacity=hash->hash_array[hash_pos]->key_array[i].capacity;

		if(hash->hash_array[hash_pos]->key_array[i].key == key){//If we find the key we want to insert


			for(p=0; p < hash->hash_array[hash_pos]->key_array[i].trans_counter; p++){//for every transaction of the key

				if(hash->hash_array[hash_pos]->key_array[i].range_array[p].trans_id == trans.trans_id){//If the transaction we insert is in the array

					hash->hash_array[hash_pos]->key_array[i].range_array[p].case_record[0]=hash->hash_array[hash_pos]->key_array[i].range_array[p].record[0];
					hash->hash_array[hash_pos]->key_array[i].range_array[p].case_record[1]=hash->hash_array[hash_pos]->key_array[i].range_array[p].record[1];
					//case_record points to the record that was inserted before
					hash->hash_array[hash_pos]->key_array[i].range_array[p].record[0]=trans.record[0];
					hash->hash_array[hash_pos]->key_array[i].range_array[p].record[1]=trans.record[1];
					//record points to the record we insert because of deletion
					return 0;
				}
			}
			if((trans_counter+1)>capacity){	//If there is not enough space to store a new transaction
				hash->hash_array[hash_pos]->key_array[i].range_array=realloc(hash->hash_array[hash_pos]->key_array[i].range_array, (capacity+1)*sizeof(Transaction));
				hash->hash_array[hash_pos]->key_array[i].capacity++;	//Update the capacity of the key array
			}
			//Copy the new Transaction
			copyTransaction(&hash->hash_array[hash_pos]->key_array[i].range_array[trans_counter], trans);

			hash->hash_array[hash_pos]->key_array[i].trans_counter++;//Update the transactions counter
			return 0;
		}
	}

	//If the key is not in the bucket where it should be

	keys_counter=hash->hash_array[hash_pos]->keys_counter;//the number of keys of the bucket
	

	if(keys_counter<BUCKET_SIZE){	//If there is space in the bucket

		hash->hash_array[hash_pos]->key_array[keys_counter].key=key;	//Write to bucket the new key

										//Copy the new Transaction
		copyTransaction(&hash->hash_array[hash_pos]->key_array[keys_counter].range_array[0], trans);

		hash->hash_array[hash_pos]->key_array[keys_counter].trans_counter++;//Update the transactions counter
		hash->hash_array[hash_pos]->keys_counter++;//Update the keys counter
		return 0;
	}

	//If there is no space in the bucket


	temp=createBucket(0,BUCKET_SIZE,C);//We create a temp bucket.
					//The size is plus 1 because we will also store the new key for insertion 

	new_bucket=createBucket(hash->hash_array[hash_pos]->local_depth+1,BUCKET_SIZE,C);//We create a new bucket with 
										//the local depth of the old bucket increased by one

	copyBucket(temp,hash->hash_array[hash_pos]);//Copy to the temp bucket the contents of the old full bucket



	old_local=hash->hash_array[hash_pos]->local_depth;
	hash->hash_array[hash_pos]->local_depth++;//Increase the local depth of the full bucket


	////If GLOBAL DEPTH == LOCAL DEPTH
	//DOUBLE INDEX CASE

	if(hash->global_depth == old_local){

		hash->global_depth++; //Increase the global depth

		step=stepCalculate(hash->global_depth, old_local);

		doubleIndex(hash->global_depth, &hash->hash_array);
		
		
		
		split_double(hash, new_bucket, hash_pos, temp, key, trans );
		//split(hash,hash->global_depth, hash->hash_array, hash_pos, step, new_bucket, temp, key, trans);
		freeBucket(temp,BUCKET_SIZE);
		return 0;
	}

	////If GLOBAL DEPTH > LOCAL DEPTH
	//SPLIT CASE
	step=stepCalculate(hash->global_depth, old_local);
	//fprintf(stderr, "SKETO SPLIT\n" );
	split(hash,hash->global_depth, hash->hash_array, hash_pos, step, new_bucket, temp, key, trans);
	//fprintf(stderr, "AFTER SKETO SPLIT\n" );
	freeBucket(temp,BUCKET_SIZE);
	return 0;
}


int stepCalculate(int global_depth,int old_local){	//Calculates the step needed to arrange the pointers when you split
	int step= pow(2,old_local);		//(pow(2,(global_depth)))/(pow(2,(global_depth-old_local)));//step = (2^global)/(2^(global-local))
	return step;
}


int doubleIndex(int global_depth,Bucket*** hash_array){

	*hash_array = realloc((*hash_array), pow(2,global_depth)*sizeof(Bucket*)); //double the index's size

	return 0;
}


int split(Hash* hash,int global_depth, Bucket** hash_array, int hash_pos, int step, Bucket* new_bucket, Bucket* temp, uint64_t key, Transaction tr){

	int j=0;
	int l=0;
	int i=0;
	Bucket* temp_ptr=NULL;
	uint64_t key_to_insert=0;
	int size = pow(2,global_depth);


	temp_ptr=hash_array[hash_pos];//temp_ptr points to full bucket
	j = hash_pos % step;
				
	for(l=j; l < size; l+=(2*step)){//from the first pointer to the full bucket till the end of the index
		hash_array[l] = temp_ptr;
		hash_array[l+step] = new_bucket;//we alternatively place the pointers of the full bucket 
							//one to the old and one to the new bucket
	}


	for(i=0; i < BUCKET_SIZE;i++){//for every key of the full bucket

		free(temp_ptr->key_array[i].range_array);//Free the array of Transactions
		temp_ptr->key_array[i].range_array=NULL;
		temp_ptr->key_array[i].key=0;
		temp_ptr->key_array[i].trans_counter=0;
		temp_ptr->key_array[i].capacity=0;
	}
	temp_ptr->keys_counter=0;//We empty the old full bucket

	for(i=0; i < BUCKET_SIZE; i++){//For every key in the temp bucket

		key_to_insert=temp->key_array[i].key;

		for(j=0; j < temp->key_array[i].trans_counter; j++){//Insert every key of temp bucket
			insertHashRecord(hash, key_to_insert, temp->key_array[i].range_array[j]);
			//we recursively call the insert function in order to redistribute the old keys
			//between the old and the new one
		}
	}
	key_to_insert=key;
	insertHashRecord(hash, key_to_insert, tr);
	return 0;
}


int split_double(Hash* hash, Bucket* new_bucket, int hash_pos, Bucket* temp, uint64_t key, Transaction tr){

	int i, j;
	int old_dep = pow(2,(hash->global_depth)-1);
	uint64_t key_to_insert;
	Bucket* temp_ptr = hash->hash_array[hash_pos];

	for(i = 0, j=old_dep; i<old_dep; i++,j++ ){

		if(i == hash_pos){
			hash->hash_array[j] = new_bucket;
		}
		else{
			hash->hash_array[j] = hash->hash_array[i];
		}
	}

	for(i=0; i < BUCKET_SIZE;i++){//for every key of the full bucket

			free(temp_ptr->key_array[i].range_array);//Free the array of Transactions
			temp_ptr->key_array[i].range_array=NULL;
			temp_ptr->key_array[i].key=0;
			temp_ptr->key_array[i].trans_counter=0;
			temp_ptr->key_array[i].capacity=0;
	}
	temp_ptr->keys_counter=0;//We empty the old full bucket

	for(i = 0; i<BUCKET_SIZE; i++){
		key_to_insert=temp->key_array[i].key;

		for(j=0; j < temp->key_array[i].trans_counter; j++){//Insert every key of temp bucket
			insertHashRecord(hash, key_to_insert, temp->key_array[i].range_array[j]);
			//we recursively call the insert function in order to redistribute the old keys
			//between the old and the new one
		}
	}
	key_to_insert=key;
	insertHashRecord(hash, key_to_insert, tr);
	return 0;
}


int destroyHash(Hash* hash){

	int i=0;
	for(i=0; i < pow(2,hash->global_depth); i++){	//For each position of the index

		if(hash->hash_array[i]->del_start == 0){//If the deletion process hasn't started for this bucket

			hash->hash_array[i]->del_start = 1;
			hash->hash_array[i]->ptr_counter = pow(2, (hash->global_depth - hash->hash_array[i]->local_depth)); //(hash->global_depth - hash->hash_array[i]->local_depth) + 1;
			//initialise the number of pointers to this bucket
			//(hash->hash_array[i]->ptr_counter)--;
		}
		else if(hash->hash_array[i]->del_start == 1){//If the deletion process has already started for this bucket

			hash->hash_array[i]->ptr_counter--;//Decrease the number of pointers to this bucket
		}
		if(hash->hash_array[i]->ptr_counter == 1){

			freeBucket(hash->hash_array[i],BUCKET_SIZE);//Free the bucket
		}
	}
	free(hash->hash_array);//Free the hash table
	return 0;
}


Transaction* getHashRecord(Hash* hash, int key, int trans_id){

	int i=0;
	int j=0;
	Transaction* trans_ptr=NULL;
	Transaction* temp_ptr=NULL;
	int hash_pos=Hash_Fun(key,hash->global_depth);//We use the hash function with the given key

	for(i=0; i < hash->hash_array[hash_pos]->keys_counter; i++){//For every key of the bucket
		if(hash->hash_array[hash_pos]->key_array[i].key == key){//If the key is the one we are looking for
			temp_ptr=hash->hash_array[hash_pos]->key_array[i].range_array;
			for(j=0; j < hash->hash_array[hash_pos]->key_array[i].trans_counter; j++){//For every transaction
				if(hash->hash_array[hash_pos]->key_array[i].range_array[j].trans_id == trans_id){//If the transaction is the one we are looking for
					trans_ptr=temp_ptr;	
					return trans_ptr;
				}
				temp_ptr++;
			}
		}
	}
}

Hash_Record_Node* getHashRecords(Hash* hash, uint64_t key, int range_start, int range_end ){

	int hash_pos = Hash_Fun(key, hash->global_depth);
	int i, j, range_array_pos, k;
	Hash_Record_Node* return_list;
	Hash_Record_Node* temp;

	return_list = malloc(sizeof(Hash_Record_Node));
	return_list->next = NULL;
	temp = return_list;


	for(i = 0; i < hash->hash_array[hash_pos]->keys_counter; i++){

		if(key == hash->hash_array[hash_pos]->key_array[i].key){

			int trans_counter = hash->hash_array[hash_pos]->key_array[i].trans_counter;
			if(range_start > hash->hash_array[hash_pos]->key_array[i].range_array[trans_counter-1].trans_id || range_end < hash->hash_array[hash_pos]->key_array[i].range_array[0].trans_id){

				return NULL;
			}

			range_array_pos = -1;

			for(j = 0; j < trans_counter; j++ ){

				for(k = range_start; k <= range_end; k++){

					if(hash->hash_array[hash_pos]->key_array[i].range_array[j].trans_id == k){
						range_array_pos = j;
						temp->transaction = &(hash->hash_array[hash_pos]->key_array[i].range_array[range_array_pos]);
						range_array_pos++;
						break;
					}
				}
				if(range_array_pos != -1){
					break;
				}
			}

			if(range_array_pos == -1){
				return NULL;
			}			

			while(range_array_pos < trans_counter){
				if(hash->hash_array[hash_pos]->key_array[i].range_array[range_array_pos].trans_id <= range_end){
					temp->next = malloc(sizeof(Hash_Record_Node));
					temp->next->next = NULL;
					temp->next->transaction = &(hash->hash_array[hash_pos]->key_array[i].range_array[range_array_pos]);
					range_array_pos++;
					temp = temp->next;
				}

				else{
					break;
				}
			}

			return return_list;
		}
	}

	return NULL;

}