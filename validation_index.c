#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include "validation_index.h"
#include <string.h>
#include <stdint.h>


ValidationIndex* createValidationIndex(){

	int table_size = pow(2,VALIDATION_INDEX_GLOBAL_DEPTH);
	int i;

	ValidationIndex* Val_Index = malloc(sizeof(ValidationIndex));
	Val_Index->global_depth = VALIDATION_INDEX_GLOBAL_DEPTH;
	Val_Index->bucket_array = malloc(table_size*sizeof(Validation_Bucket*));
	for(i = 0; i < table_size; i++){
		Val_Index->bucket_array[i] = malloc(sizeof(Validation_Bucket));
		initialiseBucket(Val_Index->bucket_array[i], Val_Index->global_depth);
	}

	return Val_Index;
}

void initialiseBucket(Validation_Bucket* bucket, int depth){
	int i;
	

	bucket->local_depth = depth;
	bucket->keys_counter = 0;
	bucket->del_start = 0;
	bucket->ptr_counter = 0;

	for(i = 0; i < VALIDATION_BUCKET_SIZE; i++){
		bucket->key_array[i].conflict = NULL;
		bucket->key_array[i].conflict_size = 0;
		bucket->key_array[i].list = NULL;
		bucket->key_array[i].last = NULL;
		bucket->key_array[i].key.range_start = 0;
		bucket->key_array[i].key.range_end = 0;
		bucket->key_array[i].key.column = 0;
		bucket->key_array[i].key.op = 0;
		bucket->key_array[i].key.value = 0;
	}
}



int insertValidationIndexRecord(ValidationIndex* index, Predicate_Key key, my_Validation* val, char* conflict, int query_number, int old_conflict_size){

	int hash_pos = predicateHashFun(key,index->global_depth);
	//fprintf(stderr, "hash_pos = %d range start = %d end=%d col=%d op=%d value=%lu\n",hash_pos,key.range_start,key.range_end,key.column,key.op,key.value );
	int i;
	Validation_Pr_Node* temp;
	int old_local, step;

	for(i = 0; i < index->bucket_array[hash_pos]->keys_counter; i++){
		if(compareKeys(key,index->bucket_array[hash_pos]->key_array[i].key ) == 1){	//if key already exists in bucket
			index->bucket_array[hash_pos]->key_array[i].last->next = malloc(sizeof(Validation_Pr_Node));
			index->bucket_array[hash_pos]->key_array[i].last->next->validation = val; //update the validation list of the predicate
			index->bucket_array[hash_pos]->key_array[i].last->next->query_number = query_number;
			index->bucket_array[hash_pos]->key_array[i].last->next->next = NULL;
			index->bucket_array[hash_pos]->key_array[i].last = index->bucket_array[hash_pos]->key_array[i].last->next;
			createValidationPointer(val,&(index->bucket_array[hash_pos]->key_array[i]),query_number);
			return 0;
		}
	}
	int keys_counter = index->bucket_array[hash_pos]->keys_counter;
	if(keys_counter < VALIDATION_BUCKET_SIZE){	//else if there is enough space in the bucket ,insert the new key(predicate)
		index->bucket_array[hash_pos]->key_array[keys_counter].key = key;
		index->bucket_array[hash_pos]->key_array[keys_counter].conflict_size = old_conflict_size;
		//fprintf(stderr, "value = %lu\n", index->bucket_array[hash_pos]->key_array[keys_counter].key.value );
		if(conflict != NULL){
			int conflict_size = old_conflict_size;
			index->bucket_array[hash_pos]->key_array[keys_counter].conflict = malloc(conflict_size);
			memset(index->bucket_array[hash_pos]->key_array[keys_counter].conflict,0,conflict_size);
			for(i = 0; i < conflict_size; i++){
				index->bucket_array[hash_pos]->key_array[keys_counter].conflict[i] = conflict[i];
			}
		}
		else{
			index->bucket_array[hash_pos]->key_array[keys_counter].conflict = NULL;
			
		}
		temp = malloc(sizeof(Validation_Pr_Node));
		temp->validation = val;
		temp->next = NULL;
		temp->query_number = query_number;
		index->bucket_array[hash_pos]->key_array[keys_counter].list = temp;
		index->bucket_array[hash_pos]->key_array[keys_counter].last = temp;
		index->bucket_array[hash_pos]->keys_counter++;
		createValidationPointer(val,&(index->bucket_array[hash_pos]->key_array[keys_counter]),query_number);
		//fprintf(stderr, "value = %lu\n", val->predicate_list_start->predicate->key.value );
	

	}

	else{
		Validation_Bucket* temp_bucket;
		Validation_Bucket* new_bucket;
		temp_bucket = malloc(sizeof(Validation_Bucket));
		new_bucket = malloc(sizeof(Validation_Bucket));
		initialiseBucket(new_bucket,index->global_depth);
		copyPredicateBucket(index->bucket_array[hash_pos], temp_bucket);
		old_local = index->bucket_array[hash_pos]->local_depth;
		index->bucket_array[hash_pos]->local_depth++;
		new_bucket->local_depth = index->bucket_array[hash_pos]->local_depth;

		if(old_local == index->global_depth){	//double index case
			index->global_depth++;
			double_predicate_index(&index->bucket_array, index->global_depth);
			split_double_predicate(index, hash_pos,new_bucket,temp_bucket, key,val , query_number);
			freeValidationBucket(temp_bucket);
			return 0;
		}

		step = stepCalculate(index->global_depth, old_local);
		split_predicate(index, hash_pos,new_bucket,temp_bucket, key,val ,step,query_number);
		freeValidationBucket(temp_bucket);
		return 0;
	}
}

int predicateHashFun(Predicate_Key key, int global_depth){

	int size = pow(2,global_depth);
	uint64_t hash = key.column;
	hash *= 37;
	hash += key.op;
	hash *= 37;
	hash += key.value;
	hash *= 37;
	hash += key.range_start;
	hash *= 37;
	hash += key.range_end;
	return hash % size;
}

int compareKeys(Predicate_Key key1, Predicate_Key key2){

	if(key1.range_start == key2.range_start && key1.range_end == key2.range_end && key1.column == key2.column && key1.op == key2.op && key1.value == key2.value){
		return 1;
	}
	return 0;
}

int copyPredicateBucket(Validation_Bucket* old_bucket, Validation_Bucket* new_bucket){

	int i,j;
	Validation_Pr_Node* temp, *temp2;
	new_bucket->local_depth = old_bucket->local_depth;
	new_bucket->keys_counter = old_bucket->keys_counter;
	new_bucket->del_start = old_bucket->del_start;
	new_bucket->ptr_counter = old_bucket->ptr_counter;

	for(i = 0; i < old_bucket->keys_counter; i++){
		new_bucket->key_array[i].key = old_bucket->key_array[i].key;
		new_bucket->key_array[i].conflict_size = old_bucket->key_array[i].conflict_size;
		if(old_bucket->key_array[i].conflict != NULL){
			int conflict_size = old_bucket->key_array[i].conflict_size;
			new_bucket->key_array[i].conflict = malloc(conflict_size);
			for(j = 0; j < conflict_size; j++){
				new_bucket->key_array[i].conflict[j] = old_bucket->key_array[i].conflict[j];
			}
		}

		else{
			new_bucket->key_array[i].conflict = NULL;
		}
	
		temp2 = malloc(sizeof(Validation_Pr_Node));
		temp2->validation = old_bucket->key_array[i].list->validation;
		temp2->next = NULL;
		new_bucket->key_array[i].list = temp2;
		temp = old_bucket->key_array[i].list;

		if(temp->next == NULL){
			new_bucket->key_array[i].last = new_bucket->key_array[i].list;
		}
		
		while(temp->next != NULL){
	
			temp2->next = malloc(sizeof(Validation_Pr_Node));
			temp2->next->validation = temp->next->validation;
			temp2->next->next = NULL;
			if(temp->next == NULL){
				new_bucket->key_array[i].last = temp2->next;
			}
			temp2 = temp2->next;
			temp = temp->next;
		}

		
		
	}
	return 0;
}

int double_predicate_index(Validation_Bucket*** bucket_array, int global_depth){

	(*bucket_array) = realloc((*bucket_array),(pow(2,global_depth))*sizeof(Validation_Bucket*));
}

int split_double_predicate(ValidationIndex* index,int hash_pos, Validation_Bucket* new_bucket,Validation_Bucket* temp_bucket, Predicate_Key key, my_Validation* new_validation, int query_number ){
	int i,j;
	int old_dep = pow(2,(index->global_depth)-1);
	Validation_Bucket* temp = index->bucket_array[hash_pos];
	Predicate_Key key_to_insert;
	Validation_Pr_Node* temp_val, *temp_val2 ;

	for(i = 0 , j = old_dep; i < old_dep; i++,j++ ){
		if(i==hash_pos){
			index->bucket_array[j] = new_bucket;
		}
		else{
			index->bucket_array[j] = index->bucket_array[i];
		}
	}

	for(i = 0; i < temp->keys_counter; i++){
		temp_val = temp->key_array[i].list;
		free(temp->key_array[i].conflict);
		while(temp_val!=NULL){
			temp_val2 = temp_val->next;
			deleteValidationPointer(temp_val->validation,&(temp->key_array[i]));
			free(temp_val);
			temp_val = temp_val2;
		}
	}
	

	for(i = 0; i < temp->keys_counter; i++){
		key_to_insert = temp_bucket->key_array[i].key;
		temp_val = temp_bucket->key_array[i].list;
		while(temp_val != NULL){
			insertValidationIndexRecord(index, key_to_insert, temp_val->validation, temp_bucket->key_array[i].conflict, temp_val->query_number,temp_bucket->key_array[i].conflict_size);
			temp_val = temp_val->next;
		}
	}
	temp->keys_counter = 0;
	key_to_insert = key;
	insertValidationIndexRecord(index, key_to_insert, new_validation, NULL, query_number,0);
	return 0;
}


int split_predicate(ValidationIndex* index,int hash_pos, Validation_Bucket* new_bucket,Validation_Bucket* temp_bucket, Predicate_Key key, my_Validation* new_validation,int step, int query_number ){
	int i,j,k,l;
	Validation_Bucket* temp = NULL;
	Predicate_Key key_to_insert;
	int size = pow(2,index->global_depth);
	Validation_Pr_Node* temp_val, *temp_val2;

	temp = index->bucket_array[hash_pos];
	j = hash_pos % step;

	for(l = j; l < size; l+=(2*step)){
		index->bucket_array[l] = temp;
		index->bucket_array[l+step] = new_bucket;
	}

	for(i = 0; i < temp->keys_counter; i++){
		temp_val = temp->key_array[i].list;
		free(temp->key_array[i].conflict);
		while(temp_val!=NULL){
			temp_val2 = temp_val->next;
			deleteValidationPointer(temp_val->validation,&(temp->key_array[i]));
			free(temp_val);
			temp_val = temp_val2;
		}
	}
	

	for(i = 0; i < temp->keys_counter; i++){
		key_to_insert = temp_bucket->key_array[i].key;
		temp_val = temp_bucket->key_array[i].list;
		while(temp_val != NULL){
			insertValidationIndexRecord(index, key_to_insert, temp_val->validation, temp_bucket->key_array[i].conflict, temp_val->query_number,temp_bucket->key_array[i].conflict_size);
			temp_val = temp_val->next;
		}
	}
	temp->keys_counter = 0;
	key_to_insert = key;
	insertValidationIndexRecord(index, key_to_insert, new_validation, NULL,query_number,0);
	return 0;
}

int freeValidationBucket(Validation_Bucket* bucket){
	int i;
	Validation_Pr_Node* temp, *temp2;

	for(i = 0; i < bucket->keys_counter; i++){
		free(bucket->key_array[i].conflict);
		temp = bucket->key_array[i].list;
		while(temp != NULL){
			temp2 = temp->next;
			free(temp);
			temp = temp2;
		}
	}

	free(bucket);
	return 0;
}


int createValidationPointer(my_Validation* val, Validation_Predicate* predicate,int query_number){
	if(val->predicate_list_start == NULL){
		predicateNode* temp;
		temp = malloc(sizeof(predicateNode));
		temp->predicate = predicate;
		temp->next = NULL;
		temp->query_number = query_number;
		val->predicate_list_start = temp;
		val->predicate_list_last = temp;

		//fprintf(stderr, "value = %lu\n", val->predicate_list_start->predicate->key.value );
		return 0;
	}
	else{
		val->predicate_list_last->next = malloc(sizeof(predicateNode));
		val->predicate_list_last->next->predicate = predicate;
		val->predicate_list_last->next->next = NULL;
		val->predicate_list_last->next->query_number = query_number;
		val->predicate_list_last = val->predicate_list_last->next;
		return 0;
	}
	return -1;
}

int deleteValidationPointer(my_Validation* val, Validation_Predicate* predicate){
	predicateNode* temp, *temp2;
	temp = val->predicate_list_start;

	//fprintf(stderr, "column = %d\n", temp->predicate->key.column);

	if(temp->predicate == predicate){
		val->predicate_list_start = val->predicate_list_start->next;
		free(temp);
		return 0;
	}
	else{
		while(temp != NULL){
			if(temp->next->predicate == predicate){

				if(temp->next == val->predicate_list_last){
					val->predicate_list_last = temp;
				}
				temp2 = temp->next;
				temp->next = temp->next->next; 
				free(temp2);
				return 0;
			}
			temp = temp->next;
		}
	}
	return -1;
}