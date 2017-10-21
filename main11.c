/*
 * Inspired from SIGMOD Programming Contest 2015.
 * http://db.in.tum.de/sigmod15contest/task.html
 * Simple requests parsing and reporting.
**/
#include <stdio.h>
#include <stdlib.h>
//#include <stdint.h>
#include <unistd.h>

#include "hashing.h"
 #include <math.h>
 #include <string.h>
#include "validation_index.h"

 #include "trans_hash.h"
 #include <pthread.h>

 #define NUM_OF_THREADS 3	//the number of threads that will be created in level 3
 #define SIZE_OF_THREAD_ARRAY 2 	//
//---------------------------------------------------------------------------


int level2=0;
int level3 = 0;
int debug_val = 21;

ValidationNode* insert_val_node(ValidationQueries_t *, ValidationIndex**);


ValidationNode *validation_list = NULL;
ValidationNode *last = NULL;


static uint32_t* schema = NULL;
static uint32_t processDefineSchema(DefineSchema_t *s){
	int i;
	printf("DefineSchema %d |", s->relationCount);
	
	if ( schema == NULL) free(schema);
	schema = malloc(sizeof(uint32_t)*s->relationCount);
	
	for(i = 0; i < s->relationCount; i++) {
		printf(" %d ",s->columnCounts[i]);
		schema[i] = s->columnCounts[i];
	}
	printf("\n");
	return s->relationCount;
}

static void processTransaction(Transaction_t *t, Hash** hash, Journal** jour_ptr,TransactionIndex** trans_hash){
	int i, j, k, m,  journal_pos, l;
	k =0;
	int key, key_pos;
	int del_count=0;

	int key_found;
	int* line_to_del = NULL;
	int trans_count;
	const char* reader = t->operations;

	
	
	JournalRecord* jour_rec =NULL;	//The record to be inserted in the journal
	Transaction trans_array ;	//The transaction to be inserted in the hash
 
	jour_rec = malloc(sizeof(JournalRecord));
	
	int counter = 0;

	int** rel_array = NULL;

	if(t->deleteCount != 0){		//If we have deletes
		int kjhg=t->deleteCount;
		rel_array = malloc(kjhg*sizeof(int*));
		for(i = 0; i < kjhg; i++){
				rel_array[i] = malloc(2*sizeof(int));	//We create  2-d array
		}

		for(i = 0; i < t->deleteCount; i++){	//And initialise each position to -1
				for(k = 0; k < 2; k++){
						rel_array[i][k] = -1;
				}
		}
	}
	k = 0;
 
	int hash_pos;
	int line, j_pos, range_pos;
	//fprintf(stderr, "tr id = %lu\n", t->transactionId );


	for(i=0; i < t->deleteCount; i++) { 		//For every delete

		const TransactionOperationDelete_t* o = (TransactionOperationDelete_t*)reader;
	 // fprintf(stderr,"opdel rid %u #rows %u \n", o->relationId, o->rowCount);
		
		trans_array.trans_id = t->transactionId;
		jour_rec->transaction_id = t->transactionId;
		jour_rec->columns_array = NULL;

		for(j = 0; j < o->rowCount; j++){    //for every key to be deleted
			hash_pos = Hash_Fun(o->keys[j], hash[o->relationId]->global_depth);//we find its hash  position

			for(m = 0; m < hash[o->relationId]->hash_array[hash_pos]->keys_counter; m++){         //for every key in the bucket

				if(hash[o->relationId]->hash_array[hash_pos]->key_array[m].key == o->keys[j]){	//if it is one of those to be deleted

					

					range_pos = ((hash[o->relationId]->hash_array[hash_pos]->key_array[m].trans_counter)-1);   //position of last transaction
					j_pos = hash[o->relationId]->hash_array[hash_pos]->key_array[m].range_array[range_pos].record[0];
					line = hash[o->relationId]->hash_array[hash_pos]->key_array[m].range_array[range_pos].record[1];

					if(jour_ptr[o->relationId]->transactions_array[j_pos]->columns_array[line][schema[o->relationId]] != 1){

						counter++;

						jour_rec->columns_array = realloc(jour_rec->columns_array, counter*sizeof(uint64_t*));//we create space in the array of records,
						jour_rec->columns_array[counter-1] = malloc((schema[o->relationId]+1)*sizeof(uint64_t));//which will be inserted to the journal

						trans_array.record[0] = jour_ptr[o->relationId]->records_counter;
						trans_array.record[1] = counter-1;

						trans_array.case_record[0] = -1;
						trans_array.case_record[1] = -1;
						


						insertHashRecord(hash[o->relationId],o->keys[j] ,trans_array);//We insert the transaction in the hash

						for(k = 0; k < schema[o->relationId]; k++){//We initialise appropriately the columns_array
						
							jour_rec->columns_array[counter-1][k] = jour_ptr[o->relationId]->transactions_array[j_pos]->columns_array[line][k];

							

							if(k==schema[o->relationId]-1){//If we reach the final column of the record
								jour_rec->columns_array[counter-1][k+1] = 1;
								//we insert one to the last column which is the dirty bit,one shows that it's a deletion
							}
						}

							
					}
				}
			}
		}

		if(counter == 0){ 

			reader+=sizeof(TransactionOperationDelete_t)+(sizeof(uint64_t)*o->rowCount);
			continue;
		}
		 
		else{

			 rel_array[i][0] = (int)(o->relationId);
			 rel_array[i][1] = counter;

			
			insertJournalRecord(jour_ptr[o->relationId], jour_rec, schema[o->relationId], counter);//Insert to journal
			
			
			if(level2 == 1){
				//fprintf(stderr, "OUOUOUO\n" );
				insertTransactionIndexRecord(trans_hash[o->relationId],trans_array.trans_id,jour_ptr[o->relationId]->records_counter-1);

			}
				for(m = 0; m < counter; m++){
					
					free(jour_rec->columns_array[m]);
				}

				free(jour_rec->columns_array);

				reader+=sizeof(TransactionOperationDelete_t)+(sizeof(uint64_t)*o->rowCount);
				counter = 0;
		}
	}
	//printf(" \t| ");
	


	for(i=0; i < t->insertCount; i++) { //For every Insert
		const TransactionOperationInsert_t* o = (TransactionOperationInsert_t*)reader;
		//fprintf(stderr,"opins rid %u #rows %u | \n", o->relationId, o->rowCount);




	
		jour_rec->transaction_id = t->transactionId;

		jour_rec->columns_array = malloc(o->rowCount*sizeof(uint64_t*));    //allocate space for lines of the array

		for(m = 0; m < o->rowCount; m++){

			jour_rec->columns_array[m]=malloc((schema[o->relationId]+1)*sizeof(uint64_t)); //allocate space for columns of the array
		}

		

		j = 0;


	

		trans_array.trans_id = t->transactionId;
		
	 k = 0;
	int  a = 0;
	int ka;

	int ins_counter = 0;
	int rel_found = 0;
	int del_rows = 0;

		if(t->deleteCount == 0){
			rel_found = 0;
		}

		else {


				
			for(ka = 0; ka < t->deleteCount; ka++){
				if(o->relationId == rel_array[ka][0]){
											
											
					rel_found = 1;
					del_rows = rel_array[ka][1];
					break;
				}

				else if(ka == ((t->deleteCount)-1)){
					rel_found = 0;
				}
			}
		}
		for(m = 0; m < o->rowCount; m++){

		
				
				 l =0;
				 int found = 0;

				
				if(rel_found == 0 ){
					trans_array.record[0] = jour_ptr[o->relationId]->records_counter;
					trans_array.record[1] = m;
				}

				else{

					trans_array.record[0] = jour_ptr[o->relationId]->records_counter-1;
					trans_array.record[1] = m+del_rows;

				}


			trans_array.case_record[0]=-1;
			trans_array.case_record[1]=-1;

			

			insertHashRecord(hash[o->relationId], o->values[(m)*(schema[o->relationId])], trans_array);

			uint64_t keympla = o->values[(m)*(schema[o->relationId])];

						


			for(j = 0; j < schema[o->relationId]; j++){

				jour_rec->columns_array[m][j] = o->values[k];   //values to be inserted into journal
				
				if(jour_rec->columns_array[m][j] < 0){
					fprintf(stderr, "E \n" );
				}
				if(j==schema[o->relationId]-1){//If we are at the final column of the record
					jour_rec->columns_array[m][j+1]=0;
					//we insert zero to the last column which is the dirty bit,zero shows that it's an insertion
				}
				k++;



				

			}
			


		}


		
		insertJournalRecord(jour_ptr[o->relationId], jour_rec, schema[o->relationId], o->rowCount);//Insert to journal
		
		
		if(level2 == 1){
			insertTransactionIndexRecord(trans_hash[o->relationId],trans_array.trans_id,jour_ptr[o->relationId]->records_counter-1);

		}
 

		for(m = 0; m < o->rowCount; m++){
			
			free(jour_rec->columns_array[m]);
		}

		free(jour_rec->columns_array);

		

	
		reader+=sizeof(TransactionOperationInsert_t)+(sizeof(uint64_t)*o->rowCount*schema[o->relationId]);
	}

	free(jour_rec);
	if(t->deleteCount != 0){
			for(i = 0; i < t->deleteCount; i++){
				free(rel_array[i]);
			}
			free(rel_array);
	}
//  printf("\n");
	
}



int cmpfunc (const void* a, const void* b)
{

	Column_t col_a = *(Column_t*)a;
	Column_t col_b = *(Column_t*)b;

	return (col_a.column - col_b.column);

}




static void processValidationQueries(ValidationQueries_t *v, ValidationIndex** predicate_index ){

	
	ValidationNode* temp;
	
	
	if(validation_list == NULL){			//if list is empty
		
		validation_list = insert_val_node(v, predicate_index);

		last=validation_list;
	}

	else{

		
		last->next = insert_val_node(v, predicate_index);
		last = last->next;
	}
}





ValidationNode* insert_val_node(ValidationQueries_t *v, ValidationIndex** predicate_index){
	ValidationNode* validation_node;
	int i , j;

		validation_node = malloc(sizeof(ValidationNode));		//malloc the first node of the list
		validation_node->validation = malloc(sizeof(my_Validation));
		validation_node->next = NULL;
		validation_node->validation->validationId = v->validationId;		//copy contents
		validation_node->validation->from = v->from;
		validation_node->validation->to = v->to;
		validation_node->validation->queryCount  = v->queryCount;
		validation_node->validation->predicate_list_start = NULL;
		validation_node->validation->predicate_list_last = NULL;
		validation_node->validation->conflict = -1;
		validation_node->validation->queries = malloc(v->queryCount*sizeof(my_Query));		//allocate space for the queries

		const char* reader = v->queries;

		if(v->validationId == debug_val){
			fprintf(stderr, "queries= %d \n",v->queryCount );
		}

		for(i = 0; i < v->queryCount; i++){					//copy contents of each query
			const Query_t* o = (Query_t*)reader;

			validation_node->validation->queries[i].relationId = o->relationId;
			validation_node->validation->queries[i].columnCount = o->columnCount;
			validation_node->validation->queries[i].columns = malloc(o->columnCount*sizeof(Column_t));	//allocate space for each column_t

			if(o->columnCount == 0 && level2 == 1){ //if query is empty
				Predicate_Key key_to_insert;
				key_to_insert.range_start = v->from;
				key_to_insert.range_end = v->to;
				key_to_insert.column = 0;
				key_to_insert.op = 100;
				key_to_insert.value = 0;
				insertValidationIndexRecord(predicate_index[o->relationId], key_to_insert, validation_node->validation, NULL, i,0);
			}

			if(v->validationId == debug_val){
				fprintf(stderr, "columns = %d\n", o->columnCount );
			}

			for(j = 0; j < o->columnCount; j++){				//copy contents of each Column_t
				validation_node->validation->queries[i].columns[j].column = o->columns[j].column;
				validation_node->validation->queries[i].columns[j].op = o->columns[j].op;
				
				validation_node->validation->queries[i].columns[j].value = o->columns[j].value;

				if(level2 == 1){
					Predicate_Key key_to_insert;
					key_to_insert.range_start = v->from;
					key_to_insert.range_end = v->to;
					key_to_insert.column = o->columns[j].column;
					key_to_insert.op = o->columns[j].op;
					key_to_insert.value = o->columns[j].value;
					if(v->validationId == debug_val){
						fprintf(stderr, "from = %lu to=%lu col = %u op=%u value = %lu\n",v->from,v->to,o->columns[j].column,o->columns[j].op, key_to_insert.value );
					}
					
					insertValidationIndexRecord(predicate_index[o->relationId], key_to_insert, validation_node->validation, NULL, i,0);
					
				}

			}

			reader+=(sizeof(Query_t) + o->columnCount*sizeof(Column_t));	//read the next query
		}

		return validation_node;

}



FILE *fp;




int compare(uint64_t journal_value, uint64_t op, uint64_t value ){
	if(op == 0){
		if(journal_value == value){
			return 1;
		}
	}

	else if(op == 1){
		if(journal_value != value){
			return 1;
		}
	}

	else if(op == 2){
		if(journal_value < value){
			return 1;
		}
	}

	else if(op == 3){
		if(journal_value <= value){
			return 1;
		}
	}

	else if(op == 4){
		if(journal_value > value){
			return 1;
		}
	}

	else if(op == 5){
		if(journal_value >= value){
			return 1;
		}
	}

	return 0;
}



typedef struct Args{

	ValidationNode** val_array;
	int size;
	Journal** journal_list;
	Hash** hash_list;
}Args;


void* thread_f(void* arg){
    Args* info = arg;
    int u;
    int i, j,k,l,t,c, hash_pos, relation, journal_hash_pos, m;
	int key, line, column,  op, b;
	uint64_t value;
	int true_val = 0;
	int found = 0;
	int journal_pos, start_pos;
	int not_null = 0;
	Records_Node* journal_record_list, *temp_record;
	Hash_Record_Node* hash_record_list, *temp_hash_record;
    for(u = 0; u < info->size; u++){
    	ValidationNode* temp;
    	temp = info->val_array[u];
    	for( i = 0; i < temp->validation->queryCount; i++){ //for every query of the validation
				
			relation = temp->validation->queries[i].relationId;
			
			if(temp->validation->queries[i].columnCount == 0){
				
				journal_record_list = getJournalRecords(info->journal_list[relation], temp->validation->from, temp->validation->to);
				
				

				if(journal_record_list == NULL){

					continue;
				}

				else{
					true_val = 1;
					
					break;
				}

			}

			qsort(&(temp->validation->queries[i].columns[0]), temp->validation->queries[i].columnCount, sizeof(Column_t), cmpfunc );
			//sort column numbers

			if(temp->validation->queries[i].columns[0].column == 0 && temp->validation->queries[i].columns[0].op == 0){

				key = temp->validation->queries[i].columns[0].value;
				hash_record_list = getHashRecords(info->hash_list[relation], key, temp->validation->from, temp->validation->to);

				if(hash_record_list == NULL){
					continue;   //next query
				}

				else{

					while(hash_record_list != NULL){

						journal_pos = hash_record_list->transaction->record[0];
						line = hash_record_list->transaction->record[1];

						for(j = 0; j < temp->validation->queries[i].columnCount; j++ ){
								column = temp->validation->queries[i].columns[j].column;
								op = temp->validation->queries[i].columns[j].op;
								value = temp->validation->queries[i].columns[j].value;

							if(compare(info->journal_list[relation]->transactions_array[journal_pos]->columns_array[line][column],op,value) != 0){

								if(j == temp->validation->queries[i].columnCount-1){

									if(temp->validation->validationId == debug_val){
										fprintf(stderr, "ISXYEI TO QUERY %d", i );
									}

									true_val=1;
									
									break;
								}

								else{

									continue;
								}
							}

							else{

								break;
							}
						}

						if(true_val == 1){

							break;
						}

						else{
							line = hash_record_list->transaction->case_record[1];

							if(line != -1){

								for(j = 0; j < temp->validation->queries[i].columnCount; j++ ){
									column = temp->validation->queries[i].columns[j].column;
									op = temp->validation->queries[i].columns[j].op;
									value = temp->validation->queries[i].columns[j].value;

									if(compare(info->journal_list[relation]->transactions_array[journal_pos]->columns_array[line][column],op,value) != 0){

										if(j == temp->validation->queries[i].columnCount-1){

											if(temp->validation->validationId == debug_val){
												fprintf(stderr, "ISXYEI TO QUERY %d", i );
											}

											true_val=1;
											
											break;
										}

										else{

											continue;
										}
									}

									else{

										break;
									}
								}

							}
						}

						if(true_val == 1){

							break;
						}

						temp_hash_record = hash_record_list;
						hash_record_list = hash_record_list->next;
						free(temp_hash_record);
					}
				}
			}



			else{
				
				journal_record_list = getJournalRecords(info->journal_list[relation], temp->validation->from, temp->validation->to);
				
				
				if(journal_record_list == NULL){

					continue;
				}
				else{

					while(journal_record_list != NULL){

						for(j = 0; j < journal_record_list->journal_record->num_of_lines; j++){

							for(k = 0; k < temp->validation->queries[i].columnCount; k++){

								column = temp->validation->queries[i].columns[k].column;
								op = temp->validation->queries[i].columns[k].op;
								value = temp->validation->queries[i].columns[k].value;

								if(compare(journal_record_list->journal_record->columns_array[j][column],op,value) != 0){

									if(k == temp->validation->queries[i].columnCount-1){

										if(temp->validation->validationId == debug_val){
												fprintf(stderr, "ISXYEI TO QUERY %d", i );
											}
										true_val = 1;
										
										
									}

									else{
										continue;
									}
								}

								else{
									break; //continue to next line if one column of the query causes no conflict
								}
							}

							if(true_val == 1){
								break; //search no more line of the journal record
							}
						}
						if(true_val == 1){
							break; //search no more nodes of the journal record list
						}
						temp_record = journal_record_list;
						journal_record_list = journal_record_list->next;
						if(temp_record!=NULL){
							free(temp_record);
						}
					}
					
				}
			}



			if(true_val == 1){	//if we have found a query to be true then the whole validation is true so dont check the
							//rest of the queries
				break;
			}


		}
		if(true_val == 1){
			info->val_array[u]->validation->conflict = 1;
			true_val = 0;
		}

		else{

			info->val_array[u]->validation->conflict = 0;
		}


    }




 //   printf("Thread %ld just before exiting with value!!\n",pthread_self());

    pthread_exit(NULL);
}



static int processFlush(Flush_t *fl,Hash** hash,Journal** journal, TransactionIndex** my_index_journal_list, ValidationIndex** my_predicate_index_list){
  //fprintf(stderr,"Flush %lu\n", fl->validationId);

	int val_id = fl->validationId;
	int i, j,k,l,t,c, hash_pos, relation, journal_hash_pos, m;
	int key, line, column,  op, b;
	uint64_t value;
	int true_val = 0;
	int found = 0;
	int journal_pos, start_pos;
	int not_null = 0;
	Records_Node* journal_record_list, *temp_record;
	Hash_Record_Node* hash_record_list, *temp_hash_record;
	predicateNode* temp_predicate;
	int range_pos;
	ValidationNode* temp = validation_list;

	int validations_counter=0;
	ValidationNode*** val_array=NULL;//an array of arrays of ValidationNode pointers.
	Args *args = NULL;
	int thread_array_lines[NUM_OF_THREADS][2];//the first column is the capacity of the array and the second is the content's counter
												//each line of this array holds info about each thread of the val_array
	for(i = 0; i < NUM_OF_THREADS; i++){//initialisation of array
		thread_array_lines[i][0] = SIZE_OF_THREAD_ARRAY;
		thread_array_lines[i][1] = 0;
	}

	

	if(level3 == 0){
		while(temp != NULL && temp->validation->validationId <= val_id){		//for every validation

			
			if(level2 == 0){
			
				for( i = 0; i < temp->validation->queryCount; i++){ //for every query of the validation
				
					relation = temp->validation->queries[i].relationId;
					
					if(temp->validation->queries[i].columnCount == 0){
						
						journal_record_list = getJournalRecords(journal[relation], temp->validation->from, temp->validation->to);
						
						

						if(journal_record_list == NULL){

							continue;
						}

						else{
							true_val = 1;
							fprintf(fp, "1" );
							break;
						}

					}

					qsort(&(temp->validation->queries[i].columns[0]), temp->validation->queries[i].columnCount, sizeof(Column_t), cmpfunc );
					//sort column numbers

					if(temp->validation->queries[i].columns[0].column == 0 && temp->validation->queries[i].columns[0].op == 0){

						key = temp->validation->queries[i].columns[0].value;
						hash_record_list = getHashRecords(hash[relation], key, temp->validation->from, temp->validation->to);

						if(hash_record_list == NULL){
							continue;   //next query
						}

						else{

							while(hash_record_list != NULL){

								journal_pos = hash_record_list->transaction->record[0];
								line = hash_record_list->transaction->record[1];

								for(j = 0; j < temp->validation->queries[i].columnCount; j++ ){
										column = temp->validation->queries[i].columns[j].column;
										op = temp->validation->queries[i].columns[j].op;
										value = temp->validation->queries[i].columns[j].value;

									if(compare(journal[relation]->transactions_array[journal_pos]->columns_array[line][column],op,value) != 0){

										if(j == temp->validation->queries[i].columnCount-1){

											if(temp->validation->validationId == debug_val){
												fprintf(stderr, "ISXYEI TO QUERY %d", i );
											}

											true_val=1;
											fprintf(fp, "1" );
											break;
										}

										else{

											continue;
										}
									}

									else{

										break;
									}
								}

								if(true_val == 1){

									break;
								}

								else{
									line = hash_record_list->transaction->case_record[1];

									if(line != -1){

										for(j = 0; j < temp->validation->queries[i].columnCount; j++ ){
											column = temp->validation->queries[i].columns[j].column;
											op = temp->validation->queries[i].columns[j].op;
											value = temp->validation->queries[i].columns[j].value;

											if(compare(journal[relation]->transactions_array[journal_pos]->columns_array[line][column],op,value) != 0){

												if(j == temp->validation->queries[i].columnCount-1){

													if(temp->validation->validationId == debug_val){
														fprintf(stderr, "ISXYEI TO QUERY %d", i );
													}

													true_val=1;
													fprintf(fp, "1" );
													break;
												}

												else{

													continue;
												}
											}

											else{

												break;
											}
										}

									}
								}

								if(true_val == 1){

									break;
								}

								temp_hash_record = hash_record_list;
								hash_record_list = hash_record_list->next;
								free(temp_hash_record);
							}
						}
					}



					else{
						
						journal_record_list = getJournalRecords(journal[relation], temp->validation->from, temp->validation->to);
						
						
						if(journal_record_list == NULL){

							continue;
						}
						else{

							while(journal_record_list != NULL){

								for(j = 0; j < journal_record_list->journal_record->num_of_lines; j++){

									for(k = 0; k < temp->validation->queries[i].columnCount; k++){

										column = temp->validation->queries[i].columns[k].column;
										op = temp->validation->queries[i].columns[k].op;
										value = temp->validation->queries[i].columns[k].value;

										if(compare(journal_record_list->journal_record->columns_array[j][column],op,value) != 0){

											if(k == temp->validation->queries[i].columnCount-1){

												if(temp->validation->validationId == debug_val){
														fprintf(stderr, "ISXYEI TO QUERY %d", i );
													}
												true_val = 1;
												fprintf(fp, "1" );
												
											}

											else{
												continue;
											}
										}

										else{
											break; //continue to next line if one column of the query causes no conflict
										}
									}

									if(true_val == 1){
										break; //search no more line of the journal record
									}
								}
								if(true_val == 1){
									break; //search no more nodes of the journal record list
								}
								temp_record = journal_record_list;
								journal_record_list = journal_record_list->next;
								if(temp_record!=NULL){
									free(temp_record);
								}
							}
							
						}
					}



					if(true_val == 1){	//if we have found a query to be true then the whole validation is true so dont check the
									//rest of the queries
						break;
					}


				}

				
			}

			else{
				char* final_result = NULL;
				int total_records = 0;
				int final_result_initialised = 0;
				char* temp_result = NULL;
				
				for(i = 0; i < temp->validation->queryCount; i++){
					
					relation = temp->validation->queries[i].relationId;
					journal_record_list = getTransactionIndexRecords(my_index_journal_list[relation],temp->validation->from,temp->validation->to,journal[relation],&start_pos);
					
					temp_record = journal_record_list;
					
					if(journal_record_list == NULL){
						if(temp->validation->validationId == debug_val){
								fprintf(stderr, "query %d =null\n",i );
							}
						continue;
					}
					total_records = 0;
					
					while(temp_record != NULL){
						total_records += temp_record->journal_record->num_of_lines;
						temp_record = temp_record->next;
					}
					temp_record = journal_record_list;
					final_result = malloc(total_records);
					memset(final_result,0,total_records);
					temp_predicate = temp->validation->predicate_list_start;
					while(temp_predicate != NULL){
						
						if(temp_predicate->query_number == i){
							if(temp->validation->validationId == debug_val){
								fprintf(stderr, "query %d:range start = %d,end = %d, col = %d, op=%d, value=%lu\n",i,temp_predicate->predicate->key.range_start,temp_predicate->predicate->key.range_end,temp_predicate->predicate->key.column,temp_predicate->predicate->key.op,temp_predicate->predicate->key.value );
							}

							
							if(temp_predicate->predicate->key.op == 100){
									free(final_result);
									final_result = NULL;
									true_val = 1;
									fprintf(fp, "1" );
									break;
							}
							
							else if(temp_predicate->predicate->conflict != NULL){

								for(j = 0; j < total_records; j++){
									if(final_result_initialised == 0){
										final_result[j] = temp_predicate->predicate->conflict[j];
									}
									else{
										final_result[j] = (final_result[j] & temp_predicate->predicate->conflict[j]);
									}
								}
								final_result_initialised = 1;	
							}

							else{
								temp_predicate->predicate->conflict = malloc(total_records);
								memset(temp_predicate->predicate->conflict, 0, total_records);
								temp_predicate->predicate->conflict_size = total_records;
								
								if(temp_predicate->predicate->key.op == 0 && temp_predicate->predicate->key.column == 0){
									hash_record_list = getHashRecords(hash[relation], temp_predicate->predicate->key.value, temp->validation->from, temp->validation->to);
									
									if(hash_record_list != NULL){
										temp_result = malloc(total_records);
										memset(temp_result,0,total_records);
										while(hash_record_list != NULL){
											//fprintf(stderr, "EDW TO START POS = %d\n",start_pos );

											

											range_pos = find_range_pos(journal[relation],hash_record_list->transaction->trans_id,hash_record_list->transaction->record[1],temp->validation->from,temp->validation->to,start_pos);
											temp_predicate->predicate->conflict[range_pos] = (temp_predicate->predicate->conflict[range_pos] | 1);
											/*if(final_result_initialised == 0){
												final_result[range_pos] = temp_predicate->predicate->conflict[range_pos];
											}
											else{
												final_result[range_pos] = (final_result[range_pos] & temp_predicate->predicate->conflict[range_pos]);											
											}*/

											temp_result[range_pos] = (temp_result[range_pos] | 1);

											if(hash_record_list->transaction->case_record[1] != -1){
												//fprintf(stderr, "EDW PALI TO START POS = %d\n",start_pos );
												range_pos = find_range_pos(journal[relation],hash_record_list->transaction->trans_id,hash_record_list->transaction->case_record[1],temp->validation->from,temp->validation->to,start_pos);
												temp_predicate->predicate->conflict[range_pos] = (temp_predicate->predicate->conflict[range_pos] | 1);
												/*if(final_result_initialised == 0){
													final_result[range_pos] = temp_predicate->predicate->conflict[range_pos];
												}
												else{
													final_result[range_pos] = (final_result[range_pos] & temp_predicate->predicate->conflict[range_pos]);											
												}*/
												temp_result[range_pos] = (temp_result[range_pos] | 1);
											}
											temp_hash_record = hash_record_list;
											hash_record_list = hash_record_list->next;
											free(temp_hash_record);
										}
										if(final_result_initialised == 0){
											for(m = 0; m < total_records; m++){
												final_result[m] = temp_result[m];
											}
											final_result_initialised = 1;
										}

										else{
											for(m = 0; m < total_records; m++){
												final_result[m] = (final_result[m] & temp_result[m]);
											}
										}
										free(temp_result);
										
									}

									else{
										if(temp->validation->validationId == debug_val){
											fprintf(stderr, "no valid record\n" );
										}
										
										free(final_result);
										final_result = NULL;
										final_result_initialised = 0;
										break;
									}
								}
								else{
									column = temp_predicate->predicate->key.column;
									op = temp_predicate->predicate->key.op;
									value = temp_predicate->predicate->key.value;
									if(temp->validation->validationId == debug_val){
										fprintf(stderr, "column = %d op =%d val = %lu \n \n", column,op,value );
									}
									int num_of_line = 0;
									int conflict_found = 0;
									temp_record = journal_record_list;
									while(temp_record != NULL){
										for(j = 0; j < temp_record->journal_record->num_of_lines; j++){
											num_of_line++;
											if(compare(temp_record->journal_record->columns_array[j][column], op, value) != 0){
												conflict_found = 1;
												//fprintf(stderr, "num_of_line = %d total = %d\n",num_of_line, total_records );
												temp_predicate->predicate->conflict[num_of_line-1] = (temp_predicate->predicate->conflict[num_of_line-1] | 1);
												if(final_result_initialised == 0){
													final_result[num_of_line-1] = temp_predicate->predicate->conflict[num_of_line-1];
												}
												else{
													final_result[num_of_line-1] = (final_result[num_of_line-1] & temp_predicate->predicate->conflict[num_of_line-1]);											
												}
											}

											else{
												if(final_result_initialised == 1){
													final_result[num_of_line-1] = (final_result[num_of_line-1] & 0);
												}
											}
										}	
										temp_record = temp_record->next;
									}
									if(conflict_found == 0){
										if(temp->validation->validationId == debug_val){
											fprintf(stderr, "no valid record (edw sto journal)\n" );
										}

										free(final_result);
										final_result = NULL;
										final_result_initialised = 0;
										break;
									}
									final_result_initialised = 1;
								}
							}
							
						}

						temp_predicate = temp_predicate->next;
					}
					temp_record = journal_record_list;
					journal_record_list = journal_record_list->next;	
					free(temp_record);
					if(final_result != NULL){

						if(temp->validation->validationId == debug_val){
							fprintf(stderr, "checking final res\n" );
						}

						for(k = 0; k < total_records; k++){
							if((final_result[k] | 0) == 1){

								if(temp->validation->validationId == debug_val){
									fprintf(stderr, "CONFLICT\n" );
								}

								true_val = 1;
								fprintf(fp, "1");
								break;
							}
						}
						free(final_result);	
					}
					final_result = NULL;
					final_result_initialised = 0;

					if(true_val == 1 ){

						break;
					}

				}


			}
			
			if(true_val == 1){
				true_val = 0;
			}

			else{

				fprintf(fp, "0" );
			}

			temp = temp->next;
			validation_list = temp;
		}	
	}


	else{//If we have the level 3 activated

		val_array=malloc(NUM_OF_THREADS*sizeof(ValidationNode**));//create an array of arrays
		for(i = 0; i < NUM_OF_THREADS; i++){
			val_array[i] = malloc(SIZE_OF_THREAD_ARRAY*sizeof(ValidationNode*));
		}


		while(temp != NULL && temp->validation->validationId <= val_id){ //for every validation
			validations_counter++;
		
			int array_pos=(validations_counter-1) % NUM_OF_THREADS;//the position in the val_array of the thread which will handle this validation

			if(thread_array_lines[array_pos][1] < thread_array_lines[array_pos][0]){//If there is space in the array of this thread

				val_array[array_pos][thread_array_lines[array_pos][1]] = temp;//we make the correct pointer point to the validation node
				thread_array_lines[array_pos][1]++;	//we increase the counter of the pointers in the other array
			}

			else{	//if there's no space in the array of this thread

				//allocate more space
				val_array[array_pos]=realloc(val_array[array_pos],(thread_array_lines[array_pos][1]+SIZE_OF_THREAD_ARRAY)*sizeof(ValidationNode*));
				//arrange the pointer of the array to the validation node
				val_array[array_pos][thread_array_lines[array_pos][1]] = temp;
				(thread_array_lines[array_pos][1])++;	//increase the number of pointers
				thread_array_lines[array_pos][0] += SIZE_OF_THREAD_ARRAY;	//increase the total pointers capacity of this thread
			}

			temp = temp->next;
			
		}
		validations_counter = 0;

		int err;
		int status;
		pthread_t threads[NUM_OF_THREADS]; //array of threads

		
		args = malloc(NUM_OF_THREADS*sizeof(Args));

		for( i = 0; i <= NUM_OF_THREADS-1; i++){//For each thread

			
			args[i].val_array=val_array[i];//malloc(thread_array_lines[i][1]*sizeof(ValidationNode*));//create an array of pointers to validation nodes

			
			args[i].size = thread_array_lines[i][1];
			args[i].journal_list = journal;
			args[i].hash_list = hash;
	        if ( err = pthread_create( &threads[i] , NULL , thread_f ,(void *) &args[i])) { //New thread 
	           	fprintf(stderr, "Error while creating thread %d !!\n", i);
	            exit(1);
	        }
	        //printf("I am original thread %ld and I created thread %ld \n ", pthread_self(), threads[i]);
    	}


    	int p=0;

	    for( p = 0; p <= NUM_OF_THREADS-1; p++){

	        if ( err = pthread_join(threads[p] , (void **) &status)) { /* Wait for thread */
	            fprintf(stderr, "Error while using pthread_join for thread %d !!\n", p); /* termination */
	            exit (1) ;
	        }
	       // printf ( " Thread %ld exited with code %d\n " , threads[p] , status ) ;
	    }
	    //printf ( " Thread %ld just before exiting ( Original )\n " , pthread_self() );




	}
	if(level3 == 1){
		temp = validation_list;
		while(temp != NULL && temp->validation->validationId <= val_id){
			if(temp->validation->conflict == 1){
				fprintf(fp, "1" );
			}
			else{
				fprintf(fp, "0" );
			}
			temp = temp->next;
			validation_list = temp;
		}
		for( i = 0; i <= NUM_OF_THREADS-1; i++){
			
			free(val_array[i]);
			
			
		}
		free(val_array);
		free(args);
	}
	
	return 0;

}


static void processForget(Forget_t *fo){
//  printf("Forget %lu\n", fo->transactionId);

}



int find_range_pos(Journal* journal, int trans_id, int offset, int range_start, int range_end, int start_pos){
	int i, journal_pos;
	int sum = 0;
	i = start_pos;
	//fprintf(stderr, "i = %d\n",i );
	while(journal->transactions_array[i]->transaction_id < trans_id){
		sum += journal->transactions_array[i]->num_of_lines;
		i++;
	}

	sum += offset;
	return sum;
}




int main(int argc, char **argv) {

	MessageHead_t head;
	void *body = NULL;
	uint32_t len, relation_count;

	Journal** my_journal_list = NULL;
	TransactionIndex** my_index_journal_list = NULL;
	ValidationIndex** my_predicate_index_list = NULL;
	Hash** my_hash_list = NULL;
	int i, j , m , k, l;
	ValidationNode* temp;
	
	if(strcmp(argv[1],"on") == 0){
		level2 = 1;
	}

	else if(strcmp(argv[1], "off") == 0){
		level2 = 0;
	}

	else{
		fprintf(stderr, "wrong attribute!! please write 'on' if you want level 2 enabled or 'off' if you want it disabled.\n" );
		exit(1);
	}
	
	fp = fopen("vals.out.bin", "w+");//open a file to write the results

		while(1){
			// Retrieve the message head
			if (read(0, &head, sizeof(head)) <= 0) { return -1; } // crude error handling, should never happen
			//  printf("HEAD LEN %u \t| HEAD TYPE %u \t| DESC ", head.messageLen, head.type);
				
			// Retrieve the message body
			if (body != NULL) free(body);

			if (head.messageLen > 0 ){
				body = malloc(head.messageLen*sizeof(char));	//allocate space for the message we read
				if (read(0, body, head.messageLen) <= 0) { printf("err");return -1; } // crude error handling, should never happen
				len-=(sizeof(head) + head.messageLen);
			}
						
			// And interpret it
			switch (head.type) {
				 case Done: 
				
				for(i = 0; i < relation_count; i++){
						destroyJournal(my_journal_list[i]);
				}

				for(i = 0; i < relation_count; i++){
						destroyHash(my_hash_list[i]);
				}
				free(schema);
				for(i = 0; i < relation_count; i++){
						free(my_hash_list[i]);
						free(my_journal_list[i]);
				}
				free(my_hash_list);
				free(my_journal_list);


				temp = validation_list;

		


				fclose(fp);
				 printf("\n");return 0;


				 case DefineSchema:

				 relation_count = processDefineSchema(body);
				 my_journal_list = malloc(relation_count*sizeof(Journal*));//create an array of pointers to all the journals
				 my_hash_list = malloc(relation_count*sizeof(Hash*));
					if(level2 == 1){
						my_index_journal_list = malloc(relation_count*sizeof(TransactionIndex*));
						my_predicate_index_list = malloc(relation_count*sizeof(ValidationIndex*));
					}
				
				 for(i = 0; i < relation_count; i++){
						my_journal_list[i] = createJournal();
						my_hash_list[i] = createHash();
						if(level2 == 1){
							my_index_journal_list[i] = createTransactionIndex();
							my_predicate_index_list[i] = createValidationIndex();
						}
				 }

				 
				 
				 break;


				 case Transaction_case: 
				 if(level2 == 0){
				 	
				 	processTransaction(body, my_hash_list, my_journal_list,NULL);
				 }
				 else{
				 	processTransaction(body, my_hash_list, my_journal_list,my_index_journal_list);
				 }
							//return;
							break;
				 case ValidationQueries: 
				 if(level2 == 0){
				 	processValidationQueries(body, NULL); 
				 }

				 else{
				 	processValidationQueries(body, my_predicate_index_list);
				 }

				 break;
				 case Flush: 
					if(level2 == 0){
						processFlush(body, my_hash_list, my_journal_list,NULL,NULL); 
					}

					else{
						processFlush(body, my_hash_list, my_journal_list,my_index_journal_list,my_predicate_index_list);
					}

				 break;
				 case Forget: processForget(body); break;
				 default: 
				 return -1; // crude error handling, should never happen
			}
		}
		
	return 0;
}
