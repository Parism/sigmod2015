#include <stdio.h> 
#include "journal.h"
#include <stdlib.h>
#include <stdint.h>


Journal* createJournal(){

	Journal* jour_ptr;

	int i = 0;

	jour_ptr = malloc(sizeof(Journal));

	jour_ptr->transactions_array = malloc(INITIAL_CAPACITY*sizeof(JournalRecord*));		//allocate INITIAL_CAPACITY pointers to journal records

	for(i = 0; i < INITIAL_CAPACITY; i++){
		jour_ptr->transactions_array[i]= NULL;

	}

	jour_ptr->records_counter = 0;

	jour_ptr->journal_capacity = INITIAL_CAPACITY;
	return jour_ptr;
}


int insertJournalRecord(Journal* jour_ptr, JournalRecord* record_ptr, int columns, int num_of_keys ){

	int i = 0, j = 0, new_array_size;
	int journal_pos;	//position of transcaction to be inserted if it already exists 
	int k;

	//fprintf(stderr, "records counter = %d\n", jour_ptr->records_counter);

	if((journal_pos = Journal_Binary_Search(jour_ptr, jour_ptr->records_counter, record_ptr->transaction_id)) != -1){
	//if the transaction has already been inserted in the journal

		//fprintf(stderr, "BEFORE REALLOC\n" );

		new_array_size = (jour_ptr->transactions_array[journal_pos]->num_of_lines) + (num_of_keys); 
		//fprintf(stderr, "old array size = %d ------- new = %d\n", jour_ptr->transactions_array[journal_pos]->num_of_lines, new_array_size );
		//fprintf(stderr, "------NEW ARRAY SIZE = %d journal pos = %d\n", new_array_size, journal_pos );
		/*if(jour_ptr->transactions_array[journal_pos] == NULL){
			fprintf(stderr, "ERROR\n" );
		}*/
		jour_ptr->transactions_array[journal_pos]->columns_array = realloc(jour_ptr->transactions_array[journal_pos]->columns_array, new_array_size*sizeof(uint64_t*) );
		//fprintf(stderr, "AFTER REALLOC\n" );
		
		for (i = jour_ptr->transactions_array[journal_pos]->num_of_lines; i<new_array_size; i++){

			jour_ptr->transactions_array[journal_pos]->columns_array[i] = malloc((columns+1)*sizeof(uint64_t));
		}

		k = 0;
		for(i = jour_ptr->transactions_array[journal_pos]->num_of_lines ; i<new_array_size; i++){

			for(j = 0; j < columns+1; j++){

				jour_ptr->transactions_array[journal_pos]->columns_array[i][j] = record_ptr->columns_array[k][j];
				//fprintf(stderr, "columns_array[%d][%d] = %d\n", i, j, jour_ptr->transactions_array[journal_pos]->columns_array[i][j] );

			}
			k++;
		}
		jour_ptr->transactions_array[journal_pos]->num_of_lines = new_array_size;
		return 0;
	}



	if(jour_ptr->records_counter < jour_ptr->journal_capacity){		//if journal not full

		jour_ptr->transactions_array[jour_ptr->records_counter] = malloc(sizeof(JournalRecord));
		jour_ptr->transactions_array[jour_ptr->records_counter]->columns_array = malloc(num_of_keys*sizeof(uint64_t*));	//allocate num_of_keys lines to the columns_array
		jour_ptr->transactions_array[jour_ptr->records_counter]->num_of_lines = 0;
		
		jour_ptr->transactions_array[jour_ptr->records_counter]->transaction_id = record_ptr->transaction_id;

		for(i = 0; i < num_of_keys; i++){

			jour_ptr->transactions_array[jour_ptr->records_counter]->columns_array[i] = malloc((columns+1)*sizeof(uint64_t)); //allocate columns for each line of the columns_array

		} 

		for(j = 0; j < num_of_keys; j++){		//for each line of the columns_array
			for(i = 0; i < columns+1; i++){		//for each column of the j line of the columns_array

				jour_ptr->transactions_array[jour_ptr->records_counter]->columns_array[j][i] = record_ptr->columns_array[j][i];
				// insert the appropriate value given by the record_ptr  argument
			}
		}
		jour_ptr->transactions_array[jour_ptr->records_counter]->num_of_lines = num_of_keys;
		
		jour_ptr->records_counter++;
		return 0;

	}

	else{		//if journal is full

		

		increase_Journal(jour_ptr);

		jour_ptr->transactions_array[jour_ptr->records_counter] = malloc(sizeof(JournalRecord));
		jour_ptr->transactions_array[jour_ptr->records_counter]->columns_array = malloc(num_of_keys*sizeof(uint64_t*));	//allocate num_of_keys lines to the columns_array
		jour_ptr->transactions_array[jour_ptr->records_counter]->num_of_lines = 0;
		jour_ptr->transactions_array[jour_ptr->records_counter]->transaction_id = record_ptr->transaction_id;

		for(i = 0; i < num_of_keys; i++){

			jour_ptr->transactions_array[jour_ptr->records_counter]->columns_array[i] = malloc((columns+1)*sizeof(uint64_t)); //allocate columns for each line of the columns_array

		} 

		for(j = 0; j < num_of_keys; j++){		//for each line of the columns_array
			for(i = 0; i < columns+1; i++){		//for each column of the j line of the columns_array

				jour_ptr->transactions_array[jour_ptr->records_counter]->columns_array[j][i] = record_ptr->columns_array[j][i];
				// insert the appropriate value given by the record_ptr  argument
			}
		}
		jour_ptr->transactions_array[jour_ptr->records_counter]->num_of_lines = num_of_keys;
		jour_ptr->records_counter++;
		return 0;
	}

}


int increase_Journal(Journal* jour_ptr){
	//fprintf(stderr, "INCREASE jour_ptr->journal_capacity = %d\n", jour_ptr->journal_capacity);

	jour_ptr->transactions_array = realloc(jour_ptr->transactions_array,2*(jour_ptr->journal_capacity)*sizeof(JournalRecord*));
	jour_ptr->journal_capacity = 2*(jour_ptr->journal_capacity);

	return 0;
}

Records_Node* getJournalRecords(Journal* jour_ptr, int range_start, int range_end){

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
		if((journal_pos=Journal_Binary_Search(jour_ptr, jour_ptr->records_counter, i)) != -1){
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

int Journal_Binary_Search(Journal* jour_ptr, int journal_records, int search_value){

	int first, last, middle;

	first = 0;		//initialise first as value 0
	last = journal_records-1;	//initialise last to be last item of the transactions_array
	middle = (first+last)/2;	//initialise middle to be the middle item of the transactions_array


	//fprintf(stderr, "-------------first = %d   last = %d---------\n", first, last);

	while(first <= last){	

	//	printf("in while\n");	

		if(jour_ptr->transactions_array[middle]->transaction_id == search_value){	//check if the middle element is the one we search for

			//printf("found\n");
			return middle;
		}

		else if(jour_ptr->transactions_array[middle]->transaction_id < search_value){	//if the value we seach for is greater than the middle one 
																						//we set the first value to be the middle item plus one (first item of the 	
		//	printf("we search for something bigger\n");																			//second subarray of the transactions_array.)
			first = middle + 1;

		}

		else{																//if the value we search for is lesser than the middle one
																			//we set last to be the previous item of the middle one(last of
		//	printf("we search for something lesser\n");																//the first subarray of the transactions_array)
			last = middle - 1;
		}

		middle = (first+last)/2;			//set middle to be (new first+ new last ) / 2
	}

	if(first > last){ 


	//	fprintf(stderr, "NOTHING FOUND IN JOURNAL\n" );
		return -1;
	}
}


int destroyJournal(Journal* jour_ptr){

	int i=0;
	int j=0;
	for(i=0; i < jour_ptr->records_counter; i++){	//For each position of the journal's index

		for(j=0; j < jour_ptr->transactions_array[i]->num_of_lines; j++){	//For each line of the array

			free(jour_ptr->transactions_array[i]->columns_array[j]);
		}
		free(jour_ptr->transactions_array[i]->columns_array);
		free(jour_ptr->transactions_array[i]);
	}



	free(jour_ptr->transactions_array);
	return 0;
}