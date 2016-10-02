#include <pthread.h> 
#include <stdio.h> 
#include <errno.h> 
#include <unistd.h> 
#include <stdlib.h> 
#include <string.h>
#include <signal.h>
void* merger(void*);
void* sorter(void*);
void* createMerger(pthread_t *tMerger, int *mNumber);
void* createSorter(pthread_t *tSorter, int *sNumber);

struct Customer
{
	int transactionNum;
	int customerNum;
	double amount;
};
void* readDataFile(struct Customer *customers, int sNumber, int * fileSize);
void* sortCustomers(struct Customer *customers, int * fileSize);
void* printCustomers(struct Customer *customers, int fileSize);
int main() 
{ 
	pthread_t merger1, merger2;
	//int status;
	int mNumber[2];
	mNumber[0] = 1;
	mNumber[1] = 2;
	
	createMerger(&merger1, &mNumber[0]);
	createMerger(&merger2, &mNumber[1]);
	sleep(10);
	return(0);
}


void* createMerger(pthread_t *tMerger, int *mNumber)
{
	int status;
	if ((status = (pthread_create (tMerger, NULL,  merger, mNumber)))!=0)
	{ 
		fprintf (stderr, "thread create error %d: %s\n", status, strerror(status)); 
	        exit(1); 
	}
	else
	{
		//Can detach
		if(pthread_detach(*tMerger))
		{
			printf("Merger1 detach error");
			exit(1);
		}
	}
	return(0);
}

void* merger(void* arg)
{
	pthread_t sorter1, sorter2;
	int * mNumber = (int *)arg;
	int *sNumbers = malloc(2*sizeof(int));
	if(*mNumber == 1)
	{
		*sNumbers = 1;
		*(sNumbers+1) = 2;
	}
	else
	{
		*sNumbers = 3;
		*(sNumbers+1)= 4;
	}
	createSorter(&sorter1, sNumbers);
	createSorter(&sorter2, (sNumbers+1));
	sleep(10);
	return(0);
}

void* createSorter(pthread_t *tSorter, int *sNumber)
{
	int status;
	if ((status = (pthread_create (tSorter, NULL,  sorter, sNumber)))!=0)
	{ 
		fprintf (stderr, "thread create error %d: %s\n", status, strerror(status)); 
	        exit(1); 
	}
	else
	{
		//Can detach
		if(pthread_detach(*tSorter))
		{
			printf("Sorter %d detach error", *sNumber);
			exit(1);
		}
	}
	return(0); 
}
	
void* sorter(void* arg)
{
	int* sNumber = (int *)arg;
	struct Customer customers;
	int fileSize = 0;
	readDataFile(&customers, *sNumber, &fileSize);
	sortCustomers(&customers, &fileSize);
	//printCustomers(&customers, fileSize);
	sleep(10);
	return(0);
}

/*
 *Code taken from http://stackoverflow.com/questions/3501338/c-read-file-line-by-line
 */
void* readDataFile(struct Customer *customers, int sNumber, int * fileSize)
{
	char * line = NULL;
	FILE * fp;
	size_t len = 0;
	ssize_t read;
	char *file = malloc(12*sizeof(char));
	sprintf(file, "file_%d.dat", sNumber);
	fp = fopen(file, "r");
	
	if(fp==NULL)
	{
		printf("file failure\n");
		fflush(stdout);
		exit(EXIT_FAILURE);
	}
	while((read = getline(&line, &len, fp)) != -1)
	{	
		//Create a customer from a line in data
		char * delim;
		char *savePtr;
		delim = strtok_r(line, " ", &savePtr);
		int column = 0;
		while(delim != NULL)
		{
			
			if(column == 0)
			{			//Trans Num
				sscanf(delim, "%d", &((customers + (*fileSize))->transactionNum)); 
			}
			else if(column == 1)
			{			//Cust Num
				sscanf(delim, "%d", &(customers + *fileSize)->customerNum);
			}
			else
			{			//Amount
				sscanf(delim, "%lf", &(customers + *fileSize)->amount);			
			}
			column++;
			delim = strtok_r(NULL, " ", &savePtr);
		}	
		(*fileSize)++;
	}
	
	fclose(fp);
	if(line)
		free(line);
	return (0);
}

void* sortCustomers(struct Customer *customers, int * fileSize)
{
	struct Customer temp, temp2;
	int i, j, swaps;
	for(i=0; i<(*fileSize)-2; i++)
	{
		swaps = 0;
		for (j=0; j<(*fileSize)-1; j++)
		{
			if((*(customers+j)).customerNum > (*(customers+j+1)).customerNum)
			{	
				temp = *(customers+j);
				temp2 = *(customers+j+1);
				*(customers + j) = *(customers+j+1);
				*(customers + j + 1) = temp;
				swaps++;
			}
		}
		if(swaps == 0){
			printf("break\n");
			break;
		}
	}
	printf("After Sort\n");
	return(0);
}

void* printCustomers(struct Customer *customers, int fileSize)
{
	int i;
	for(i = 0; i < fileSize; i++)
	{
		printf("customerNum = %d\n",(*(customers+i)).customerNum); 
	}
	return(0);	
}
