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
void* readDataFile(struct Customer *customers, int sNumber);
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
	printf("Merger Number %d\n", *mNumber);
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
	printf("Sorter number: %d\n", *sNumber);
	fflush(stdout);
	if(*sNumber == 1)
	{
		printf("Hello\n");
		readDataFile(&customers, *sNumber);	
	}
	return(0);
}

/*
 *Code taken from http://stackoverflow.com/questions/3501338/c-read-file-line-by-line
 */
void* readDataFile(struct Customer *customers, int sNumber)
{
	char * line = NULL;
	FILE * fp;
	size_t len = 0;
	ssize_t read;
	char *file = malloc(12*sizeof(char));
	sprintf(file, "file_%d.dat", sNumber);
	printf("%s\n", file);
 	
	fp = fopen(file, "r");
	if(fp==NULL)
		exit(EXIT_FAILURE);
	int i = 0;
	while((read = getline(&line, &len, fp)) != -1)
	{	
		//Create a customer from a line in data
		while
	}
	
	fclose(fp);
	if(line)
		free(line);
	exit(EXIT_SUCCESS);
	return(0);	
}

