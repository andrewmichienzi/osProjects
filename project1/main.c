#include <pthread.h> 
#include <stdio.h> 
#include <errno.h> 
#include <unistd.h> 
#include <stdlib.h> 
#include <string.h>
#include <signal.h>
#define false 0
#define true 1
#define READ 0
#define WRITE 1

struct Customer
{
	int transactionNum;
	int customerNum;
	double amount;
};

struct threadArgs
{
	int threadNumber;
	int flag[2];
	int turn;
	int ready;
	int open;
	int child;
	int custPtr;
	struct Customer nextCustomer;
	int fd[2];
	int isMaster;
	int treeSizeArg;
};

void* merger(void*);
void* sorter(void*);
void* createMerger(struct threadArgs *mergerArgs);
void* createSorter(struct threadArgs *sorterArgs);
void* readDataFile(struct Customer *customers, int sNumber);
void* sortCustomers(struct Customer *customers, int * fileSize);
void* printCustomers(struct Customer *customers, int fileSize);
void* printCustomer(struct Customer *customer);
int countLines (int sNumber, int * fileSize);
void writeToParent(struct threadArgs * parentArgs, int * fileSize);
void readFromChild(struct threadArgs * childArgs);
int processInformation(struct threadArgs * childArgs1, struct threadArgs * childArgs2, struct threadArgs * parentArgs);
void findTreeSizeArgs(int numOfFiles, int * treeSizeArg);
int power(int base, int exp);
struct Customer* getNextCustomer(struct threadArgs * args1, struct threadArgs * args2);
/*
 * A file sorter with ability to sort 4 data files based on customer number from smallest customer number to largest. The sorter processes use bubble sort to sort the customer numbers, then send one customer up at a time to the merger. The merger has two children which it evaluates the 2 customers that are passed up to it and finds the smallest customer number, sending it to the master. The master again has 2 children that it finds the smallest customer number between the 2 and prints the smallest.
 */
int main(int argc, char* argv[]) 
{ 


	//int status;
	struct threadArgs childArgs1 = {1,{false, false}, 0, 1, 1, 1};
	struct threadArgs childArgs2 = {2, {false, false}, 0, 1, 1, 1};	
	struct threadArgs masterArgs = {0, {false, false}, 0, 1, 1, 1};
	masterArgs.isMaster = 1;
	childArgs1.isMaster = 0;
	childArgs1.isMaster = 0;
	int treeSizeArg;
	if(argc == 2)
	{
		int numOfFiles = atoi(argv[1]);
		findTreeSizeArgs(numOfFiles, &treeSizeArg);
	}
	
	else
	{
		treeSizeArg = 1;	
	}

	childArgs1.treeSizeArg = treeSizeArg;
	childArgs2.treeSizeArg = treeSizeArg;

	createMerger(&childArgs1);
	createMerger(&childArgs2);
	
	int fileSize;
	fileSize = processInformation(&childArgs1, &childArgs2, &masterArgs);
	printf("\n\nTotal Customers = %d\n", fileSize);
	return(0);
}

/*
 * This function creates a merger process and sends it to the merger function
 */
void* createMerger(struct threadArgs *mergerArgs)
{	

	pid_t pid;
	if(pipe((*mergerArgs).fd) < 0) 
	{
		perror("plumbing error");
		exit(1);
	}
	
	if((pid=fork())<0) 
	{
		perror("fork failed");
		exit(1);
	}

	if(pid == 0)
	{
		//child
		close ((*mergerArgs).fd[READ]);
		merger(mergerArgs);
		exit(0);
	}
	
	else
	{
		//parent
		close((*mergerArgs).fd[WRITE]);
	}
	return(0);
}

/*
 * This function runs the merger processes and creates 2 sorters and then runs the processing information
 */
void* merger(void* arg)
{
	struct threadArgs * parentArgs = (struct threadArgs *)arg;
	int * mNumber = &parentArgs->threadNumber;
	printf("Merger %d created\n", *mNumber);
	struct threadArgs childArgs1 = {0, {false, false}, 0, 1, 1, 1};
	struct threadArgs childArgs2 = {0, {false, false}, 0, 1, 1, 2};
	childArgs1.isMaster = 0;
	childArgs2.isMaster = 0;
	childArgs1.threadNumber = (((*mNumber)*2)-1);
	childArgs2.threadNumber = ((*mNumber)*2);

	if((*parentArgs).treeSizeArg == 1)
	{
		printf("if\n");
		createSorter(&childArgs1);
		createSorter(&childArgs2);
	}
	else
	{
		printf("else\n");
		childArgs1.treeSizeArg = (parentArgs->treeSizeArg - 1);
		childArgs2.treeSizeArg = (parentArgs->treeSizeArg - 1);
		createMerger(&childArgs1);
		createMerger(&childArgs2);
	}
	
	processInformation(&childArgs1, &childArgs2, parentArgs);
		
	return(0);
}

/*
 * creates a sorter process
 */
void* createSorter(struct threadArgs *sorterArgs)
{
	pid_t pid;
	if(pipe((*sorterArgs).fd) < 0) 
	{
		perror("plumbing error");
		exit(1);
	}
	
	if((pid=fork())<0) 
	{
		perror("fork failed");
		exit(1);
	}

	if(pid == 0)
	{
		//child
		close ((*sorterArgs).fd[READ]);
		sorter(sorterArgs);
		exit(0);
	}
	
	else
	{
		//parent
		close((*sorterArgs).fd[WRITE]);
	}
	return(0);
}
	
/*
 *Counts the lines in the necessary file, puts the customers from the file in an array, sorts the customer and then writes one customer at a time to it's parent merger
 */
void* sorter(void* arg)
{
	int couldFindFile;
	struct threadArgs * mergerArgs = (struct threadArgs *)arg;
	//sleep(30);
	int * sNumber = &mergerArgs->threadNumber;
	printf("Sorter %d created\n", *sNumber);
	int fileSize = 0;	
	printf("%d counting lines\n", *sNumber);
	couldFindFile = (int)countLines (*sNumber, &fileSize);

	if(!couldFindFile)
	{	
		close((*mergerArgs).fd[WRITE]);
		return 0;
	}
	struct Customer * customers = malloc(fileSize*sizeof(struct Customer));
	printf("%d reading file\n", *sNumber);
	readDataFile(customers, *sNumber);
	printf("%d sorting file\n", *sNumber);
	sortCustomers(customers, &fileSize);
	//printCustomers(customers, fileSize);
	int i;
	printf("%d sending files\n", *sNumber);
	for(i = 0; i < fileSize; i++)
	{	
		write((*mergerArgs).fd[WRITE], (customers + i), sizeof(struct Customer));
		printf("Sorter %d writing to parent\n", (*mergerArgs).threadNumber);
	}
	close((*mergerArgs).fd[WRITE]);
	printf("Sorter %d closed\tFile Size = %d\n",*sNumber, fileSize);
	return(0);
}
/*
 * counts the number of lines in the file
 */
int countLines (int sNumber, int * fileSize)
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
		printf("Could not find file_%d.dat\n", sNumber);
		fflush(stdout);
		return 0;
	}
	while((read = getline(&line, &len, fp)) != -1)
	{	
		(*fileSize)++;
	}
	
	fclose(fp);
	if(line)
		free(line);
	return (1);		
}

/*
 * Reads information from file and then puts said information into the customer array
 */
void* readDataFile(struct Customer *customers, int sNumber)
{
	char * line = NULL;
	FILE * fp;
	size_t len = 0;
	ssize_t read;
	int linePtr;
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
		delim = strtok(line, " ");
		int column = 0;
		while(delim != NULL)
		{
			
			if(column == 0)
			{			//Trans Num
				sscanf(delim, "%d", &((customers + linePtr)->transactionNum));
			}
			else if(column == 1)
			{			//Cust Num
				sscanf(delim, "%d", &(customers + linePtr)->customerNum);
			}
			else
			{			//Amount
				sscanf(delim, "%lf", &(customers + linePtr)->amount);			
			}
			column++;
			delim = strtok(NULL, " ");
		}	
		linePtr++;
	}
	
	fclose(fp);
	if(line)
		free(line);
	//sleep(10);
	return (0);
}

/*
 * Sorts customers in customer array using bubble sort
 */
void* sortCustomers(struct Customer *customers, int * fileSize)
{
	struct Customer temp;
	int i, j, swaps;
	for(i=0; i<(*fileSize)-2; i++)
	{
		swaps = 0;
		for (j=0; j<(*fileSize)-1; j++)
		{
			if((*(customers+j)).customerNum > (*(customers+j+1)).customerNum)
			{	
				temp = *(customers+j);
				*(customers + j) = *(customers+j+1);
				*(customers + j + 1) = temp;
				swaps++;
			}
		}
		if(swaps == 0){
			break;
		}
	}
	return(0);
}

/*
 * Prints a customer
 */
void* printCustomers(struct Customer *customers, int fileSize)
{
	int i;
	for(i = 0; i < fileSize; i++)
	{
		printf("customerNum = %d\n",(*(customers+i)).customerNum); 
	}
	return(0);	
}

/*
 * Prints array of customers FOR TESTING
 */
void* printCustomer(struct Customer * customer)
{
	printf("transactionNum = %d, customerNum = %d, amount = %lf\n", (*customer).transactionNum, (*customer).customerNum, (*customer).amount);
	return(0);
}

/*
 * Takes in 2 threadArgs. Returns the customer in the args with the smallest customer Num.  The ready allows for reading of the customer that was used in the function processInformation
 */
struct Customer * getNextCustomer(struct threadArgs * args1, struct threadArgs * args2)
{
	if((*args1).nextCustomer.customerNum < (*args2).nextCustomer.customerNum)
	{
		//args1
		(*args1).ready = 1;
		(*args2).ready = 0;
		return &args1->nextCustomer;
	}

	else
	{
		//args2	
		(*args2).ready = 1;
		(*args1).ready = 0;
		return &args2->nextCustomer;
	}
	return(0);
}

/*
 * Processes information from child args. It will read while children are writing and send the appropriate customer up to the parent arguments.  Master and Merger processes use this function
 */
int processInformation(struct threadArgs * childArgs1, struct threadArgs * childArgs2, struct threadArgs * parentArgs)
{	
	int fileSize = 0;
	do
	{
		//sleep(1);
		readFromChild(childArgs1);
		readFromChild(childArgs2);
		struct Customer * cust = getNextCustomer(childArgs1, childArgs2);
		parentArgs->nextCustomer = *cust;
		if(childArgs1->open || childArgs2->open)
			writeToParent(parentArgs, &fileSize);

	} while((*childArgs1).open && (*childArgs2).open);
	
	while((*childArgs1).open && !(*childArgs2).open)
	{
		(*childArgs1).ready = 1;
		readFromChild(childArgs1);
		parentArgs->nextCustomer = (*childArgs1).nextCustomer;
		if(childArgs1->open)
			writeToParent(parentArgs, &fileSize);
	}
	close((*childArgs1).fd[READ]);
	while((*childArgs2).open && !(*childArgs1).open)
	{
		(*childArgs2).ready = 1;
		readFromChild(childArgs2);
		parentArgs->nextCustomer = (*childArgs2).nextCustomer;
		if(childArgs2->open)
			writeToParent(parentArgs, &fileSize);
	}
	close((*childArgs2).fd[READ]);
	return fileSize;
}

/*
 * This function reads from the pipe of a child process. Used by Master and Merger processes
 */
void readFromChild(struct threadArgs * childArgs)
{
	if((*childArgs).ready)
		(*childArgs).open = read((*childArgs).fd[READ], &childArgs->nextCustomer, sizeof(struct Customer));
}

/*
 * This process writes a customer to the parent process. Used by Merger and Master processes
 */
void writeToParent(struct threadArgs * parentArgs, int * fileSize)
{
	if((*parentArgs).isMaster)
	{
		(*fileSize)++;
		printCustomer(&(*parentArgs).nextCustomer);	
	}
	else
	{
		printf("Merger %d writing to parent\n", (*parentArgs).threadNumber);
		write((*parentArgs).fd[WRITE], &(*parentArgs).nextCustomer, sizeof(struct Customer));
		(*fileSize)++;
	}
}

/*
 * This function takes the amount of files needed to sort and determines treeSizeArg, which is stored in the Customer struct and is used to determine how many mergers will be needed.
 */
void findTreeSizeArgs(int numOfFiles, int * treeSizeArg)
{	
	(*treeSizeArg) = 1;
	int p;
	p = power(2, 1);
	while(numOfFiles > p)
	{
		(*treeSizeArg)++;
		p = power(2, (*treeSizeArg));	
	}
	(*treeSizeArg)--;
}
/*
 * Got from stack overflow
 * stackoverflow.com/questions/213042/how-do-you-do-exponentation-in-c
 * user: ephemient
 * Probably could have figured this out on my own, but the internet
 */
int power(int base, int exp)
{	
	if(exp == 0)
		return 1;
	else if(exp % 2)
		return base * power(base, exp - 1);
	else
	{
		int temp = power(base, exp/2);
		return temp * temp;
	}
}


/*Samples Output

*/
