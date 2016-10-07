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
void* countLines (int sNumber, int * fileSize);
void writeToParent(struct threadArgs * parentArgs, int * fileSize);
void readFromChild(struct threadArgs * childArgs);
int processInformation(struct threadArgs * childArgs1, struct threadArgs * childArgs2, struct threadArgs * parentArgs);
void findTreeSizeArgs(int numOfFiles, int * treeSizeArg);
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
/*
	if(*mNumber == 1)
	{
		childArgs1.threadNumber = 1;
		childArgs2.threadNumber = 2;
	}
	else
	{
		childArgs1.threadNumber = 3;
		childArgs2.threadNumber = 4;
	}
*/
	childArgs1.threadNumber = (((*mNumber)*2)-1);
	childArgs2.threadNumber = ((*mNumber)*2);

	if((*parentArgs).treeSizeArg == 1)
	{
		createSorter(&childArgs1);
		createSorter(&childArgs2);
	}
	else
	{
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
	sleep(30);
	int * sNumber = &mergerArgs->threadNumber;
	printf("Sorter %d created\n", *sNumber);
	int fileSize = 0;	
	printf("%d counting lines\n", *sNumber);
	couldFindFile = countLines (*sNumber, &fileSize);

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
int countLines (int sNumber, int * fileSize, )
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
	sleep(10);
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


void findTreeSizeArgs(int numOfFiles, int * treeSizeArg)
{	
	(*treeSizeArg) = 1;	
	while(numOfFiles < (2**(*treeSizeArg)))
	{
		(*treeSizeArg)++;
	}

}

/*Sample Output
Sorter 4 created
4 counting lines
4 reading file
4 sorting file
4 sending files
Sorter 4 writing to parent
Sorter 4 writing to parent
Sorter 4 writing to parent
Sorter 4 writing to parent
Sorter 4 writing to parent
Sorter 4 writing to parent
Sorter 4 closed	File Size = 6
Sorter 1 created
1 counting lines
1 reading file
1 sorting file
1 sending files
Sorter 1 writing to parent
Sorter 1 writing to parent
Sorter 1 writing to parent
Sorter 1 writing to parent
Sorter 1 writing to parent
Sorter 1 writing to parent
Sorter 1 writing to parent
Sorter 1 writing to parent
Sorter 1 writing to parent
Sorter 1 writing to parent
Sorter 1 writing to parent
Sorter 1 writing to parent
Sorter 1 writing to parent
Sorter 1 writing to parent
Sorter 1 writing to parent
Sorter 1 writing to parent
Sorter 1 writing to parent
Sorter 1 writing to parent
Sorter 1 writing to parent
Sorter 1 writing to parent
Sorter 1 writing to parent
Sorter 1 writing to parent
Sorter 1 closed	File Size = 22
Sorter 3 created
3 counting lines
3 reading file
3 sorting file
3 sending files
Sorter 3 writing to parent
Sorter 3 writing to parent
Sorter 3 writing to parent
Sorter 3 writing to parent
Sorter 3 writing to parent
Sorter 3 writing to parent
Sorter 3 writing to parent
Sorter 3 writing to parent
Sorter 3 writing to parent
Sorter 3 writing to parent
Sorter 3 writing to parent
Sorter 3 writing to parent
Sorter 3 writing to parent
Sorter 3 writing to parent
Sorter 3 writing to parent
Sorter 3 writing to parent
Sorter 3 writing to parent
Sorter 3 writing to parent
Sorter 3 writing to parent
Sorter 3 writing to parent
Sorter 3 writing to parent
Sorter 3 writing to parent
Sorter 3 writing to parent
Sorter 3 writing to parent
Sorter 3 writing to parent
Sorter 3 closed	File Size = 25
Sorter 2 created
2 counting lines
2 reading file
2 sorting file
2 sending files
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 writing to parent
Sorter 2 closed	File Size = 35
Merger 2 writing to parent
Merger 2 writing to parent
Merger 2 writing to parent
Merger 2 writing to parent
Merger 2 writing to parent
Merger 2 writing to parent
Merger 2 writing to parent
Merger 2 writing to parent
Merger 2 writing to parent
Merger 2 writing to parent
Merger 2 writing to parent
Merger 2 writing to parent
Merger 2 writing to parent
Merger 2 writing to parent
Merger 2 writing to parent
Merger 2 writing to parent
Merger 2 writing to parent
Merger 2 writing to parent
Merger 2 writing to parent
Merger 2 writing to parent
Merger 2 writing to parent
Merger 2 writing to parent
Merger 2 writing to parent
Merger 2 writing to parent
Merger 2 writing to parent
Merger 2 writing to parent
Merger 2 writing to parent
Merger 2 writing to parent
Merger 2 writing to parent
Merger 2 writing to parent
Merger 2 writing to parent
Merger 2 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
Merger 1 writing to parent
transactionNum = 15, customerNum = 1006, amount = 143.280000
transactionNum = 1, customerNum = 1007, amount = 64.890000
transactionNum = 17, customerNum = 1016, amount = 83.200000
transactionNum = 25, customerNum = 1021, amount = 117.700000
transactionNum = 10, customerNum = 1021, amount = 54.370000
transactionNum = 6, customerNum = 1026, amount = 31.730000
transactionNum = 14, customerNum = 1030, amount = 17.950000
transactionNum = 7, customerNum = 1031, amount = 90.950000
transactionNum = 2, customerNum = 1038, amount = 98.700000
transactionNum = 33, customerNum = 1046, amount = 7.800000
transactionNum = 15, customerNum = 1050, amount = 26.430000
transactionNum = 11, customerNum = 1055, amount = 114.720000
transactionNum = 19, customerNum = 1057, amount = 131.430000
transactionNum = 9, customerNum = 1057, amount = 92.870000
transactionNum = 22, customerNum = 1067, amount = 131.740000
transactionNum = 12, customerNum = 1079, amount = 59.800000
transactionNum = 3, customerNum = 1087, amount = 16.710000
transactionNum = 1, customerNum = 1095, amount = 132.410000
transactionNum = 20, customerNum = 1099, amount = 92.430000
transactionNum = 22, customerNum = 1103, amount = 100.630000
transactionNum = 7, customerNum = 1107, amount = 108.980000
transactionNum = 2, customerNum = 1111, amount = 59.360000
transactionNum = 8, customerNum = 1129, amount = 13.870000
transactionNum = 18, customerNum = 1149, amount = 22.270000
transactionNum = 13, customerNum = 1153, amount = 47.430000
transactionNum = 31, customerNum = 1157, amount = 62.870000
transactionNum = 17, customerNum = 1158, amount = 119.830000
transactionNum = 5, customerNum = 1166, amount = 138.220000
transactionNum = 1, customerNum = 1166, amount = 121.580000
transactionNum = 30, customerNum = 1166, amount = 91.280000
transactionNum = 13, customerNum = 1172, amount = 17.620000
transactionNum = 21, customerNum = 1176, amount = 92.700000
transactionNum = 5, customerNum = 1183, amount = 24.600000
transactionNum = 14, customerNum = 1187, amount = 147.980000
transactionNum = 20, customerNum = 1188, amount = 149.730000
transactionNum = 5, customerNum = 1196, amount = 9.390000
transactionNum = 10, customerNum = 1196, amount = 119.410000
transactionNum = 4, customerNum = 1198, amount = 110.120000
transactionNum = 6, customerNum = 1205, amount = 106.310000
transactionNum = 11, customerNum = 1219, amount = 37.740000
transactionNum = 16, customerNum = 1224, amount = 110.980000
transactionNum = 9, customerNum = 1230, amount = 103.180000
transactionNum = 24, customerNum = 1243, amount = 60.300000
transactionNum = 3, customerNum = 1243, amount = 94.460000
transactionNum = 13, customerNum = 1246, amount = 147.000000
transactionNum = 3, customerNum = 1260, amount = 65.320000
transactionNum = 27, customerNum = 1262, amount = 87.980000
transactionNum = 29, customerNum = 1262, amount = 50.650000
transactionNum = 21, customerNum = 1272, amount = 102.300000
transactionNum = 8, customerNum = 1279, amount = 10.420000
transactionNum = 18, customerNum = 1279, amount = 6.100000
transactionNum = 1, customerNum = 1283, amount = 104.950000
transactionNum = 4, customerNum = 1292, amount = 135.420000
transactionNum = 8, customerNum = 1302, amount = 93.510000
transactionNum = 22, customerNum = 1306, amount = 9.000000
transactionNum = 7, customerNum = 1306, amount = 82.400000
transactionNum = 4, customerNum = 1315, amount = 77.550000
transactionNum = 6, customerNum = 1319, amount = 94.770000
transactionNum = 24, customerNum = 1324, amount = 109.980000
transactionNum = 5, customerNum = 1337, amount = 73.340000
transactionNum = 28, customerNum = 1350, amount = 110.150000
transactionNum = 32, customerNum = 1361, amount = 136.610000
transactionNum = 17, customerNum = 1364, amount = 133.750000
transactionNum = 11, customerNum = 1379, amount = 31.520000
transactionNum = 21, customerNum = 1381, amount = 31.260000
transactionNum = 10, customerNum = 1395, amount = 92.950000
transactionNum = 35, customerNum = 1399, amount = 124.220000
transactionNum = 26, customerNum = 1403, amount = 86.880000
transactionNum = 25, customerNum = 1405, amount = 117.830000
transactionNum = 16, customerNum = 1407, amount = 145.830000
transactionNum = 16, customerNum = 1409, amount = 71.930000
transactionNum = 3, customerNum = 1410, amount = 92.440000
transactionNum = 2, customerNum = 1411, amount = 15.370000
transactionNum = 19, customerNum = 1412, amount = 78.580000
transactionNum = 34, customerNum = 1413, amount = 57.820000
transactionNum = 12, customerNum = 1424, amount = 63.840000
transactionNum = 23, customerNum = 1426, amount = 13.000000
transactionNum = 23, customerNum = 1431, amount = 127.800000
transactionNum = 2, customerNum = 1433, amount = 8.160000
transactionNum = 12, customerNum = 1434, amount = 80.270000
transactionNum = 6, customerNum = 1438, amount = 76.340000
transactionNum = 6, customerNum = 1438, amount = 76.340000
transactionNum = 4, customerNum = 1450, amount = 5.130000
transactionNum = 15, customerNum = 1454, amount = 53.130000
transactionNum = 18, customerNum = 1479, amount = 118.940000
transactionNum = 19, customerNum = 1479, amount = 100.310000
transactionNum = 19, customerNum = 1479, amount = 100.310000
transactionNum = 14, customerNum = 1480, amount = 7.400000
transactionNum = 14, customerNum = 1480, amount = 7.400000
transactionNum = 9, customerNum = 1495, amount = 119.200000
transactionNum = 9, customerNum = 1495, amount = 119.200000


Total Customers = 91
*/
