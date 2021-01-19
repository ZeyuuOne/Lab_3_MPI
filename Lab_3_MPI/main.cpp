#include "stdio.h"
#include "stdlib.h"
#include "io.h"
#include "string"
#include "vector"
#include "unordered_map"
#include "fstream"
#include "mpi.h"
using namespace std;

#define DICT_SIZE_MSG 0
#define FILE_NAME_MSG 1
#define VECTOR_MSG 2
#define EMPTY_MSG 3
#define MAX_WORD_LEN 20

int myId, numProcs;
int dictSize, terminated, assignedCount;
int workerId, fileNameLen;
int* assigned;
char* dictList, * fileName;
vector<string> docList;
vector<string> dictVector;
unordered_map<string, int> dictMap;
ifstream dictFile;
ofstream outFile;
double startTime, endTime;
unsigned char* result, * buffer, * resultVector;
MPI_Comm workerComm;
MPI_Request pending;
MPI_Status status;

int main(int argc, char** argv)
{
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &numProcs);
	MPI_Comm_rank(MPI_COMM_WORLD, &myId);

	if (numProcs < 2) {
		printf("Program needs at least 2 threads.\n");
		return -1;
	}

	if (myId == 0) {

		MPI_Comm_split(MPI_COMM_WORLD, MPI_UNDEFINED, myId, &workerComm);
		MPI_Irecv(&dictSize, 1, MPI_INT, MPI_ANY_SOURCE, DICT_SIZE_MSG, MPI_COMM_WORLD, &pending);

		struct _finddata_t file;
		intptr_t handle;
		if ((handle = _findfirst(".\\test\\*", &file)) != -1) {
			do {
				if ((file.attrib & _A_SUBDIR)) continue;
				string s(".\\test\\");
				s += file.name;
				docList.push_back(s);
			} while (_findnext(handle, &file) == 0);
			_findclose(handle);
		}

		MPI_Wait(&pending, &status);
		buffer = (unsigned char*)malloc(dictSize * sizeof(MPI_UNSIGNED_CHAR));
		startTime = MPI_Wtime();

		result = (unsigned char*)malloc(sizeof(unsigned char) * docList.size() * dictSize);
		memset(result, 0, sizeof(unsigned) * docList.size() * dictMap.size());

		terminated = 0;
		assignedCount = 0;
		assigned = (int*)malloc(numProcs * sizeof(int));

		do {

			MPI_Recv(buffer, dictSize, MPI_UNSIGNED_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

			if (status.MPI_TAG == VECTOR_MSG) {
				for (int i = 0; i < dictSize; i++) result[assigned[status.MPI_SOURCE] * dictSize + i] = buffer[i];
			}

			if (assignedCount < docList.size()) {
				const char* ch = docList[assignedCount].c_str();
				MPI_Send(ch, strlen(ch) + 1, MPI_CHAR, status.MPI_SOURCE, FILE_NAME_MSG, MPI_COMM_WORLD);
				assigned[status.MPI_SOURCE] = assignedCount;
				assignedCount++;
			}
			else {
				MPI_Send(NULL, 0, MPI_CHAR, status.MPI_SOURCE, FILE_NAME_MSG, MPI_COMM_WORLD);
				terminated++;
			} 

		} while (terminated < numProcs - 1);

		endTime = MPI_Wtime();

		outFile.open("out.txt");
		for (int i = 0; i < docList.size(); i++) {
			for (int j = 0; j < dictSize; j++) {
				outFile << (unsigned int)result[i * dictSize + j] << " ";
			}
			outFile << endl;
		}
		outFile.close();

		printf("Thread Num: %d, Time: %fs\n", numProcs, endTime - startTime);

		free(buffer);
		free(result);
		free(assigned);

	}
	else {

		MPI_Comm_split(MPI_COMM_WORLD, 0, myId, &workerComm);

		MPI_Comm_rank(workerComm, &workerId);

		MPI_Isend(NULL, 0, MPI_UNSIGNED_CHAR, 0, EMPTY_MSG, MPI_COMM_WORLD, &pending);

		if (workerId == 0) {
			dictFile.open("dict.txt");
			while (!dictFile.fail()) {
				string s;
				dictFile >> s;
				if (!s.empty()) dictVector.push_back(s);
			}
			dictFile.close();

			dictList = (char*)malloc(dictVector.size() * MAX_WORD_LEN * sizeof(char));
			for (int i = 0; i < dictVector.size(); i++) {
				dictMap[dictVector[i]] = i;
				strcpy(&dictList[i * MAX_WORD_LEN], dictVector[i].c_str());
			}

			dictSize = dictVector.size();
		}
		
		MPI_Bcast(&dictSize, 1, MPI_INT, 0, workerComm);
		if (workerId != 0) dictList = (char*)malloc(dictSize * MAX_WORD_LEN * sizeof(char));
		resultVector = (unsigned char*)malloc(dictSize * sizeof(unsigned char));
		MPI_Bcast(dictList, dictSize * MAX_WORD_LEN, MPI_CHAR, 0, workerComm);

		if (workerId != 0) {
			for (int i = 0; i < dictSize; i++) {
				string s = string(&dictList[i * MAX_WORD_LEN]);
				dictMap[s] = i;
			}
		}

		if (workerId == 0) MPI_Send(&dictSize, 1, MPI_INT, 0, DICT_SIZE_MSG, MPI_COMM_WORLD);

		while (1) {
			MPI_Probe(0, FILE_NAME_MSG, MPI_COMM_WORLD, &status);
			MPI_Get_count(&status, MPI_CHAR, &fileNameLen);
			if (fileNameLen == 0) break;

			fileName = (char*)malloc(fileNameLen);
			MPI_Recv(fileName, fileNameLen, MPI_CHAR, 0, FILE_NAME_MSG, MPI_COMM_WORLD, &status);

			ifstream docFile(fileName);
			memset(resultVector, 0, dictSize * sizeof(unsigned char));
			while (!docFile.fail()) {
				string s;
				getline(docFile, s, ' ');
				if (dictMap.find(s) == dictMap.end()) continue;
				resultVector[dictMap[s]]++;
			}
			docFile.close();

			free(fileName);
			MPI_Send(resultVector, dictSize, MPI_UNSIGNED_CHAR, 0, VECTOR_MSG, MPI_COMM_WORLD);
		}

		free(dictList);
		free(resultVector);

	}

	MPI_Finalize();
	return 0;
}