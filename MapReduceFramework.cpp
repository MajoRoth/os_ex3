//
// Created by Amit Roth on 08/05/2023.
//
#include <map>
#include <pthread.h>

#include "MapReduceFramework.h"


enum ERR{SYS_ERR, UTHREADS_ERR};

typedef struct {
    JobState job_state;
} JobContext;

int error(ERR err, const std::string& text);


/**
 * PUBLIC FUNCTIONS
 */



map<InputVec> split_inputs;

JobHandle startMapReduceJob(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec, int multiThreadLevel){
    // Create vectors as the number of threads with size size(input) / threads
    for (InputPair: inputVec) {


    }

    // Creates the threads
    for (int i = 0; i < multiThreadLevel; i++){
        pthread_attr_t attr
        auto start_routine = client.map;
        auto arg =
        pthread_create()
    }


}



/**
* PRIVATE FUNCTIONS
*/

int error(ERR err, const std::string& text){
    if (err == SYS_ERR){
        std::cerr << "system error: " << text <<std::endl;
        exit(1);
    }
    else{
        std::cerr << "thread library error: " << text <<std::endl;
        return -1;
    }
}