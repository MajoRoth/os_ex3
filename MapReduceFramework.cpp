//
// Created by Amit Roth on 08/05/2023.
//
#include <map>
#include <pthread.h>
#include <iostream>
#include <memory>

#include "MapReduceFramework.h"


enum ERR{SYS_ERR, UTHREADS_ERR};

typedef struct {
    JobState job_state;
    std::unique_ptr<pthread_t[]> threads;
    int multiThreadLevel;

} JobContext;



typedef struct {
    InputVec input;
    OutputVec output;
    MapReduceClient *client;
} GlobalContext;

typedef struct {
    IntermediateVec intermediateVec;
    GlobalContext *globalContext;
} ThreadContext;

std::vector<JobContext> jobsVector;


int error(ERR err, const std::string& text);
void *thread_wrapper(void *input);


/**
 * PUBLIC FUNCTIONS
 */


JobHandle startMapReduceJob(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec, int multiThreadLevel){
    JobContext jobContext;
    jobContext.multiThreadLevel = multiThreadLevel;
    jobContext.job_state = {UNDEFINED_STAGE, 0};
    jobContext.threads = std::unique_ptr<pthread_t[]>(new pthread_t[multiThreadLevel]);

    GlobalContext globalContext; // Define the variables

    // split inputs and create threads
    for (int i=0; i < multiThreadLevel; i++){
        std::cout << "Created Thread: " << i << std::endl;
        ThreadContext threadContext;
        threadContext.globalContext = &globalContext;
        // need to do manipulation with casting
        // erel - maybe need to save context array
        if(pthread_create(&jobContext.threads[i], NULL, thread_wrapper, threadContext)){
            // error
        }

    }

    jobsVector.push_back(jobContext);


    return &jobContext; //cast: note that you need to ive ponter from the vector
}

void waitForJob(JobHandle job){
    // send to pthreads join - the thread from JobsVector[job]
}

void getJobState(JobHandle job, JobState* state){
    // returns JobState
}

void closeJobHandle(JobHandle job){
    // remove from the vector
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

void *thread_wrapper(void *input){
    // map - takes <k1, v1> from input and apply map function

    // when map process is done IN ALL THREADS - proceed

    // shufle - only 1 thread

    // reduce - takes <k2, v2> form intermediat, process it and save in output vec
    return input;
}