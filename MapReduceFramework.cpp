//
// Created by Amit Roth on 08/05/2023.
//
#include <map>
#include <pthread.h>
#include <iostream>
#include <memory>

#include "Barrier.h"

#include "MapReduceFramework.h"
#include "MapReduceJob.h"


enum ERR{SYS_ERR, UTHREADS_ERR};

int error(ERR err, const std::string& text);
void *thread_wrapper(void *input);


/**
 * PUBLIC FUNCTIONS
 */
void emit2 (K2* key, V2* value, void* context){
    auto threadContext = (ThreadContext *) context;
    threadContext->intermediateVec.push_back(IntermediatePair(key, value));
}

void emit3 (K3* key, V3* value, void* context){
    auto threadContext = (ThreadContext *) context;
    threadContext->mapReduceJob->outputVec.push_back(OutputPair(key, value));
}

JobHandle startMapReduceJob(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec, int multiThreadLevel){
    new MapReduceJob(client, inputVec, outputVec, multiThreadLevel);
}

void waitForJob(JobHandle job){
    // send to pthreads join - the thread from JobsVector[job]
}

void getJobState(JobHandle job, JobState* state){
    *state = ((MapReduceJob*)job)->getJobState();
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

void mutex_lock(pthread_mutex_t *mutex){
    if (pthread_mutex_lock(mutex) != 0){
        error(SYS_ERR, "mutex error");
    }
}

void mutex_unlock(pthread_mutex_t *mutex){
    if (pthread_mutex_unlock(mutex) != 0){
        error(SYS_ERR, "mutex error");
    }
}

void *thread_wrapper(void *input){
    // map - takes <k1, v1> from input and apply map function
    auto threadContext = (ThreadContext *) input;
    auto mutex = threadContext->jobContext->mutex;
    auto jobContext =  threadContext->jobContext;
    jobContext->jobState.stage = MAP_STAGE;
    mutex_lock(&mutex);
    while (jobContext->inputVec.empty()){
        auto inputPair = jobContext->inputVec.back();
        jobContext->inputVec.pop_back();
        mutex_unlock(&mutex);
        jobContext->client->map(inputPair.first, inputPair.second, threadContext);
        mutex_lock(&mutex);
    }
    mutex_unlock(&mutex);

    // when map process is done IN ALL THREADS - proceed
    jobContext->barrier->barrier();
    // shufle - only 1 thread
    if (threadContext->id == 0){
        // Shuffle
        for (int i=0; i < jobContext->multiThreadLevel; i++){

        }
    }

    // reduce - takes <k2, v2> form intermediat, process it and save in output vec
    return input;
}