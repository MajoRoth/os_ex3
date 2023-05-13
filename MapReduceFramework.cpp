//
// Created by Amit Roth on 08/05/2023.
//
#include <map>
#include <pthread.h>
#include <iostream>
#include <memory>

#include "Barrier.h"

#include "MapReduceFramework.h"


enum ERR{SYS_ERR, UTHREADS_ERR};

std::vector<JobContext> jobsVector;


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
    threadContext->jobContext->outputVec.push_back(OutputPair(key, value));
}

JobHandle startMapReduceJob(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec, int multiThreadLevel){
    JobContext jobContext;
    jobContext.multiThreadLevel = multiThreadLevel;
    jobContext.job_state = {UNDEFINED_STAGE, 0};
    jobContext.threads = std::unique_ptr<pthread_t[]>(new pthread_t[multiThreadLevel]);
    jobContext.barrier = std::unique_ptr<Barrier>(new Barrier(multiThreadLevel));

    // split inputs and create threads
    for (int i=0; i < multiThreadLevel; i++){
        std::cout << "Created Thread: " << i << std::endl;
        ThreadContext threadContext;
        threadContext.jobContext = &jobContext;
        threadContext.id = i;
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
    jobContext->job_state.stage = MAP_STAGE;
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