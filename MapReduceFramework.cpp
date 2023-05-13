//
// Created by Amit Roth on 08/05/2023.
//
#include <map>
#include <pthread.h>
#include <iostream>
#include <memory>

#include "Barrier.h"
#include "utils.h"

#include "MapReduceFramework.h"
#include "MapReduceJob.h"

void emit2 (K2* key, V2* value, void* context){
    auto threadContext = (ThreadContext *) context;
    threadContext->intermediateVec.push_back(IntermediatePair(key, value));
}

void emit3 (K3* key, V3* value, void* context){
    auto threadContext = (ThreadContext *) context;
    threadContext->mapReduceJob->outputVec.push_back(OutputPair(key, value));
}

JobHandle startMapReduceJob(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec, int multiThreadLevel){
    JobContext jobContext;
    jobContext.multiThreadLevel = multiThreadLevel;
    jobContext.jobState = {UNDEFINED_STAGE, 0};
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



