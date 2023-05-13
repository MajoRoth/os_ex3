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
    new MapReduceJob(client, inputVec, outputVec, multiThreadLevel);
}

void waitForJob(JobHandle job){
    static_cast<MapReduceJob*>(job)->waitForJob();
}

void getJobState(JobHandle job, JobState* state){
    *state = static_cast<MapReduceJob*>(job)->getJobState();
}

void closeJobHandle(JobHandle job){
    delete (MapReduceJob*)job;
}



