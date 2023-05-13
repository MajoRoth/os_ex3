//
// Created by amitroth on 5/13/23.
//
#include <map>
#include <pthread.h>
#include <iostream>
#include <memory>

#include "Barrier.h"

#include "MapReduceFramework.h"


#ifndef OS_EX3_MAPREDUCEJOB_H
#define OS_EX3_MAPREDUCEJOB_H

class MapReduceJob;

class ThreadContext {
public:
    IntermediateVec intermediateVec;
    MapReduceJob *mapReduceJob;
    int id;

    ThreadContext(int id, MapReduceJob *map) : id(id), mapReduceJob(map) {};
    ThreadContext() = default;
};

class MapReduceJob {
public:
    const MapReduceClient &client;
    InputVec inputVec;
    OutputVec outputVec;
    int multiThreadLevel;

    JobState jobState;
    pthread_t *threads;
    ThreadContext *contexts;
    pthread_mutex_t mutex;
    Barrier barrier;

    MapReduceJob(const MapReduceClient& mapReduceClient, const InputVec& inputVec,
                 const OutputVec& outputVec, int multiThreadLevel);

    // setters
    void setJobStage(stage_t stage){
        job_state.stage = stage;
    }

    // getters
    const MapReduceClient &getClient() const{
        return client;
    }
    stage_t getJobStage() const{
        return job_state.stage;
    }

    InputPair popInputPair(){
        auto inputPair = inputVec.back();
        inputVec.pop_back();
        return inputPair;
    }



private:
    void mutex_unlock();
    void mutex_lock();
    void apply_barrier();
    static void *thread_wrapper(void *input);
};


#endif //OS_EX3_MAPREDUCEJOB_H
