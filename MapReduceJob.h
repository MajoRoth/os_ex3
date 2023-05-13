//
// Created by amitroth on 5/13/23.
//
#include <map>
#include <iostream>
#include <atomic>

#include "Barrier.h"

#include "MapReduceFramework.h"


#ifndef OS_EX3_MAPREDUCEJOB_H
#define OS_EX3_MAPREDUCEJOB_H

class MapReduceJob;

class ThreadContext {
public:
    int id;
    IntermediateVec intermediateVec;
    MapReduceJob *mapReduceJob;

    ThreadContext(int id, MapReduceJob *map) : id(id), mapReduceJob(map) {};
    ThreadContext() = default;

    int getId(){
        return id;
    }
};

class MapReduceJob {
public:
    const MapReduceClient &client;
    InputVec inputVec;
    OutputVec outputVec;
    std::map<K2 *, IntermediateVec *> intermediateMap;

private:
    int multiThreadLevel;
    pthread_t *threads;
    ThreadContext *contexts;
    pthread_mutex_t mutex;
    Barrier barrier;
    JobState jobState;
    std::atomic<int> size;


public:
    MapReduceJob(const MapReduceClient& mapReduceClient, const InputVec& inputVec,
                 const OutputVec& outputVec, int multiThreadLevel);
    ~MapReduceJob();

    void waitForJob();

    // setters
    void setJobStage(stage_t stage){
        mutex_lock();
        jobState.stage = stage;
        switch (jobState.stage) {
            case UNDEFINED_STAGE:
                break;
            case MAP_STAGE:
                size = inputVec.size();
                break;
            case SHUFFLE_STAGE:
                size = getIntermediateVecLen();
                break;
            case REDUCE_STAGE:
                size = getIntermediateMapLen();
                break;
        }

        mutex_unlock();
    }

    // getters
    const MapReduceClient &getClient() const{
        return client;
    }
    stage_t getJobStage() const{
        return jobState.stage;
    }
    JobState getJobState()
    {
        jobState.percentage = getPercentage();
        return jobState;
    }
    int getMultiThreadLevel(){
        return multiThreadLevel;
    }
    float getPercentage() const;


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

    int getIntermediateMapLen() const;
    int getIntermediateVecLen() const;
};


#endif //OS_EX3_MAPREDUCEJOB_H
