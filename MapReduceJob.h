//
// Created by amitroth on 5/13/23.
//
#include <map>
#include <iostream>
#include <atomic>

#include "Barrier.h"
#include <pthread.h>
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
    std::atomic<uint64_t> progress;


public:
    MapReduceJob(const MapReduceClient& mapReduceClient, const InputVec& inputVec,
                 const OutputVec& outputVec, int multiThreadLevel);
    ~MapReduceJob();

    void waitForJob();

    // setters
    void setJobStage(stage_t stage);

    // getters
    const MapReduceClient &getClient() const{
        return client;
    }
    stage_t getJobStage() const{
        return jobState.stage;
    }
    JobState getJobState();

    int getMultiThreadLevel(){
        return multiThreadLevel;
    }
    float getPercentage();


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

    int getIntermediateMapLen();
    int getIntermediateVecLen();


//    stage_t getAtomicStage(){
//        unsigned long long MASK = 3;
//        return (stage_t) (atomic_variable & MASK);
//    }
//    int getAtomicSize(){
//        unsigned long long MASK = 8589934588;
//        return (stage_t) (atomic_variable & MASK) >> 2;
//    }
//    int getAtomicCurrentSize(){
//        unsigned long long MASK = 18446744065119617024;
//        return (stage_t) (atomic_variable & MASK) >> 33;
//    }
//
//    int incAtomicCurrentSize(){
//
//    }
//
//    void setAtomicStage(stage_t stage){
//
//    }
//    void setAtomicSize(){
//
//    }
//    void setAtomicCurrentSize(){
//
//    }
};


#endif //OS_EX3_MAPREDUCEJOB_H
