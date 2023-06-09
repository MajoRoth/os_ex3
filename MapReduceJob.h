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

struct
{
    bool operator()(std::pair<K2*, V2*> a, std::pair<K2*, V2*> b) const { return *(a.first) < *(b.first); }
}
pairPtrComparison;

struct k2PtrComparison
{
    bool operator()(K2* const&a, K2* const&b) const { return *a < *b; }
};

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
    OutputVec &outputVec;
    std::map<K2*, IntermediateVec*, k2PtrComparison> intermediateMap;
    std::vector<IntermediateVec> afterShuffleVec;

private:
    int multiThreadLevel;
    pthread_t *threads;
    ThreadContext *contexts;
    pthread_mutex_t mutex;
    Barrier barrier;
    JobState jobState;
    std::atomic<uint64_t> progress;
    bool hasWaited;



public:
    MapReduceJob(const MapReduceClient& mapReduceClient, const InputVec& inputVec,
                 OutputVec& outputVec, int multiThreadLevel);
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


    InputPair popInputPair(){
        auto inputPair = inputVec.back();
        inputVec.pop_back();
        return inputPair;
    }

    void mutex_unlock();
    void mutex_lock();
    void debug();



private:
    void apply_barrier();
    static void *thread_wrapper(void *input);

    int getAfterShuffleVecLen();
    int getIntermediateVecLen();

    int allIntermediateVecsAreEmpty();
};


#endif //OS_EX3_MAPREDUCEJOB_H
