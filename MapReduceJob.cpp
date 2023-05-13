//
// Created by amitroth on 5/13/23.
//
#include "MapReduceFramework.h"
#include "MapReduceJob.h"
#include "utils.h"

#include <memory>

void MapReduceJob::mutex_lock() {
    if(pthread_mutex_lock(&mutex)){
        error(SYS_ERR, "pthread mutex lock");
    }
}

void MapReduceJob::mutex_unlock() {
    if(pthread_mutex_lock(&mutex)){
        error(SYS_ERR, "pthread mutex unlock");
    }
}

MapReduceJob::MapReduceJob(const MapReduceClient& mapReduceClient, const InputVec& inputVec,
                           const OutputVec& outputVec, int multiThreadLevel)
                           : client(mapReduceClient), inputVec(inputVec), outputVec(outputVec),
                           multiThreadLevel(multiThreadLevel), mutex(PTHREAD_MUTEX_INITIALIZER),
                           barrier(multiThreadLevel), jobState({UNDEFINED_STAGE, 0})
{
    threads = new pthread_t[multiThreadLevel];
    contexts = new ThreadContext[multiThreadLevel];

    for (int tid = 0; tid < multiThreadLevel; tid++)
    {
        contexts[tid] = ThreadContext(tid, this);
        pthread_create(threads + tid, nullptr, thread_wrapper, contexts + tid);
    }
}
MapReduceJob::~MapReduceJob()
{
    //todo: what else?
    delete[] threads;
    delete[] contexts;
}

void *MapReduceJob::thread_wrapper(void *input) {
    // map - takes <k1, v1> from input and apply map function
    auto threadContext = (ThreadContext *) input;
    auto mapReduceJob = threadContext->mapReduceJob;

    mapReduceJob->setJobStage(MAP_STAGE);
    mapReduceJob->mutex_lock();

    while (!mapReduceJob->inputVec.empty()){
        auto inputPair = mapReduceJob->popInputPair();
        mapReduceJob->mutex_unlock();
        mapReduceJob->getClient().map(inputPair.first, inputPair.second, threadContext);
        mapReduceJob->mutex_lock();
    }
    mapReduceJob->mutex_unlock(); // Finished Map

    mapReduceJob->apply_barrier(); // Applying barrier to perform shuffle
    mapReduceJob->setJobStage(SHUFFLE_STAGE);
    if (threadContext->getId() == 0){
        // Shuffle
        for (int i=0; i < mapReduceJob->getMultiThreadLevel(); i++){
            IntermediateVec intermediateVec = mapReduceJob->contexts[i].intermediateVec;
            for (auto pair: intermediateVec) {
                if (mapReduceJob->intermediateMap.find(pair.first) == mapReduceJob->intermediateMap.end()){
                    mapReduceJob->intermediateMap.insert({
                        pair.first,
                        new IntermediateVec ()
                    });
                }
                mapReduceJob->intermediateMap[pair.first]->push_back(pair);
            }
        }
    }

    mapReduceJob->apply_barrier(); // Starting Reduce
    mapReduceJob->setJobStage(REDUCE_STAGE);
    mapReduceJob->mutex_lock();
    while (!mapReduceJob->intermediateMap.empty()){
        auto intermediatePair = mapReduceJob->intermediateMap.begin();
        mapReduceJob->intermediateMap.erase(mapReduceJob->intermediateMap.begin());
        mapReduceJob->mutex_unlock();
        mapReduceJob->getClient().reduce(intermediatePair->second, threadContext);
        mapReduceJob->mutex_lock();
    }

    mapReduceJob->mutex_unlock();
    return threadContext;
}
