//
// Created by amitroth on 5/13/23.
//
#include "MapReduceFramework.h"
#include "MapReduceJob.h"
#include "utils.h"

#include <pthread.h>

void MapReduceJob::mutex_lock(){
    if(pthread_mutex_lock(&mutex)){
        error(SYS_ERR, "pthread mutex lock");
    }
}

void MapReduceJob::mutex_unlock(){
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
    size = inputVec.size();

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

float MapReduceJob::getPercentage(){
    mutex_lock();
    float p = 0;
    switch (jobState.stage) {
        case UNDEFINED_STAGE:
            break;
        case MAP_STAGE:
            p = (inputVec.size() / getAtomicCurrentSize()) * 100;
            break;
        case SHUFFLE_STAGE:
            p = (getIntermediateVecLen() / getAtomicCurrentSize()) * 100;
            break;
        case REDUCE_STAGE:
            p = (getIntermediateMapLen() / getAtomicCurrentSize()) * 100;
            break;
    }
    mutex_unlock();
    return p;
}

int MapReduceJob::getIntermediateMapLen() const{
    int size = 0;
    for (auto &vec:  intermediateMap){
        size += vec.second->size();
    }
    return size;
}

int MapReduceJob::getIntermediateVecLen() const{
    int size = 0;
    for (int i=0; i < multiThreadLevel; i++) {
        size += contexts[i].intermediateVec.size();
    }
    return size;
}

void MapReduceJob::waitForJob() {
    for (int tid = 0; tid < multiThreadLevel; tid++)
    {
        pthread_join(threads[tid], nullptr);
    }
}

void MapReduceJob::apply_barrier() {
    barrier.barrier();
}

void MapReduceJob::setJobStage(stage_t stage) {
    mutex_lock();
    setAtomicStage(stage);
    jobState.stage = stage;
    switch (jobState.stage) {
        case UNDEFINED_STAGE:
            break;
        case MAP_STAGE:
            setAtomicSize(inputVec.size());
            break;
        case SHUFFLE_STAGE:
            setAtomicSize(getIntermediateVecLen());
            break;
        case REDUCE_STAGE:
            setAtomicSize(getIntermediateMapLen());
            break;
    }
    mutex_unlock();
}

