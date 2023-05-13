//
// Created by amitroth on 5/13/23.
//
#include "MapReduceFramework.h"
#include "MapReduceJob.h"

#include <memory>

void MapReduceJob::mutex_lock() {
    pthread_mutex_lock(&mutex);
}

void MapReduceJob::mutex_unlock() {
    pthread_mutex_lock(&mutex);
}

MapReduceJob::MapReduceJob(const MapReduceClient& mapReduceClient, const InputVec& inputVec,
                           const OutputVec& outputVec, int multiThreadLevel)
                           : client(mapReduceClient), inputVec(inputVec), outputVec(outputVec),
                           multiThreadLevel(multiThreadLevel), mutex(PTHREAD_MUTEX_INITIALIZER)
{
    threads = new pthread_t[multiThreadLevel];
    contexts = new ThreadContext[multiThreadLevel];
    jobState = {UNDEFINED_STAGE, 0};
    barrier = Barrier(multiThreadLevel);

    for (int tid = 0; tid < multiThreadLevel; tid++)
    {
        contexts[tid] = ThreadContext(tid, this);
        pthread_create(threads + tid, nullptr, thread_wrapper, contexts + tid);
    }


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
    mapReduceJob->mutex_unlock();

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
