//
// Created by amitroth on 5/13/23.
//
#include "MapReduceJob.h"

void MapReduceJob::mutex_lock() {
    pthread_mutex_lock(&mutex);
}

void MapReduceJob::mutex_unlock() {
    pthread_mutex_lock(&mutex);
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
    if (threadContext->getId() == 0){
        // Shuffle
        for (int i=0; i < mapReduceJob->getMultiThreadLevel(); i++){

        }
    }

    // reduce - takes <k2, v2> form intermediat, process it and save in output vec
    return input;
}
