//
// Created by amitroth on 5/13/23.
//
#include "MapReduceFramework.h"
#include "MapReduceJob.h"
#include "utils.h"
#include "MapReduceClient.h"
#include <algorithm>

//#include <pthread.h>

// Atomic counter
// |2|______ 31 _______||________ 31 ______|
// state     current           total

#define BIT_31_MASK (0x7fffffff)
#define BIT_2_MASK (0x3)

#define PROGRESS_GET_TOTAL(x) (x.load() & BIT_31_MASK)
#define PROGRESS_GET_CURRENT(x) (x.load()>>31 & BIT_31_MASK)
#define PROGRESS_GET_STATE(x) (x.load()>>62)

#define PROGRESS_INC_CURRENT(x) (x += 0x80000000)
#define PROGRESS_ADD_CURRENT(x, a) (x += static_cast<uint64_t>(a)<<31)
#define PROGRESS_SET(x, state, current, total) ((static_cast<uint64_t>(state)<<62) + (static_cast<uint64_t>(current)<<31) + total)



void MapReduceJob::mutex_lock(){
    if(pthread_mutex_lock(&mutex)){
        error(SYS_ERR, "pthread mutex lock");
    }
}

void MapReduceJob::mutex_unlock(){
    if(pthread_mutex_unlock(&mutex)){
        error(SYS_ERR, "pthread mutex unlock");
    }
}

MapReduceJob::MapReduceJob(const MapReduceClient& mapReduceClient, const InputVec& inputVec,
                           OutputVec& outputVec, int multiThreadLevel)
                           : client(mapReduceClient), inputVec(inputVec), outputVec(outputVec),
                           multiThreadLevel(multiThreadLevel), mutex(PTHREAD_MUTEX_INITIALIZER),
                           barrier(multiThreadLevel), jobState({UNDEFINED_STAGE, 0}),
                           hasWaited(false), jobFinished(false)
{
    threads = new pthread_t[multiThreadLevel];
    contexts = new ThreadContext[multiThreadLevel];

    for (int tid = 0; tid < multiThreadLevel; tid++)
    {
        contexts[tid] = ThreadContext(tid, this);
        if(pthread_create(threads + tid, nullptr, thread_wrapper, contexts + tid)){
            error(SYS_ERR, "pthread_create");
        }
    }
}
MapReduceJob::~MapReduceJob()
{
    delete[] threads;
    threads = nullptr;
    delete[] contexts;
    contexts = nullptr;
    pthread_mutex_destroy(&mutex); // TODO erel i dont think we need it because we are not creating it using new
}

int MapReduceJob::allIntermediateVecsAreEmpty()
{
    for (int i = 0; i < multiThreadLevel; i++)
    {
        if (!contexts[i].intermediateVec.empty())
        {
            return i;
        }
    }
    return -1;
}

void *MapReduceJob::thread_wrapper(void *input) {
    auto threadContext = (ThreadContext *) input;
    auto mapReduceJob = threadContext->mapReduceJob;

    //
    //  MAP
    //
    if (threadContext->getId() == 0)
    {
        threadContext->mapReduceJob->mutex_lock();
        auto temp = PROGRESS_SET(threadContext->mapReduceJob->progress, MAP_STAGE, 0, threadContext->mapReduceJob->inputVec.size());
        threadContext->mapReduceJob->progress = temp;
        threadContext->mapReduceJob->mutex_unlock();
    }
    mapReduceJob->apply_barrier();
    mapReduceJob->mutex_lock();

    while (!mapReduceJob->inputVec.empty()){
        auto inputPair = mapReduceJob->popInputPair();
        mapReduceJob->mutex_unlock();
        mapReduceJob->getClient().map(inputPair.first, inputPair.second, threadContext);
        mapReduceJob->mutex_lock();
        PROGRESS_INC_CURRENT(threadContext->mapReduceJob->progress);
    }

    std::sort(threadContext->intermediateVec.begin(), threadContext->intermediateVec.end(), pairPtrComparison);

    mapReduceJob->mutex_unlock(); // Finished Map

    mapReduceJob->apply_barrier(); // Applying barrier to perform shuffle

    //
    //  SHUFFLE
    //
    if (threadContext->getId() == 0)
    {
        threadContext->mapReduceJob->mutex_lock();
        auto temp = PROGRESS_SET(threadContext->mapReduceJob->progress, SHUFFLE_STAGE, 0, threadContext->mapReduceJob->getIntermediateVecLen());
        threadContext->mapReduceJob->progress = temp;
        threadContext->mapReduceJob->mutex_unlock();
        std::cout << "set state to 2" << std::endl;
        int firstNonEmpty = 0;
        while ((firstNonEmpty = mapReduceJob->allIntermediateVecsAreEmpty()) != -1)
        {
            K2* biggestKey = mapReduceJob->contexts[firstNonEmpty].intermediateVec.back().first; // mapReduceJob->contexts[0].intermediateVec.back().first;
            for (int i = 1; i < mapReduceJob->getMultiThreadLevel(); i++)
            {
                IntermediateVec intermediateVec = mapReduceJob->contexts[i].intermediateVec;
                if (!intermediateVec.empty() && *biggestKey < *(intermediateVec.back().first))
                {
                    biggestKey = intermediateVec.back().first;
                }
            }


            IntermediateVec ivec;
            K2* currentKey;
            for (int i = 0; i < mapReduceJob->getMultiThreadLevel(); i++)
            {
                if (mapReduceJob->contexts[i].intermediateVec.empty())
                {
                    continue;
                }
                currentKey = mapReduceJob->contexts[i].intermediateVec.back().first;
                if (!(*currentKey < *biggestKey || *biggestKey < *currentKey))
                {
                    ivec.push_back(mapReduceJob->contexts[i].intermediateVec.back());
                    PROGRESS_INC_CURRENT(mapReduceJob->progress);
                    mapReduceJob->contexts[i].intermediateVec.pop_back();
                }
            }
            mapReduceJob->afterShuffleVec.push_back(ivec);
        }
    }


    mapReduceJob->apply_barrier();
    //
    //  REDUCE
    //
    if (threadContext->getId() == 0)
    {
        threadContext->mapReduceJob->mutex_lock();
        auto temp = PROGRESS_SET(threadContext->mapReduceJob->progress, REDUCE_STAGE, 0, threadContext->mapReduceJob->getAfterShuffleVecLen());
        threadContext->mapReduceJob->progress = temp;
        threadContext->mapReduceJob->mutex_unlock();
    }
    mapReduceJob->apply_barrier();

    mapReduceJob->mutex_lock();
    while (!mapReduceJob->afterShuffleVec.empty()){
        IntermediateVec reduceIntermediateVec = mapReduceJob->afterShuffleVec.back();
        mapReduceJob->afterShuffleVec.pop_back();
        int size = reduceIntermediateVec.size();
//        mapReduceJob->debug();
        mapReduceJob->mutex_unlock();
        mapReduceJob->getClient().reduce(&reduceIntermediateVec, threadContext);
        mapReduceJob->mutex_lock();
        PROGRESS_ADD_CURRENT(threadContext->mapReduceJob->progress, size);
    }

    mapReduceJob->mutex_unlock();
    mapReduceJob->apply_barrier();
    if (threadContext->getId() == 0)
    {
        mapReduceJob->jobFinished = true;
    }

    return threadContext;
}

int MapReduceJob::getAfterShuffleVecLen(){
    mutex_lock();
    int size = 0;
    for (auto &vec:  afterShuffleVec){
        size += vec.size();
    }
    mutex_unlock();
    return size;
}

int MapReduceJob::getIntermediateVecLen(){
    mutex_lock();

    int size = 0;
    for (int i=0; i < multiThreadLevel; i++) {
        size += contexts[i].intermediateVec.size();
    }
    mutex_unlock();
    return size;
}

void MapReduceJob::waitForJob() {
    if (!hasWaited && !jobFinished)
    {
        for (int tid = 0; tid < multiThreadLevel; tid++)
        {
            if(pthread_join(threads[tid], nullptr)){
                error(SYS_ERR, "pthread_join");
            }
        }
        hasWaited = true;
    }
}

void MapReduceJob::apply_barrier() {
    barrier.barrier();
}

JobState MapReduceJob::getJobState() {
    mutex_lock();
     jobState.stage = static_cast<stage_t>(PROGRESS_GET_STATE(progress));
     if (jobState.stage == UNDEFINED_STAGE)
     {
         std::cout << "ERROR: undefined state" << std::endl;
         jobState.percentage = 0;
     }
     else
     {
         //std::cout << "State: " << PROGRESS_GET_STATE(progress) << " Current: " << PROGRESS_GET_CURRENT(progress) << " Total: " << PROGRESS_GET_TOTAL(progress) << std::endl;
         jobState.percentage = static_cast<float>(100) * PROGRESS_GET_CURRENT(progress) / PROGRESS_GET_TOTAL(progress);
         std::cout << "This means percentage of " << jobState.percentage << std::endl;
     }
     mutex_unlock();
     return jobState;
}


void MapReduceJob::debug() {
//    std::cout << "DEBUG" << std::endl;
//    for (const auto& v: afterShuffleVec) {
//        //std::cout << static_cast<KChar *>(pair.first)->c << std::endl;
//        for (const auto& pair: v){
//            std::cout << "(" << static_cast<KChar *>(pair.first)->c << ", " << static_cast<VCount *>(pair.second)->count << "), ";
//        }
//        std::cout << std::endl << "-------------" << std::endl;
//    }
}
