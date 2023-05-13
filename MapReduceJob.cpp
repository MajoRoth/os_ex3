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
