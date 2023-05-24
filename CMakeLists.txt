cmake_minimum_required(VERSION 3.22)
project(os_ex3)

set(CMAKE_CXX_STANDARD 14)

set(CMAKE_CXX_FLAGS "-pthread -Wall")
#add(CMAKE_CXX_FLAGS -Wall)


include_directories(.)

add_executable(os_ex3
        Barrier.cpp
        Barrier.h
        MapReduceClient.cpp
        MapReduceClient.h
        MapReduceFramework.cpp
        MapReduceFramework.h
        MapReduceJob.cpp
        MapReduceJob.h
        utils.cpp
        utils.h)
