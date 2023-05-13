//
// Created by amitroth on 5/13/23.
//
#include <iostream>

#ifndef OS_EX3_UTILS_H
#define OS_EX3_UTILS_H

enum ERR{SYS_ERR, UTHREADS_ERR};


int error(ERR err, const std::string& text);



#endif //OS_EX3_UTILS_H
