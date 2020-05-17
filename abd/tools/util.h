#ifndef UTIL_H
#define UTIL_H

#include <vector>
#include <string>

/**
 * separate a input string by assigned delemeter.
 * 
 * s                input string
 * result           result of split will be stored in this vector 
 * delim            delimeter used to separate the input
 * 
 */ 
void split(const std::string& s, std::vector<std::string>& result, const char delim = ' ');

#endif