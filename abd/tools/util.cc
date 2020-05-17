#include "util.h"
#include <string>
#include <sstream>
#include <vector>

void split(const std::string& s, std::vector<std::string>& result, const char delim) {
    result.clear();
    std::istringstream iss(s);
    std::string temp;

    while (std::getline(iss, temp, delim)) {
        result.emplace_back(std::move(temp));
    }
    return;
}