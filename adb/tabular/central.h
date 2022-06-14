#pragma once

#include <algorithm>
#include <vector>
#include <string>
#include <memory>
#include <iterator>
#include <optional>


enum class Aggr {
    uSum,
    uFirst,
    uLast
};


typedef std::vector<std::string> CS;
typedef std::vector<std::tuple<Aggr, std::string, std::optional<std::string>> > AS;