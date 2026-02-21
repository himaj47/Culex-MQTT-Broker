#pragma once

#include <string>
#include <cstring>
#include <iostream>

namespace culex {

struct Packet {
    std::string type;
    std::string topic;
    std::string payload;

    // test data packet = "{type;topic;payload}"
    bool pack(std::string data) {
        int counter = 1;
        std::string temp = "";
        size_t len = data.length();

        for (int i = 0; i < len; i++) {

            if ((i == 0) && (data[i] != '{')) {
                std::cerr << "corrupt data\n";
                return false;

            } else if ((i == len-1) && (data[i] != '}')) {
                std::cerr << "corrupt data\n";
                return false;
            }

            if ((i > 0 && i < len-1) && (data[i] == '{' || data[i] == '}')) {
                std::cerr << "corrupt data\n";
                return false;

            } else if (data[i] == ';' || data[i] == '}') {
                if (counter == 1) type = temp;
                if (counter == 2) topic = temp;
                if (counter == 3) payload = temp;

                counter++;
                temp = "";

            } else if (data[i] != '{' && data[i] != '}') {
                temp += data[i];
            }

        }

        return true;
    }
};

}