#include "packets.h"

namespace culex {

int encode_length(int len, std::vector<uint8_t>& buff) {
    int encodedbyte = 0;

    do {
        if (encodedbyte + 1 > 4) return encodedbyte;

        short d = len % 128;
        len /= 128;
        if (len > 0) d |= 128;

        buff.push_back(d);

    } while (len > 0);
    return encodedbyte;
}

size_t decode_length(const uint8_t** buff, size_t available_bytes) {
    int bytes_read = 0;

    int len = 0;
    int multiplier = 1;
    uint8_t nextbyte = 0;

    do {

        if (bytes_read >= available_bytes) {
            return -1; // Incomplete Packet
        }

        nextbyte = **buff;
        len += (nextbyte & 127) * multiplier;
        multiplier *= 128;
        
        if (multiplier > 128*128*128) 
            throw std::runtime_error("Malformed Remaining Length!!");

        (*buff)++;
        bytes_read++;
    } while ((nextbyte & 128) != 0);

    return len;
}

}