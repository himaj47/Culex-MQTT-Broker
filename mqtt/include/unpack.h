#pragma once

#include <functional>
#include <vector>
#include <cstdint>
#include <memory>

#include "packets.h"

namespace culex {

using unpackHandler = std::function<size_t(Header& header, 
                                        Packet& pkt, 
                                        const uint8_t** buff, 
                                        size_t available_bytes)>;

extern const unpackHandler unpack_handlers[11];

bool is_partial(const uint8_t** buff, 
            size_t available_bytes, 
            int& remaining_len);

int unpack_connect(Header& header, 
                Packet& packet, 
                const uint8_t** buff, 
                size_t available_bytes);

int unpack_publish(Header& header, 
                Packet& packet, 
                const uint8_t** buff, 
                size_t available_bytes);

int unpack_ack(Header& header, 
            Packet& packet, 
            const uint8_t** buff, 
            size_t available_bytes);

int unpack_subscribe(Header& header, 
                    Packet& packet, 
                    const uint8_t** buff, 
                    size_t available_bytes);

int unpack_unsubscribe(Header& header, 
                    Packet& packet, 
                    const uint8_t** buff, 
                    size_t available_bytes);

int unpack(Packet& pkt, 
        const uint8_t** buff, 
        size_t available_bytes);

}