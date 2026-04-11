#pragma once

#include <functional>
#include <vector>
#include <cstdint>
#include <memory>

#include "packets.h"

namespace culex {

/**
 * @typedef unpackHandler
 * @brief Function signature for specialized MQTT packet deserializers.
 * 
 * @param header The pre-parsed MQTT fixed header.
 * @param pkt The packet structure to populate with unpacked data.
 * @param buff Double pointer to the raw buffer; advanced as data is read.
 * @param available_bytes Total bytes currently held in the receive buffer.
 * @return size_t Status code (MQTT_OK, MQTT_PARTIAL_PACKET, or -MQTT_ERR).
 */
using unpackHandler = std::function<size_t(Header& header, 
                                        Packet& pkt, 
                                        const uint8_t** buff, 
                                        size_t available_bytes)>;

/** 
 * @brief Jump table mapping PacketType to its specific unpacking function.
 * Indexed by the numeric value of the MQTT control packet type.
 */
extern const unpackHandler unpack_handlers[11];

/**
 * @brief Checks if the buffer contains a complete MQTT packet.
 * Decodes the 'Remaining Length' and compares it against available bytes.
 * @param buff Double pointer to the start of the 'Remaining Length' field.
 * @param available_bytes Bytes currently in the buffer.
 * @param[out] remaining_len The decoded length of the packet payload if successful.
 * @return true if the packet is incomplete (needs more data), false if complete.
 */
bool is_partial(const uint8_t** buff, 
                size_t available_bytes, 
                int& remaining_len);

/** @brief Deserializes an MQTT CONNECT packet. */
int unpack_connect(Header& header, 
                   Packet& packet, 
                   const uint8_t** buff, 
                   size_t available_bytes);

/** @brief Deserializes an MQTT PUBLISH packet. */
int unpack_publish(Header& header, 
                   Packet& packet, 
                   const uint8_t** buff, 
                   size_t available_bytes);

/** @brief Deserializes acknowledgment packets (PUBACK, PUBREC, PUBREL, etc.). */
int unpack_ack(Header& header, 
               Packet& packet, 
               const uint8_t** buff, 
               size_t available_bytes);

/** @brief Deserializes an MQTT SUBSCRIBE packet and its list of topic filters. */
int unpack_subscribe(Header& header, 
                     Packet& packet, 
                     const uint8_t** buff, 
                     size_t available_bytes);

/** @brief Deserializes an MQTT UNSUBSCRIBE packet and its list of topics. */
int unpack_unsubscribe(Header& header, 
                       Packet& packet, 
                       const uint8_t** buff, 
                       size_t available_bytes);

/**
 * @brief The top-level entry point for packet deserialization.
 * Extracts the fixed header and routes the buffer to the appropriate handler.
 * @param pkt The packet structure to be filled.
 * @param buff Double pointer to the raw data buffer.
 * @param available_bytes Number of bytes available to read.
 * @return int result code (e.g., MQTT_OK).
 */
int unpack(Packet& pkt, 
           const uint8_t** buff, 
           size_t available_bytes);

}