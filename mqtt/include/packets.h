#pragma once

#include <string>
#include <cstring>
#include <iostream>
#include <cstdint>
#include <optional>
#include <variant>
#include "core/utils.h"

#define MQTT_OK             0
#define MQTT_ERR            1

// Return codes for connect packet
#define MQTT_CONNECTION_ACCEPTED           0x00
#define MQTT_UNACCEPTABLE_PROTOCOL_VERSION 0x01

namespace culex {

static const int MAX_LEN_BYTES = 4;

enum qos {
    AT_MOST_ONCE,
    AT_LEAST_ONCE,
    EXACTLY_ONCE
}; 

enum class PacketType : uint8_t {
    CONNECT          = 0x01,
    CONNACK          = 0x02,
    PUBLISH          = 0x03,
    PUBACK           = 0x04,
    PUBREC           = 0x05,
    PUBREL           = 0x06,
    PUBCOMP          = 0x07,
    SUBSCRIBE        = 0x08,
    SUBACK           = 0x09,
    UNSUBSCRIBE      = 0x0A,
    UNSUBACK         = 0x0B,
    PINGREQ          = 0x0C,
    PINGRESP         = 0x0D,
    DISCONNECT       = 0x0E
};

struct Header {
    PacketType type{PacketType::CONNECT};
    bool dup{false};
    uint8_t qos{0};
    bool retain{false};

    uint8_t pack() {
        uint8_t byte = 0;
        byte |= (static_cast<uint8_t>(type) << 4);
        byte |= (dup << 3);
        byte |= (qos << 1);
        byte |= retain;
        return byte;
    }

    static Header unpack(uint8_t byte) {
        Header h;
        h.type   = static_cast<PacketType>(((byte >> 4) & 0x0F));
        h.dup    = (byte & 0x08) != 0;
        h.qos    = (byte >> 1) & 0x03;
        h.retain = (byte & 0x01) != 0;
        return h;
    }
};


struct ConnectFlags {
    bool username{false};
    bool passwd{false};
    bool will_retain{false};
    uint8_t will_qos{0};
    bool will{false};
    bool cleansession{false};

    uint8_t pack() {
        uint8_t byte = 0;
        byte |= (username << 7);
        byte |= (passwd << 6);
        byte |= (will_retain << 5);
        byte |= (will_qos << 3);
        byte |= (will << 2);
        byte |= (cleansession << 1);
        return byte;
    }

    static ConnectFlags unpack(uint8_t byte) {
        ConnectFlags cf;
        cf.username     = (byte & 0x80) != 0;
        cf.passwd       = (byte & 0x40) != 0;
        cf.will_retain  = (byte & 0x20) != 0;
        cf.will_qos     = (byte >> 3) & 0x03;
        cf.will         = (byte & 0x04) != 0;
        cf.cleansession = (byte & 0x02) != 0;
        return cf;
    }
};

struct Connect {
    uint8_t level{4};
    ConnectFlags cf{};
    uint16_t keep_alive{0};

    std::string client_id;
    std::string will_topic;
    std::string will_msg;
    std::string username;
    std::string passwd;
};


struct Connack {
    uint8_t ack_flags{0};
    uint8_t return_code{0};

    uint16_t pack() {
        uint16_t bytes = 0;
        bytes |= ack_flags << 8;
        bytes |= return_code;
        return htons(bytes);
    }
};

struct Packet {
    Header header{};
    size_t pkt_len{0};

    using packet = std::variant<
        Connack,
        Connect
    >;

    packet pkt;
};


static int encode_length(int len, uint8_t* buff) {
    int encodedbyte = 0;

    do {
        if (encodedbyte + 1 > 4) return encodedbyte;

        short d = len % 128;
        len /= 128;
        if (len > 0) d |= 128;

        buff[encodedbyte++] = d;

    } while (len > 0);
    return encodedbyte;
}

static size_t decode_length(const uint8_t** buff) {
    int len = 0;
    int multiplier = 1;
    uint8_t nextbyte = 0;

    do {
        nextbyte = **buff;
        len += (nextbyte & 127) * multiplier;
        multiplier *= 128;
        
        if (multiplier > 128*128*128) 
            throw std::runtime_error("Malformed Remaining Length!!");

        (*buff)++;
    } while ((nextbyte & 128) != 0);
    return len;
}


static int unpack_connect(Header& header, Packet& packet, const uint8_t** buff, size_t available_bytes) {
    Connect cn{};
    const uint8_t* start = *buff;

    if (available_bytes < 5) 
        return -MQTT_ERR;

    int remaining_len = decode_length(buff);
    if ((remaining_len + 1) > available_bytes)
        return -MQTT_ERR;


    int length_of_remaining_len = (*buff - start) + 1;
    // fixed header (1 byte) + remaining_len (1 - 4 bytes) + rest (remaining_len = variable header + payload)
    packet.pkt_len = sizeof(uint8_t) + length_of_remaining_len + remaining_len;

    std::string protocol_name;
    unpack_string16(buff, protocol_name);

    if (protocol_name != "MQTT") {
        std::cout << "Invalid protocol name!\n";
        return -MQTT_ERR; 
    }

    cn.level = unpack_u8(buff);

    uint8_t byte = unpack_u8(buff);
    cn.cf = ConnectFlags::unpack(byte);

    cn.keep_alive = unpack_u16(buff);
    unpack_string16(buff, cn.client_id);

    if (cn.cf.will) {
        unpack_string16(buff, cn.will_topic);
        unpack_string16(buff, cn.will_msg);
    }
    if (cn.cf.username) {
        unpack_string16(buff, cn.username);
    }
    if (cn.cf.passwd) {
        unpack_string16(buff, cn.passwd);
    }

    packet.pkt = std::move(cn);
    return MQTT_OK;
}

using unpackHandler = std::function<size_t(Header& header, Packet& pkt, const uint8_t** buff, size_t available_bytes)>;
static const unpackHandler unpack_handlers[] = {
    nullptr,
    unpack_connect,
    nullptr,
    unpack_publish,
    unpack_ack,
    unpack_ack,
    unpack_ack,
    unpack_ack,
    unpack_subscribe,
    nullptr,
    unpack_unsubscribe
};

int unpack(Packet& pkt, const uint8_t** buff, size_t available_bytes) {
    int rc = 0;

    Header header;
    uint8_t byte = unpack_u8(buff);
    header = Header::unpack(byte);

    PacketType type = static_cast<PacketType>(header.type);
    if (type == PacketType::DISCONNECT ||
        type == PacketType::PINGREQ ||
        type == PacketType::PINGRESP) {
        pkt.header = header;

    } else {
        const unpackHandler& handler = unpack_handlers[static_cast<uint8_t>(header.type)];
        if (handler) 
            rc = handler(header, pkt, buff, available_bytes);
        else {
            std::cout << "invalid type!!\n";
            rc = -MQTT_ERR;
        }
    }

    return rc;
}

}