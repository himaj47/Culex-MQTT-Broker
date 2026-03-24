#pragma once

#include <string>
#include <cstring>
#include <iostream>
#include <cstdint>
#include <optional>
#include <variant>
#include "core/utils.h"

#define MQTT_OK               0
#define MQTT_ERR              1
#define MQTT_PARTIAL_PACKET   2

// return codes for connect packet
#define MQTT_CONNECTION_ACCEPTED            0x06
#define MQTT_UNACCEPTABLE_PROTOCOL_VERSION  0x07

// return codes for publish packet
#define MQTT_PUBACK 0x05

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


struct Publish {
    std::string topic;
    uint16_t packet_id{0};
    std::vector<uint8_t> payload;
};

// for QoS 1
struct Ack {
    uint16_t packet_id{0};

    uint16_t pack() const {
        return htons(packet_id);
    }
};

typedef Ack Puback;
// for QoS 2
typedef Ack Pubrec;
// response to a PUBREC Packet
typedef Ack Pubrel;
// response to a PUBREL Packet
typedef Ack Pubcomp;

// response to UNSUBSCRIBE packet
typedef Ack Unsuback;


struct tuple {
    std::string topic;
    uint8_t qos;
};

struct Subscribe {
    uint16_t packet_id{0};
    std::vector<tuple> payload;
};


struct Suback {
    uint16_t packet_id{0};
    std::vector<uint8_t> return_codes;
};


struct Unsubscribe {
    uint16_t packet_id{0};
    std::vector<std::string> topics;
};


struct Packet {
    Header header{};
    size_t pkt_len{0};

    using packet = std::variant<
        Connack,
        Ack,
        Connect,
        Publish,
        Subscribe,
        Unsubscribe
    >;

    packet pkt;
};


int encode_length(int len, std::vector<uint8_t>& buff);
size_t decode_length(const uint8_t** buff, size_t available_bytes);

}