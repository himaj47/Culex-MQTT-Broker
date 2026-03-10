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

// return codes for connect packet
#define MQTT_CONNECTION_ACCEPTED           0x00
#define MQTT_UNACCEPTABLE_PROTOCOL_VERSION 0x01

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
// response to a PUBREC Packet.
typedef Ack Pubrel;
// response to a PUBREL Packet
typedef Ack Pubcomp;


struct tuple {
    std::string topic;
    uint8_t qos;
};

struct Subscribe {
    uint16_t packet_id{0};
    std::vector<tuple> payload;
};


struct Suback {
    uint16_t packet_id;
    std::vector<uint8_t> return_codes;
};


struct Packet {
    Header header{};
    size_t pkt_len{0};

    using packet = std::variant<
        Connack,
        Ack,
        Connect,
        Publish,
        Subscribe
    >;

    packet pkt;
};


static int encode_length(int len, std::vector<uint8_t>& buff) {
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

bool is_partial(const uint8_t** buff, size_t available_bytes, int& remaining_len) {
    const uint8_t* start = *buff;

    if (available_bytes < 5) 
        return true;

    remaining_len = decode_length(buff);
    if ((remaining_len + 1) > available_bytes)
        return true;

    return false;
}

static int unpack_connect(Header& header, Packet& packet, const uint8_t** buff, size_t available_bytes) {
    Connect cn{};
    const uint8_t* start = *buff;
    int remaining_len = 0;

    if (is_partial(buff, available_bytes, remaining_len)) 
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

static int unpack_publish(Header& header, Packet& packet, const uint8_t** buff, size_t available_bytes) {
    Publish pub{};
    const uint8_t* start = *buff;
    int remaining_len = 0;

    if (is_partial(buff, available_bytes, remaining_len)) 
        return -MQTT_ERR;

    int length_of_remaining_len = (*buff - start) + 1;
    // fixed header (1 byte) + remaining_len (1 - 4 bytes) + rest (remaining_len = variable header + payload)
    packet.pkt_len = sizeof(uint8_t) + length_of_remaining_len + remaining_len;

    unpack_string16(buff, pub.topic);
    int payload_size = remaining_len - (pub.topic.length() + sizeof(uint16_t));

    if (packet.header.qos > 0) {
        pub.packet_id = unpack_u16(buff);
        payload_size -= sizeof(uint16_t);
    }

    pub.payload.resize(payload_size);
    memcpy(pub.payload.data(), *buff, sizeof(uint8_t) * payload_size);
    *buff += payload_size;

    packet.pkt = std::move(pub);
    return MQTT_OK;
}

static int unpack_ack(Header& header, Packet& packet, const uint8_t** buff, size_t available_bytes) {
    Ack ack{};
    const uint8_t* start = *buff;
    int remaining_len = 0;

    if (is_partial(buff, available_bytes, remaining_len)) 
        return -MQTT_ERR;

    int length_of_remaining_len = (*buff - start) + 1;
    // fixed header (1 byte) + remaining_len (1 - 4 bytes) + rest (remaining_len = variable header + payload)
    packet.pkt_len = sizeof(uint8_t) + length_of_remaining_len + remaining_len;

    ack.packet_id = unpack_u16(buff);

    packet.pkt = std::move(ack);
}

static int unpack_subscribe(Header& header, Packet& packet, const uint8_t** buff, size_t available_bytes) {
    Subscribe sub{};
    const uint8_t* start = *buff;
    int remaining_len = 0;

    if (is_partial(buff, available_bytes, remaining_len)) 
        return -MQTT_ERR;

    int length_of_remaining_len = (*buff - start) + 1;
    // fixed header (1 byte) + remaining_len (1 - 4 bytes) + rest (remaining_len = variable header + payload)
    packet.pkt_len = sizeof(uint8_t) + length_of_remaining_len + remaining_len;

    sub.packet_id = unpack_u16(buff);
    
    int payload_len = remaining_len - sizeof(uint16_t);
    
    while (payload_len > 0) {
        tuple t{};
        unpack_string16(buff, t.topic);
        t.qos = unpack_u8(buff);
        sub.payload.push_back(t);

        payload_len -= (sizeof(uint16_t) + t.topic.length() + sizeof(uint8_t));
    }

    packet.pkt = std::move(sub);
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

static void build_publish(const Packet& publisher, uint16_t packet_id, std::vector<uint8_t>& buff) {
    const Publish& pub = std::get<Publish>(publisher.pkt);

    Header header = publisher.header;
    // how to handle dup and retain flag??
    buff.push_back(header.pack());

    int remaining_len = (2 + pub.topic.length()) + pub.payload.size();

    // currently only handled QoS 0
    if (publisher.header.qos > 0) {
        remaining_len += 2;
    }

    std::vector<uint8_t> b;
    int num_bytes = encode_length(remaining_len, b);
    buff.insert(buff.end(), b.begin(), b.end());

    pack_u16(buff, pub.topic.length());

    std::vector<uint8_t> str_buff;
    pack_string16(str_buff, pub.topic);
    buff.insert(buff.end(), str_buff.begin(), str_buff.end());

    if (publisher.header.qos > 0) {
        // unhandled case: inserting packet id
        pack_u16(buff, 0);
    }

    buff.insert(buff.end(), pub.payload.begin(), pub.payload.end());
}

static Packet build_ack(uint16_t packet_id, PacketType type, std::vector<uint8_t>& buff) {
    Ack ack{};
    ack.packet_id = packet_id;

    return pack_ack(ack, type, buff);
}

static Packet pack_ack(const Ack& ack, PacketType type, std::vector<uint8_t>& buff) {
    Header header{};
    header.type = type;

    if (type == PacketType::PUBREL) {
        uint8_t hdr = (static_cast<uint8_t>(header.type) << 4) & 0xF2;
        buff.push_back(hdr);
    }

    else 
        buff.push_back(header.pack());

    uint8_t remaining_len = 2;
    buff.push_back(remaining_len);

    buff.push_back(ack.pack());

    Packet packet;
    packet.header = header;
    packet.pkt = ack;
    packet.pkt_len = sizeof(uint8_t)*2 + remaining_len;

    return packet;
}

static void build_suback(uint16_t packet_id, std::vector<uint8_t>& return_codes, std::vector<uint8_t>& buff) {
    Suback suback{};
    suback.packet_id = packet_id;
    suback.return_codes = return_codes;

    pack_suback(suback, buff);
}

static void pack_suback(const Suback& suback, std::vector<uint8_t>& buff) {
    Header header{};
    header.type = PacketType::SUBACK;

    buff.push_back(static_cast<uint8_t>(header.type));

    int remaining_len = sizeof(uint16_t) + suback.return_codes.size();
    std::vector<uint8_t> b;
    int num_bytes = encode_length(remaining_len, b);
    buff.insert(buff.end(), b.begin(), b.end());

    pack_u16(buff, suback.packet_id);

    for (auto rc : suback.return_codes) {
        buff.push_back(rc);
    }
}

}