#include "unpack.h"

namespace culex {

const unpackHandler unpack_handlers[11] = {
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

bool is_partial(const uint8_t** buff, size_t available_bytes, int& remaining_len) {
    const uint8_t* start = *buff;

    if (available_bytes <= 1) {
        return true;
    }

    remaining_len = decode_length(buff, available_bytes); 

    if (remaining_len < 0)
        return true;

    int length_of_remaining_len = (*buff - start);

    if ((1 + length_of_remaining_len + remaining_len) > available_bytes) {
        return true;
    }

    return false;
}

int unpack_connect(Header& header, Packet& packet, const uint8_t** buff, size_t available_bytes) {
    Connect cn{};
    const uint8_t* start = *buff;
    int remaining_len = 0;

    if (is_partial(buff, available_bytes, remaining_len)) 
        return MQTT_PARTIAL_PACKET;

    int length_of_remaining_len = (*buff - start);

    // fixed header (1 byte) + remaining_len (1 - 4 bytes) + rest (remaining_len = variable header + payload)
    packet.pkt_len = sizeof(uint8_t) + length_of_remaining_len + remaining_len;

    std::string protocol_name;
    unpack_string16(buff, protocol_name);

    if (protocol_name != "MQTT") {
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

int unpack_publish(Header& header, Packet& packet, const uint8_t** buff, size_t available_bytes) {
    Publish pub{};
    const uint8_t* start = *buff;
    int remaining_len = 0;

    if (is_partial(buff, available_bytes, remaining_len)) 
        return MQTT_PARTIAL_PACKET;

    int length_of_remaining_len = (*buff - start);
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

int unpack_ack(Header& header, Packet& packet, const uint8_t** buff, size_t available_bytes) {
    Ack ack{};
    const uint8_t* start = *buff;
    int remaining_len = 0;

    if (is_partial(buff, available_bytes, remaining_len)) 
        return MQTT_PARTIAL_PACKET;

    int length_of_remaining_len = (*buff - start);
    // fixed header (1 byte) + remaining_len (1 - 4 bytes) + rest (remaining_len = variable header + payload)
    packet.pkt_len = sizeof(uint8_t) + length_of_remaining_len + remaining_len;

    ack.packet_id = unpack_u16(buff);
    packet.pkt = std::move(ack);

    return MQTT_OK;
}

int unpack_subscribe(Header& header, Packet& packet, const uint8_t** buff, size_t available_bytes) {
    Subscribe sub{};
    const uint8_t* start = *buff;
    int remaining_len = 0;

    if (is_partial(buff, available_bytes, remaining_len)) 
        return MQTT_PARTIAL_PACKET;

    int length_of_remaining_len = (*buff - start);
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


int unpack_unsubscribe(Header& header, 
                       Packet& packet, 
                       const uint8_t** buff, 
                       size_t available_bytes) {

    Unsubscribe unsub{};
    const uint8_t* start = *buff;
    int remaining_len = 0;

    if (is_partial(buff, available_bytes, remaining_len)) 
        return MQTT_PARTIAL_PACKET;

    uint8_t first_byte = header.pack();
    if ((first_byte & 0x02) != 0x02) {
        return -MQTT_ERR;
    }

    int length_of_remaining_len = (*buff - start);
    // fixed header (1 byte) + remaining_len (1 - 4 bytes) + rest (remaining_len = variable header + payload)
    packet.pkt_len = sizeof(uint8_t) + length_of_remaining_len + remaining_len;

    unsub.packet_id = unpack_u16(buff);
    
    size_t payload_len = remaining_len - sizeof(uint16_t);
    while (payload_len) {
        std::string topic;
        unpack_string16(buff, topic);
        unsub.topics.push_back(topic);
        payload_len -= (sizeof(uint16_t) + topic.length());
    }

    packet.pkt = std::move(unsub);
    return MQTT_OK;
}

int unpack(Packet& pkt, const uint8_t** buff, size_t available_bytes) {
    int rc = 0;

    uint8_t byte = unpack_u8(buff);
    pkt.header = Header::unpack(byte);
    const unpackHandler& handler = unpack_handlers[static_cast<uint8_t>(pkt.header.type)];

    if (handler) 
        rc = handler(pkt.header, pkt, buff, available_bytes);
    else {
        std::cout << "invalid type!!\n";
        rc = -MQTT_ERR;
    }

    return rc;
}

}