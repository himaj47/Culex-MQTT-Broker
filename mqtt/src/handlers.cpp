#include "handlers.h"
#include "managesessions.h"
#include "packets.h"
#include <chrono>

namespace culex {

const handler handlers[15] = {
    nullptr,  
    connectHandler,
    nullptr,
    publishHandler,
    pubackHandler,
    pubrecHandler,
    pubrelHandler,
    pubcompHandler,
    subscribeHandler,
    nullptr,
    unsubscribeHandler,
    nullptr,
    pingreqHandler,
    nullptr,
    disconnectHandler
};

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


void resendMessages(const Packet& packet, 
                    uint16_t packet_id, 
                    std::shared_ptr<PacketHandler> packet_handler) {

    std::vector<uint8_t> buff;
    
    // set dup flag to 1
    Packet pkt = packet;
    pkt.header.dup = true;

    PacketType type = packet.header.type;

    switch (type) {
        case PacketType::PUBLISH:
            build_publish(pkt, packet_id, buff);
            packet_handler->pushDataToSend(buff);
            break;

        case PacketType::PUBREL:
            build_ack(packet_id, PacketType::PUBREL, buff);
            packet_handler->pushDataToSend(buff);
            break;
        
        default:
            break;
    }
}


int connectHandler(Packet& pkt, 
                   std::vector<uint8_t>& buff, 
                   ManageSessions& session_manager, 
                   std::shared_ptr<PacketHandler> packet_handler) {

    int return_code = -MQTT_ERR;

    if (std::holds_alternative<Connect>(pkt.pkt)) {
        return_code = MQTT_CONNECTION_ACCEPTED;

        Connect& cn_pkt = std::get<Connect>(pkt.pkt);
        Connack connack_pkt{};

        bool add_to_registry = false;

        std::shared_ptr<ClientSession> cs = session_manager.sessionPresent(cn_pkt.client_id);
        // if client id is present in registry and is connected, then disconnect it
        if (cs) {
            if (cs->connected) {
                if (auto old_session = cs->session.lock()) {
                    old_session->forceDisconnect();
                }
                cs->connected = false;
            }

            if (cn_pkt.cf.cleansession) {
                // session present = 0
                connack_pkt.ack_flags = 0x00;

                session_manager.removeFromRegistry(cn_pkt.client_id);
                cs = std::make_shared<ClientSession>();

                add_to_registry = true;
            }

            else {
                // resume with old session
                if (auto old_session = cs->session.lock()) {
                    // resend inflight packets
                    {
                        std::lock_guard<std::mutex> lock(cs->inflight_mutex);
                        for (auto& pair : cs->inflight) {
                            resendMessages(pair.second, pair.first, old_session);
                        }
                    }

                    // resend inflight ack packets
                    {
                        std::lock_guard<std::mutex> lock(cs->inflight_ack_mutex);
                        for (auto& pair : cs->inflight_ack) {
                            resendMessages(pair.second, pair.first, old_session);
                        }
                    }

                    // TODO: resend stored messages
                }

                // session present = 1
                connack_pkt.ack_flags = 0x01;
                // attach current packet handler to ClientSession
            }
        }

        else {
            cs = std::make_shared<ClientSession>();
            add_to_registry = true;
        }

        cs->client_id = cn_pkt.client_id;
        cs->keep_alive = cn_pkt.keep_alive;
        cs->cleansession = cn_pkt.cf.cleansession;
        
        // build connack packet
        Header header{};
        header.type = static_cast<PacketType>(PacketType::CONNACK);

        uint8_t rc = 0x00;
        if (cn_pkt.level != 0x04) {
            rc = 0x01;

            cs->connected = false;
            add_to_registry = false;
            
            return_code = MQTT_UNACCEPTABLE_PROTOCOL_VERSION;
        }
        // TODO: add condition for return code 0x02 (server rejected client id)

        connack_pkt.return_code = rc;

        if (add_to_registry) {
            cs->connected = true;
            cs->session = packet_handler;
            cs->last_activity.store(session_manager.now_ms(), std::memory_order_relaxed);

            session_manager.addToRegistry(cs);
            session_manager.scheduleExpiry(cs);
        }

        // pack connack
        buff.push_back(header.pack());

        std::vector<uint8_t> remaining_len_buff;
        encode_length(2, remaining_len_buff);

        buff.insert(buff.end(), remaining_len_buff.begin(), remaining_len_buff.end());
        buff.push_back(connack_pkt.pack());

        return return_code;
    }

    return return_code;
}

int publishHandler(Packet& pkt, 
                   std::vector<uint8_t>& buff, 
                   ManageSessions& session_manager, 
                   std::shared_ptr<PacketHandler> packet_handler) {

    int return_code = -MQTT_ERR;

    if (std::holds_alternative<Publish>(pkt.pkt)) {
        session_manager.updateActivity(packet_handler->getClientId());

        Publish& pub = std::get<Publish>(pkt.pkt);
        Header header{};

        if (pkt.header.qos == 0) {
            // send publish packet to subscribed clients
            session_manager.routePacket(pkt);
        }

        else if (pkt.header.qos == 1) {
            session_manager.routePacket(pkt);

            build_ack(pub.packet_id, PacketType::PUBACK, buff);
            packet_handler->pushDataToSend(buff);
        }

        else {
            // store as inflight
            auto cs = session_manager.sessionPresent(packet_handler->getClientId());

            // check if publish packet already present in inflight
            auto present_packet = cs->getPacket(pub.packet_id, PacketType::PUBLISH);
            if (!present_packet.pkt_len)
                cs->storeInflight(pkt);

            // build and send pubrec
            build_ack(pub.packet_id, PacketType::PUBREC, buff);
            packet_handler->pushDataToSend(buff);
        }

        return_code = MQTT_OK;
    }

    return return_code;
}

int subscribeHandler(Packet& pkt, 
                  std::vector<uint8_t>& buff, 
                  ManageSessions& session_manager, 
                  std::shared_ptr<PacketHandler> packet_handler) {

    int return_code = -MQTT_ERR;

    if (std::holds_alternative<Subscribe>(pkt.pkt)) {
        session_manager.updateActivity(packet_handler->getClientId());

        Subscribe& sub = std::get<Subscribe>(pkt.pkt);
        std::vector<uint8_t> return_codes;

        for (auto const& tuple : sub.payload) {
            if (tuple.qos < 3) {
                session_manager.createSubscription(tuple.topic, 
                                                tuple.qos, 
                                                packet_handler->getClientId());
                return_codes.push_back(tuple.qos);
            }
            else
                return_codes.push_back(0x80);
        }

        build_suback(sub.packet_id, return_codes, buff);
        packet_handler->pushDataToSend(buff);
        return_code = MQTT_OK;
    }

    return return_code;
}

int pubackHandler(Packet& pkt, 
                  std::vector<uint8_t>& buff, 
                  ManageSessions& session_manager, 
                  std::shared_ptr<PacketHandler> packet_handler) {

    int return_code = -MQTT_ERR;

    if (std::holds_alternative<Puback>(pkt.pkt)) {
        session_manager.updateActivity(packet_handler->getClientId());
        
        Puback& puback = std::get<Puback>(pkt.pkt);
        auto cs = session_manager.sessionPresent(packet_handler->getClientId());

        // remove publish packet from inflight
        cs->removeFromInflight(puback.packet_id);

        return_code = MQTT_OK;
    }

    return return_code;
}

int pubrecHandler(Packet& pkt, 
                  std::vector<uint8_t>& buff, 
                  ManageSessions& session_manager, 
                  std::shared_ptr<PacketHandler> packet_handler) {
    
    int return_code = -MQTT_ERR;

    if (std::holds_alternative<Pubrec>(pkt.pkt)) {
        session_manager.updateActivity(packet_handler->getClientId());

        Pubrec& pubrec = std::get<Pubrec>(pkt.pkt);
        auto cs = session_manager.sessionPresent(packet_handler->getClientId());

        // store pubrel in inflight
        auto pkt_pubrel = build_ack(pubrec.packet_id, PacketType::PUBREL, buff);
        cs->storeInflightAcknowlegement(pkt_pubrel);

        // send pubrel packet
        packet_handler->pushDataToSend(buff);

        return_code = MQTT_OK;
    }

    return return_code;
}

int pubrelHandler(Packet& pkt, 
                  std::vector<uint8_t>& buff, 
                  ManageSessions& session_manager, 
                  std::shared_ptr<PacketHandler> packet_handler) {
    
    int return_code = -MQTT_ERR;

    if (std::holds_alternative<Pubrel>(pkt.pkt)) {
        session_manager.updateActivity(packet_handler->getClientId());

        Pubrel& pubrel = std::get<Pubrel>(pkt.pkt);
        auto cs = session_manager.sessionPresent(packet_handler->getClientId());

        // remove publish packet from inflight
        Packet pub_pkt = std::move(cs->removeFromInflight(pubrel.packet_id));

        if (pub_pkt.header.type == PacketType::PUBLISH) {
            session_manager.routePacket(pub_pkt);
            return_code = MQTT_OK;
        } 

        // send pubcomp even if packet id not present in inflight
        build_ack(pubrel.packet_id, PacketType::PUBCOMP, buff);
        packet_handler->pushDataToSend(buff);
    }

    return return_code;
}

int pubcompHandler(Packet& pkt, 
                   std::vector<uint8_t>& buff, 
                   ManageSessions& session_manager, 
                   std::shared_ptr<PacketHandler> packet_handler) {

    int return_code = -MQTT_ERR;

    if (std::holds_alternative<Pubrec>(pkt.pkt)) {
        session_manager.updateActivity(packet_handler->getClientId());

        Pubrec& pubcomp = std::get<Pubcomp>(pkt.pkt);

        // remove pubrel packet from inflight acknowlegement
        auto cs = session_manager.sessionPresent(packet_handler->getClientId());
        cs->removeFromInflightAcknowlegement(pubcomp.packet_id);

        return_code = MQTT_OK;
    }

    return return_code;
}

int unsubscribeHandler(Packet& pkt, 
                       std::vector<uint8_t>& buff, 
                       ManageSessions& session_manager, 
                       std::shared_ptr<PacketHandler> packet_handler) {
    
    int return_code = -MQTT_ERR;

    if (std::holds_alternative<Pubrec>(pkt.pkt)) {
        session_manager.updateActivity(packet_handler->getClientId());

        Unsubscribe& unsub = std::get<Unsubscribe>(pkt.pkt);

        // unsubscribe from topics 
        for (auto topic : unsub.topics) {
            session_manager.removeSubscription(topic, packet_handler->getClientId());
        }

        // build unsuback packet
        build_ack(unsub.packet_id, PacketType::UNSUBACK, buff);
        packet_handler->pushDataToSend(buff);

        return_code = MQTT_OK;
    }

    return return_code;
}

int pingreqHandler(Packet& pkt, 
                   std::vector<uint8_t>& buff, 
                   ManageSessions& session_manager, 
                   std::shared_ptr<PacketHandler> packet_handler) {
    
    int return_code = -MQTT_ERR;

    session_manager.updateActivity(packet_handler->getClientId());

    Header header{};
    header.type = PacketType::PINGREQ;

    buff.push_back(header.pack());
    packet_handler->pushDataToSend(buff);

    return return_code;
}

int disconnectHandler(Packet& pkt, 
                      std::vector<uint8_t>& buff, 
                      ManageSessions& session_manager, 
                      std::shared_ptr<PacketHandler> packet_handler) {
    return MQTT_OK;
}


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

void build_publish(const Packet& publisher, uint16_t packet_id, std::vector<uint8_t>& buff) {
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

Packet build_ack(uint16_t packet_id, PacketType type, std::vector<uint8_t>& buff) {
    Ack ack{};
    ack.packet_id = packet_id;

    return pack_ack(ack, type, buff);
}

Packet pack_ack(const Ack& ack, PacketType type, std::vector<uint8_t>& buff) {
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

void build_suback(uint16_t packet_id, std::vector<uint8_t>& return_codes, std::vector<uint8_t>& buff) {
    Suback suback{};
    suback.packet_id = packet_id;
    suback.return_codes = return_codes;

    pack_suback(suback, buff);
}

void pack_suback(const Suback& suback, std::vector<uint8_t>& buff) {
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