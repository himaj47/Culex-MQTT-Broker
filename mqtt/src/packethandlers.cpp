#include "packethandlers.h"
#include "managesessions.h"
#include <chrono>

namespace culex {

const packethandler handlers[15] = {
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


void resendMessages(const Packet& packet, 
                    uint16_t packet_id, 
                    std::shared_ptr<MqttSession> mqtt_session) {

    std::vector<uint8_t> buff;
    
    // set dup flag to 1
    Packet pkt = packet;
    pkt.header.dup = true;

    PacketType type = packet.header.type;

    switch (type) {
        case PacketType::PUBLISH:
            build_publish(pkt, packet_id, buff);
            mqtt_session->pushDataToSend(buff);
            break;

        case PacketType::PUBREL:
            build_ack(packet_id, PacketType::PUBREL, buff);
            mqtt_session->pushDataToSend(buff);
            break;
        
        default:
            break;
    }
}


int connectHandler(Packet& pkt, 
                   std::vector<uint8_t>& buff, 
                   ManageSessions& session_manager, 
                   std::shared_ptr<MqttSession> mqtt_session) {

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
            cs->session = mqtt_session;
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
                   std::shared_ptr<MqttSession> mqtt_session) {

    int return_code = -MQTT_ERR;

    if (std::holds_alternative<Publish>(pkt.pkt)) {
        session_manager.updateActivity(mqtt_session->getClientId());

        Publish& pub = std::get<Publish>(pkt.pkt);
        Header header{};

        if (pkt.header.qos == 0) {
            // send publish packet to subscribed clients
            session_manager.routePacket(pkt);
        }

        else if (pkt.header.qos == 1) {
            session_manager.routePacket(pkt);

            build_ack(pub.packet_id, PacketType::PUBACK, buff);
            mqtt_session->pushDataToSend(buff);
        }

        else {
            // store as inflight
            auto cs = session_manager.sessionPresent(mqtt_session->getClientId());

            // check if publish packet already present in inflight
            auto present_packet = cs->getPacket(pub.packet_id, PacketType::PUBLISH);
            if (!present_packet.pkt_len)
                cs->storeInflight(pkt);

            // build and send pubrec
            build_ack(pub.packet_id, PacketType::PUBREC, buff);
            mqtt_session->pushDataToSend(buff);
        }

        return_code = MQTT_OK;
    }

    return return_code;
}

int subscribeHandler(Packet& pkt, 
                  std::vector<uint8_t>& buff, 
                  ManageSessions& session_manager, 
                  std::shared_ptr<MqttSession> mqtt_session) {

    int return_code = -MQTT_ERR;

    if (std::holds_alternative<Subscribe>(pkt.pkt)) {
        session_manager.updateActivity(mqtt_session->getClientId());

        Subscribe& sub = std::get<Subscribe>(pkt.pkt);
        std::vector<uint8_t> return_codes;

        for (auto const& tuple : sub.payload) {
            if (tuple.qos < 3) {
                session_manager.createSubscription(tuple.topic, 
                                                tuple.qos, 
                                                mqtt_session->getClientId());
                return_codes.push_back(tuple.qos);
            }
            else
                return_codes.push_back(0x80);
        }

        build_suback(sub.packet_id, return_codes, buff);
        mqtt_session->pushDataToSend(buff);
        return_code = MQTT_OK;
    }

    return return_code;
}

int pubackHandler(Packet& pkt, 
                  std::vector<uint8_t>& buff, 
                  ManageSessions& session_manager, 
                  std::shared_ptr<MqttSession> mqtt_session) {

    int return_code = -MQTT_ERR;

    if (std::holds_alternative<Puback>(pkt.pkt)) {
        session_manager.updateActivity(mqtt_session->getClientId());
        
        Puback& puback = std::get<Puback>(pkt.pkt);
        auto cs = session_manager.sessionPresent(mqtt_session->getClientId());

        // remove publish packet from inflight
        cs->removeFromInflight(puback.packet_id);

        return_code = MQTT_OK;
    }

    return return_code;
}

int pubrecHandler(Packet& pkt, 
                  std::vector<uint8_t>& buff, 
                  ManageSessions& session_manager, 
                  std::shared_ptr<MqttSession> mqtt_session) {
    
    int return_code = -MQTT_ERR;

    if (std::holds_alternative<Pubrec>(pkt.pkt)) {
        session_manager.updateActivity(mqtt_session->getClientId());

        Pubrec& pubrec = std::get<Pubrec>(pkt.pkt);
        auto cs = session_manager.sessionPresent(mqtt_session->getClientId());

        // store pubrel in inflight
        auto pkt_pubrel = build_ack(pubrec.packet_id, PacketType::PUBREL, buff);
        cs->storeInflightAcknowlegement(pkt_pubrel);

        // send pubrel packet
        mqtt_session->pushDataToSend(buff);

        return_code = MQTT_OK;
    }

    return return_code;
}

int pubrelHandler(Packet& pkt, 
                  std::vector<uint8_t>& buff, 
                  ManageSessions& session_manager, 
                  std::shared_ptr<MqttSession> mqtt_session) {
    
    int return_code = -MQTT_ERR;

    if (std::holds_alternative<Pubrel>(pkt.pkt)) {
        session_manager.updateActivity(mqtt_session->getClientId());

        Pubrel& pubrel = std::get<Pubrel>(pkt.pkt);
        auto cs = session_manager.sessionPresent(mqtt_session->getClientId());

        // remove publish packet from inflight
        Packet pub_pkt = std::move(cs->removeFromInflight(pubrel.packet_id));

        if (pub_pkt.header.type == PacketType::PUBLISH) {
            session_manager.routePacket(pub_pkt);
            return_code = MQTT_OK;
        } 

        // send pubcomp even if packet id not present in inflight
        build_ack(pubrel.packet_id, PacketType::PUBCOMP, buff);
        mqtt_session->pushDataToSend(buff);
    }

    return return_code;
}

int pubcompHandler(Packet& pkt, 
                   std::vector<uint8_t>& buff, 
                   ManageSessions& session_manager, 
                   std::shared_ptr<MqttSession> mqtt_session) {

    int return_code = -MQTT_ERR;

    if (std::holds_alternative<Pubrec>(pkt.pkt)) {
        session_manager.updateActivity(mqtt_session->getClientId());

        Pubrec& pubcomp = std::get<Pubcomp>(pkt.pkt);

        // remove pubrel packet from inflight acknowlegement
        auto cs = session_manager.sessionPresent(mqtt_session->getClientId());
        cs->removeFromInflightAcknowlegement(pubcomp.packet_id);

        return_code = MQTT_OK;
    }

    return return_code;
}

int unsubscribeHandler(Packet& pkt, 
                       std::vector<uint8_t>& buff, 
                       ManageSessions& session_manager, 
                       std::shared_ptr<MqttSession> mqtt_session) {
    
    int return_code = -MQTT_ERR;

    if (std::holds_alternative<Pubrec>(pkt.pkt)) {
        session_manager.updateActivity(mqtt_session->getClientId());

        Unsubscribe& unsub = std::get<Unsubscribe>(pkt.pkt);

        // unsubscribe from topics 
        for (auto topic : unsub.topics) {
            session_manager.removeSubscription(topic, mqtt_session->getClientId());
        }

        // build unsuback packet
        build_ack(unsub.packet_id, PacketType::UNSUBACK, buff);
        mqtt_session->pushDataToSend(buff);

        return_code = MQTT_OK;
    }

    return return_code;
}

int pingreqHandler(Packet& pkt, 
                   std::vector<uint8_t>& buff, 
                   ManageSessions& session_manager, 
                   std::shared_ptr<MqttSession> mqtt_session) {
    
    int return_code = -MQTT_ERR;

    session_manager.updateActivity(mqtt_session->getClientId());

    Header header{};
    header.type = PacketType::PINGREQ;

    buff.push_back(header.pack());
    mqtt_session->pushDataToSend(buff);

    return return_code;
}

int disconnectHandler(Packet& pkt, 
                      std::vector<uint8_t>& buff, 
                      ManageSessions& session_manager, 
                      std::shared_ptr<MqttSession> mqtt_session) {
    return MQTT_OK;
}



// ** additional utility functions **

void build_publish(const Packet& publisher, uint16_t packet_id, std::vector<uint8_t>& buff) {
    const Publish& pub = std::get<Publish>(publisher.pkt);

    Header header = publisher.header;
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