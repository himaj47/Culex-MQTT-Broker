#include "handlers.h"
#include "managesessions.h"
#include "packets.h"
#include <chrono>

namespace culex {

const handler handlers[15] = {nullptr,  
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
                              disconnectHandler};

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
        if (cn_pkt.level != 4) {
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

    if (std::holds_alternative<Subscribe>(pkt.pkt)) {
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
    }
}

int pubackHandler(Packet& pkt, 
                  std::vector<uint8_t>& buff, 
                  ManageSessions& session_manager, 
                  std::shared_ptr<PacketHandler> packet_handler) {

    int return_code = -MQTT_ERR;

    if (std::holds_alternative<Puback>(pkt.pkt)) {
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
        Pubrec& pubrec = std::get<Pubrec>(pkt.pkt);
        auto cs = session_manager.sessionPresent(packet_handler->getClientId());

        // store pubrel in inflight
        auto pkt_pubrel = build_ack(pubrec.packet_id, PacketType::PUBREL, buff);
        cs->storeInflightAcknowlegement(pkt_pubrel);

        // schedule retransmission for pubrel
        session_manager.scheduleRetransmission(pubrec.packet_id, PacketType::PUBREL, cs);

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
        Pubrec& pubcomp = std::get<Pubcomp>(pkt.pkt);

        // remove pubrel packet from inflight acknowlegement
        auto cs = session_manager.sessionPresent(packet_handler->getClientId());
        cs->removeFromInflightAcknowlegement(pubcomp.packet_id);

        return_code = MQTT_OK;
    }

    return return_code;
}

}