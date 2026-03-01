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

int connectHandler(const Packet& pkt, 
                   std::vector<uint8_t>& buff, 
                   ManageSessions& session_manager, 
                   std::shared_ptr<PacketHandler> packet_handler) {

    int return_code = -MQTT_ERR;

    if (std::holds_alternative<Connect>(pkt.pkt)) {
        return_code = MQTT_CONNECTION_ACCEPTED;

        const Connect& cn_pkt = std::get<Connect>(pkt.pkt);
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
            cs->last_activity = std::chrono::system_clock::now();

            session_manager.addToRegistry(cs);
        }

        // pack connack
        buff.push_back(header.pack());

        uint8_t remaining_len = 0;
        encode_length(2, &remaining_len);
        buff.push_back(remaining_len);
        buff.push_back(connack_pkt.pack());

        return return_code;
    }

    return return_code;
}

}