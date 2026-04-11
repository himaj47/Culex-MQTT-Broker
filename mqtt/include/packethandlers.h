#pragma once

#include <functional>
#include <vector>
#include <cstdint>
#include <memory>

#include "packets.h"

namespace culex {

class ManageSessions;
class MqttSession;

/**
 * @typedef packethandler
 * @brief Function signature for MQTT control packet handlers.
 * 
 * @param pkt The unpacked MQTT packet to process.
 * @param buff Output buffer to store the response packet.
 * @param session_manager Reference to the global session manager.
 * @param packet_handler Shared pointer to the active transport session (MqttSession).
 * @return size_t Return code (e.g., MQTT_OK, MQTT_CONNECTION_ACCEPTED).
 */
using packethandler = std::function<size_t(Packet& pkt, 
                                     std::vector<uint8_t>& buff,
                                     ManageSessions& session_manager, 
                                     std::shared_ptr<MqttSession> packet_handler)>;

/** 
 * @brief Jump table mapping MQTT PacketType to its corresponding handler function.
 * Indexed by the numeric value of the MQTT control packet type (1-14).
 */
extern const packethandler handlers[15];

/** @brief Processes incoming CONNECT packets, handles authentication and session resumption. */
int connectHandler(Packet& pkt, 
                   std::vector<uint8_t>& buff, 
                   ManageSessions& session_manager, 
                   std::shared_ptr<MqttSession> packet_handler);

/** @brief Processes incoming PUBLISH packets and routes them to the Topic Tree. */
int publishHandler(Packet& pkt, 
                   std::vector<uint8_t>& buff, 
                   ManageSessions& session_manager, 
                   std::shared_ptr<MqttSession> packet_handler);

/** @brief Processes PUBACK (QoS 1) response from a client. */
int pubackHandler(Packet& pkt, 
                  std::vector<uint8_t>& buff, 
                  ManageSessions& session_manager, 
                  std::shared_ptr<MqttSession> packet_handler);

/** @brief Processes PUBREC (QoS 2, Step 1) response from a client. */
int pubrecHandler(Packet& pkt, 
                  std::vector<uint8_t>& buff, 
                  ManageSessions& session_manager, 
                  std::shared_ptr<MqttSession> packet_handler);

/** @brief Processes PUBREL (QoS 2, Step 2) command from a client. */
int pubrelHandler(Packet& pkt, 
                  std::vector<uint8_t>& buff, 
                  ManageSessions& session_manager, 
                  std::shared_ptr<MqttSession> packet_handler);

/** @brief Processes PUBCOMP (QoS 2, Step 3) response from a client. */
int pubcompHandler(Packet& pkt, 
                   std::vector<uint8_t>& buff, 
                   ManageSessions& session_manager, 
                   std::shared_ptr<MqttSession> packet_handler);

/** @brief Processes SUBSCRIBE requests and updates the Topic Tree. */
int subscribeHandler(Packet& pkt, 
                  std::vector<uint8_t>& buff, 
                  ManageSessions& session_manager, 
                  std::shared_ptr<MqttSession> packet_handler);

/** @brief Processes UNSUBSCRIBE requests and removes links from the Topic Tree. */
int unsubscribeHandler(Packet& pkt, 
                       std::vector<uint8_t>& buff, 
                       ManageSessions& session_manager, 
                       std::shared_ptr<MqttSession> packet_handler);

/** @brief Processes PINGREQ and prepares a PINGRESP to maintain Keep-Alive. */
int pingreqHandler(Packet& pkt, 
                   std::vector<uint8_t>& buff, 
                   ManageSessions& session_manager, 
                   std::shared_ptr<MqttSession> packet_handler);

/** @brief Processes DISCONNECT and signals graceful session termination. */
int disconnectHandler(Packet& pkt, 
                      std::vector<uint8_t>& buff, 
                      ManageSessions& session_manager, 
                      std::shared_ptr<MqttSession> packet_handler);


// ** additional utility functions **
/** @brief Serializes a PUBLISH packet for transmission to a client. */
void build_publish(const Packet& publisher, 
                   uint16_t packet_id, 
                   std::vector<uint8_t>& buff);

/** @brief Creates and serializes a generic acknowledgment (PUBACK, PUBREC, etc.). */
Packet build_ack(uint16_t packet_id, 
                 PacketType type, 
                 std::vector<uint8_t>& buff);

/** @brief Internal helper to pack an Ack struct into a byte vector. */
Packet pack_ack(const Ack& ack, 
                PacketType type, 
                std::vector<uint8_t>& buff);
                
/** @brief Creates and serializes a SUBACK packet with specific return codes. */
void build_suback(uint16_t packet_id, 
                  std::vector<uint8_t>& return_codes, 
                  std::vector<uint8_t>& buff);

/** @brief Internal helper to pack a Suback struct into a byte vector. */
void pack_suback(const Suback& suback, 
                 std::vector<uint8_t>& buff);

/** @brief Resends pending QoS 1/2 messages upon session reconnection. */
void resendMessages(Packet& packet, 
                    uint16_t packet_id,
                    std::shared_ptr<MqttSession> packet_handler);

}