#pragma once

#include <functional>
#include <vector>
#include <cstdint>
#include <memory>

#include "packets.h"

namespace culex {

class ManageSessions;
class MqttSession;

using handler = std::function<size_t(Packet& pkt, 
                                     std::vector<uint8_t>& buff,
                                     ManageSessions& session_manager, 
                                     std::shared_ptr<MqttSession> packet_handler)>;

extern const handler handlers[15];

int connectHandler(Packet& pkt, 
                   std::vector<uint8_t>& buff, 
                   ManageSessions& session_manager, 
                   std::shared_ptr<MqttSession> packet_handler);

int publishHandler(Packet& pkt, 
                   std::vector<uint8_t>& buff, 
                   ManageSessions& session_manager, 
                   std::shared_ptr<MqttSession> packet_handler);

int pubackHandler(Packet& pkt, 
                  std::vector<uint8_t>& buff, 
                  ManageSessions& session_manager, 
                  std::shared_ptr<MqttSession> packet_handler);

int pubrecHandler(Packet& pkt, 
                  std::vector<uint8_t>& buff, 
                  ManageSessions& session_manager, 
                  std::shared_ptr<MqttSession> packet_handler);

int pubrelHandler(Packet& pkt, 
                  std::vector<uint8_t>& buff, 
                  ManageSessions& session_manager, 
                  std::shared_ptr<MqttSession> packet_handler);

int pubcompHandler(Packet& pkt, 
                   std::vector<uint8_t>& buff, 
                   ManageSessions& session_manager, 
                   std::shared_ptr<MqttSession> packet_handler);

int subscribeHandler(Packet& pkt, 
                  std::vector<uint8_t>& buff, 
                  ManageSessions& session_manager, 
                  std::shared_ptr<MqttSession> packet_handler);

int unsubscribeHandler(Packet& pkt, 
                       std::vector<uint8_t>& buff, 
                       ManageSessions& session_manager, 
                       std::shared_ptr<MqttSession> packet_handler);

int pingreqHandler(Packet& pkt, 
                   std::vector<uint8_t>& buff, 
                   ManageSessions& session_manager, 
                   std::shared_ptr<MqttSession> packet_handler);

int disconnectHandler(Packet& pkt, 
                      std::vector<uint8_t>& buff, 
                      ManageSessions& session_manager, 
                      std::shared_ptr<MqttSession> packet_handler);


// additional utility functions

void build_publish(const Packet& publisher, 
                   uint16_t packet_id, 
                   std::vector<uint8_t>& buff);

Packet build_ack(uint16_t packet_id, 
                 PacketType type, 
                 std::vector<uint8_t>& buff);

Packet pack_ack(const Ack& ack, 
                PacketType type, 
                std::vector<uint8_t>& buff);
                
void build_suback(uint16_t packet_id, 
                  std::vector<uint8_t>& return_codes, 
                  std::vector<uint8_t>& buff);

void pack_suback(const Suback& suback, 
                 std::vector<uint8_t>& buff);

void resendMessages(Packet& packet, 
                    uint16_t packet_id,
                    std::shared_ptr<MqttSession> packet_handler);

}