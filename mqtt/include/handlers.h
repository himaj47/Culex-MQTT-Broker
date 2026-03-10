#pragma once

#include <functional>
#include <vector>
#include <cstdint>
#include <memory>

namespace culex {

class ManageSessions;
class PacketHandler;
struct Packet;
struct Header;

using handler = std::function<size_t(Packet& pkt, 
                                     std::vector<uint8_t>& buff,
                                     ManageSessions& session_manager, 
                                     std::shared_ptr<PacketHandler> packet_handler)>;

using unpackHandler = std::function<size_t(Header& header, 
                                           Packet& pkt, 
                                           const uint8_t** buff, 
                                           size_t available_bytes)>;

extern const unpackHandler unpack_handlers[11];
extern const handler handlers[15];

int connectHandler(Packet& pkt, 
                   std::vector<uint8_t>& buff, 
                   ManageSessions& session_manager, 
                   std::shared_ptr<PacketHandler> packet_handler);

int publishHandler(Packet& pkt, 
                   std::vector<uint8_t>& buff, 
                   ManageSessions& session_manager, 
                   std::shared_ptr<PacketHandler> packet_handler);

int pubackHandler(Packet& pkt, 
                  std::vector<uint8_t>& buff, 
                  ManageSessions& session_manager, 
                  std::shared_ptr<PacketHandler> packet_handler);

int pubrecHandler(Packet& pkt, 
                  std::vector<uint8_t>& buff, 
                  ManageSessions& session_manager, 
                  std::shared_ptr<PacketHandler> packet_handler);

int pubrelHandler(Packet& pkt, 
                  std::vector<uint8_t>& buff, 
                  ManageSessions& session_manager, 
                  std::shared_ptr<PacketHandler> packet_handler);

int pubcompHandler(Packet& pkt, 
                   std::vector<uint8_t>& buff, 
                   ManageSessions& session_manager, 
                   std::shared_ptr<PacketHandler> packet_handler);

int subscribeHandler(Packet& pkt, 
                  std::vector<uint8_t>& buff, 
                  ManageSessions& session_manager, 
                  std::shared_ptr<PacketHandler> packet_handler);

int unsubscribeHandler(Packet& pkt, 
                       std::vector<uint8_t>& buff, 
                       ManageSessions& session_manager, 
                       std::shared_ptr<PacketHandler> packet_handler);

int pingreqHandler(Packet& pkt, 
                   std::vector<uint8_t>& buff, 
                   ManageSessions& session_manager, 
                   std::shared_ptr<PacketHandler> packet_handler);

int disconnectHandler(Packet& pkt, 
                      std::vector<uint8_t>& buff, 
                      ManageSessions& session_manager, 
                      std::shared_ptr<PacketHandler> packet_handler);


bool is_partial(const uint8_t** buff, 
                size_t available_bytes, 
                int& remaining_len);

int unpack_connect(Header& header, 
                   Packet& packet, 
                   const uint8_t** buff, 
                   size_t available_bytes);

int unpack_publish(Header& header, 
                   Packet& packet, 
                   const uint8_t** buff, 
                   size_t available_bytes);

int unpack_ack(Header& header, 
               Packet& packet, 
               const uint8_t** buff, 
               size_t available_bytes);

int unpack_subscribe(Header& header, 
                     Packet& packet, 
                     const uint8_t** buff, 
                     size_t available_bytes);

int unpack_unsubscribe(Header& header, 
                       Packet& packet, 
                       const uint8_t** buff, 
                       size_t available_bytes);

int unpack(Packet& pkt, 
           const uint8_t** buff, 
           size_t available_bytes);

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

}