#pragma once

#include <functional>
#include <vector>
#include <cstdint>
#include <memory>

namespace culex {

class ManageSessions;
class Packet;
class PacketHandler;

using handler = std::function<size_t(Packet& pkt, 
                                     std::vector<uint8_t>& buff,
                                     ManageSessions& session_manager, 
                                     std::shared_ptr<PacketHandler> packet_handler)>;

extern const handler handlers[15];

}