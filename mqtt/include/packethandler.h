#pragma once
#include "core/session.h"
#include "handlers.h"
#include <chrono>

namespace culex {

static const int RETRY_INTERVAL_S = 5; // 5 seconds

struct ClientSession {
    std::string client_id;
    uint16_t keep_alive{0};
    bool cleansession{false};
    bool connected{false};

    std::unordered_set<std::string> subscriptions;
    std::weak_ptr<PacketHandler> session;

    std::atomic<int64_t> expiry_ms{0};
    std::atomic<int64_t> last_activity{0};

    std::unordered_map<uint16_t, const Packet> inflight;
    std::unordered_map<uint16_t, const Packet> inflight_ack;
    std::deque<Packet> message_queue;

    std::mutex inflight_mutex;
    std::mutex inflight_ack_mutex;
    std::mutex msg_q_mutex;

    uint16_t next_packet_id = 1;

    uint16_t allocatePacketId() {
        if (++next_packet_id == 0) {
            next_packet_id = 1;
            return next_packet_id;
        }

        return next_packet_id;
    }

    void storeInflight(const Packet& packet) {
        {
            std::lock_guard<std::mutex> lock(inflight_mutex);
            const Publish& pub = std::get<Publish>(packet.pkt); 

            inflight.insert({pub.packet_id, packet});
        }
    }

    void storeInflightAcknowlegement(const Packet& packet) {
        {
            std::lock_guard<std::mutex> lock(inflight_ack_mutex);
            const Ack& ack = std::get<Ack>(packet.pkt); 

            inflight_ack.insert({ack.packet_id, packet});
        }
    }

    Packet removeFromInflight(uint16_t packet_id) {
        std::lock_guard<std::mutex> lock(inflight_mutex);
        auto it = inflight.find(packet_id);
        if (it != inflight.end()) {
            auto pkt = it->second;
            inflight.erase(packet_id);
            return pkt;
        }
        return {};
    }

    Packet removeFromInflightAcknowlegement(uint16_t packet_id) {
        std::lock_guard<std::mutex> lock(inflight_ack_mutex);
        auto it = inflight_ack.find(packet_id);
        if (it != inflight_ack.end()) {
            auto pkt = it->second;
            inflight_ack.erase(packet_id);
            return pkt;
        }
        return {};
    }

    Packet getPacket(uint16_t packet_id, PacketType type) {
        if (type == PacketType::PUBLISH){
            std::lock_guard<std::mutex> lock(inflight_mutex);
            auto it = inflight.find(packet_id);

            if (it != inflight.end()) 
                return it->second;
            else 
                return {};
        }

        else {
            std::lock_guard<std::mutex> lock(inflight_ack_mutex);
            auto it = inflight_ack.find(packet_id);

            if (it != inflight_ack.end()) 
                return it->second;
            else 
                return {};
        }
    }

    void storeMessage(const Packet& packet) {
        std::lock_guard<std::mutex> lock(msg_q_mutex);
        message_queue.push_back(packet);
    }
};

class TopicTree;
class ManageSessions;
class Executor;

class PacketHandler : public Session {
public:
    PacketHandler(int fd, ManageSessions& session_manager, Executor& executor, TopicTree&);
    std::string getClientId();
    void parseData() override;
    bool forceDisconnect();

private:
    TopicTree& m_topicTree;
    ManageSessions& m_manageSessions;
    std::string client_id;
};

}