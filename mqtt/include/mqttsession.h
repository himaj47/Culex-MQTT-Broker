#pragma once
#include "core/session.h"
#include "packethandlers.h"
#include <chrono>
#include <unordered_set>

namespace culex {

/** @brief Default interval in seconds before a QoS 1/2 packet is resent. */
static const int RETRY_INTERVAL_S = 5; // 5 seconds

/**
 * @struct ClientSession
 * @brief Represents the persistent state of an MQTT client.
 * 
 * Unlike MqttSession, which is tied to a socket, ClientSession can persist 
 * across reconnections if 'CleanSession' is set to false. It stores 
 * subscriptions, inflight packets, and queued messages for offline clients.
 */
struct ClientSession {
    std::string client_id;
    uint16_t keep_alive{0};
    bool cleansession{false};
    bool connected{false}; // Indicates whether the session is active

    /** @brief Set of topics this client is currently subscribed to. */
    std::unordered_set<std::string> subscriptions;

    /** @brief Weak pointer to the active mqtt session. */
    std::weak_ptr<MqttSession> session;

    std::atomic<int64_t> expiry_ms{0};
    std::atomic<int64_t> last_activity{0};

    /** @brief Map of Packet ID to Packet for QoS 1/2 PUBLISH flows. */
    std::unordered_map<uint16_t, const Packet> inflight;

    /** @brief Map of Packet ID to Packet for QoS 2 PUBREL flows. */
    std::unordered_map<uint16_t, const Packet> inflight_ack;

    /** @brief Queue for messages received while the client was offline (Persistent Session). */
    std::deque<Packet> message_queue;

    std::mutex inflight_mutex;
    std::mutex inflight_ack_mutex;
    std::mutex msg_q_mutex;

    uint16_t next_packet_id = 1; // Generator for unique MQTT packet identifiers.

    /**
     * @brief Generates the next available Packet ID (1-65535).
     * @return A unique 16-bit identifier.
     */
    uint16_t allocatePacketId() {
        if (++next_packet_id == 0) {
            next_packet_id = 1;
            return next_packet_id;
        }

        return next_packet_id;
    }

    /** @brief Stores a PUBLISH packet in the inflight map for QoS tracking. */
    void storeInflight(const Packet& packet) {
        std::lock_guard<std::mutex> lock(inflight_mutex);
        const Publish& pub = std::get<Publish>(packet.pkt);  

        inflight.insert({pub.packet_id, packet});
    }

    /** @brief Stores an ACK/PUBREL packet in the second-stage inflight map. */
    void storeInflightAcknowlegement(const Packet& packet) {
        std::lock_guard<std::mutex> lock(inflight_ack_mutex);
        const Ack& ack = std::get<Ack>(packet.pkt); 

        inflight_ack.insert({ack.packet_id, packet});
    }

    /** @brief Removes and returns a packet from the inflight map. */
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

    /** @brief Removes and returns a packet from the inflight acknowledgment map. */
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

    /** @brief Retrieves a specific packet from inflight storage without removing it. */
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

    /** @brief Queues a message for later delivery to the client. */
    void storeMessage(const Packet& packet) {
        std::lock_guard<std::mutex> lock(msg_q_mutex);
        message_queue.push_back(packet);
    }
};

class TopicTree;
class ManageSessions;
class Executor;

/**
 * @class MqttSession
 * @brief Handles the MQTT protocol logic for an active network connection.
 * 
 * Inherits from Session class to provide asynchronous MQTT packet parsing 
 * and processing via the Executor thread pool.
 */
class MqttSession : public Session {
public:
    /** @brief Initializes an MQTT-specific session. */
    MqttSession(int fd, ManageSessions& session_manager, Executor& executor, TopicTree&);

    /** @return The Client ID associated with this network session. */
    std::string getClientId();

    /** 
     * @brief Overrides base to perform MQTT packet unpacking, parsing and dispatching. 
     * Uses the jump table (handlers) to process specific MQTT packets.
     */
    void parseData() override;

    /** @brief Immediately closes the transport and triggers session manager cleanup. */
    bool forceDisconnect();

private:
    TopicTree& m_topicTree;
    ManageSessions& m_manageSessions;
    std::string client_id;
};

}