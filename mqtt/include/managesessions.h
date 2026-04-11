#pragma once

#include "core/sessionmanager.h"
#include "topictree.h"
#include "mqttsession.h"

#include <unordered_set>

namespace culex {

/**
 * @struct ExpiryEntry
 * @brief Represents a session's expiration timestamp for the priority queue.
 * 
 * Used by the expiry heap to identify which sessions have timed out based on Keep-Alive.
 */
struct ExpiryEntry {
    int64_t expiry_ms;
    std::weak_ptr<ClientSession> session;

    bool operator>(const ExpiryEntry& other) const {
        return expiry_ms > other.expiry_ms;
    }
};

/**
 * @struct RetransmitEntry
 * @brief Represents a scheduled MQTT packet retransmission.
 * 
 * Used for QoS 1 and QoS 2 flows where an ACK has not been received 
 * within the retry interval or after reconnection.
 */
struct RetransmitEntry {
    int64_t retry_ms{0};
    uint16_t packet_id;

    // The type of packet (e.g., PUBLISH or PUBREL).
    PacketType packet_type; 

    // The target client session for retransmission.
    std::weak_ptr<ClientSession> session;

    bool operator>(const RetransmitEntry& other) const {
        return retry_ms > other.retry_ms;
    }
};


/**
 * @class ManageSessions
 * @brief Extends SessionManager to handle MQTT protocol logic and persistent state.
 * 
 * This class manages the Topic Tree, Keep-Alive timeouts via an expiry heap, 
 * and reliable message delivery via a retransmission heap.
 */
class ManageSessions : public SessionManager {
public:
    /** @brief Initializes the manager and the internal thread pool. 
     * @param pool_size Worker threads. 
     */
    ManageSessions(size_t pool_size);
    ~ManageSessions();

    /** @brief Overrides base to create MqttSession instances instead of generic sessions. */
    void addSession(int fd) override;
    /** @brief Overrides base to remove active transport sessions. */
    void removeSession(int fd) override;

    /** @brief Checks if a client ID has a registered (active or persistent) session. */
    std::shared_ptr<ClientSession> sessionPresent(std::string client_id);

    /** @brief Registers a ClientSession for persistence. */
    void addToRegistry(std::shared_ptr<ClientSession> cs);

    /** @brief Purges a client from the registry (used for Clean Session = 1). */
    void removeFromRegistry(std::string client_id);

    /** @brief Updates the 'last_activity' timestamp for a specific client. */
    void updateActivity(std::string client_id);

    /** @brief Places a session into the expiry heap based on its Keep-Alive value. */
    void scheduleExpiry(std::shared_ptr<ClientSession> cs);

    /** @brief Calculates the time until the next session expires. 
      * @return ms for poll() timeout. 
      */
    int getNextTimeoutMs();

    /** @brief Checks the expiry heap and disconnects clients that exceeded Keep-Alive. */
    void processExpiredSessions();

    /** @brief Matches a packet against the topic tree and routes it to subscribers. */
    void routePacket(Packet& publisher, std::shared_ptr<ClientSession> cs = nullptr);

    /** @brief Serializes and sends an MQTT publish packet to a specific client. */
    void sendToClient(std::shared_ptr<ClientSession> session,
                      Packet& pub);
    
    /** @brief Adds a subscription link between a topic and a client in the TopicTree. */
    void createSubscription(std::string topic,
                            uint8_t qos, 
                            std::string client_id);
    
    /** @brief Removes a subscription link from the TopicTree. */
    void removeSubscription(std::string topic,
                            std::string client_id);
    
    /** @brief Schedules a packet to be resent if an ACK is not received. */
    void scheduleRetransmission(uint16_t packet_id, PacketType type, std::shared_ptr<ClientSession> cs);
    /** @brief Processes the retransmission heap and resends pending packets. */
    void processRetransmissions();
    
    /** @brief Utility to get current steady clock time in milliseconds. */
    static int64_t now_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()
        ).count();
    }

private:
    /** @brief Hierarchical storage for MQTT topics and their subscribers. */
    TopicTree m_topicTree;

    /** @brief Registry of all client states (mapped by ClientID). */
    std::unordered_map<std::string, std::shared_ptr<ClientSession>> m_sessionRegistry;

    /** @brief Min-heap for managing session Keep-Alive timeouts. */
    std::priority_queue<
        ExpiryEntry,
        std::vector<ExpiryEntry>,
        std::greater<ExpiryEntry>
    > m_expiryHeap;

    /** @brief Min-heap for managing QoS retransmission timings. */
    std::priority_queue<
        RetransmitEntry,
        std::vector<RetransmitEntry>,
        std::greater<RetransmitEntry>
    > m_retransmitHeap;

    std::mutex m_registryMutex;
    std::mutex m_expiryMutex;
    std::mutex m_retransmitMutex;
};

}