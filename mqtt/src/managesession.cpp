#include "managesessions.h"

namespace culex {

ManageSessions::ManageSessions(size_t pool_size) 
    : SessionManager(pool_size) {
}

ManageSessions::~ManageSessions() {
}

void ManageSessions::addSession(int fd) {
    std::shared_ptr<PacketHandler> session = std::make_shared<PacketHandler>(fd, *this, *m_executor, m_topicTree);
    {
        std::unique_lock<std::shared_mutex> lock(m_sessionsRWMutex);
        m_sessions[fd] = std::move(session);
    }
    std::cout << "packet handler session added!\n";
}

void ManageSessions::removeSession(int fd) {
    std::unique_lock<std::shared_mutex> lock(m_sessionsRWMutex);
    m_sessions.erase(fd);
    std::cout << "packet handler session erased!!\n";
}

std::shared_ptr<ClientSession> ManageSessions::sessionPresent(std::string client_id) {
    std::shared_ptr<ClientSession> cs;
    {
        std::lock_guard<std::mutex> lock(m_registryMutex);
        auto it = m_sessionRegistry.find(client_id);

        if (it != m_sessionRegistry.end())
            cs = it->second;
        else
            cs = nullptr;
    }
    return cs;
}

void ManageSessions::removeFromRegistry(std::string client_id) {
    std::lock_guard<std::mutex> lock(m_registryMutex);
    auto it = m_sessionRegistry.find(client_id);

    if (it != m_sessionRegistry.end()) {
        m_sessionRegistry.erase(client_id);
    }
}

void ManageSessions::addToRegistry(std::shared_ptr<ClientSession> cs) {
    std::lock_guard<std::mutex> lock(m_registryMutex);
    m_sessionRegistry[cs->client_id] = cs;
}

void ManageSessions::updateActivity(std::string client_id) {
    if (auto cs = sessionPresent(client_id)) {
        std::lock_guard<std::mutex> lock(m_registryMutex);
        cs->last_activity.store(now_ms(), std::memory_order_relaxed);
    }
}

void ManageSessions::scheduleExpiry(std::shared_ptr<ClientSession> cs) {
    if (!cs->keep_alive) return;

    int64_t expiry = cs->last_activity + (cs->keep_alive * 1500); // 1.5 * keep_alive, where keep_alive is in seconds
    cs->expiry_ms.store(expiry, std::memory_order_relaxed);
    
    ExpiryEntry entry{expiry, cs};

    std::lock_guard<std::mutex> lock(m_expiryMutex);
    m_expiryHeap.push(std::move(entry));
}

int ManageSessions::getNextTimeoutMs() {
    std::lock_guard<std::mutex> lock(m_expiryMutex);

    if (m_expiryHeap.empty())
        return -1; // block indefinitely

    int64_t now = now_ms();
    int64_t diff = m_expiryHeap.top().expiry_ms - now;

    if (diff <= 0)
        return 0;

    return static_cast<int>(diff);
}

void ManageSessions::processExpiredSessions() {
    int64_t now = now_ms();

    std::vector<std::shared_ptr<ClientSession>> to_disconnect;
    std::vector<std::shared_ptr<ClientSession>> new_entries;
    {
        std::lock_guard<std::mutex> lock(m_expiryMutex);

        while (!m_expiryHeap.empty()) {
            auto entry = m_expiryHeap.top();
            int64_t recalculate_expiry = 0;

            if (entry.expiry_ms > now)
                break;

            // do i need lock here?
            if (auto cs = entry.session.lock()) {
                recalculate_expiry = cs->last_activity + (cs->keep_alive * 1500);

                if (recalculate_expiry > now) {
                    new_entries.push_back(cs);
                }

                else {
                    if (cs->connected)
                        to_disconnect.push_back(cs);
                }
                
                m_expiryHeap.pop();
            }
        }
    }

    for (auto& session : to_disconnect) {
        if (auto transport = session->session.lock()) {
            transport->forceDisconnect(); 

            if (session->cleansession) {
                removeFromRegistry(session->client_id);
            } else {
                session->connected = false;
            }
        }
    }

    for (auto& cs : new_entries) {
        scheduleExpiry(cs);
    }
}

void ManageSessions::scheduleRetransmission(uint16_t packet_id, PacketType type, std::shared_ptr<ClientSession> cs) {
    int64_t retry = now_ms() + RETRY_INTERVAL_S*1000;

    RetransmitEntry entry{retry, packet_id, type, cs};
    {
        std::lock_guard<std::mutex> lock(m_retransmitMutex);
        m_retransmitHeap.push(entry);
    }
}

void ManageSessions::processRetransmissions() {
    int64_t now = now_ms();
    {
        std::lock_guard<std::mutex> lock(m_registryMutex);

        while (!m_retransmitHeap.empty()) {
            auto entry = m_retransmitHeap.top();

            if (entry.retry_ms <= now) {
                if (auto cs = entry.session.lock()) {
                    Packet pkt = cs->getPacket(entry.packet_id, entry.packet_type);
                    if (pkt.pkt_len) {
                        std::vector<uint8_t> buff;

                        if (entry.packet_type == PacketType::PUBLISH) {
                            pkt.header.dup = true;
                            routePacket(pkt);
                        } 
                        else {
                            build_ack(entry.packet_id, pkt.header.type, buff);

                            if (auto transport = cs->session.lock()) {
                                transport->pushDataToSend(buff);
                            }
                        }

                        m_retransmitHeap.pop();
                        continue;
                    }
                }
            }

            break;
        }
    }
}

void ManageSessions::routePacket(Packet& packet, std::shared_ptr<ClientSession> cs) {
    if (packet.header.type == PacketType::PUBLISH) {
        const Publish& pub = std::get<Publish>(packet.pkt);

        auto subs = m_topicTree.match(pub.topic);
        for (auto& sub : subs) {
            if (auto session = sub.session.lock()) {

                // persistent session
                if (!session->connected) {
                    if (packet.header.qos > 0) {
                        session->storeMessage(packet);
                    }
                    // drop messages for QoS level 0
                }
                else {
                    uint8_t effective_qos = std::min(packet.header.qos, sub.qos);
                    packet.header.qos = effective_qos;
                    sendToClient(session, packet);
                }
            }
        }
    }

    else {
        sendToClient(cs, packet);
    }
}

void ManageSessions::sendToClient(std::shared_ptr<ClientSession> session,
                                  Packet& packet) {

    std::vector<uint8_t> buff;
    
    // broker becomes the sender of publish packet
    if (packet.header.type == PacketType::PUBLISH) {
        uint16_t packet_id = 0;

        if (packet.header.qos > 0) {
            auto pub = std::get<Publish>(packet.pkt);

            packet_id = session->allocatePacketId();
            pub.packet_id = packet_id;

            session->storeInflight(packet);
            scheduleRetransmission(pub.packet_id, packet.header.type, session);
        }

        build_publish(packet, packet_id, buff);

    } else {
        const Ack& ack = std::get<Ack>(packet.pkt);
        build_ack(ack.packet_id, packet.header.type, buff);
    }

    if (auto transport = session->session.lock()) {
        transport->pushDataToSend(std::move(buff));
    }
}

void ManageSessions::createSubscription(std::string topic,
                                        uint8_t qos, 
                                        std::string client_id) {

    if (auto cs = sessionPresent(client_id)) {
        m_topicTree.subscribe(topic, cs, qos);
    }
}

void ManageSessions::removeSubscription(std::string topic,
                        std::string client_id) {

    if (auto cs = sessionPresent(client_id)) {
        m_topicTree.unsubscribe(topic, cs);
    }
}

}