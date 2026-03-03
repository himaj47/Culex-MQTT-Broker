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

}