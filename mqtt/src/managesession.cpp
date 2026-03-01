#include "managesessions.h"

namespace culex {

ManageSessions::ManageSessions(size_t pool_size) 
    : SessionManager(pool_size) {
}

ManageSessions::~ManageSessions() {
}

void ManageSessions::addSession(int fd) {
    std::shared_ptr<PacketHandler> session = std::make_shared<PacketHandler>(fd, *this, m_executor, m_topicTree);
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

}