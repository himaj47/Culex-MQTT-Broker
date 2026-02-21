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

}