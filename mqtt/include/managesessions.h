#pragma once

#include "core/sessionmanager.h"
#include "topictree.h"
#include "packethandler.h"

#include <unordered_set>

namespace culex {

class ManageSessions : public SessionManager {
public:
    ManageSessions(size_t pool_size);
    ~ManageSessions();

    void addSession(int fd) override;
    void removeSession(int fd) override;

    std::shared_ptr<ClientSession> sessionPresent(std::string client_id);
    void addToRegistry(std::shared_ptr<ClientSession> cs);
    void removeFromRegistry(std::string client_id);

private:
    TopicTree m_topicTree;
    std::unordered_map<std::string, std::shared_ptr<ClientSession>> m_sessionRegistry;
    std::mutex m_registryMutex;
};

}