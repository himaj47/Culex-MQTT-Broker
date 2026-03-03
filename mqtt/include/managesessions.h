#pragma once

#include "core/sessionmanager.h"
#include "topictree.h"
#include "packethandler.h"

#include <unordered_set>

namespace culex {

struct ExpiryEntry {
    int64_t expiry_ms;
    std::weak_ptr<ClientSession> session;

    bool operator>(const ExpiryEntry& other) const {
        return expiry_ms > other.expiry_ms;
    }
};

class ManageSessions : public SessionManager {
public:
    ManageSessions(size_t pool_size);
    ~ManageSessions();

    void addSession(int fd) override;
    void removeSession(int fd) override;

    std::shared_ptr<ClientSession> sessionPresent(std::string client_id);
    void addToRegistry(std::shared_ptr<ClientSession> cs);
    void removeFromRegistry(std::string client_id);
    void updateActivity(std::string client_id);

    void scheduleExpiry(std::shared_ptr<ClientSession> cs);
    int getNextTimeoutMs();
    void processExpiredSessions();

    static int64_t now_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()
        ).count();
    }

private:
    TopicTree m_topicTree;
    std::unordered_map<std::string, std::shared_ptr<ClientSession>> m_sessionRegistry;

    std::priority_queue<
        ExpiryEntry,
        std::vector<ExpiryEntry>,
        std::greater<ExpiryEntry>
    > m_expiryHeap;

    std::mutex m_registryMutex;
    std::mutex m_expiryMutex;
};

}