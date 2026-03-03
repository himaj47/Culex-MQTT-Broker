#pragma once
#include "core/session.h"
#include "handlers.h"
#include <chrono>

namespace culex {

struct ClientSession {
    std::string client_id;
    uint16_t keep_alive{0};
    bool cleansession{false};
    bool connected{false};

    std::unordered_set<std::string> subscriptions;
    std::weak_ptr<PacketHandler> session;

    std::atomic<int64_t> expiry_ms{0};
    std::atomic<int64_t> last_activity{0};

    // future:
    // inflight QoS1
    // QoS2 state
    // message queue
};

class TopicTree;
class ManageSessions;
class Executor;

class PacketHandler : public Session {
public:
    PacketHandler(int fd, ManageSessions& session_manager, Executor& executor, TopicTree&);
    void parseData() override;
    bool forceDisconnect();

private:
    TopicTree& m_topicTree;
    ManageSessions& m_manageSessions;
};

}