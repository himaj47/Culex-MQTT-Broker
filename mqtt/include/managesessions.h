#pragma once

#include "core/sessionmanager.h"
#include "topictree.h"
#include "packethandler.h"

namespace culex {

class ManageSessions : public SessionManager {
public:
    ManageSessions(size_t pool_size);
    ~ManageSessions();

    void addSession(int fd) override;
    void removeSession(int fd) override;

private:
    TopicTree m_topicTree;
};

}