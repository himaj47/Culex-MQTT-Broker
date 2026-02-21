#pragma once
#include "core/client.h"

namespace culex {

class TopicTree;
class ManageSessions;
class Executor;

class PacketHandler : public Client {
public:
    PacketHandler(int fd, ManageSessions& session_manager, Executor& executor, TopicTree&);

    // for testing, packets look like "{packet_type;topic;payload}"
    void parseData() override;

private:
    TopicTree& m_topicTree;
};

}