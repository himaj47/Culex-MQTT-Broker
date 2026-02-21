#include "packethandler.h"
#include "managesessions.h"
#include "packet.h"

namespace culex {

PacketHandler::PacketHandler(int fd, ManageSessions& session_manager, Executor& executor, TopicTree& topic_tree)
    : Client(fd, session_manager, executor), m_topicTree(topic_tree) {
}

void PacketHandler::parseData() {
    std::lock_guard<std::mutex> lock(m_recvBuffMutex);
    std::string_view temp(m_recvBuff.data(), m_recvBuff.size());

    Packet pkt;
    if (pkt.pack(temp.data())) {
        if (pkt.type == "PUB") m_topicTree.publish(pkt.topic, pkt.payload);
        else if (pkt.type == "SUB") m_topicTree.subscribe(pkt.topic, this);
    }

    // handle the case where the parser receives partial/corrupted data packet

    m_recvBuff.erase(m_recvBuff.begin(), m_recvBuff.begin() + temp.length());
}

}
