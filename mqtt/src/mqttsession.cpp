#include "mqttsession.h"
#include "managesessions.h"
#include "unpack.h"

namespace culex {

MqttSession::MqttSession(int fd, ManageSessions& session_manager, Executor& executor, TopicTree& topic_tree)
    : Session(fd, session_manager, executor), m_topicTree(topic_tree), m_manageSessions(session_manager) {
}

std::string MqttSession::getClientId() {
    return client_id;
}

void MqttSession::parseData() {
    int rc = 0;
    size_t available_bytes = 0;
    Packet pkt;
    {
        std::lock_guard<std::mutex> lock(m_recvBuffMutex);

        available_bytes = m_recvBuff.availableBytes();

        const uint8_t* buff = m_recvBuff.data();
        rc = unpack(pkt, &buff, available_bytes);
    }

    std::vector<uint8_t> send_buff;
    if (rc == MQTT_OK) {

        // erase packet from buffer
        {
            std::lock_guard<std::mutex> lock(m_recvBuffMutex);
            m_recvBuff.erase(pkt.pkt_len);
        }

        // call the handler based on packet type
        const handler& handle = handlers[static_cast<uint8_t>(pkt.header.type)];
        if (handle) {
            rc = handle(pkt, send_buff, m_manageSessions, std::static_pointer_cast<MqttSession>(shared_from_this()));
        }

        if (rc == MQTT_CONNECTION_ACCEPTED) {
            const Connect& cn = std::get<Connect>(pkt.pkt);
            client_id = cn.client_id;
            pushDataToSend(std::move(send_buff));
        }

        else if (rc == MQTT_UNACCEPTABLE_PROTOCOL_VERSION) {
            pushDataToSend(std::move(send_buff));
            forceDisconnect();
        }

        else if (rc == -MQTT_ERR) {
            std::cout << "[MqttSession] error while building response packet!\n";
        }
    }

    else if (rc == MQTT_PARTIAL_PACKET) {
        std::cout << "[MqttSession] partial packet!\n";
    }

    else {
        std::cout << "[MqttSession] error! Disconnecting...\n";
        
        // check if persistent session
        auto cs = m_manageSessions.sessionPresent(getClientId());
        if (cs->cleansession) {
            m_manageSessions.removeFromRegistry(getClientId());
        }
        forceDisconnect();
    }
}

bool MqttSession::forceDisconnect() {
    m_manageSessions.removeSession(m_fd.fd());
    return true;
}

}
