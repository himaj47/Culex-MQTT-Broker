#pragma once

#include "core/tcpserver.h"
#include "managesessions.h"

namespace culex {

class Broker : TCPServer {
public:
    Broker(uint16_t port, size_t pool_size, ManageSessions* session_manager = nullptr);
    ~Broker();

    bool initialize();
    void run() override;

private:
    std::atomic<bool> m_running{true};
    ManageSessions &m_manageSessions;
};

Broker::Broker(uint16_t port, size_t pool_size, ManageSessions* session_manager)
    : TCPServer(port, pool_size, session_manager), m_manageSessions(*session_manager) {
}

Broker::~Broker() {
    m_running = false;
}

bool Broker::initialize() {
    return init();
}

void Broker::run() {
    while (m_running) {
        int timeout = m_manageSessions.getNextTimeoutMs();
        int n = m_datapoller.poll(timeout);
        for (int i = 0; i < n; i++) {
            auto& ev = m_datapoller[i];
            int fd = ev.data.fd;

            if (ev.events & (EPOLLHUP | EPOLLERR | EPOLLRDHUP)) {
                std::cout << "Session disconnected or crashed (detected by epoll flags)\n";
                closeConnection(fd);
            } else if (fd == server_fd_) {
                newConnection();
            } else if (ev.events & EPOLLIN) {
                SessionEvent(ev.data.fd);
            }
        }

        m_manageSessions.processExpiredSessions();
    }
}

}