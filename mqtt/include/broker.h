#pragma once

#include "core/tcpserver.h"
#include "managesessions.h"

namespace culex {

/**
 * @class Broker
 * @brief The MQTT Broker class, extending the base TCP server logic.
 * 
 * This class orchestrates the high-level MQTT event loop. It manages the transition 
 * from raw TCP connections to MQTT sessions and handles periodic maintenance 
 * tasks like session expiry and QoS retransmissions.
 */
class Broker : TCPServer {
public:
    /**
     * @brief Construct a new Broker object.
     * @param port The port to listen for incoming MQTT connections (typically 1883).
     * @param pool_size Number of worker threads for the background executor.
     * @param session_manager Pointer to the MQTT-specific session manager.
     */
    Broker(uint16_t port, size_t pool_size, ManageSessions* session_manager = nullptr);

    /** @brief Destructor that shuts down the broker and stops the event loop. */
    ~Broker();

    /**
     * @brief Initializes the underlying TCPServer resources (socket, bind, listen).
     * @return true if initialization succeeded, false otherwise.
     */
    bool initialize();

    /**
     * @brief Main execution loop of the MQTT broker.
     * 
     * Overrides the default run loop to include logic for:
     * 
     * - Dynamic polling timeouts based on the next session expiry.
     * 
     * - Processing expired MQTT sessions (Keep-Alive).
     * 
     * - Handling QoS 1/2 packet retransmissions.
     */
    void run() override;

private:
    /** @brief Atomic flag to control the execution state of the main run loop. */
    std::atomic<bool> m_running{true};

    /** @brief Reference to the MQTT-specific session manager for lifecycle logic. */
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
        // Calculate timeout based on the closest upcoming session expiry
        int timeout = m_manageSessions.getNextTimeoutMs();
        int n = m_datapoller.poll(timeout);
        for (int i = 0; i < n; i++) {
            auto& ev = m_datapoller[i];
            int fd = ev.data.fd;

            // Handle socket errors or disconnections detected by epoll
            if (ev.events & (EPOLLHUP | EPOLLERR | EPOLLRDHUP)) {
                std::cout << "Session disconnected or crashed (detected by epoll flags)\n";
                closeConnection(fd);
            } 
            // Handle new incoming client connections
            else if (fd == getServerFd()) {
                newConnection();
            } 
            // Handle incoming data on an existing session
            else if (ev.events & EPOLLIN) {
                SessionEvent(ev.data.fd);
            }
        }

        // recalculate and process expiry of sessions
        m_manageSessions.processExpiredSessions();
        m_manageSessions.processRetransmissions();
    }
}

}