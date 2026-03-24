#include "common.h"

int main() {
    int sock = connect_to_broker();
    send_connect(sock);

    std::string topic = "/test/topic0";
    std::string payload = "[QoS0] hello";

    while (true) {
        uint8_t packet[100];
        int idx = 0;

        // Fixed header
        packet[idx++] = 0x30; // QoS 0

        int remaining = 2 + topic.size() + payload.size();
        packet[idx++] = remaining;

        // Topic
        packet[idx++] = 0x00;
        packet[idx++] = topic.size();
        memcpy(&packet[idx], topic.data(), topic.size());
        idx += topic.size();

        // Payload
        memcpy(&packet[idx], payload.data(), payload.size());
        idx += payload.size();

        send(sock, packet, idx, 0);

        std::cout << "[QoS0] Published\n";

        std::this_thread::sleep_for(std::chrono::milliseconds(10000)); // 10 Hz
    }
}