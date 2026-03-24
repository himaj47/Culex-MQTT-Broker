#include "common.h"

int main() {
    int sock = connect_to_broker();
    send_connect(sock);

    std::string topic = "/test/topic1";
    std::string payload = "[QoS1] hello";
    uint16_t packet_id = 1;

    while (true) {
        uint8_t packet[100];
        int idx = 0;

        packet[idx++] = 0x32; // QoS 1

        int remaining = 2 + topic.size() + 2 + payload.size();
        packet[idx++] = remaining;

        // Topic
        packet[idx++] = 0x00;
        packet[idx++] = topic.size();
        memcpy(&packet[idx], topic.data(), topic.size());
        idx += topic.size();

        // Packet ID
        packet[idx++] = packet_id >> 8;
        packet[idx++] = packet_id & 0xFF;

        // Payload
        memcpy(&packet[idx], payload.data(), payload.size());
        idx += payload.size();

        send(sock, packet, idx, 0);

        std::cout << "[QoS1] Published, waiting PUBACK\n";

        uint8_t ack[4];
        recv(sock, ack, sizeof(ack), 0);

        if ((ack[0] >> 4) == 4) {
            std::cout << "[QoS1] PUBACK received\n";
        }

        packet_id++;

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}