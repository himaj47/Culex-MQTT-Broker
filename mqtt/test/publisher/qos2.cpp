#include "common.h"

int main() {
    int sock = connect_to_broker();
    send_connect(sock);

    std::string topic = "test/topic2";
    std::string payload = "[QoS2] hello";
    uint16_t packet_id = 1;

    while (true) {
        uint8_t packet[100];
        int idx = 0;

        packet[idx++] = 0x34; // QoS 2

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

        std::cout << "[QoS2] Published\n";

        // ---- PUBREC ----
        uint8_t resp[4];
        recv(sock, resp, sizeof(resp), 0);

        if ((resp[0] >> 4) == 5) {
            std::cout << "[QoS2] PUBREC received\n";

            // Send PUBREL
            uint8_t pubrel[] = {
                0x62, 0x02,
                (uint8_t)(packet_id >> 8),
                (uint8_t)(packet_id & 0xFF)
            };

            send(sock, pubrel, sizeof(pubrel), 0);

            // ---- PUBCOMP ----
            recv(sock, resp, sizeof(resp), 0);

            if ((resp[0] >> 4) == 7) {
                std::cout << "[QoS2] PUBCOMP received\n";
            }
        }

        packet_id++;

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}