#include "common.h"

int main() {
    int sock = connect_to_broker();
    send_connect(sock);

    std::string topic = "/test/topic2";
    send_subscribe(sock, 1, 2, topic);

    while (true) {
        uint8_t buffer[1024];
        int n = recv(sock, buffer, sizeof(buffer), 0);

        if (n <= 0) break;

        uint8_t type = buffer[0] >> 4;

        // ---- PUBLISH ----
        if (type == 3) {
            int topic_len = (buffer[2] << 8) | buffer[3];
            int idx = 4 + topic_len;

            uint16_t packet_id = (buffer[idx] << 8) | buffer[idx + 1];
            idx += 2;

            std::string payload((char*)&buffer[idx], n - idx);

            std::cout << "[QoS2 SUB] " << payload << "\n";

            // ---- PUBREC ----
            uint8_t pubrec[] = {
                0x50, 0x02,
                (uint8_t)(packet_id >> 8),
                (uint8_t)(packet_id & 0xFF)
            };

            send(sock, pubrec, sizeof(pubrec), 0);
        }

        // ---- PUBREL ----
        else if (type == 6) {
            uint16_t packet_id = (buffer[2] << 8) | buffer[3];

            // ---- PUBCOMP ----
            uint8_t pubcomp[] = {
                0x70, 0x02,
                (uint8_t)(packet_id >> 8),
                (uint8_t)(packet_id & 0xFF)
            };

            send(sock, pubcomp, sizeof(pubcomp), 0);

            std::cout << "[QoS2 SUB] PUBCOMP sent\n";
        }
    }
}