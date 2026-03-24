#include "common.h"

int main() {
    int sock = connect_to_broker();
    send_connect(sock);

    std::string topic = "/test/topic1";
    send_subscribe(sock, 1, 1, topic);

    while (true) {
        uint8_t buffer[1024];
        int n = recv(sock, buffer, sizeof(buffer), 0);

        if (n <= 0) break;

        uint8_t type = buffer[0] >> 4;

        if (type == 3) { // PUBLISH
            int qos = (buffer[0] >> 1) & 0x03;

            int topic_len = (buffer[2] << 8) | buffer[3];
            int idx = 4 + topic_len;

            uint16_t packet_id = 0;
            if (qos > 0) {
                packet_id = (buffer[idx] << 8) | buffer[idx + 1];
                idx += 2;
            }

            std::string payload((char*)&buffer[idx], n - idx);

            std::cout << "[QoS1 SUB] " << payload << "\n";

            // ---- PUBACK ----
            uint8_t puback[] = {
                0x40, 0x02,
                (uint8_t)(packet_id >> 8),
                (uint8_t)(packet_id & 0xFF)
            };

            send(sock, puback, sizeof(puback), 0);
        }
    }
}