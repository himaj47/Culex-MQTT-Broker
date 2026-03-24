#include "common.h"

int main() {
    int sock = connect_to_broker();
    send_connect(sock);

    std::string topic = "/test/topic0";
    send_subscribe(sock, 1, 0, topic);

    while (true) {
        uint8_t buffer[1024];
        int n = recv(sock, buffer, sizeof(buffer), 0);

        if (n <= 0) break;

        uint8_t type = buffer[0] >> 4;

        if (type == 3) { // PUBLISH
            int topic_len = (buffer[2] << 8) | buffer[3];
            std::string topic((char*)&buffer[4], topic_len);

            int payload_start = 4 + topic_len;
            std::string payload((char*)&buffer[payload_start], n - payload_start);

            std::cout << "[QoS0 SUB] " << topic << " -> " << payload << "\n";
        }
    }
}