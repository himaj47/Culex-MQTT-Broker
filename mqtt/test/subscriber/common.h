#pragma once
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <string>
#include <thread>
#include <chrono>
#include <iostream>

#define BROKER_IP "127.0.0.1"
#define BROKER_PORT 1883

inline int connect_to_broker() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(BROKER_PORT);
    inet_pton(AF_INET, BROKER_IP, &addr.sin_addr);

    if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("connect");
        exit(1);
    }

    return sock;
}

inline void send_connect(int sock) {
    uint8_t packet[] = {
        0x10, 0x11,
        0x00, 0x04, 'M','Q','T','T',
        0x04,
        0x02,
        0x00, 0x3C,
        0x00, 0x05, 't','e','s','t', '1'
    };

    send(sock, packet, sizeof(packet), 0);

    uint8_t resp[4];
    recv(sock, resp, sizeof(resp), 0);

    std::cout << "Connected to broker\n";
}

inline void send_subscribe(int sock, uint16_t packet_id, uint8_t qos, const std::string& topic) {
    uint8_t packet[100];
    int idx = 0;

    packet[idx++] = 0x82;

    int remaining = 2 + 2 + topic.size() + 1;
    packet[idx++] = remaining;

    // Packet ID
    packet[idx++] = packet_id >> 8;
    packet[idx++] = packet_id & 0xFF;

    // Topic
    packet[idx++] = 0x00;
    packet[idx++] = topic.size();
    memcpy(&packet[idx], topic.data(), topic.size());
    idx += topic.size();

    // QoS
    packet[idx++] = qos;

    send(sock, packet, idx, 0);

    uint8_t resp[5];
    recv(sock, resp, sizeof(resp), 0);

    std::cout << "Subscribed to " << topic << "\n";
}