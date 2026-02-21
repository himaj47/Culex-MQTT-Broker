#pragma once

#include <unordered_map>
#include <cstring>
#include <string>
#include <vector>
#include <iostream>
#include <memory>

#include "packethandler.h"

namespace culex {

class Subscribers {
public:
    Subscribers(){}

    void subscribe(PacketHandler* client) {
        m_subscribers.push_back(client);
    }

    void publish(std::string msg) {
        if (m_subscribers.empty()) {
            std::cout << "no subscribers yet!!\n";
        }

        for (auto client : m_subscribers) {
            client->pushDataToSend(msg);
        }
    }

private:
    std::vector<PacketHandler*> m_subscribers;
};

struct Node {
    std::string level;
    std::unordered_map<std::string, std::unique_ptr<Node>> links;
    std::unique_ptr<Subscribers> subs = std::make_unique<Subscribers>();

    Node(std::string level) {
        this->level = level;
    }

    bool ispresent(std::string str) {
        auto it = links.find(str);
        if (it == links.end())
            return false;
        return true;
    }

    void addLink(std::string str) {
        links[str] = std::make_unique<Node>(str);
    }

    Node* getLink(std::string str) {
        return links[str].get();
    } 

    std::vector<std::string> getLinkTopics() {
        std::vector<std::string> temp;
        for (auto const& pair : links) {
            temp.push_back(pair.first);
        }

        return temp;
    }

    void publish(std::string msg) {
        subs->publish(msg);
    }

    void subscribe(PacketHandler* client) {
        subs->subscribe(client);
    }
};

class TopicTree {
public:
    TopicTree();
    ~TopicTree();

    bool searchTopic(std::string);
    void publish(std::string topic, std::string msg);
    void subscribe(std::string topic, PacketHandler*);

private:
    void getWords(std::string str, std::vector<std::string>& words);
    void handleWildCard(Node* root, std::vector<std::string>& sub_topics, std::vector<Node*>& vec);
    void getNode(std::vector<Node*>& vec, Node* root, std::vector<std::string>& sub_topics, int idx = 0);
    std::unique_ptr<Node> m_root;
};

}