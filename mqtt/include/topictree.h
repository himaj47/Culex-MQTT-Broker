#pragma once

#include <unordered_map>
#include <cstring>
#include <string>
#include <vector>
#include <iostream>
#include <memory>

#include "mqttsession.h"

namespace culex {

struct Subscription {
    std::weak_ptr<ClientSession> session;
    uint8_t qos;
};

struct Node {
    std::string level;
    std::unordered_map<std::string, std::unique_ptr<Node>> links;

    // <client_id, subscription>
    std::unordered_map<std::string, Subscription> subscribers;

    std::mutex links_mutex;
    std::mutex subscribers_mutex;

    Node(std::string level) {
        this->level = level;
    }

    bool ispresent(std::string str) {
        std::lock_guard<std::mutex> lock(links_mutex);
        auto it = links.find(str);
        if (it == links.end())
            return false;
        return true;
    }

    void addLink(std::string str) {
        std::lock_guard<std::mutex> lock(links_mutex);
        links[str] = std::make_unique<Node>(str);
    }

    Node* getLink(std::string str) {
        std::lock_guard<std::mutex> lock(links_mutex);
        return links[str].get();
    } 

    std::vector<std::string> getLinkTopics() {
        std::lock_guard<std::mutex> lock(links_mutex);
        std::vector<std::string> temp;
        for (auto const& pair : links) {
            temp.push_back(pair.first);
        }

        return temp;
    }

    void subscribe(std::shared_ptr<ClientSession> cs, uint8_t qos) {
        std::lock_guard<std::mutex> lock(subscribers_mutex);
        Subscription subscription{cs, qos};
        subscribers[cs->client_id] = subscription;
    }

    void unsubscribe(std::shared_ptr<ClientSession> cs) {
        std::lock_guard<std::mutex> lock(subscribers_mutex);
        if (subscribers.find(cs->client_id) != subscribers.end())
            subscribers.erase(cs->client_id);
    }
};

class TopicTree {
public:
    TopicTree();
    ~TopicTree();

    std::vector<Subscription> match(const std::string& topic);
    void subscribe(const std::string& topic,
                   std::shared_ptr<ClientSession> session,
                   uint8_t qos);

    void unsubscribe(const std::string& topic,
                     std::shared_ptr<ClientSession> session);

private:
    void split(std::string str,
               std::vector<std::string>& words);

    void handleWildCard(Node* root, 
                        std::vector<std::string>& sub_topics, 
                        std::vector<Node*>& vec);

    void getNode(std::vector<Node*>& vec, 
                 Node* root, 
                 std::vector<std::string>& sub_topics, 
                 int idx = 0);

    std::unique_ptr<Node> m_root;
};

}