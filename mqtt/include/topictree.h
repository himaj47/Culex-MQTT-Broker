#pragma once

#include <unordered_map>
#include <cstring>
#include <string>
#include <vector>
#include <iostream>
#include <memory>

#include "mqttsession.h"

namespace culex {

/**
 * @struct Subscription
 * @brief Represents a single client's subscription to a topic.
 * 
 * Uses a weak_ptr to the ClientSession to allow the session registry 
 * to clean up the actual session object without leaving dangling pointers in the tree.
 */
struct Subscription {
    std::weak_ptr<ClientSession> session;
    uint8_t qos;
};

/**
 * @struct Node
 * @brief A single node within the hierarchical topic tree.
 * 
 * Each node represents one level of a topic (e.g., in "home/livingroom/temp", 
 * "home", "livingroom", and "temp" are individual nodes).
 */
struct Node {
    std::string level; // The string identifier for this topic level.

    /** @brief Map of child topic levels to their respective Node objects. */
    std::unordered_map<std::string, std::unique_ptr<Node>> links;

    /** @brief Map of Client IDs to their subscription details at this specific node. */
    std::unordered_map<std::string, Subscription> subscribers;

    std::mutex links_mutex;
    std::mutex subscribers_mutex;

    /** @brief Constructor initializing a node at a specific topic level. */
    Node(std::string level) {
        this->level = level;
    }

    /** @brief Checks if a child topic level exists. */
    bool ispresent(std::string str) {
        std::lock_guard<std::mutex> lock(links_mutex);
        auto it = links.find(str);
        if (it == links.end())
            return false;
        return true;
    }

    /** @brief Adds a new child level to this node. */
    void addLink(std::string str) {
        std::lock_guard<std::mutex> lock(links_mutex);
        links[str] = std::make_unique<Node>(str);
    }

    /** @brief Retrieves a raw pointer to a child node. */
    Node* getLink(std::string str) {
        std::lock_guard<std::mutex> lock(links_mutex);
        return links[str].get();
    } 

    /** @brief Gets a list of all child topic level strings. */
    std::vector<std::string> getLinkTopics() {
        std::lock_guard<std::mutex> lock(links_mutex);
        std::vector<std::string> temp;
        for (auto const& pair : links) {
            temp.push_back(pair.first);
        }

        return temp;
    }

    /** @brief Adds a client subscription to this specific node. */
    void subscribe(std::shared_ptr<ClientSession> cs, uint8_t qos) {
        std::lock_guard<std::mutex> lock(subscribers_mutex);
        Subscription subscription{cs, qos};
        subscribers[cs->client_id] = subscription;
    }

    /** @brief Removes a client subscription from this node. */
    void unsubscribe(std::shared_ptr<ClientSession> cs) {
        std::lock_guard<std::mutex> lock(subscribers_mutex);
        if (subscribers.find(cs->client_id) != subscribers.end())
            subscribers.erase(cs->client_id);
    }
};

/**
 * @class TopicTree
 * @brief Manages the hierarchical MQTT topic structure and wildcard matching.
 * 
 * Provides thread-safe methods to subscribe, unsubscribe, and find all matching 
 * subscribers for a published topic (including + and # wildcards).
 */
class TopicTree {
public:
    /** @brief Initializes the tree with a root node. */
    TopicTree();
    ~TopicTree();

    /**
     * @brief Finds all subscriptions that match a given published topic.
     * @param topic The topic string (e.g., "sensor/temp").
     * @return A vector of all matching subscriptions (handles wildcards).
     */
    std::vector<Subscription> match(const std::string& topic);

    /**
     * @brief Adds a subscription for a client.
     * @param topic The topic filter (can include wildcards).
     * @param session The client session to link.
     * @param qos The maximum requested QoS.
     */
    void subscribe(const std::string& topic,
                   std::shared_ptr<ClientSession> session,
                   uint8_t qos);

    /**
     * @brief Removes a client's subscription for a specific topic filter.
     * @param topic The topic filter to remove.
     * @param session The client session to unlinked.
     */
    void unsubscribe(const std::string& topic,
                     std::shared_ptr<ClientSession> session);

private:
    /** @brief Helper to split a topic string by the '/' delimiter. */
    void split(std::string str,
               std::vector<std::string>& words);

    /** @brief Handles the '#' multi-level wildcard matching recursively. */
    void handleWildCard(Node* root, 
                        std::vector<std::string>& sub_topics, 
                        std::vector<Node*>& vec);

    /** @brief recursive function to traverse the tree and identify target nodes. */
    void getNode(std::vector<Node*>& vec, 
                 Node* root, 
                 std::vector<std::string>& sub_topics, 
                 int idx = 0);

    /** @brief The root node of the topic hierarchy. */
    std::unique_ptr<Node> m_root;
};

}