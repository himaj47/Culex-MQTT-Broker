#include "topictree.h"

namespace culex {

TopicTree::TopicTree() {
    m_root = std::make_unique<Node>("*");
}

TopicTree::~TopicTree() {

}

void TopicTree::split(std::string str, 
                      std::vector<std::string>& words) {
    std::string temp = "";
    int length = str.length();

    for (int i = 0; i < length; i++) {
        if (i == 0) {
            words.push_back("/");
        }
        else if (str[i] == '/') {
            words.push_back(temp);
            temp = "";
        }
        else if (i == length-1) {
            temp += str[i];
            words.push_back(temp);
            temp = "";
        }
        else {
            temp += str[i];
        }
    }
}

void TopicTree::handleWildCard(Node* root, 
                               std::vector<std::string>& sub_topics, 
                               std::vector<Node*>& vec) {
    bool pushed = false;
    if (sub_topics.empty()) {
        vec.push_back(root);
    }

    for (auto sub_topic : sub_topics) {
        Node* nextLink = root->getLink(sub_topic);
        std::vector<std::string> link_topics = nextLink->getLinkTopics();

        if (!pushed) {
            vec.push_back(root);
            pushed = true;
        }
        handleWildCard(nextLink, link_topics, vec);
    }
}

// vector of Node pointer, vec, is passed to retrieve all the nodes with matching topic
// vector<Node*> is passed, since it also deals with wildcard characters, so it might return multiple Node pointers
void TopicTree::getNode(std::vector<Node*>& vec,
                        Node* root, 
                        std::vector<std::string>& sub_topics, 
                        int idx) {

    for (int i = idx; i < sub_topics.size(); i++) {
        if (sub_topics[i] == "+" || sub_topics[i] == "#") {
            std::vector<std::string> link_topics = root->getLinkTopics();

            if (!link_topics.empty()) {
                if (sub_topics[i] == "+") {
                    for (auto topic : link_topics) {
                        getNode(vec, root->getLink(topic), sub_topics, i+1);
                    }
                }
                else {
                    handleWildCard(root, link_topics, vec);
                }
            }

            return;
        }
        else if (!root->ispresent(sub_topics[i])) {
            root->addLink(sub_topics[i]);
        }
        root = root->getLink(sub_topics[i]);
    }
    vec.push_back(root);
}

std::vector<Subscription> TopicTree::match(const std::string& topic) {
    std::vector<Node*> nodes;
    std::vector<std::string> words;
    split(topic, words);

    getNode(nodes, m_root.get(), words);

    std::vector<Subscription> result;

    for (auto node : nodes) {
        for (auto& sub : node->subscribers) {
            result.push_back(sub.second);
        }
    }

    return result;
}

void TopicTree::subscribe(const std::string& topic,
                          std::shared_ptr<ClientSession> session,
                          uint8_t qos) {

    std::vector<std::string> words;
    split(topic, words);
    
    std::vector<Node*> nodes;
    getNode(nodes, m_root.get(), words);

    for (auto node : nodes) {
        node->subscribe(session, qos);
    }
}

void TopicTree::unsubscribe(const std::string& topic,
                            std::shared_ptr<ClientSession> session) {

    std::vector<std::string> words;
    split(topic, words);
    
    std::vector<Node*> nodes;
    getNode(nodes, m_root.get(), words);

    for (auto node : nodes) {
        node->unsubscribe(session);
    }
}

}