#include <unordered_map>
#include <cstring>
#include <string>
#include <vector>
#include <iostream>

class Client {
public:
    Client(int id){m_clientId = id;}
    
    void send(std::string msg) {
        std::cout << "Client id = " << m_clientId << ": " << msg + "\n";
    }

private:
    int m_clientId;
};

class Subscribers {
public:
    Subscribers(){}

    void subscribe(Client* client) {
        m_subscribers.push_back(client);
    }

    void publish(std::string msg) {
        if (m_subscribers.empty()) {
            std::cout << "no subscribers yet!!\n";
        }

        for (auto clients : m_subscribers) {
            clients->send(msg);
        }
    }

private:
    std::vector<Client*> m_subscribers;
};

struct Node {
    std::string level;
    std::unordered_map<std::string, Node*> links;
    Subscribers* subs = new Subscribers();

    Node(std::string level) {
        this->level = level;
    }

    bool ispresent(std::string str) {
        auto it = links.find(str);
        if (it == links.end())
            return false;
        return true;
    }

    void addLink(std::string str, Node* node) {
        links.insert({str, node});
    }

    Node* getLink(std::string str) {
        return links[str];
    } 

    std::unordered_map<std::string, Node*>& getLinks() {
        return links;
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

    void subscribe(Client* client) {
        subs->subscribe(client);
    }


};

class TopicTree {
public:
    TopicTree();
    ~TopicTree();

    bool searchTopic(std::string);
    void publish(std::string topic, std::string msg);
    void subscribe(std::string topic, Client*);

private:
    Node* m_root;
};

TopicTree::TopicTree() {
    m_root = new Node("*");
}

TopicTree::~TopicTree() {

}

void getWords(std::string str, std::vector<std::string>& words) {
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

void handleWildCard(Node* root, std::vector<std::string>& sub_topics, std::vector<Node*>& vec) {
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

void getNode(std::vector<Node*>& vec, Node* root, std::vector<std::string>& sub_topics, int idx = 0) {
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
                    std::cout << "entered\n";
                    handleWildCard(root, link_topics, vec);
                }
            }

            return;
        }
        else if (!root->ispresent(sub_topics[i])) {
            root->addLink(sub_topics[i], new Node(sub_topics[i]));
        }
        root = root->getLink(sub_topics[i]);
    }
    vec.push_back(root);
}

bool TopicTree::searchTopic(std::string str) {
    return true;
}   

void TopicTree::publish(std::string topic, std::string msg) {
    std::vector<std::string> words;
    getWords(topic, words);

    std::vector<Node*> nodes;
    getNode(nodes, m_root, words);
    
    for (auto node : nodes) {
        node->publish(msg);
    }
}

void TopicTree::subscribe(std::string topic, Client* client) {
    std::vector<std::string> words;
    getWords(topic, words);

    std::vector<Node*> nodes;
    getNode(nodes, m_root, words);

    for (auto node : nodes) {
        node->subscribe(client);
    }
}


int main(int argc, char const *argv[])
{
    TopicTree topics;

    std::string topic1 = "/sensor";
    std::string topic2 = "/sensor/kitchen/temp";
    std::string topic3 = "/sensor/living_room/temp";

    std::string topic4 = "/sensor/living_room";
    std::string topic5 = "/sensor/kitchen";

    std::string topic6 = "/sensor/living_room/temp/a";
    std::string topic7 = "/sensor/living_room/temp/b";

    Client* client1 = new Client(1);
    Client* client2 = new Client(2);
    Client* client3 = new Client(3);

    Client* client4 = new Client(4);
    Client* client5 = new Client(5);

    Client* client6 = new Client(6);
    Client* client7 = new Client(7);

    topics.subscribe(topic1, client1);
    topics.subscribe(topic2, client2);
    topics.subscribe(topic3, client3);

    topics.subscribe(topic4, client4);
    topics.subscribe(topic5, client5);

    topics.subscribe(topic6, client6);
    topics.subscribe(topic7, client7);

    // topics.publish(topic1, "topic1 msg");
    // topics.publish(topic2, "topic2 msg");
    // topics.publish(topic3, "topic3 msg");

    // topics.publish("/sensor/+/temp", "*********** star message");
    // topics.publish("/sensor/+/temp/+", "*********** star message");
    // topics.publish("/sensor/+", "*********** star message");
    // topics.publish("/+", "*********** star message");

    std::cout << "----------------------------------------\n";
    topics.publish("/sensor/#", "*********** wild card message");

    return 0;
}

