#include "core/tcpserver.h"
#include "managesessions.h"

using namespace culex;

int main(int argc, char const *argv[])
{
    size_t pool_size = 4;
    ManageSessions* sessions = new ManageSessions(pool_size);
    TCPServer* server = new TCPServer(1883, 4, sessions);
    
    if (!server->init())
        return 1;

    try {
        server->run();
    }
    catch(const std::exception& e) {
        std::cerr << e.what() << '\n';
    }

    delete server;
    return 0;
}
