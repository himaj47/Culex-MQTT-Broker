#include "broker.h"
#include "managesessions.h"

using namespace culex;

int main(int argc, char const *argv[])
{
    size_t pool_size = 4;
    ManageSessions* sessions = new ManageSessions(pool_size);
    Broker* broker = new Broker(1883, 4, sessions);
    
    if (!broker->initialize())
        return 1;

    try {
        broker->run();
    }
    catch(const std::exception& e) {
        std::cerr << e.what() << '\n';
    }

    delete broker;
    return 0;
}
