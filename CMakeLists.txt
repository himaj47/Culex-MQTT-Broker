cmake_minimum_required(VERSION 3.0.2)
project(culex-mqtt)

# Set C++17 standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(
  mqtt/include
  core/include
)


add_subdirectory(core)

add_executable(${PROJECT_NAME}
    mqtt/src/mqttbroker.cpp
    mqtt/src/packethandler.cpp
    mqtt/src/topictree.cpp
    mqtt/src/managesession.cpp
)

target_link_directories(${PROJECT_NAME} PRIVATE /usr/local/lib)
target_link_libraries(${PROJECT_NAME} culex-core)