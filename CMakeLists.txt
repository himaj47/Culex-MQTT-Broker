cmake_minimum_required(VERSION 3.0.2)
project(culex-mqtt)

# Set C++17 standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")

include_directories(
  mqtt/include
  core/include
)


add_subdirectory(core)

add_executable(${PROJECT_NAME}
    mqtt/src/mqttsession.cpp
    mqtt/src/topictree.cpp
    mqtt/src/managesession.cpp
    mqtt/src/handlers.cpp
    mqtt/src/packets.cpp
    mqtt/src/broker.cpp
    mqtt/src/unpack.cpp
)

target_compile_options(${PROJECT_NAME} PRIVATE -fsanitize=address -g -O0)
target_link_options(${PROJECT_NAME} PRIVATE -fsanitize=address)

target_link_directories(${PROJECT_NAME} PRIVATE /usr/local/lib)
target_link_libraries(${PROJECT_NAME} culex-core)