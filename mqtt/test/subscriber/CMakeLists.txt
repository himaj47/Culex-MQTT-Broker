cmake_minimum_required(VERSION 3.0.2)
project(subscriber)

# Set C++17 standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(
  include
)

add_executable(${PROJECT_NAME}_qos0
    qos0.cpp 
)

add_executable(${PROJECT_NAME}_qos1
    qos1.cpp 
)

add_executable(${PROJECT_NAME}_qos2
    qos2.cpp 
)


