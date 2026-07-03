cmake_minimum_required(VERSION 3.12.0)

project(test_boost_beast LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_compile_options(-Wall -Wextra -pthread)

if (MINGW)
    add_compile_options(-Wa,-mbig-obj)
endif ()

set(FILE_SERVER_DIR "server")
set(FILE_CLIENT_DIR "client")

find_package(Boost REQUIRED CONFIG COMPONENTS locale)
find_package(OpenSSL REQUIRED)
find_package(spdlog REQUIRED)

include_directories(
    ${Boost_INCLUDE_DIRS}
    ${FILE_SERVER_DIR}
    ${FILE_CLIENT_DIR}
    ${SPDLOG_INCLUDE_DIR}
)

file(GLOB_RECURSE SERVER_FILES ${FILE_SERVER_DIR}/*.cpp)
file(GLOB_RECURSE CLIENT_FILES ${FILE_CLIENT_DIR}/*.cpp)

add_executable(beast_wss_file_server
    ${SERVER_FILES}
)

add_executable(beast_wss_file_client
    ${CLIENT_FILES}
)

message(STATUS "Boost_LIBRARIES: ${Boost_LIBRARIES}")

target_link_libraries(beast_wss_file_server
    ${Boost_LIBRARIES}
    OpenSSL::SSL
    OpenSSL::Crypto
    ws2_32
    mswsock
    spdlog::spdlog
)

target_link_libraries(beast_wss_file_client
    ${Boost_LIBRARIES}
    OpenSSL::SSL
    OpenSSL::Crypto
    ws2_32
    mswsock
    spdlog::spdlog
)

set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/bin)

set(CMAKE_INSTALL_PREFIX ${CMAKE_CURRENT_SOURCE_DIR}/beast_wss_file_transfer_install)
install(TARGETS beast_wss_file_server DESTINATION server)
install(DIRECTORY DESTINATION server/files)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/server/certificate DESTINATION server/)
install(DIRECTORY DESTINATION server/log)

install(TARGETS beast_wss_file_client DESTINATION client)
install(DIRECTORY DESTINATION client/download)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/client/certificate DESTINATION client/)
install(DIRECTORY DESTINATION client/log)
