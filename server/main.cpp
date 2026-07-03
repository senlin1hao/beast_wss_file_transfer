#include <chrono>

#include "beast_wss_file_server.h"

const char* CERT_FILE = "./certificate/fullchain.pem";
const char* CERT_KEY_FILE = "./certificate/privkey.pem";

int main()
{
    spdlog::init_thread_pool(wss_file_server::SESSION_LOG_QUEUE_SIZE, wss_file_server::SESSION_LOG_THREAD_COUNT);
    spdlog::flush_every(std::chrono::seconds(3));
    spdlog::flush_on(spdlog::level::level_enum::warn);

    WssFileServer server("::", 34094, 4, CERT_FILE, CERT_KEY_FILE);
    server.start();

    return 0;
}
