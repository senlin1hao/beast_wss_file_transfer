#ifndef _BEAST_WSS_FILE_SERVER_H_INCLUDED_
#define _BEAST_WSS_FILE_SERVER_H_INCLUDED_

#include <string>
#include <vector>
#include <fstream>
#include <cstdint>
#include <cstddef>
#include <memory>
#include <functional>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>
#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <nlohmann/json.hpp>

namespace wss_file_server
{
    constexpr int64_t NETWORK_TIMEOUT = 10; // seconds
    constexpr size_t FILE_BUFFER_SIZE = 32 * 1024;
    constexpr size_t MAX_LOG_SIZE = 5 * 1024 * 1024;
    constexpr size_t MAX_LOG_COUNT = 3;
    constexpr size_t SESSION_LOG_QUEUE_SIZE = 8192;
    constexpr size_t SESSION_LOG_THREAD_COUNT = 1;

    enum FILE_SIZE_RESPONSE_CODE
    {
        OK = 0,
        DESERIALIZE_ERROR = -1,
        FILE_NOT_FOUND = -2
    };
}

struct FileRequest
{
    std::string file_name;

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(FileRequest, file_name);
};

struct FileSizeResponse
{
    int code;
    std::string file_name;
    size_t size;

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(FileSizeResponse, code, file_name, size);
};

class WssFileServerSession : public std::enable_shared_from_this<WssFileServerSession>
{
private:
    boost::beast::websocket::stream<boost::beast::ssl_stream<boost::beast::tcp_stream>> ws;
    boost::beast::flat_buffer net_buffer;
    std::string file_name;
    std::ifstream file;
    std::vector<char> file_buffer;
    static std::shared_ptr<spdlog::async_logger> logger;

    void on_ssl_handshake();
    void on_websocket_accept();
    void on_read_request();
    bool is_save_path(const std::string& path);
    void send_file();
    void send_next_block(size_t file_size, size_t sent_size);
    void send_file_end();
    void session_close();
    void async_write_response(FileSizeResponse response, std::function<void()> on_written);
    void send_error_response_and_close(int code, const std::string& name, size_t size);

public:
    WssFileServerSession(boost::asio::ip::tcp::socket&& socket, boost::asio::ssl::context& ctx);

    void run();
};

class WssFileServer
{
private:
    bool running;
    size_t thread_num;
    boost::asio::io_context net_context;
    boost::asio::ip::tcp::endpoint endpoint;
    boost::asio::ssl::context ssl_context;
    boost::asio::ip::tcp::acceptor acceptor;
    static std::shared_ptr<spdlog::logger> logger;

    void run();

public:
    WssFileServer(const char* ip, uint16_t port, size_t thread_num, const char* cert_file, const char* cert_key_file);

    void start();
};

#endif /* _BEAST_WSS_FILE_SERVER_H_INCLUDED_ */
