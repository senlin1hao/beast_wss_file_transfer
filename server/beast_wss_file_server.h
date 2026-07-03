#ifndef _BEAST_WSS_FILE_SERVER_H_INCLUDED_
#define _BEAST_WSS_FILE_SERVER_H_INCLUDED_

#include <string>
#include <vector>
#include <fstream>
#include <cstdint>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>
#include <spdlog/spdlog.h>
#include <spdlog/async.h>

namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace net = boost::asio;
namespace ssl = boost::asio::ssl;

using tcp = boost::asio::ip::tcp;
using std::ifstream;
using std::string;
using std::thread;
using std::vector;
using std::shared_ptr;

namespace wss_file_server
{
    constexpr int64_t NETWORK_TIMEOUT = 10; // seconds
    constexpr size_t FILE_BUFFER_SIZE = 32 * 1024;
    constexpr size_t MAX_LOG_SIZE = 5 * 1024 * 1024;
    constexpr size_t MAX_LOG_COUNT = 3;
    constexpr size_t SESSION_LOG_QUEUE_SIZE = 8192;
    constexpr size_t SESSION_LOG_THREAD_COUNT = 1;
}

class WssFileServerSession : public std::enable_shared_from_this<WssFileServerSession>
{
private:
    websocket::stream<beast::ssl_stream<beast::tcp_stream>> ws;
    beast::flat_buffer net_buffer;
    string file_name;
    ifstream file;
    vector<char> file_buffer;
    static shared_ptr<spdlog::async_logger> logger;

    void on_ssl_handshake();
    void on_websocket_accept();
    void on_read_request();
    bool is_save_path(const string& path);
    void send_file();
    void send_next_block(size_t file_size, size_t sent_size);
    void send_file_end();
    void session_close();

public:
    WssFileServerSession(tcp::socket&& socket, ssl::context& ctx);

    void run();
};

class WssFileServer
{
private:
    bool running;
    size_t thread_num;
    net::io_context net_context;
    tcp::endpoint endpoint;
    ssl::context ssl_context;
    tcp::acceptor acceptor;
    static shared_ptr<spdlog::logger> logger;

    void run();

public:
    WssFileServer(const char* ip, uint16_t port, size_t thread_num, const char* cert_file, const char* cert_key_file);

    void start();
};

#endif /* _BEAST_WSS_FILE_SERVER_H_INCLUDED_ */
