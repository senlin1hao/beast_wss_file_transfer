#include <cstdlib>
#include <thread>
#include <cstdint>
#include <filesystem>

#include <boost/locale.hpp>
#include <boost/beast/core.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/rotating_file_sink.h>

#include "beast_wss_file_server.h"

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

const char* FILE_DIR = "./files";
const char* SERVER_LOG_PATH = "./log/wss_file_server.log";
const char* SERVER_SESSION_LOG_PATH = "./log/wss_file_server_session.log";

shared_ptr<spdlog::async_logger> WssFileServerSession::logger = nullptr;
shared_ptr<spdlog::logger> WssFileServer::logger = nullptr;

void WssFileServerSession::on_ssl_handshake()
{
    ws.next_layer().next_layer().expires_after(std::chrono::seconds(wss_file_server::NETWORK_TIMEOUT));
    ws.async_accept([self = shared_from_this()](beast::error_code ec) {
        if (ec)
        {
            logger->error("accept error: {}", boost::locale::conv::between(ec.message(), "UTF-8", "GBK"));
            return;
        }

        self->on_websocket_accept();
    });
}

void WssFileServerSession::on_websocket_accept()
{
    ws.next_layer().next_layer().expires_after(std::chrono::seconds(wss_file_server::NETWORK_TIMEOUT));
    ws.async_read(net_buffer, [self = shared_from_this()](beast::error_code ec, size_t) {
        if (ec)
        {
            if (ec == websocket::error::closed)
            {
                return;
            }

            logger->error("read error: {}", boost::locale::conv::between(ec.message(), "UTF-8", "GBK"));
            return;
        }
        self->on_read_request();
    });
}

void WssFileServerSession::on_read_request()
{
    string request = beast::buffers_to_string(net_buffer.data());
    logger->info("request: {}", request);
    net_buffer.consume(net_buffer.size());

    if ((request.size() < 6) || (request.substr(0, 6) != "FILE: "))
    {
        ws.next_layer().next_layer().expires_after(std::chrono::seconds(wss_file_server::NETWORK_TIMEOUT));
        std::shared_ptr<string> response = std::make_shared<string>("INVALID REQUEST");
        ws.async_write(net::buffer(*response), [self = shared_from_this()](beast::error_code ec, size_t) {
            if (ec)
            {
                logger->error("write error: {}", boost::locale::conv::between(ec.message(), "UTF-8", "GBK"));
                return;
            }

            self->session_close();
        });

        return;
    }

    file_name = request.substr(6);

    if (!is_save_path(file_name))
    {
        ws.next_layer().next_layer().expires_after(std::chrono::seconds(wss_file_server::NETWORK_TIMEOUT));
        std::shared_ptr<string> response = std::make_shared<string>("FILE NOT FOUND");
        ws.async_write(net::buffer(*response), [self = shared_from_this()](beast::error_code ec, size_t) {
            if (ec)
            {
                logger->error("write error: {}", boost::locale::conv::between(ec.message(), "UTF-8", "GBK"));
                return;
            }

            self->session_close();
        });
        return;
    }

    send_file();
}

bool WssFileServerSession::is_save_path(const string& path)
{
    std::filesystem::path request_path(FILE_DIR);
    request_path.append(path);
    request_path.lexically_normal();

    std::filesystem::path save_path(FILE_DIR);
    save_path.lexically_normal();

    std::filesystem::path relative_path = std::filesystem::relative(request_path, save_path);
    string relative_path_str = relative_path.string();

    return ((!relative_path_str.empty()) && (relative_path_str.find("..") == string::npos));
}

void WssFileServerSession::send_file()
{
    std::filesystem::path file_path(FILE_DIR);
    file_path.append(file_name);

    file.open(file_path.string(), std::ios::binary);
    if (!file.is_open())
    {
        ws.next_layer().next_layer().expires_after(std::chrono::seconds(wss_file_server::NETWORK_TIMEOUT));
        std::shared_ptr<string> response = std::make_shared<string>("SERVER FILE OPEN ERROR");
        ws.async_write(net::buffer(*response), [self = shared_from_this()](beast::error_code ec, size_t) {
            if (ec)
            {
                logger->error("write error: {}", boost::locale::conv::between(ec.message(), "UTF-8", "GBK"));
                return;
            }

            self->session_close();
        });
        return;
    }

    file.seekg(0, file.end);
    size_t file_size = file.tellg();
    file.seekg(0, file.beg);

    ws.next_layer().next_layer().expires_after(std::chrono::seconds(wss_file_server::NETWORK_TIMEOUT));
    std::shared_ptr<string> response = std::make_shared<string>("FILE: " + file_name + " SIZE: " + std::to_string(file_size));
    ws.async_write(net::buffer(*response), [self = shared_from_this(), file_size](beast::error_code ec, size_t) {
        if (ec)
        {
            logger->error("write error: {}", boost::locale::conv::between(ec.message(), "UTF-8", "GBK"));
            return;
        }

        self->ws.binary(true);
        self->send_next_block(file_size, 0);
    });
}

void WssFileServerSession::send_next_block(size_t file_size, size_t sent_size)
{
    file.read(file_buffer.data(), file_buffer.size());
    size_t read_size = file.gcount();

    ws.next_layer().next_layer().expires_after(std::chrono::seconds(wss_file_server::NETWORK_TIMEOUT));
    ws.async_write(net::buffer(file_buffer.data(), read_size), [self = shared_from_this(), sent_size, read_size, file_size](beast::error_code ec, size_t) {
        if (ec)
        {
            logger->error("write error: {}", boost::locale::conv::between(ec.message(), "UTF-8", "GBK"));
            return;
        }
        if (sent_size + read_size < file_size)
        {
            self->send_next_block(file_size, sent_size + read_size);
        }
        else
        {
            self->ws.binary(false);
            self->send_file_end();
        }
    });
}

void WssFileServerSession::send_file_end()
{
    file.close();

    ws.next_layer().next_layer().expires_after(std::chrono::seconds(wss_file_server::NETWORK_TIMEOUT));
    std::shared_ptr<string> response = std::make_shared<string>("FILE END");
    ws.async_write(net::buffer(*response), [self = shared_from_this()](beast::error_code ec, size_t) {
        if (ec)
        {
            logger->error("write error: {}", boost::locale::conv::between(ec.message(), "UTF-8", "GBK"));
            return;
        }

        self->on_websocket_accept();
    });
}

void WssFileServerSession::session_close()
{
    ws.next_layer().next_layer().expires_after(std::chrono::seconds(wss_file_server::NETWORK_TIMEOUT));
    ws.async_close(websocket::close_code::normal, [self = shared_from_this()](beast::error_code ec) {
        if (ec)
        {
            logger->error("close error: {}", boost::locale::conv::between(ec.message(), "UTF-8", "GBK"));
            return;
        }

        self->file.close();
        logger->info("session closed");
    });
}

WssFileServerSession::WssFileServerSession(tcp::socket&& socket, ssl::context& ctx)
    : ws(std::move(socket), ctx), file_buffer(wss_file_server::FILE_BUFFER_SIZE)
{
    if (logger == nullptr)
    {
        auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(SERVER_SESSION_LOG_PATH, wss_file_server::MAX_LOG_SIZE, wss_file_server::MAX_LOG_COUNT);
        vector<spdlog::sink_ptr> sinks = {console_sink, file_sink};
        logger = std::make_shared<spdlog::async_logger>("wss_file_server_session", sinks.begin(), sinks.end(), spdlog::thread_pool(), spdlog::async_overflow_policy::block);
        spdlog::register_logger(logger);
    }
}

void WssFileServerSession::run()
{
    ws.next_layer().next_layer().expires_after(std::chrono::seconds(wss_file_server::NETWORK_TIMEOUT));
    ws.next_layer().async_handshake(ssl::stream_base::server, [self = shared_from_this()](beast::error_code ec) {
        if (ec)
        {
            logger->error("handshake error: {}", boost::locale::conv::between(ec.message(), "UTF-8", "GBK"));
            return;
        }
        self->on_ssl_handshake();
    });
}

void WssFileServer::run()
{
    acceptor.async_accept([this](beast::error_code ec, tcp::socket socket) {
        if (ec)
        {
            logger->error("accept error: {}", boost::locale::conv::between(ec.message(), "UTF-8", "GBK"));
            return;
        }
        std::make_shared<WssFileServerSession>(std::move(socket), ssl_context)->run();
        run();
    });
}

WssFileServer::WssFileServer(const char* ip, uint16_t port, size_t thread_num, const char* cert_file, const char* cert_key_file)
    : running(false), thread_num(thread_num), net_context(thread_num), endpoint(net::ip::make_address(ip), port),
      ssl_context(ssl::context::tls_server), acceptor(net_context)
{
    if (logger == nullptr)
    {
        auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(SERVER_LOG_PATH, wss_file_server::MAX_LOG_SIZE, wss_file_server::MAX_LOG_COUNT);
        vector<spdlog::sink_ptr> sinks = {console_sink, file_sink};
        logger = std::make_shared<spdlog::logger>("wss_file_server", sinks.begin(), sinks.end());
        spdlog::register_logger(logger);
    }

    ssl_context.set_options(ssl::context::default_workarounds | ssl::context::no_sslv2 | ssl::context::no_sslv3 | ssl::context::no_tlsv1 | ssl::context::no_tlsv1_1 | ssl::context::single_dh_use);
    ssl_context.use_certificate_file(cert_file, ssl::context::pem);
    ssl_context.use_private_key_file(cert_key_file, ssl::context::pem);

    beast::error_code ec;
    acceptor.open(endpoint.protocol(), ec);
    if (ec)
    {
        logger->error("open acceptor error: {}", boost::locale::conv::between(ec.message(), "UTF-8", "GBK"));
        return;
    }

    if (endpoint.address().is_v6())
    {
        acceptor.set_option(net::ip::v6_only(false));
    }

    acceptor.bind(endpoint, ec);
    if (ec)
    {
        logger->error("bind acceptor error: {}", boost::locale::conv::between(ec.message(), "UTF-8", "GBK"));
        return;
    }

    acceptor.listen(net::socket_base::max_listen_connections, ec);
    if (ec)
    {
        logger->error("listen acceptor error: {}", boost::locale::conv::between(ec.message(), "UTF-8", "GBK"));
        return;
    }
}

void WssFileServer::start()
{
    running = true;
    run();

    vector<thread> threads(thread_num);
    for (auto& i : threads)
    {
        i = thread([this]() { net_context.run(); });
    }

    for (auto& i : threads)
    {
        i.join();
    }

    running = false;
}
