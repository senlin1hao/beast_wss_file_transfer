#include <cstdlib>
#include <thread>
#include <cstdint>
#include <mutex>
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
using json = nlohmann::json;

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
    json request_json;
    try
    {
        std::string_view sv(static_cast<const char*>(net_buffer.data().data()), net_buffer.data().size());
        request_json = json::parse(sv);
        net_buffer.consume(net_buffer.size());

        logger->info("request: {}", request_json.dump());
        file_name = request_json.get<FileRequest>().file_name;
    }
    catch (const json::exception& e)
    {
        logger->error("request json deserialize error: {}", e.what());

        send_error_response_and_close(wss_file_server::FILE_SIZE_RESPONSE_CODE::DESERIALIZE_ERROR, "", 0);

        return;
    }

    if (!is_save_path(file_name))
    {
        logger->error("request file path is invalid: {}", file_name);

        send_error_response_and_close(wss_file_server::FILE_SIZE_RESPONSE_CODE::FILE_NOT_FOUND, file_name, 0);
        return;
    }

    send_file();
}

bool WssFileServerSession::is_save_path(const string& path)
{
    if (path.empty())
    {
        return false;
    }

    std::filesystem::path request_path(path);

    // 拒绝绝对路径、盘符路径以及以根目录分隔符开头的路径，同时拒绝 NTFS 备用数据流（如 "test.txt:stream"）
    if (request_path.has_root_name() || request_path.has_root_directory() || (path.find(':') != string::npos))
    {
        return false;
    }

    std::error_code ec;

    // 规范化 FILE_DIR（会解析已存在部分的软链接），失败则拒绝请求
    std::filesystem::path save_path = std::filesystem::weakly_canonical(std::filesystem::path(FILE_DIR), ec);
    if (ec)
    {
        return false;
    }

    // 归一化请求路径
    std::filesystem::path target_path = (save_path / request_path).lexically_normal();

    // 归一化后必须仍位于 FILE_DIR 之内：相对路径非空、不是当前目录，且首段不是 ".."
    std::filesystem::path relative_path = target_path.lexically_relative(save_path);
    if (relative_path.empty() || (relative_path == ".") || (*relative_path.begin() == ".."))
    {
        return false;
    }

    // 再解析一次软链接，防止在 FILE_DIR 内通过软链接逃逸到目录之外
    std::filesystem::path canonical_path = std::filesystem::weakly_canonical(target_path, ec);
    if (ec)
    {
        return false;
    }

    std::filesystem::path canonical_relative = canonical_path.lexically_relative(save_path);
    if (canonical_relative.empty() || (canonical_relative == ".") || (*canonical_relative.begin() == ".."))
    {
        return false;
    }

    return true;
}

void WssFileServerSession::send_file()
{
    std::filesystem::path file_path(FILE_DIR);
    file_path.append(file_name);

    file.open(file_path.string(), std::ios::binary);
    if (!file.is_open())
    {
        logger->error("open file error: {}", file_name);
        send_error_response_and_close(wss_file_server::FILE_SIZE_RESPONSE_CODE::FILE_NOT_FOUND, file_name, 0);
        return;
    }

    file.seekg(0, file.end);
    const std::streamoff file_size_offset = file.tellg();
    if (file_size_offset < 0)
    {
        logger->error("get file size error: {}", file_name);
        session_close();
        return;
    }
    size_t file_size = static_cast<size_t>(file_size_offset);
    file.seekg(0, file.beg);

    FileSizeResponse response;
    response.code = wss_file_server::FILE_SIZE_RESPONSE_CODE::OK;
    response.file_name = file_name;
    response.size = file_size;
    async_write_response(response, [self = shared_from_this(), file_size]() {
        self->ws.binary(true);
        self->send_next_block(file_size, 0);
    });
}

void WssFileServerSession::send_next_block(size_t file_size, size_t sent_size)
{
    file.read(file_buffer.data(), file_buffer.size());
    size_t read_size = file.gcount();

    if (read_size == 0)
    {
        if (sent_size < file_size)
        {
            logger->error("read file error or file truncated, file: {}, sent: {} / {}", file_name, sent_size, file_size);
            session_close();
        }
        else
        {
            ws.binary(false);
            send_file_end();
        }
        return;
    }

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
    ws.async_write(net::buffer(*response), [self = shared_from_this(), response](beast::error_code ec, size_t) {
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

void WssFileServerSession::async_write_response(FileSizeResponse response, std::function<void()> on_written)
{
    json response_json = response;
    ws.next_layer().next_layer().expires_after(std::chrono::seconds(wss_file_server::NETWORK_TIMEOUT));
    std::shared_ptr<string> response_str = std::make_shared<string>(response_json.dump());
    ws.async_write(net::buffer(*response_str), [self = shared_from_this(), response_str, on_written = std::move(on_written)](beast::error_code ec, size_t) {
        if (ec)
        {
            logger->error("write error: {}", boost::locale::conv::between(ec.message(), "UTF-8", "GBK"));
            return;
        }

        on_written();
    });
}

void WssFileServerSession::send_error_response_and_close(int code, const string& name, size_t size)
{
    FileSizeResponse response;
    response.code = code;
    response.file_name = name;
    response.size = size;
    async_write_response(response, [self = shared_from_this()]() { self->session_close(); });
}

WssFileServerSession::WssFileServerSession(tcp::socket&& socket, ssl::context& ctx)
    : ws(std::move(socket), ctx), file_buffer(wss_file_server::FILE_BUFFER_SIZE)
{
    static std::once_flag logger_once;
    std::call_once(logger_once, []() {
        auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(SERVER_SESSION_LOG_PATH, wss_file_server::MAX_LOG_SIZE, wss_file_server::MAX_LOG_COUNT);
        vector<spdlog::sink_ptr> sinks = {console_sink, file_sink};
        logger = std::make_shared<spdlog::async_logger>("wss_file_server_session", sinks.begin(), sinks.end(), spdlog::thread_pool(), spdlog::async_overflow_policy::block);
        spdlog::register_logger(logger);
    });
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

            // 出错后继续接受新连接；acceptor 已关闭（如主动停止）时才不再重新注册
            if (acceptor.is_open())
            {
                run();
            }
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
