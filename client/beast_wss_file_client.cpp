#include <filesystem>
#include <fstream>
#include <regex>
#include <string>
#include <vector>

#include <boost/locale.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/rotating_file_sink.h>

#include "beast_wss_file_client.h"

namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace net = boost::asio;
namespace ssl = boost::asio::ssl;

using tcp = boost::asio::ip::tcp;
using std::ofstream;
using std::string;
using std::vector;
using std::shared_ptr;

const char* DOWNLOAD_DIR = "./download";
const char* CLIENT_LOG_PATH = "./log/wss_file_client.log";

shared_ptr<spdlog::logger> WssFileClient::logger = nullptr;

WssFileClient::WssFileClient(const char* host, uint16_t port, const char* cert_file)
    : connected(false), net_context(1), ssl_context(ssl::context::tls_client), ws(net_context, ssl_context),
      host(host), port(port)
{
    if (logger == nullptr)
    {
        auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(CLIENT_LOG_PATH, wss_file_client::MAX_LOG_SIZE, wss_file_client::MAX_LOG_COUNT);
        vector<spdlog::sink_ptr> sinks = {console_sink, file_sink};
        logger = std::make_shared<spdlog::logger>("wss_file_client", sinks.begin(), sinks.end());
        spdlog::register_logger(logger);
    }

    ssl_context.set_options(ssl::context::default_workarounds | ssl::context::no_sslv2 | ssl::context::no_sslv3 | ssl::context::no_tlsv1 | ssl::context::no_tlsv1_1 | ssl::context::single_dh_use);
    std::filesystem::path cert_file_path(cert_file);
    if (!std::filesystem::exists(cert_file_path))
    {
        logger->error("cert file not exist: {}", cert_file);
        return;
    }
    ssl_context.load_verify_file(cert_file);
    ws.next_layer().set_verify_mode(ssl::context::verify_peer | ssl::context::verify_fail_if_no_peer_cert);
    ws.next_layer().set_verify_callback(ssl::host_name_verification(this->host));
}

WssFileClient::~WssFileClient()
{
    if (connected)
    {
        disconnect();
    }
}

int WssFileClient::connect()
{
    if (connected)
    {
        return 0;
    }

    beast::error_code ec;

    tcp::resolver resolver(net_context);
    const auto results = resolver.resolve(host, std::to_string(port));

    ws.next_layer().next_layer().connect(results, ec);
    if (ec)
    {
        logger->error("connect error: {}", boost::locale::conv::between(ec.message(), "UTF-8", "GBK"));
        return -1;
    }

    SSL_set_tlsext_host_name(ws.next_layer().native_handle(), host.c_str());  // 设置SNI

    ws.next_layer().handshake(ssl::stream_base::client, ec);
    if (ec)
    {
        logger->error("handshake error: {}", boost::locale::conv::between(ec.message(), "UTF-8", "GBK"));
        return -1;
    }

    ws.handshake(host, "/", ec);
    if (ec)
    {
        logger->error("handshake error: {}", boost::locale::conv::between(ec.message(), "UTF-8", "GBK"));
        return -1;
    }

    connected = true;

    return 0;
}

int WssFileClient::download_file(string_view file_name)
{
    if (!connected)
    {
        logger->error("not connected");
        return -1;
    }

    string request = "FILE: ";
    request += file_name;
    ws.write(net::buffer(request));

    beast::flat_buffer net_buffer;
    ws.read(net_buffer);
    string response = beast::buffers_to_string(net_buffer.data());
    net_buffer.consume(net_buffer.size());

    std::regex valid_response("FILE: (.*) SIZE: (\\d+)");
    if (!std::regex_match(response, valid_response))
    {
        logger->error("response error: {}", response);
        return -1;
    }

    logger->info("response: {}", response);
    string file_name_received = std::regex_replace(response, valid_response, "$1");
    if (file_name_received != file_name)
    {
        logger->error("file name error: {}", file_name_received);
        ws.close(websocket::close_code::normal);
        return -1;
    }
    size_t file_size = std::stoull(std::regex_replace(response, valid_response, "$2"));

    std::filesystem::path file_path(DOWNLOAD_DIR);
    file_path.append(file_name);

    ofstream file(file_path.string(), std::ios::binary);
    if (!file.is_open())
    {
        logger->error("open file error: {}", file_path.string());
        return -1;
    }

    ws.binary(true);
    size_t received_size = 0;
    while (received_size < file_size)
    {
        ws.read(net_buffer);
        file.write(static_cast<const char*>(net_buffer.data().data()), net_buffer.size());
        received_size += net_buffer.size();
        net_buffer.consume(net_buffer.size());
    }

    ws.binary(false);
    ws.read(net_buffer);
    response = beast::buffers_to_string(net_buffer.data());
    net_buffer.consume(net_buffer.size());
    if (response != "FILE END")
    {
        logger->error("response error: {}", response);
        return -1;
    }

    file.close();

    logger->info("download success: {}", file_path.string());

    return 0;
}

int WssFileClient::disconnect()
{
    if (!connected)
    {
        logger->error("not connected");
        return -1;
    }

    ws.close(websocket::close_code::normal);

    connected = false;

    return 0;
}
