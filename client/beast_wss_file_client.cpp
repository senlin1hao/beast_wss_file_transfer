#include <filesystem>
#include <fstream>
#include <regex>
#include <string>
#include <vector>

#include <openssl/evp.h>

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
using std::string_view;
using std::vector;
using std::shared_ptr;
using nlohmann::json;

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
    const auto results = resolver.resolve(host, std::to_string(port), ec);
    if (ec)
    {
        logger->error("resolve error: {}", boost::locale::conv::between(ec.message(), "UTF-8", "GBK"));
        return -1;
    }

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

    beast::error_code ec;

    FileRequest request;
    request.file_name = file_name;
    json request_json = request;
    ws.write(net::buffer(request_json.dump()), ec);
    if (ec)
    {
        logger->error("write request error: {}", boost::locale::conv::between(ec.message(), "UTF-8", "GBK"));
        disconnect();
        return -1;
    }

    beast::flat_buffer net_buffer;
    ws.read(net_buffer, ec);
    if (ec)
    {
        logger->error("read response error: {}", boost::locale::conv::between(ec.message(), "UTF-8", "GBK"));
        disconnect();
        return -1;
    }

    std::string_view response_sv;
    json response_json;
    FileResponse file_size_response;
    try
    {
        response_sv = std::string_view(static_cast<const char*>(net_buffer.data().data()), net_buffer.data().size());
        response_json = json::parse(response_sv);
        net_buffer.consume(net_buffer.size());
        file_size_response = response_json.get<FileResponse>();
    }
    catch (const json::exception& e)
    {
        logger->error("response json deserialize error: {}, response: {}", e.what(), response_sv);

        disconnect();
        return -1;
    }

    if (file_size_response.code != wss_file_client::FILE_SIZE_RESPONSE_CODE::OK)
    {
        logger->error("file size response error: {}", file_size_response.code);

        disconnect();
        return -1;
    }

    logger->info("response: {}", response_json.dump());
    string file_name_received = file_size_response.file_name;
    if (file_name_received != file_name)
    {
        logger->error("file name error: {}", file_name_received);
        disconnect();
        return -1;
    }
    size_t file_size = file_size_response.size;

    std::filesystem::path file_path(DOWNLOAD_DIR);
    file_path.append(file_name);

    ofstream file(file_path.string(), std::ios::binary);
    if (!file.is_open())
    {
        logger->error("open file error: {}", file_path.string());

        disconnect();
        return -1;
    }

    // 边下载边计算 sha256，用于完整性校验
    std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> md_ctx(EVP_MD_CTX_new(), &EVP_MD_CTX_free);
    if (!md_ctx || (EVP_DigestInit_ex(md_ctx.get(), EVP_sha256(), nullptr) != 1))
    {
        logger->error("init sha256 error");
        disconnect();
        return -1;
    }

    ws.binary(true);
    size_t received_size = 0;
    while (received_size < file_size)
    {
        ws.read(net_buffer, ec);
        if (ec)
        {
            logger->error("read file data error: {}", boost::locale::conv::between(ec.message(), "UTF-8", "GBK"));
            disconnect();
            return -1;
        }
        if (EVP_DigestUpdate(md_ctx.get(), net_buffer.data().data(), net_buffer.size()) != 1)
        {
            logger->error("update sha256 error");
            disconnect();
            return -1;
        }
        file.write(static_cast<const char*>(net_buffer.data().data()), net_buffer.size());
        received_size += net_buffer.size();
        net_buffer.consume(net_buffer.size());
    }

    ws.binary(false);
    ws.read(net_buffer, ec);
    if (ec)
    {
        logger->error("read file end error: {}", boost::locale::conv::between(ec.message(), "UTF-8", "GBK"));
        disconnect();
        return -1;
    }
    string response = beast::buffers_to_string(net_buffer.data());
    net_buffer.consume(net_buffer.size());
    if (response != "FILE END")
    {
        logger->error("response error: {}", response);

        disconnect();
        return -1;
    }

    file.close();

    unsigned char hash[EVP_MAX_MD_SIZE];
    unsigned int hash_len = 0;
    if (EVP_DigestFinal_ex(md_ctx.get(), hash, &hash_len) != 1)
    {
        logger->error("finalize sha256 error");
        disconnect();
        return -1;
    }

    static const char* hex_digits = "0123456789abcdef";
    string sha256;
    sha256.reserve(static_cast<size_t>(hash_len) * 2);
    for (unsigned int i = 0; i < hash_len; ++i)
    {
        sha256.push_back(hex_digits[hash[i] >> 4]);
        sha256.push_back(hex_digits[hash[i] & 0x0F]);
    }

    if (sha256 != file_size_response.sha256)
    {
        logger->error("sha256 mismatch: expected {}, actual {}", file_size_response.sha256, sha256);

        disconnect();
        return -1;
    }

    logger->info("download success: {}", file_path.string());

    return 0;
}

int WssFileClient::disconnect()
{
    if (!connected)
    {
        logger->info("not connected");
        return 0;
    }

    beast::error_code close_ec;
    ws.close(websocket::close_code::normal, close_ec);
    connected = false;

    if (close_ec && close_ec != websocket::error::closed)
    {
        logger->warn("close error: {}", boost::locale::conv::between(close_ec.message(), "UTF-8", "GBK"));
    }

    return 0;
}
