#ifndef _BEAST_WSS_FILE_CLIENT_H_INCLUDED_
#define _BEAST_WSS_FILE_CLIENT_H_INCLUDED_

#include <string>
#include <string_view>
#include <vector>
#include <cstdint>
#include <cstddef>
#include <memory>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/connect.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>

namespace wss_file_client
{
    constexpr size_t MAX_LOG_SIZE = 5 * 1024 * 1024;
    constexpr size_t MAX_LOG_COUNT = 3;

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

class WssFileClient
{
private:
    bool connected;
    boost::asio::io_context net_context;
    boost::asio::ssl::context ssl_context;
    boost::beast::websocket::stream<boost::beast::ssl_stream<boost::beast::tcp_stream>> ws;
    std::string host;
    uint16_t port;
    static std::shared_ptr<spdlog::logger> logger;

public:
    WssFileClient(const char* host, uint16_t port, const char* cert_file);
    ~WssFileClient();

    int connect();
    int download_file(std::string_view file_name);
    int disconnect();
};

#endif /* _BEAST_WSS_FILE_CLIENT_H_INCLUDED_ */
