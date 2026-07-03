#include <spdlog/spdlog.h>

#include "beast_wss_file_client.h"

const char* CERT_FILE = "./certificate/fullchain.pem";

int main(int argc, char* argv[])
{
    if (argc != 2)
    {
        spdlog::error("Usage: {} <file_name>", argv[0]);
        return -1;
    }

    const char* file_name = argv[1];

    WssFileClient client("127.0.0.1", 34094, CERT_FILE);

    client.connect();
    client.download_file(file_name);
    client.disconnect();

    return 0;
}
