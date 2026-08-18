# WSS文件传输
一个基于Boost.Beast和OpenSSL的安全文件传输系统，使用WebSocket over TLS (WSS)协议实现文件下载功能。项目CMakeLists.txt适用于Windows平台MinGW编译工具链

## 依赖项
- C++17 或更高版本

- Boost.Asio

- Boost.Beast

- Boost.Locale

- Boost.System

- OpenSSL

- spdlog

- nlohmann_json

## 协议说明
客户端和服务器使用json和简单的文本协议进行通信：

1. 客户端发送：{"file_name":"<文件名>"}

2. 服务器响应：{"code":<响应码>,"file_name":"<文件名>","size":<文件大小>}

3. 服务器发送文件数据（二进制模式）

4. 服务器发送结束标记：FILE END

## 安全注意事项
1. 默认使用自签名证书，生产环境应使用可信证书颁发机构签发的证书

2. 服务器实施了路径遍历保护，确保客户端无法访问系统其他文件

3. 支持 TLS 1.2+，禁用不安全的旧版协议
