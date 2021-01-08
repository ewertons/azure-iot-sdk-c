// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// CAVEAT: This sample is to demonstrate azure IoT client concepts only and is not a guide design principles or style
// Checking of return codes and error values shall be omitted for brevity.  Please practice sound engineering practices 
// when writing production code.

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "iothub.h"
#include "iothub_client_streaming.h"
#include "iothub_device_client.h"
#include "iothub_client_options.h"
#include "iothub_message.h"
#include "azure_c_shared_utility/socketio.h"
#include "azure_c_shared_utility/platform.h"
#include "azure_c_shared_utility/threadapi.h"
#include "azure_c_shared_utility/crt_abstractions.h"
#include "azure_c_shared_utility/shared_util_options.h"
#include "azure_c_shared_utility/ws_url.h"
#include "azure_c_shared_utility/uws_client.h"
#include "azure_c_shared_utility/tlsio.h"
#include "azure_c_shared_utility/http_proxy_io.h"

#ifdef SET_TRUSTED_CERT_IN_SAMPLES
#include "certs.h"
#endif // SET_TRUSTED_CERT_IN_SAMPLES

/* This sample uses the _LL APIs of iothub_client for example purposes.
Simply changing the using the convenience layer (functions not having _LL)
and removing calls to _DoWork will yield the same results. */

// The protocol you wish to use should be uncommented
//
#define SAMPLE_MQTT
//#define SAMPLE_MQTT_OVER_WEBSOCKETS
//#define SAMPLE_AMQP
//#define SAMPLE_AMQP_OVER_WEBSOCKETS
//#define SAMPLE_HTTP

#ifdef SAMPLE_MQTT
    #include "iothubtransportmqtt.h"
#endif // SAMPLE_MQTT
#ifdef SAMPLE_MQTT_OVER_WEBSOCKETS
    #include "iothubtransportmqtt_websockets.h"
#endif // SAMPLE_MQTT_OVER_WEBSOCKETS
#ifdef SAMPLE_AMQP
    #include "iothubtransportamqp.h"
#endif // SAMPLE_AMQP
#ifdef SAMPLE_AMQP_OVER_WEBSOCKETS
    #include "iothubtransportamqp_websockets.h"
#endif // SAMPLE_AMQP_OVER_WEBSOCKETS
#ifdef SAMPLE_HTTP
    #include "iothubtransporthttp.h"
#endif // SAMPLE_HTTP

#ifdef SET_TRUSTED_CERT_IN_SAMPLES
#include "certs.h"
#endif // SET_TRUSTED_CERT_IN_SAMPLES

MU_DEFINE_ENUM_STRINGS(WS_OPEN_RESULT, WS_OPEN_RESULT_VALUES)
MU_DEFINE_ENUM_STRINGS(WS_ERROR, WS_ERROR_VALUES)
MU_DEFINE_ENUM_STRINGS(WS_SEND_FRAME_RESULT, WS_SEND_FRAME_RESULT_VALUES)
MU_DEFINE_ENUM_STRINGS(IO_SEND_RESULT, IO_SEND_RESULT_VALUES)
MU_DEFINE_ENUM_STRINGS(IO_OPEN_RESULT, IO_OPEN_RESULT_VALUES)

/* Paste in the your iothub connection string  */
static const char* connectionString = "[device connection string]";
static const char* localHost = "127.0.0.1"; // Address of the local server to connect to.
static const int localPort = 22; // Port of the local server to connect to.

static const char* proxy_host = NULL;     // "Web proxy name here"
static int proxy_port = 0;                // Proxy port
static const char* proxy_username = NULL; // Proxy user name
static const char* proxy_password = NULL; // Proxy password

static bool g_continueRunning = true;

#define MAX_CONNECTION_COUNT 100

typedef enum DS_PROXY_CONNECTION_STATE_ENUM
{
    DS_PROXY_CONNECTION_NONE,
    DS_PROXY_CONNECTION_ACTIVE,
    DS_PROXY_CONNECTION_DESTROY
} DS_PROXY_CONNECTION_STATE;

typedef struct DS_PROXY_INFO_TAG
{
    int id;
    DS_PROXY_CONNECTION_STATE state;
    bool is_ws_client_connected;
    bool is_local_socket_connected;
    UWS_CLIENT_HANDLE ws_client_handle;
    XIO_HANDLE local_socket_handle;
    size_t incomingByteCount;
    size_t outgoingByteCount;
    time_t lastTrafficPrintTime;
} DS_PROXY_INFO;

static DS_PROXY_INFO g_connections[MAX_CONNECTION_COUNT];

#define TRAFFIC_COUNTERS_PRINT_FREQ_IN_SECS 10
#define INDEFINITE_TIME ((time_t)-1)

// This is the maximum individual payload size currently supported by Azure IoT Device Streaming Gateway.
// When sending more data, split it it chunks with sizes no bigger than the value below.
static const size_t MAX_WEBSOCKET_PAYLOAD_SIZE_IN_BYTES = 65536;


// Functions for socket connection to local service:

static void on_ws_send_frame_complete(void* context, WS_SEND_FRAME_RESULT ws_send_frame_result)
{
    DS_PROXY_INFO* connection = (DS_PROXY_INFO*)context;

    if (ws_send_frame_result != WS_SEND_FRAME_OK)
    {
        printf("[%d] ERROR: on_ws_send_frame_complete (%d)\r\n", connection->id, ws_send_frame_result);

        connection->state = DS_PROXY_CONNECTION_DESTROY;
    }
}

static void on_bytes_received(void* context, const unsigned char* buffer, size_t size)
{
    DS_PROXY_INFO* connection = (DS_PROXY_INFO*)context;

    if (buffer == NULL)
    {
        (void)printf("[%d] ERROR: on_bytes_received invoked with NULL buffer (unexpected)\r\n", connection->id);
        exit(1);
    }

    if (connection->is_ws_client_connected)
    {
        size_t send_size;

        while (size > 0)
        {
            send_size = size <= MAX_WEBSOCKET_PAYLOAD_SIZE_IN_BYTES ? size : MAX_WEBSOCKET_PAYLOAD_SIZE_IN_BYTES;

            if (uws_client_send_frame_async(
                connection->ws_client_handle, 2, buffer, send_size, true, on_ws_send_frame_complete, connection) != 0)
            {
                (void)printf("ERROR: [%d] Failed sending data to the local service\r\n", connection->id);
                connection->state = DS_PROXY_CONNECTION_DESTROY;
                break;
            }

            connection->outgoingByteCount += size;
            size -= send_size;
            buffer += send_size;
        }
    }
}
static void on_io_open_complete(void* context, IO_OPEN_RESULT open_result)
{
    DS_PROXY_INFO* connection = (DS_PROXY_INFO*)context;

    if (open_result != IO_OPEN_OK)
    {
        (void)printf("ERROR: [%d] Failed opening connection to the local service (%s)\r\n",
            connection->id,
            MU_ENUM_TO_STRING(IO_OPEN_RESULT, open_result));
        
        connection->state = DS_PROXY_CONNECTION_DESTROY;
    }
    else
    {
        (void)printf("[%d] Opened connection to the local service (%s)\r\n",
            connection->id,
            MU_ENUM_TO_STRING(IO_OPEN_RESULT, open_result));

        connection->is_local_socket_connected = true;
    }
}

static void on_io_error(void* context)
{
    DS_PROXY_INFO* connection = (DS_PROXY_INFO*)context;

    (void)printf("ERROR: [%d] Connection to the local service has failed\r\n", connection->id);
    
    connection->state = DS_PROXY_CONNECTION_DESTROY;
}

static XIO_HANDLE connect_to_local_service(DS_PROXY_INFO* connection)
{
    XIO_HANDLE result;
    SOCKETIO_CONFIG io_config;
    io_config.hostname = localHost;
    io_config.port = localPort;

    const IO_INTERFACE_DESCRIPTION* io_interface_description = socketio_get_interface_description();

    result = xio_create(io_interface_description, &io_config);
    (void)xio_open(result, on_io_open_complete, connection, on_bytes_received, connection, on_io_error, connection);

    return result;
}

// Functions for connection to streaming gateway (cloud):

static void on_ws_open_complete(void* context, WS_OPEN_RESULT ws_open_result)
{
    DS_PROXY_INFO* connection = (DS_PROXY_INFO*)context;
    
    if (ws_open_result == WS_OPEN_OK)
    {
        (void)printf("[%d] Client connected to the streaming gateway (%s)\r\n",
            connection->id,
            MU_ENUM_TO_STRING(WS_OPEN_RESULT, ws_open_result));

        connection->local_socket_handle = connect_to_local_service(connection);
        connection->lastTrafficPrintTime = time(NULL);
        connection->is_ws_client_connected = true;

        (void)printf("[%d] Reporting traffic statistics every 10 seconds.\r\n", connection->id);
    }
    else
    {
        (void)printf("ERROR: [%d] Failed connecting to the streaming gateway (%s)\r\n",
            connection->id,
            MU_ENUM_TO_STRING(WS_OPEN_RESULT, ws_open_result));
        
        connection->state = DS_PROXY_CONNECTION_DESTROY;
    }
}

static void on_send_complete(void* context, IO_SEND_RESULT send_result) 
{
    DS_PROXY_INFO* connection = (DS_PROXY_INFO*)context;

    if (send_result != IO_SEND_OK)
    {
        printf("ERROR: [%d] on_send_complete (%d)\r\n", connection->id, send_result);
        connection->state = DS_PROXY_CONNECTION_DESTROY;
    }
}

static void on_ws_frame_received(void* context, unsigned char frame_type, const unsigned char* buffer, size_t size)
{
    (void)frame_type;
    DS_PROXY_INFO* connection = (DS_PROXY_INFO*)context;

    if (connection->is_local_socket_connected)
    {
        if (xio_send(connection->local_socket_handle, buffer, size, on_send_complete, connection) != 0)
        {
            (void)printf("ERROR: [%d] Failed sending data to the local service\r\n", connection->id);
            connection->state = DS_PROXY_CONNECTION_DESTROY;
        }
        else
        {
            connection->incomingByteCount += size;
        }
    }
}

static void on_ws_peer_closed(void* context, uint16_t* close_code, const unsigned char* extra_data, size_t extra_data_length)
{
    (void)extra_data_length;
    DS_PROXY_INFO* connection = (DS_PROXY_INFO*)context;

    (void)printf("[%d] on_ws_peer_closed (", connection->id);

    if (close_code != NULL)
    {
        (void)printf("Code: %d, ", *close_code);
    }

    if (extra_data != NULL)
    {
        (void)printf("Data: %.*s", (int)extra_data_length, extra_data);
    }

    (void)printf(")\r\n");

    connection->state = DS_PROXY_CONNECTION_DESTROY;
}

static void on_ws_error(void* context, WS_ERROR error_code)
{
    DS_PROXY_INFO* connection = (DS_PROXY_INFO*)context;

    (void)printf("ERROR: [%d] on_ws_error (%s)\r\n", connection->id, MU_ENUM_TO_STRING(WS_ERROR, error_code));
    
    connection->state = DS_PROXY_CONNECTION_DESTROY;
}

static UWS_CLIENT_HANDLE create_websocket_client(const DEVICE_STREAM_C2D_REQUEST* stream_request, DS_PROXY_INFO* connection)
{
    UWS_CLIENT_HANDLE result;
    HTTP_PROXY_IO_CONFIG http_proxy_io_config;
    TLSIO_CONFIG tls_io_config;
    const IO_INTERFACE_DESCRIPTION* tlsio_interface;

    WS_URL_HANDLE ws_url;
    WS_PROTOCOL protocols;
    char auth_header_value[1024];

    const char* host;
    size_t host_length;
    const char* path;
    size_t path_length;
    size_t port;

    char host_address[1024];
    char resource_name[1024];

    ws_url = ws_url_create(stream_request->url);
    (void)ws_url_get_host(ws_url, &host, &host_length);
    (void)ws_url_get_path(ws_url, &path, &path_length);
    (void)ws_url_get_port(ws_url, &port);

    (void)memcpy(host_address, host, host_length);
    host_address[host_length] = '\0';

    (void)memcpy(resource_name + 1, path, path_length);
    resource_name[0] = '/';
    resource_name[path_length + 1] = '\0';

    // Establishing the connection to the streaming gateway
#if defined SAMPLE_AMQP || defined SAMPLE_AMQP_OVER_WEBSOCKETS
    protocols.protocol = "AMQP";
#elif defined SAMPLE_HTTP
    protocols.protocol = "HTTP";
#else
    protocols.protocol = "MQTT";
#endif

    (void)sprintf(auth_header_value, "Bearer %s", stream_request->authorization_token);

    // Setting up optional HTTP proxy configuration for connecting to streaming gateway:
    tlsio_interface = platform_get_default_tlsio();

    tls_io_config.hostname = host_address;
    tls_io_config.port = (int)port;

    if (proxy_host != NULL)
    {
        http_proxy_io_config.proxy_hostname = proxy_host;
        http_proxy_io_config.proxy_port = proxy_port;
        http_proxy_io_config.username = proxy_username;
        http_proxy_io_config.password = proxy_password;
        http_proxy_io_config.hostname = host_address;
        http_proxy_io_config.port = (int)port;

        tls_io_config.underlying_io_interface = http_proxy_io_get_interface_description();
        tls_io_config.underlying_io_parameters = &http_proxy_io_config;

        result = uws_client_create_with_io(tlsio_interface, &tls_io_config, host_address, (int)port, resource_name, &protocols, 1);
    }
    else
    {
        result = uws_client_create(host_address, (int)port, resource_name, true, &protocols, 1);
    }
    (void)uws_client_set_request_header(result, "Authorization", auth_header_value);
    (void)uws_client_open_async(result, on_ws_open_complete, connection, 
        on_ws_frame_received, connection, 
        on_ws_peer_closed, connection, 
        on_ws_error, connection);

    ws_url_destroy(ws_url);

    return result;
}

static int g_request_count = 0;

static DEVICE_STREAM_C2D_RESPONSE* streamRequestCallback(const DEVICE_STREAM_C2D_REQUEST* stream_request, void* context)
{
    (void)context;

    for (int i = 0; i < MAX_CONNECTION_COUNT; i++)
    {
        if (g_connections[i].state == DS_PROXY_CONNECTION_NONE)
        {
            (void)printf("Received stream request (%s)\r\n", stream_request->name);

            g_connections[i].id = g_request_count++;
            g_connections[i].ws_client_handle = create_websocket_client(stream_request, &g_connections[i]);
            g_connections[i].state = DS_PROXY_CONNECTION_ACTIVE;

            return IoTHubClient_StreamC2DResponseCreate(stream_request, true);
        }
    }

    (void)printf("Rejected stream request (%s) - Max connections reached!\r\n", stream_request->name);
    
    return IoTHubClient_StreamC2DResponseCreate(stream_request, false);
}

static void print_traffic_counters(DS_PROXY_INFO* connection)
{
    if (connection->lastTrafficPrintTime != INDEFINITE_TIME)
    {
        time_t current_time = time(NULL);

        if (current_time != INDEFINITE_TIME)
        {
            if ((connection->incomingByteCount > 0 || connection->outgoingByteCount > 0) && 
                difftime(current_time, connection->lastTrafficPrintTime) >= TRAFFIC_COUNTERS_PRINT_FREQ_IN_SECS)
            {
                char time_str[128];
                struct tm* current_time_tm = localtime(&current_time);
                strftime(time_str, 128, "%F %T UTC%z", current_time_tm);

                (void)printf("[%s][%d] Network traffic (in bytes) (sent=%d; received=%d)\r\n", 
                    time_str, connection->id, (int)connection->outgoingByteCount, (int)connection->incomingByteCount);
                connection->incomingByteCount = 0;
                connection->outgoingByteCount = 0;
                connection->lastTrafficPrintTime = current_time;
            }
        }
    }
}

static void reset_connection(DS_PROXY_INFO* connection)
{
    connection->id = 0;
    connection->state = DS_PROXY_CONNECTION_NONE;
    connection->is_local_socket_connected = false;
    connection->is_ws_client_connected = false;
    connection->ws_client_handle = NULL;
    connection->local_socket_handle = NULL;
    connection->outgoingByteCount = 0;
    connection->incomingByteCount = 0;
    connection->lastTrafficPrintTime = INDEFINITE_TIME;
}

static void destroy_connection(DS_PROXY_INFO* connection)
{
    if (connection->ws_client_handle != NULL)
    {
        uws_client_destroy(connection->ws_client_handle);
    }

    if (connection->local_socket_handle != NULL)
    {
        xio_destroy(connection->local_socket_handle);
    }

    reset_connection(connection);
}

int main(void)
{
    IOTHUB_CLIENT_TRANSPORT_PROVIDER protocol;

    // Select the Protocol to use with the connection
#ifdef SAMPLE_MQTT
    protocol = MQTT_Protocol;
#endif // SAMPLE_MQTT
#ifdef SAMPLE_MQTT_OVER_WEBSOCKETS
    protocol = MQTT_WebSocket_Protocol;
#endif // SAMPLE_MQTT_OVER_WEBSOCKETS
#ifdef SAMPLE_AMQP
    protocol = AMQP_Protocol;
#endif // SAMPLE_AMQP
#ifdef SAMPLE_AMQP_OVER_WEBSOCKETS
    protocol = AMQP_Protocol_over_WebSocketsTls;
#endif // SAMPLE_AMQP_OVER_WEBSOCKETS
#ifdef SAMPLE_HTTP
    protocol = HTTP_Protocol;
#endif // SAMPLE_HTTP

    // Used to initialize IoTHub SDK subsystem
    (void)IoTHub_Init();

    for (int i = 0; i < MAX_CONNECTION_COUNT; i++) 
    { 
        reset_connection(&g_connections[i]);
    }

    IOTHUB_DEVICE_CLIENT_HANDLE device_handle;

    // Create the iothub handle here
    device_handle = IoTHubDeviceClient_CreateFromConnectionString(connectionString, protocol);

    if (device_handle == NULL)
    {
        (void)printf("Failure creating the IotHub device. Hint: Check you connection string.\r\n");
    }
    else
    {
        // Set any option that are neccessary.
        // For available options please see the iothub_sdk_options.md documentation

        bool traceOn = true;
        IoTHubDeviceClient_SetOption(device_handle, OPTION_LOG_TRACE, &traceOn);

#ifdef SET_TRUSTED_CERT_IN_SAMPLES
        // Setting the Trusted Certificate.  This is only necessary on system with without
        // built in certificate stores.
            IoTHubDeviceClient_SetOption(device_handle, OPTION_TRUSTED_CERT, certificates);
#endif // SET_TRUSTED_CERT_IN_SAMPLES

#if defined SAMPLE_MQTT || defined SAMPLE_MQTT_WS
        //Setting the auto URL Encoder (recommended for MQTT). Please use this option unless
        //you are URL Encoding inputs yourself.
        //ONLY valid for use with MQTT
        //bool urlEncodeOn = true;
        //IoTHubDeviceClient_SetOption(iothub_ll_handle, OPTION_AUTO_URL_ENCODE_DECODE, &urlEncodeOn);
#endif

        if (proxy_host)
        {
            HTTP_PROXY_OPTIONS http_proxy_options = { 0 };
            http_proxy_options.host_address = proxy_host;
            http_proxy_options.port = proxy_port;
            http_proxy_options.username = proxy_username;
            http_proxy_options.password = proxy_password;

            if (IoTHubDeviceClient_SetOption(device_handle, OPTION_HTTP_PROXY, &http_proxy_options) != IOTHUB_CLIENT_OK)
            {
                (void)printf("failure to set proxy\n");
            }
        }

        if (IoTHubDeviceClient_SetStreamRequestCallback(device_handle, streamRequestCallback, NULL) != IOTHUB_CLIENT_OK)
        {
            (void)printf("Failed setting the stream request callback");
        }
        else
        {
            do
            {
                for (int i = 0; i < MAX_CONNECTION_COUNT; i++)
                {
                    if (g_connections[i].state == DS_PROXY_CONNECTION_ACTIVE)
                    {
                        if (g_connections[i].ws_client_handle != NULL)
                        {
                            uws_client_dowork(g_connections[i].ws_client_handle);
                        }

                        if (g_connections[i].local_socket_handle != NULL)
                        {
                            xio_dowork(g_connections[i].local_socket_handle);

                            print_traffic_counters(&g_connections[i]);
                        }
                    }
                    else if (g_connections[i].state == DS_PROXY_CONNECTION_DESTROY)
                    {
                        destroy_connection(&g_connections[i]);
                    }
                }

                ThreadAPI_Sleep(100);

            } while (g_continueRunning);

        }
 
        // Clean up the iothub sdk handle
        IoTHubDeviceClient_Destroy(device_handle);

        for (int i = 0; i < MAX_CONNECTION_COUNT; i++)
        {
            if (g_connections[i].state != DS_PROXY_CONNECTION_NONE)
            {
                destroy_connection(&g_connections[i]);
            }
        }
    }

    // Free all the sdk subsystem
    IoTHub_Deinit();

    return 0;
}
