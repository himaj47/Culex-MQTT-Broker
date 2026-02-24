#include <iostream>
#include <string>
#include <cstring>
#include <cstdint>
#include <arpa/inet.h>
#include <stdexcept>
#include <functional>

namespace mqtt {

static const int MAX_LEN_BYTES = 4;

enum qos {
    AT_MOST_ONCE,
    AT_LEAST_ONCE,
    EXACTLY_ONCE
};

enum class PacketType : uint8_t {
    CONNECT          = 0x01,
    CONNACK          = 0x02,
    PUBLISH          = 0x03,
    PUBACK           = 0x04,
    PUBREC           = 0x05,
    PUBREL           = 0x06,
    PUBCOMP          = 0x07,
    SUBSCRIBE        = 0x08,
    SUBACK           = 0x09,
    UNSUBSCRIBE      = 0x0A,
    UNSUBACK         = 0x0B,
    PINGREQ          = 0x0C,
    PINGRESP         = 0x0D,
    DISCONNECT       = 0x0E
};

typedef union header {
    uint8_t byte;
    struct {
        uint8_t retain : 1;
        uint8_t qos : 2;
        uint8_t dup : 1;
        uint8_t type : 4;
    };
} header;

typedef struct connect {
    header fixed_header;
    uint8_t level;

    union {
        uint8_t byte;
        struct {
            uint8_t reserved : 1;
            uint8_t clean_session : 1;
            uint8_t will : 1;
            uint8_t will_qos : 2;
            uint8_t will_retain : 1;
            uint8_t passwd : 1;
            uint8_t username : 1;
        };
    } connect_flags;

    uint16_t keep_alive;

    struct {
        uint8_t* client_id;
        uint8_t* will_topic;
        uint8_t* will_msg;
        uint8_t* username;
        uint8_t* passwd;
    } payload;

} connect;

typedef struct publish {
    header fixed_header;

    struct {
        uint16_t topic_length;
        uint8_t* topic_name;
        uint16_t packet_id;
    } var_header;

    uint16_t payload_length;
    uint8_t* payload;

} publish;

typedef struct subscribe {
    header fixed_header;
    uint16_t packet_id;
    uint16_t tuples_len;

    struct tuple {
        uint16_t topic_length;
        uint8_t* topic_name;

        union {
            uint8_t byte;
            struct {
                uint8_t qos : 2;
                uint8_t reserved : 6;
            };
        };
    } *payload;

} subscribe;

typedef struct unsubscribe {
    header fixed_header;
    uint16_t packet_id;
    uint16_t tuples_len;

    struct tuple {
        uint16_t topic_length;
        uint8_t* topic_name;
    } *payload;

} unsubscribe;

typedef struct connack {
    header fixed_header;

    union {
        uint8_t byte;
        struct {
            uint8_t session_present : 1;
            uint8_t reserved : 7;
        };
    } ack_flags;

    uint8_t return_code;
} connack;

typedef struct ack {
    header fixed_header;
    uint16_t packet_id;
} ack;

typedef ack puback;
typedef ack pubrec;

union packet {
    ack ack_;
    header header_;
    connack connack_;
    connect connect_;
    publish publish_;
    subscribe subscribe_;
    unsubscribe unsubscribe_;
};

}

uint8_t unpack_u8(const uint8_t** buff) {
    uint8_t byte;
    memcpy(&byte, *buff, sizeof(uint8_t));
    (*buff) += sizeof(uint8_t);
    return byte;
}

uint16_t unpack_u16(const uint8_t** buff) {
    uint16_t byte;
    memcpy(&byte, *buff, sizeof(uint16_t));
    (*buff) += sizeof(uint16_t);
    return ntohs(byte);
}

void unpack_bytes(const uint8_t** buff, uint8_t** dest, size_t len) {
    *dest = new uint8_t[len + 1];
    memcpy(*dest, *buff, (size_t)len);
    *buff += len;
}

uint16_t unpack_string16(const uint8_t** buff, uint8_t** dest) {
    uint16_t len = unpack_u16(buff);
    unpack_bytes(buff, dest, len);
    (*dest)[len+1] = '\0';
    return len;
}

static int encode_length(int len, uint8_t* buff) {
    int encodedbyte = 0;

    do {
        if (encodedbyte + 1 > 4) return encodedbyte;

        short d = len % 128;
        len /= 128;
        if (len > 0) d |= 128;

        buff[encodedbyte++] = d;

    } while (len > 0);
    return encodedbyte;
}

static size_t decode_length(const uint8_t** buff) {
    int len = 0;
    int multiplier = 1;
    uint8_t nextbyte = 0;

    do {
        nextbyte = **buff;
        len += (nextbyte & 127) * multiplier;
        multiplier *= 128;
        
        if (multiplier > 128*128*128) 
            throw std::runtime_error("Malformed Remaining Length!!");

        (*buff)++;
    } while ((nextbyte & 128) != 0);
    return len;
}

static size_t unpack_connect(mqtt::header* header, mqtt::packet* pkt, const uint8_t** buff) {
    std::memset(&pkt->connect_, 0, sizeof(pkt->connect_));

    pkt->connect_.fixed_header = *header;
    size_t remaining_len = decode_length(buff);

    // skipping protocol name
    *buff += 6;

    pkt->connect_.level = unpack_u8(buff);
    pkt->connect_.connect_flags.byte = unpack_u8(buff);
    pkt->connect_.keep_alive = unpack_u16(buff);

    unpack_string16(buff, &pkt->connect_.payload.client_id);

    if (pkt->connect_.connect_flags.will) {
        unpack_string16(buff, &pkt->connect_.payload.will_topic);
        unpack_string16(buff, &pkt->connect_.payload.will_msg);
    }
    if (pkt->connect_.connect_flags.username) {
        unpack_string16(buff, &pkt->connect_.payload.username);
    }
    if (pkt->connect_.connect_flags.passwd) {
        unpack_string16(buff, &pkt->connect_.payload.passwd);
    }

    return remaining_len;
}

static size_t unpack_publish(mqtt::header* header, mqtt::packet* pkt, const uint8_t** buff) {
    std::memset(&pkt->publish_, 0, sizeof(pkt->publish_));

    pkt->publish_.fixed_header = *header;
    size_t remaining_len = decode_length(buff);
    size_t payload_len = remaining_len;

    pkt->publish_.var_header.topic_length = unpack_string16(buff, &pkt->publish_.var_header.topic_name);
    payload_len -= sizeof(uint16_t) + pkt->publish_.var_header.topic_length;

    std::cout << "remaining len = " << remaining_len << std::endl;

    if (pkt->publish_.fixed_header.qos > mqtt::qos::AT_MOST_ONCE) {
        pkt->publish_.var_header.packet_id = unpack_u16(buff);
        payload_len -= sizeof(uint16_t);
    }

    pkt->publish_.payload_length = payload_len;
    unpack_bytes(buff, &pkt->publish_.payload, payload_len);
    return remaining_len;
}

static size_t unpack_subscribe(mqtt::header* header, mqtt::packet* pkt, const uint8_t** buff) {
    mqtt::subscribe subscribe;
    std::memset(&subscribe, 0, sizeof(subscribe));

    subscribe.fixed_header = *header;
    size_t remaining_len = decode_length(buff);

    subscribe.packet_id = unpack_u16(buff);

    size_t payload_len = remaining_len - sizeof(uint16_t);
    uint16_t total_tuples = 0;

    while (payload_len > 0) {
        payload_len -= sizeof(uint16_t);

        subscribe.payload = (mqtt::subscribe::tuple*)realloc(subscribe.payload, (total_tuples+1)*sizeof(mqtt::subscribe::tuple));
        if (subscribe.payload) {
            subscribe.payload[total_tuples].topic_length = unpack_string16(buff, &subscribe.payload[total_tuples].topic_name);
            subscribe.payload[total_tuples].qos = unpack_u8(buff);

            payload_len -= subscribe.payload[total_tuples].topic_length;
            payload_len -= sizeof(uint8_t);
        }
        total_tuples++;
    }
    subscribe.tuples_len = total_tuples;
    pkt->subscribe_ = subscribe;

    return remaining_len;
}

static size_t unpack_unsubscribe(mqtt::header* header, mqtt::packet* pkt, const uint8_t** buff) {
    mqtt::unsubscribe unsubscribe;
    std::memset(&unsubscribe, 0, sizeof(unsubscribe));

    unsubscribe.fixed_header = *header;
    size_t remaining_len = decode_length(buff);

    unsubscribe.packet_id = unpack_u16(buff);

    size_t payload_len = remaining_len - sizeof(uint16_t);
    uint16_t total_tuples = 0;

    while (payload_len > 0) {
        payload_len -= sizeof(uint16_t);
        unsubscribe.payload = (mqtt::unsubscribe::tuple*)realloc(unsubscribe.payload, (total_tuples+1)*sizeof(mqtt::unsubscribe::tuple));
        if (unsubscribe.payload) {
            // std::cout << "entered\n";
            unsubscribe.payload[total_tuples].topic_length = unpack_string16(buff, &unsubscribe.payload[total_tuples].topic_name);

            payload_len -= unsubscribe.payload[total_tuples].topic_length;
        }
        total_tuples++;
    }
    unsubscribe.tuples_len = total_tuples;
    pkt->unsubscribe_ = unsubscribe;

    return remaining_len;

}

static size_t unpack_ack(mqtt::header* header, mqtt::packet* pkt, const uint8_t** buff) {
    std::memset(&pkt->ack_, 0, sizeof(pkt->ack_));

    pkt->ack_.fixed_header = *header;
    size_t remaining_len = decode_length(buff);

    pkt->ack_.packet_id = unpack_u16(buff);
    return remaining_len;
}

// size_t unpack_connack(mqtt::header* header, mqtt::connack* pkt, const uint8_t** buff) {
//     pkt->fixed_header = *header;
//     size_t remaining_len = decode_length(buff);

//     pkt->ack_flags.byte = unpack_u8(buff);
//     pkt->return_code = unpack_u8(buff);

//     return remaining_len;
// }

using unpackHandler = std::function<size_t(mqtt::header* header, mqtt::packet* pkt, const uint8_t** buff)>;
static const unpackHandler unpack_handlers[] = {
    nullptr,
    unpack_connect,
    nullptr,
    unpack_publish,
    unpack_ack,
    unpack_ack,
    unpack_ack,
    unpack_ack,
    unpack_subscribe,
    nullptr,
    unpack_unsubscribe
};

size_t unpack(mqtt::packet* pkt, const uint8_t** buff) {
    size_t len = 0;

    mqtt::header header;
    header.byte = unpack_u8(buff);

    mqtt::PacketType type = static_cast<mqtt::PacketType>(header.type);
    if (type == mqtt::PacketType::DISCONNECT ||
        type == mqtt::PacketType::PINGREQ ||
        type == mqtt::PacketType::PINGRESP) {
        pkt->header_ = header;

    } else {
        const unpackHandler& handler = unpack_handlers[static_cast<uint8_t>(header.type)];
        if (handler) 
            len = handler(&header, pkt, buff);
        else 
            std::cout << "invalid type!!\n";
    }

    return len;
}

int main(int argc, char const *argv[])
{
    // uint8_t val[4] = {0x00, 0x05, 0x00, 0x07};
    // uint8_t* valptr = val;

    // uint16_t len = unpack(&valptr);   
    // uint16_t len2 = unpack(&valptr);     

    // std::cout << "len = " << len << std::endl;
    // std::cout << "val[2] = " << len2 << std::endl;

    /////////////////////////////////////////////////////////
    // uint8_t buffer[] = {0x30};

    // mqtt::header h;
    // uint8_t *raw_byte = buffer;
    // h.byte = unpack_u8(&raw_byte);

    // std::cout << "type = " << (int)h.type << std::endl;


    ////////////////////////////////////////////////////////
    // const uint8_t raw_buffer[] = {0x00, 0x04, 'm', 'q', 't', 't'};
    // const uint8_t *raw_byte = raw_buffer;
    // uint8_t* dest;
    // int len = unpack_string16(&raw_byte, &dest);

    // std::cout << "len = " << len << " string = " << (char)dest[1] << std::endl;


    ////////////////////////////////////////////////////////
    // uint8_t buffer[4] = {0xc1, 0x02};
    // const uint8_t *raw_byte = buffer;
    // int len = decode_length(&raw_byte);

    // std::cout << "len = " << len << std::endl;

    // uint8_t bytes[4];
    // int encodedbyte = encode_length(321, bytes);

    // std::cout << "byte = " << (int)(bytes[1]) << std::endl;


    ///////////////////////////////////////////////////////
    uint8_t connect_packet[] = {
        // --- FIXED HEADER ---
        0x10,                       // Control Packet Type (1 = CONNECT)
        0x12,                       // Remaining Length (18 bytes follow)

        // --- VARIABLE HEADER ---
        0x00, 0x04,                 // Protocol Name Length (4 bytes)
        'M', 'Q', 'T', 'T',         // Protocol Name ("MQTT")
        0x04,                       // Protocol Level (4 = v3.1.1)
        0x02,                       // Connect Flags (Clean Session = 1, others = 0)
        0x00, 0x3C,                 // Keep Alive (60 seconds)

        // --- PAYLOAD ---
        0x00, 0x06,                 // Client ID Length (6 bytes)
        'c', 'l', 'i', 'e', 'n', 't' // Client ID ("client")
    };

    uint8_t connack_packet[] = {
        0x20,   // Fixed Header: Type = 2 (CONNACK), Flags = 0
        0x02,   // Remaining Length: 2 bytes follow
        0x00,   // Connect Acknowledge Flags: bit 0 is Session Present
        0x00    // Connect Reason Code: 0 = Success
    };

    uint8_t publish_packet[] = {
        // --- FIXED HEADER ---
        0x32,                       // Type = 3 (PUBLISH), QoS = 1, DUP = 0, Retain = 0
        0x09,                       // Remaining Length (9 bytes follow)

        // --- VARIABLE HEADER ---
        0x00, 0x03,                 // Topic Name Length (3 bytes)
        'a', '/', 'b',              // Topic Name ("a/b")
        0x00, 0x01,                 // Packet Identifier (Required for QoS > 0)

        // --- PAYLOAD ---
        'h', 'i'                    // Message Payload ("hi")
    };

    uint8_t subscribe_packet[] = {
        // --- FIXED HEADER ---
        0x82,                       // Type = 8 (SUBSCRIBE), Flags = 2 (Required by Spec)
        0x0E,                       // Remaining Length (14 bytes follow)

        // --- VARIABLE HEADER ---
        0x00, 0x0A,                 // Packet Identifier (10) - Used for SUBACK match

        // --- PAYLOAD (Topic List) ---
        // Topic 1
        0x00, 0x03,                 // Length (3)
        'a', '/', 'b',              // Topic "a/b"
        0x00,                       // Requested QoS 0

        // Topic 2
        0x00, 0x03,                 // Length (3)
        'c', '/', 'd',              // Topic "c/d"
        0x01                        // Requested QoS 1
    };

    uint8_t unsubscribe_packet[] = {
        // --- FIXED HEADER ---
        0xA2,                       // Type = 10 (UNSUBSCRIBE), Flags = 2 (Required by Spec)
        0x0C,                       // Remaining Length (12 bytes follow)

        // --- VARIABLE HEADER ---
        0x00, 0x0F,                 // Packet Identifier (15)

        // --- PAYLOAD (Topic List) ---
        // Topic 1
        0x00, 0x03,                 // Length (3)
        'a', '/', 'b',              // Topic Name ("a/b")

        // Topic 2
        0x00, 0x03,                 // Length (3)
        'c', '/', 'd'               // Topic Name ("c/d")
    };



    // const uint8_t* buff = connect_packet;
    // const uint8_t* buff = connack_packet;
    // const uint8_t* buff = publish_packet;
    const uint8_t* buff = subscribe_packet;
    // const uint8_t* buff = unsubscribe_packet;

    mqtt::packet pkt;
    size_t len = unpack(&pkt, &buff);

    // std::cout << "protocol level = " << (int)pkt.connect_.level << std::endl;
    // std::cout << "client_id = " << pkt.connect_.payload.client_id[2] << std::endl;

    // std::cout << "remaining length = " << (int)len << std::endl;
    // std::cout << "return code = " << (int)pkt.connack_.return_code << std::endl;

    // std::cout << "topic = " << (char)pkt.publish_.var_header.topic_name[0] << std::endl;
    // std::cout << "payload = " << (char)pkt.publish_.payload[0] << std::endl;

    std::cout << "topic1 = " << (char)pkt.subscribe_.payload[0].topic_name[0] << std::endl;

    // std::cout << "topic1 = " << (char)pkt.unsubscribe_.payload[1].topic_name[0] << std::endl;

    return 0;
}
