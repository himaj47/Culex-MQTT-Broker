#pragma once
#include <stddef.h>

namespace mqtt {

static const int MAX_LEN_BYTES = 4;

// message types
enum class packet_type {
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


// QoS levels
enum class qos_level {
    AT_MOST_ONCE,
    AT_LEAST_ONCE,
    EXACTLY_ONCE
};


// fixed header
typedef union header {
    unsigned char byte;
    struct {
        unsigned char retain : 1;
        unsigned char qos    : 2;
        unsigned char dup    : 1;
        unsigned char type   : 4;
    };
} header;


// packet structures
typedef struct connect {
    header header;

    // connect flags
    union {
        unsigned char byte;
        struct {
            unsigned char reserved      : 1;
            unsigned char clean_session : 1;
            unsigned char will          : 1;
            unsigned char will_qos      : 2;
            unsigned char will_retain   : 1;
            unsigned char passwd        : 1;
            unsigned char username      : 1;
        } connect_flags;
    };

    unsigned short keep_alive;

    // payload
    struct {
        unsigned char* client_id;
        unsigned char* username;
        unsigned char* passwd;
        unsigned char* will_topic;
        unsigned char* will_msg;
    } payload;
    
} connect;

typedef struct publish {
    union header header;
    struct {
        unsigned short topic_len;
        unsigned char* topic;
        unsigned short packet_id;
    };

    struct {
        unsigned short payload_len;
        unsigned char* payload;
    };
    
} publish;

typedef struct subscribe {
    union header header;
    unsigned short packet_id;
    unsigned short tuples_len;

    struct {
        unsigned short topic_len;
        unsigned char* topic;

        union {
            unsigned char byte;
            struct {
                unsigned char reserved : 6;
                unsigned char qos      : 2;
            };
        };
    } *tuples;
} subscribe;

typedef struct unsubscribe {
    union header header;
    unsigned short packet_id;
    unsigned short tuples_len;

    struct {
        unsigned short topic_len;
        unsigned char* topic;
    } *tuples;
} unsubscribe;


typedef struct ack {
    union header header;
    unsigned short packet_id;
} ack;

typedef struct connack {
    union header header;
    
    // connect_flags
    union {
        unsigned char byte;

        struct {
            unsigned char session_present : 1;
            unsigned char reserved : 7;
        } connect_flags;
    };

    unsigned char return_code;    
} connack;

typedef struct suback {
    union header header;
    unsigned short packet_id;

    unsigned short rcs_len;
    unsigned char* rcs;
} suback;

typedef ack puback;
typedef ack pubrec;
typedef ack pubrel;
typedef ack pubcomp;
typedef ack unsuback;
typedef header pingreq;
typedef header pingresp;
typedef header disconnect;


union packet {
    struct ack ack;
    union header header;
    
};

typedef struct packet {
    header header;
    union {
        // This will cover PUBACK, PUBREC, PUBREL, PUBCOMP and UNSUBACK
        ack ack;
        // This will cover PINGREQ, PINGRESP and DISCONNECT
        pingreq pingreq;
        connect connect;
        connack connack;
        suback suback;
        publish publish;
        subscribe subscribe;
        unsubscribe unsubscribe;
    };
} packet;


// encoding remaining length of fixed header
int encode_length(unsigned char*, size_t);

// decoding remaining length of fixed header
unsigned long long decode_length(unsigned char*);

}


int mqtt::encode_length(unsigned char* buff, size_t len) {
    int bytes = 0;

    do {
        if (bytes + 1 > MAX_LEN_BYTES)
            return bytes;

        short encodedbyte = len % 128;
        len /= 128;

        if (len > 0) {
            encodedbyte |= 128;
        }

        buff[bytes++] = (unsigned char)encodedbyte;
    } while (len > 0);

    return bytes;
}

// TODO: Handle case where multiplier > 128 * 128 * 128
unsigned long long mqtt::decode_length(unsigned char* buff) {
    char encodedbyte;
    int multiplier = 1;
    unsigned long long value = 0;

    do {
        encodedbyte = *buff;
        value += (encodedbyte & 127) * multiplier;
        multiplier *= 128;

        ++buff;
    } while ((encodedbyte & 128) != 0);
    
    return value;
}