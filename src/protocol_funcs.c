#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "debug.h"
#include "protocol.h"
#include "protocol_funcs.h"

char *xacto_packet_type_names[] = {
    "NONE", "PUT", "GET", "DATA", "COMMIT", "REPLY"
};

/*
 * Display the contents of a packet.
 */
void proto_debug_packet(XACTO_PACKET *pkt, char *payload) {
    printf("%u.%u: type=%s, status=%d, size=%u, null=%u",
	   pkt->timestamp_sec, pkt->timestamp_nsec,
	   xacto_packet_type_names[(int)pkt->type], pkt->status, pkt->size, pkt->null);
    if(pkt->type == XACTO_DATA_PKT) {
	if(pkt->null) {
	    debug("NULL");
	} else {
	    char str[20];
	    memset(str, 0, sizeof(str));
	    size_t n = (pkt->size < sizeof(str)-1 ? pkt->size : sizeof(str)-1);
	    memcpy(str, payload, n);
	    debug("PAYLOAD: %s", str);
	}
    }
}

/*
 * Initialize a packet.
 *   pkt - pointer to header
 *   type - packet type
 *   size - size of payload
 */
void proto_init_packet(XACTO_PACKET *pkt, XACTO_PACKET_TYPE type, size_t size) {
    memset(pkt, 0, sizeof(*pkt));
    pkt->type = type;
    struct timespec ts;
    if(clock_gettime(CLOCK_MONOTONIC, &ts) == -1) {
	perror("clock_gettime");
    }
    pkt->timestamp_sec = ts.tv_sec;
    pkt->timestamp_nsec = ts.tv_nsec;
    pkt->size = size;
}
