#include <criterion/criterion.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#include <sys/time.h>

#include "protocol.h"
#include "transaction.h"
#include "excludes.h"

static void init() {
}

Test(protocol_suite, send_no_payload, .init = init, .timeout = 5) {
#ifdef NO_PROTOCOL
    cr_assert_fail("Protocol was not implemented");
#endif
    int fd;
    void *payload = NULL;
    XACTO_PACKET pkt = {0};

    pkt.type = XACTO_COMMIT_PKT;
    pkt.status = TRANS_PENDING;
    pkt.null = 0;
    pkt.size = 0;
    pkt.timestamp_sec = 0x11223344;
    pkt.timestamp_nsec = 0x55667788;

    fd = open("pkt_no_payload", O_CREAT|O_TRUNC|O_RDWR, 0644);
    cr_assert(fd > 0, "Failed to create output file");
    int ret = proto_send_packet(fd, &pkt, payload);
    cr_assert_eq(ret, 0, "Returned value was not 0");
    close(fd);

    ret = system("cmp pkt_no_payload testfiles/pkt_no_payload");
    cr_assert_eq(ret, 0, "Packet sent did not match expected");
}

Test(protocol_suite, send_with_payload, .init = init, .timeout = 5) {
#ifdef NO_PROTOCOL
    cr_assert_fail("Protocol was not implemented");
#endif

    int fd;
    void *payload = "0123456789012345";
    XACTO_PACKET pkt = {0};

    pkt.type = XACTO_DATA_PKT;
    pkt.status = TRANS_PENDING;
    pkt.null = 0;
    pkt.size = 0xf;
    pkt.timestamp_sec = 0x11223344;
    pkt.timestamp_nsec = 0x55667788;

    fd = open("pkt_with_payload", O_CREAT|O_TRUNC|O_RDWR, 0644);
    cr_assert(fd > 0, "Failed to create output file");
    int ret = proto_send_packet(fd, &pkt, payload);
    cr_assert_eq(ret, 0, "Returned value was not 0");
    close(fd);

    ret = system("cmp pkt_with_payload testfiles/pkt_with_payload");
    cr_assert_eq(ret, 0, "Packet sent did not match expected");
}

Test(protocol_suite, send_null_payload, .init = init, .timeout = 5) {
#ifdef NO_PROTOCOL
    cr_assert_fail("Protocol was not implemented");
#endif

    int fd;
    void *payload = "0123456789012345";
    XACTO_PACKET pkt = {0};

    pkt.type = XACTO_DATA_PKT;
    pkt.status = TRANS_PENDING;
    pkt.null = 1;
    pkt.size = 0;
    pkt.timestamp_sec = 0x11223344;
    pkt.timestamp_nsec = 0x55667788;

    fd = open("pkt_null_payload", O_CREAT|O_TRUNC|O_RDWR, 0644);
    cr_assert(fd > 0, "Failed to create output file");
    int ret = proto_send_packet(fd, &pkt, payload);
    cr_assert_eq(ret, 0, "Returned value was not 0");
    close(fd);

    ret = system("cmp pkt_null_payload testfiles/pkt_null_payload");
    cr_assert_eq(ret, 0, "Packet sent did not match expected");
}

Test(protocol_suite, send_error, .init = init, .timeout = 5) {
#ifdef NO_PROTOCOL
    cr_assert_fail("Protocol was not implemented");
#endif
    int fd;
    void *payload = NULL;
    XACTO_PACKET pkt = {0};

    pkt.type = XACTO_COMMIT_PKT;
    pkt.status = TRANS_PENDING;
    pkt.null = 0;
    pkt.size = 0;
    pkt.timestamp_sec = 0x11223344;
    pkt.timestamp_nsec = 0x55667788;

    fd = open("pkt_no_payload", O_CREAT|O_TRUNC|O_RDWR, 0644);
    cr_assert(fd > 0, "Failed to create output file");
    // Here is the error.
    close(fd);
    int ret = proto_send_packet(fd, &pkt, payload);
    cr_assert_eq(ret, -1, "Returned value was not -1");
}

Test(protocol_suite, recv_no_payload, .init = init, .timeout = 5) {
#ifdef NO_PROTOCOL
    cr_assert_fail("Protocol was not implemented");
#endif
    int fd;
    void *payload = NULL;
    XACTO_PACKET pkt = {0};

    fd = open("testfiles/pkt_no_payload", O_RDONLY, 0);
    cr_assert(fd > 0, "Failed to open test input file");
    int ret = proto_recv_packet(fd, &pkt, &payload);
    cr_assert_eq(ret, 0, "Returned value was not 0");
    close(fd);

    cr_assert_eq(pkt.type, XACTO_COMMIT_PKT, "Received packet type did not match expected");
    cr_assert_eq(pkt.status, TRANS_PENDING, "Received status did not match expected");
    cr_assert_eq(pkt.null, 0, "Received 'null' did not match expected");
    cr_assert_eq(pkt.size, 0, "Received payload size was not zero");
    cr_assert_eq(pkt.timestamp_sec, 0x11223344,
		 "Received message timestamp_sec did not match expected");
    cr_assert_eq(pkt.timestamp_nsec, 0x55667788,
		 "Received message timestamp_nsec did not match expected");
}

Test(protocol_suite, recv_null_payload, .init = init, .timeout = 5) {
#ifdef NO_PROTOCOL
    cr_assert_fail("Protocol was not implemented");
#endif
    int fd;
    void *payload = NULL;
    XACTO_PACKET pkt = {0};

    fd = open("testfiles/pkt_null_payload", O_RDONLY, 0);
    cr_assert(fd > 0, "Failed to open test input file");
    int ret = proto_recv_packet(fd, &pkt, &payload);
    cr_assert_eq(ret, 0, "Returned value was not 0");
    close(fd);

    cr_assert_eq(pkt.type, XACTO_DATA_PKT, "Received packet type did not match expected");
    cr_assert_eq(pkt.status, TRANS_PENDING, "Received status did not match expected");
    cr_assert_eq(pkt.null, 1, "Received 'null' did not match expected");
    cr_assert_eq(pkt.size, 0, "Received payload size was not zero");
    cr_assert_eq(pkt.timestamp_sec, 0x11223344,
		 "Received message timestamp_sec did not match expected");
    cr_assert_eq(pkt.timestamp_nsec, 0x55667788,
		 "Received message timestamp_nsec did not match expected");
}

Test(protocol_suite, recv_with_payload, .init = init, .timeout = 5) {
#ifdef NO_PROTOCOL
    cr_assert_fail("Protocol was not implemented");
#endif
    int fd;
    void *payload = NULL;
    XACTO_PACKET pkt = {0};

    fd = open("testfiles/pkt_with_payload", O_RDONLY, 0);
    cr_assert(fd > 0, "Failed to open test input file");
    int ret = proto_recv_packet(fd, &pkt, &payload);
    cr_assert_eq(ret, 0, "Returned value was not 0");
    close(fd);

    cr_assert_eq(pkt.type, XACTO_DATA_PKT, "Received packet type did not match expected");
    cr_assert_eq(pkt.status, TRANS_PENDING, "Received status did not match expected");
    cr_assert_eq(pkt.null, 0, "Received 'null' did not match expected");
    cr_assert_eq(pkt.size, 0xf, "Received payload size did not match expected");
    cr_assert_eq(pkt.timestamp_sec, 0x11223344,
		 "Received message timestamp_sec did not match expected");
    cr_assert_eq(pkt.timestamp_nsec, 0x55667788,
		 "Received message timestamp_nsec did not match expected");
    int n = strncmp(payload, "0123456789012345", 0xf);
    cr_assert_eq(n, 0, "Received message payload did not match expected");
}

Test(protocol_suite, recv_empty, .init = init, .timeout = 5) {
#ifdef NO_PROTOCOL
    cr_assert_fail("Protocol was not implemented");
#endif
    int fd;
    void *payload = NULL;
    XACTO_PACKET pkt = {0};

    fd = open("testfiles/pkt_empty", O_RDONLY, 0);
    cr_assert(fd > 0, "Failed to open test input file");
    int ret = proto_recv_packet(fd, &pkt, &payload);
    cr_assert_eq(ret, -1, "Returned value was not -1");
    close(fd);
}

Test(protocol_suite, recv_short_header, .init = init, .timeout = 5) {
#ifdef NO_PROTOCOL
    cr_assert_fail("Protocol was not implemented");
#endif
    int fd;
    void *payload = NULL;
    XACTO_PACKET pkt = {0};

    fd = open("testfiles/pkt_short_header", O_RDONLY, 0);
    cr_assert(fd > 0, "Failed to open test input file");
    int ret = proto_recv_packet(fd, &pkt, &payload);
    cr_assert_eq(ret, -1, "Returned value was not -1");
    close(fd);
}

Test(protocol_suite, recv_short_payload, .init = init, .signal = SIGALRM) {
#ifdef NO_PROTOCOL
    cr_assert_fail("Protocol was not implemented");
#endif
    int fd;
    void *payload = NULL;
    XACTO_PACKET pkt = {0};
    struct itimerval itv = {{0, 0}, {1, 0}};

    fd = open("testfiles/pkt_short_payload", O_RDONLY, 0);
    cr_assert(fd > 0, "Failed to open test input file");
    setitimer(ITIMER_REAL, &itv, NULL);
    int ret = proto_recv_packet(fd, &pkt, &payload);
    cr_assert_eq(ret, -1, "Returned value was not -1");
    close(fd);
}

Test(protocol_suite, recv_error, .init = init, .timeout = 5) {
#ifdef NO_PROTOCOL
    cr_assert_fail("Protocol was not implemented");
#endif
    int fd;
    void *payload = NULL;
    XACTO_PACKET pkt = {0};

    fd = open("testfiles/pkt_empty", O_RDONLY, 0);
    cr_assert(fd > 0, "Failed to open test input file");
    close(fd);
    int ret = proto_recv_packet(fd, &pkt, &payload);
    cr_assert_eq(ret, -1, "Returned value was not -1");
}
