#include <criterion/criterion.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <wait.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#include "protocol.h"
#include "transaction.h"
#include "protocol_funcs.h"
#include "debug.h"

#define SERVER_PORT 9999
#define SERVER_HOSTNAME "localhost"

/*
 * Get the server address.
 * This is done once, so we don't have to worry about the library
 * functions being thread safe.
 */
static struct in_addr server_addr;

static int get_server_address(void) {
    struct hostent *he;
    if((he = gethostbyname(SERVER_HOSTNAME)) == NULL) {
	perror("gethostbyname");
	return -1;
    }
    memcpy(&server_addr, he->h_addr, sizeof(server_addr));
    return 0;
}

/*
 * Connect to the server.
 *
 * Returns: connection file descriptor in case of success.
 * Returns -1 and sets errno in case of error.
 */
static int proto_connect(void) {
    struct sockaddr_in sa;
    int sfd;

    if((sfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
	return(-1);
    }
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons(SERVER_PORT);
    memcpy(&sa.sin_addr.s_addr, &server_addr, sizeof(sa));
    if(connect(sfd, (struct sockaddr *)(&sa), sizeof(sa)) < 0) {
	perror("connect");
	close(sfd);
	return(-1);
    }
    return sfd;
}

/*
 * End the current transaction -- try to commit.
 */
static int req_commit(int fd) {
    XACTO_PACKET pkt;
    proto_init_packet(&pkt, XACTO_COMMIT_PKT, 0);
    if(proto_send_packet(fd, &pkt, NULL) == -1) {
	perror("req_commit");
	return -1;
    }
    return 0;
}

/*
 * Put a (key, value) pair into the map.
 */
static int req_put(int fd, char *key, char *value) {
    XACTO_PACKET pkt;
    proto_init_packet(&pkt, XACTO_PUT_PKT, 0);
    if(proto_send_packet(fd, &pkt, NULL) == -1) {
	perror("req_put");
	return -1;
    }
    proto_init_packet(&pkt, XACTO_DATA_PKT, strlen(key));
    if(proto_send_packet(fd, &pkt, key) == -1) {
	perror("req_put(key)");
	return -1;
    }
    proto_init_packet(&pkt, XACTO_DATA_PKT, strlen(value));
    if(proto_send_packet(fd, &pkt, value) == -1) {
	perror("req_put(value)");
	return -1;
    }
    return 0;
}

/*
 * Get the value associated with a key in the map.
 */
static int req_get(int fd, char *key) {
    XACTO_PACKET pkt;
    proto_init_packet(&pkt, XACTO_GET_PKT, 0);
    if(proto_send_packet(fd, &pkt, NULL) == -1) {
	perror("req_get");
	return -1;
    }
    proto_init_packet(&pkt, XACTO_DATA_PKT, strlen(key));
    if(proto_send_packet(fd, &pkt, key) == -1) {
	perror("req_get(key)");
	return -1;
    }
    return 0;
}

/*
 * Process data that has arrived in a reply packet.
 */
static int process_data(XACTO_PACKET *pkt, char *data) {
    if(pkt->null) {
	debug("NULL");
    } else {
	char str[60];
	memset(str, 0, sizeof(str));
	size_t n = (pkt->size < sizeof(str)-1 ? pkt->size : sizeof(str)-1);
	memcpy(str, data, n);
	debug("PAYLOAD: %s", str);
    }
    return 0;
}

/*
 * This should be a test of the server at the client interface level.
 * The basic idea is to feed the client command lines and analyze the responses.
 *
 * WARNING: These tests are coordinated to all run concurrently.
 * You must use --jobs XXX where XXX is sufficiently large to allow them all to
 * run, otherwise there will be issues.  The sleep times in the tests also have
 * to be adjusted if any changes are made.
 */

static void init() {
#ifndef NO_SERVER
    int ret;
    int i = 0;
    do { // Wait for server to start
	ret = system("netstat -an | fgrep '0.0.0.0:9999' > /dev/null");
	sleep(1);
    } while(++i < 30 && WEXITSTATUS(ret));
#endif
}

static void fini() {
}

/*
 * Thread to run a command using system() and collect the exit status.
 */
void *system_thread(void *arg) {
    long ret = system((char *)arg);
    return (void *)ret;
}

// Criterion seems to sort tests by name.  This one can't be delayed
// or others will time out.
Test(server_suite, 00_start_server, .timeout = 60) {
    fprintf(stderr, "server_suite/00_start_server\n");
    int server_pid = 0;
    int ret = system("netstat -an | fgrep '0.0.0.0:9999' > /dev/null");
    cr_assert_neq(WEXITSTATUS(ret), 0, "Server was already running");
    fprintf(stderr, "Starting server...");
    if((server_pid = fork()) == 0) {
	execlp("valgrind", "xacto", "--leak-check=full", "--track-fds=yes",
	       "--error-exitcode=37", "--log-file=valgrind.out", "bin/xacto", "-p", "9999", NULL);
	fprintf(stderr, "Failed to exec server\n");
	abort();
    }
    fprintf(stderr, "pid = %d\n", server_pid);
    char *cmd = "sleep 40";
    pthread_t tid;
    pthread_create(&tid, NULL, system_thread, cmd);
    pthread_join(tid, NULL);
    cr_assert_neq(server_pid, 0, "Server was not started by this test");
    fprintf(stderr, "Sending SIGHUP to server pid %d\n", server_pid);
    kill(server_pid, SIGHUP);
    sleep(5);
    kill(server_pid, SIGKILL);
    wait(&ret);
    fprintf(stderr, "Server wait() returned = 0x%x\n", ret);
    if(WIFSIGNALED(ret)) {
	fprintf(stderr, "Server terminated with signal %d\n", WTERMSIG(ret));	
	system("cat valgrind.out");
	if(WTERMSIG(ret) == 9)
	    cr_assert_fail("Server did not terminate after SIGHUP");
    }
    if(WEXITSTATUS(ret) == 37)
	system("cat valgrind.out");
    cr_assert_neq(WEXITSTATUS(ret), 37, "Valgrind reported errors");
    cr_assert_eq(WEXITSTATUS(ret), 0, "Server exit status was not 0");
}

Test(server_suite, 01_connect_abort, .init = init, .fini = fini, .timeout = 5) {
#ifdef NO_SERVER
    cr_assert_fail("Server was not implemented");
#endif
    int ret = system("util/client -p 9999 </dev/null | grep 'Connected to server'");
    cr_assert_eq(ret, 0, "expected %d, was %d\n", 0, ret);
}

Test(server_suite, 02_connect_commit, .init = init, .fini = fini, .timeout = 5) {
#ifdef NO_SERVER
    cr_assert_fail("Server was not implemented");
#endif
    int ret = system("(echo 'commit' | util/client -q -p 9999) 2>&1 | grep 'Transaction committed' > /dev/null");
    cr_assert_eq(ret, 0, "expected %d, was %d\n", 0, ret);
}

#ifdef NOTDEF
Test(server_suite, 03_getA_commit, .init = init, .fini = fini, .timeout = 5) {
#ifdef NO_SERVER
    cr_assert_fail("Server was not implemented");
#endif
    int ret = system("(cat testfiles/03_getA_commit | util/client -q -p 9999 > 03_getA_commit.out) 2>&1");
    cr_assert_eq(ret, 0, "expected %d, was %d\n", 0, ret);
}

Test(server_suite, 03_getB_commit, .init = init, .fini = fini, .timeout = 5) {
#ifdef NO_SERVER
    cr_assert_fail("Server was not implemented");
#endif
    int ret = system("(cat testfiles/03_getB_commit | util/client -q -p 9999 > 03_getB_commit.out) 2>&1");
    cr_assert_eq(ret, 0, "expected %d, was %d\n", 0, ret);
}

Test(server_suite, 04_putA1_getA_commit, .init = init, .fini = fini, .timeout = 5) {
#ifdef NO_SERVER
    cr_assert_fail("Server was not implemented");
#endif
    int ret = system("(cat testfiles/04_putA1_getA_commit | util/client -q -p 9999 > 04_putA1_getA_commit.out) 2>&1");
    cr_assert_eq(ret, 0, "expected %d, was %d\n", 0, ret);
}

Test(server_suite, 04_putB2_getB_commit, .init = init, .fini = fini, .timeout = 5) {
#ifdef NO_SERVER
    cr_assert_fail("Server was not implemented");
#endif
    int ret = system("(cat testfiles/04_putB2_getB_commit | util/client -q -p 9999 > 04_putB2_getB_commit.out) 2>&1");
    cr_assert_eq(ret, 0, "expected %d, was %d\n", 0, ret);
}
#endif

/*
 * "Bank" simulation.
 * There are a certain number of accounts, represented by keys in the store.
 * An initialization transaction stores an initial quantity of "funds" in each account.
 * Then several "transfer threads" are spawned, each of which repeatedly tries to
 * transfer funds from its "own" account to another account.
 * After the transfer threads have performed a sufficient number of transfers,
 * they terminate.
 * At that point, an audit transaction is run to tally up the amount of funds,
 * to ensure that funds have been conserved.
 */

#define EXPECT_REPLY(sfd, pktp) \
    do { \
	int ret; \
	ret = proto_recv_packet((sfd), (pktp), NULL); \
	if(ret != 0) { \
	    perror("proto_recv_packet"); \
	    fprintf(stderr, "ret = %d\n", ret); \
	} \
        cr_assert_eq(ret, 0, "Receive reply failed"); \
        cr_assert_eq((pktp)->type, XACTO_REPLY_PKT, "Expected a REPLY packet"); \
    } while(0)

#define EXPECT_DATA(sfd, pktp, datap) \
    do { \
	int ret; \
	void *pp; char *pps; \
	ret = proto_recv_packet((sfd), (pktp), &pp); \
        cr_assert_eq(ret, 0, "Receive data failed"); \
        cr_assert_eq((pktp)->type, XACTO_DATA_PKT, "Expected a DATA packet"); \
	cr_assert_not_null(pp, "Received NULL payload"); \
        pps = malloc((pktp)->size+1); \
	memcpy(pps, pp, (pktp)->size); \
        pps[(pktp)->size] = '\0'; \
        *datap = atoi(pps); \
        free(pps); \
        free(pp); \
    } while(0)

/*
 * Thread to perform a specified number of "payments" from a specified
 * account torandomly chosen other accounts.
 */

#define NUM_ACCOUNTS (20)
#define NUM_TRANSFERS (10)
#define INITIAL_FUNDS (100)

struct bank_transfer_args {
    char my_acct[10];
    int transfers;
    volatile int done_flag;
};

static void *bank_transfer_thread(void *arg) {
    struct bank_transfer_args *ap = arg;
    unsigned int seed = pthread_self();
    XACTO_PACKET pkt;
    int completed = 0;
    while(ap->transfers--) {
	int sfd;
	int aborts = 0;
	char other_acct[10];
	char balance[10];
	int amt = rand_r(&seed) % (INITIAL_FUNDS / 10);
	snprintf(other_acct, sizeof(other_acct), "%.6d", rand_r(&seed) % NUM_ACCOUNTS);
	
	// Keep trying the same transfer until it commits.
	do {
	    int my_bal, other_bal;
	    sfd = proto_connect();
	    cr_assert_neq(sfd, -1, "Connection to server failed");

	    // Get my current balance.
	    req_get(sfd, ap->my_acct);
	    EXPECT_REPLY(sfd, &pkt);
	    EXPECT_DATA(sfd, &pkt, &my_bal);
	    if(my_bal < amt) {
		// Insufficient funds -- give up on this transfer.
		fprintf(stderr, "Insufficient funds: [%s]\n", ap->my_acct);
		close(sfd);
		break;
	    }

	    // Debit my account by the transfer amount.
	    my_bal -= amt;
	    snprintf(balance, sizeof(balance), "%d", my_bal);
	    req_put(sfd, ap->my_acct, balance);
	    EXPECT_REPLY(sfd, &pkt);

	    // Get the other account balance.
	    req_get(sfd, other_acct);
	    EXPECT_REPLY(sfd, &pkt);
	    EXPECT_DATA(sfd, &pkt, &other_bal);

	    // Credit the other account by the transfer amount.
	    other_bal += amt;
	    snprintf(balance, sizeof(balance), "%d", other_bal);
	    req_put(sfd, other_acct, balance);
	    EXPECT_REPLY(sfd, &pkt);

	    // All done, try to commit.
	    req_commit(sfd);
	    EXPECT_REPLY(sfd, &pkt);
	    close(sfd);
	    cr_assert_neq(pkt.status, TRANS_PENDING, "Transfer transaction did not complete");
	    if(pkt.status == TRANS_ABORTED) {
		aborts++;
		//fprintf(stderr, "[%s]: Transaction aborted (%d so far)\n", ap->my_acct, aborts);
		// There seems to be a kind of dining philosophers effect, so limit retries.
		if(aborts >= 10) {
		    //fprintf(stderr, "[%s]: Giving up on transfer #%d after %d aborts\n",
		    //	    ap->my_acct, ap->transfers+1, aborts);
		    completed--;
		    break;
		}
	    }
	} while(pkt.status != TRANS_COMMITTED);
	completed++;
    }
    ap->done_flag = 1;
    fprintf(stderr, "[%s]: Completed %d transfers\n", ap->my_acct, completed);
    return NULL;
}

Test(server_suite, 05_bank_simulation, .init = init, .fini = fini, .timeout = 30) {
    int sfd, data;
    XACTO_PACKET pkt;
    char acct[10], value[10];
    pthread_t tid[NUM_ACCOUNTS];

    // Set up the server address.
    if(get_server_address())
	abort();

    // Run initialization transaction.
    fprintf(stderr, "***Initialization phase\n");
    sfd = proto_connect();
    cr_assert_neq(sfd, -1, "Connection to server failed");
    for(int i = 0; i < NUM_ACCOUNTS; i++) {
	snprintf(acct, sizeof(acct), "%.6d", i);
	snprintf(value, sizeof(value), "%d", INITIAL_FUNDS);
	req_put(sfd, acct, value);
	EXPECT_REPLY(sfd, &pkt);
    }
    req_commit(sfd);
    EXPECT_REPLY(sfd, &pkt);
    close(sfd);
    cr_assert_eq(pkt.status, TRANS_COMMITTED, "Initialization transaction did not commit");

    // Spawn transfer transactions and then wait for them to finish.
    fprintf(stderr, "***Transfer phase\n");
    for(int i = 0; i < NUM_ACCOUNTS; i++) {
	struct bank_transfer_args *ap = malloc(sizeof *ap);
	snprintf(ap->my_acct, sizeof(ap->my_acct), "%.6d", i);
	ap->transfers = NUM_TRANSFERS;
	ap->done_flag = 0;
	pthread_create(&tid[i], NULL, bank_transfer_thread, ap);
    }
    for(int i = 0; i < NUM_ACCOUNTS; i++)
	pthread_join(tid[i], NULL);

    // Run audit transaction.
    fprintf(stderr, "***Audit phase\n");
    int total = 0;
    sfd = proto_connect();
    cr_assert_neq(sfd, -1, "Connection to server failed");
    for(int i = 0; i < NUM_ACCOUNTS; i++) {
	snprintf(acct, sizeof(acct), "%.6d", i);
	snprintf(value, sizeof(value), "%d", INITIAL_FUNDS);
	req_get(sfd, acct);
	EXPECT_REPLY(sfd, &pkt);
	EXPECT_DATA(sfd, &pkt, &data);
	total += data;
    }
    req_commit(sfd);
    EXPECT_REPLY(sfd, &pkt);
    close(sfd);
    cr_assert_eq(pkt.status, TRANS_COMMITTED, "Audit transaction did not commit");
    fprintf(stderr, "***Audit returns: %d\n", total);
    cr_assert_eq(total, NUM_ACCOUNTS * INITIAL_FUNDS,
		 "Funds were not conserved (expected: %d, was: %d)",
		 NUM_ACCOUNTS * INITIAL_FUNDS, total);
}
