#include <criterion/criterion.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

#include "debug.h"
#include "store.h"
#include "excludes.h"

/* Number of keys we use in some tests. */
#define NKEYS (100)

/* Number of transactions we use in some tests. */
#define NTRANS (10)

/* Number of threads we create in multithreaded tests. */
#define NTHREAD (1000)

/* Number of iterations we use in several tests. */
#define NITER (1000000)

/*
 * Get the list of versions for a key.
 */
static VERSION *get_versions_for_key(KEY *kp) {
    for(int i = 0; i < NUM_BUCKETS; i++) {
	for(MAP_ENTRY *ep = the_map.table[i]; ep != NULL; ep = ep->next) {
	    if(!key_compare(kp, ep->key))
		return(ep->versions);
	} 
    }
    return NULL;
}

/*
 * Assert that the number of versions for a particular key matches
 * an expected value.
 */
static void assert_number_of_versions(KEY *kp, int exp) {
    int n = 0;
    VERSION *vp = get_versions_for_key(kp);
    while(vp != NULL) {
	n++;
	vp = vp->next;
    }
    cr_assert_eq(n, exp, "Wrong number of versions for key [%s], was %d, expected %d",
		 kp->blob->prefix, n, exp);
}

/*
 * Assert that an entry with a key equal to a specified key exists
 * exactly once in the store.
 */
static void assert_key_present(KEY *kp) {
    int n = 0;
    for(int i = 0; i < NUM_BUCKETS; i++) {
	for(MAP_ENTRY *ep = the_map.table[i]; ep != NULL; ep = ep->next) {
	    if(!key_compare(kp, ep->key))
		n++;
	}
    }
    cr_assert_neq(n, 0, "Key [%s] not present when it should be", kp->blob->prefix);
    cr_assert_eq(n, 1, "Multiple entries (%d) for key [%s] are present", n, kp->blob->prefix);
}

/*
 * Assert that there is no entry with a key equal to a specified key.
 */
static void assert_key_absent(KEY *kp) {
    int n = 0;
    for(int i = 0; i < NUM_BUCKETS; i++) {
	for(MAP_ENTRY *ep = the_map.table[i]; ep != NULL; ep = ep->next) {
	    if(!key_compare(kp, ep->key))
		n++;
	}
    }
    cr_assert_eq(n, 0, "Key [%s] present when it shouldn't be", kp->blob->prefix);
}

/*
 * Check that the total number of keys in the store matches an expected value.
 */
static void assert_number_of_keys(int exp) {
    int n = 0;
    for(int i = 0; i < NUM_BUCKETS; i++) {
	for(MAP_ENTRY *ep = the_map.table[i]; ep != NULL; ep = ep->next)
	    n++;
    }
    cr_assert_eq(n, exp, "Wrong number of keys in store, was %d, expected %d", n, exp);
}

/*
 * Check that the list of versions in a map entry appears sane.
 * The invariants checked are those that should hold immediately
 * after "garbage collection".
 */
static void assert_versions_are_sane(VERSION *vp, KEY *kp) {
    int pending = 0, committed = 0, aborted = 0;
    while(vp != NULL) {
	cr_assert_not_null(vp->creator, "Version has NULL creator for key [%s]", kp->blob->prefix);
	switch(vp->creator->status) {
	case TRANS_PENDING:
	    cr_assert_eq(aborted, 0, "Pending version occurs after aborted version for key [%s]",
			 kp->blob->prefix);
	    pending++;
	    break;
	case TRANS_COMMITTED:
	    cr_assert(committed <= 2, "More than two committed versions for key [%s]", kp->blob->prefix);
	    cr_assert_eq(pending, 0, "Committed version follows pending version for key [%s]", kp->blob->prefix);
	    cr_assert_eq(aborted, 0, "Committed version follows aborted version [%s]", kp->blob->prefix);
	    committed++;
	    break;
	case TRANS_ABORTED:
	    aborted++;
	    break;
	}
	vp = vp->next;
    }
}

/*
 * Check that a bucket in the store appears sane.
 */
static void assert_bucket_is_sane(MAP_ENTRY *ep) {
    while(ep != NULL) {
	cr_assert_not_null(ep->key, "Key is null in map entry");
	assert_versions_are_sane(ep->versions, ep->key);
	ep = ep->next;
    }
}

/*
 * Check that the store appears to be properly initialized.
 */
static void assert_store_is_sane(void) {
    cr_assert_not_null(the_map.table);
    cr_assert_eq(the_map.num_buckets, NUM_BUCKETS, "Number of buckets %d is incorrect",
                 the_map.num_buckets);
    // Check each bucket for integrity.
    for(int i = 0; i < NUM_BUCKETS; i++)
	assert_bucket_is_sane(the_map.table[i]);
}

/*
 * Make a key from content -- to avoid blob ref count annoyance.
 */
static KEY *make_key(char *content, size_t size) {
    return(key_create(blob_create(content, size)));
}

static void init() {
    trans_init();
    store_init();
}

/*
 * Test the initialization of the store.
 */
Test(store_suite, init, .init = init, .timeout = 5) {
#ifdef NO_STORE
    cr_assert_fail("Store was not implemented");
#endif
    assert_store_is_sane();
    assert_number_of_keys(0);
}

/*
 * Test the result of a single get on an empty store.
 */
Test(store_suite, get_on_empty_store, .init = init, .timeout = 5) {
#ifdef NO_STORE
    cr_assert_fail("Store was not implemented");
#endif
    char content[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    BLOB *bp = blob_create(content, 26);
    KEY *kp = key_create(bp);
    TRANSACTION *tp = trans_create();
    BLOB *value = NULL;
    TRANS_STATUS st = store_get(tp, kp, &value);
    cr_assert_eq(st, TRANS_PENDING, "Transaction status is not pending as it should be");
    cr_assert_null(value, "Value returned is not NULL as it should be");
    assert_store_is_sane();
    assert_number_of_keys(1);
    assert_key_present(kp);
    assert_number_of_versions(kp, 1);
}

/*
 * Test the result of a single put on an empty store.
 */
Test(store_suite, put_on_empty_store, .init = init, .timeout = 5) {
#ifdef NO_STORE
    cr_assert_fail("Store was not implemented");
#endif
    char content[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    BLOB *bp = blob_create(content, 26);
    KEY *kp = key_create(bp);
    TRANSACTION *tp = trans_create();
    TRANS_STATUS st = store_put(tp, kp, bp);
    cr_assert_eq(st, TRANS_PENDING, "Transaction status is not pending as it should be");
    assert_store_is_sane();
    assert_number_of_keys(1);
    assert_key_present(kp);
    assert_number_of_versions(kp, 1);
}

/*
 * Test single put then a single get.
 */
Test(store_suite, put_then_get, .init = init, .timeout = 5) {
#ifdef NO_STORE
    cr_assert_fail("Store was not implemented");
#endif
    char content[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    BLOB *bp = blob_create(content, 26);
    blob_ref(bp, "For blob in key");
    KEY *kp = key_create(bp);
    TRANSACTION *tp = trans_create();
    TRANS_STATUS st = store_put(tp, kp, bp);
    cr_assert_eq(st, TRANS_PENDING, "Transaction status is not pending as it should be");
    BLOB *value = NULL;
    st = store_get(tp, kp, &value);
    cr_assert_eq(st, TRANS_PENDING, "Transaction status is not pending as it should be");
    cr_assert_eq(bp, value, "Wrong value returned");
    assert_store_is_sane();
    assert_number_of_keys(1);
    assert_key_present(kp);
    assert_number_of_versions(kp, 1);
}

/*
 * Test multiple puts for two different keys.
 * Idea is to see that the versions are replaced, rather than multiplied,
 * and that the keys are not confused with each other.
 */
Test(store_suite, multi_put_then_get, .init = init, .timeout = 5) {
#ifdef NO_STORE
    cr_assert_fail("Store was not implemented");
#endif
    char content[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    TRANSACTION *tp = trans_create();
    store_put(tp, make_key(content, 26), blob_create(content, 26));
    store_put(tp, make_key(content, 25), blob_create(content, 25));
    BLOB *bp1 = blob_create(content, 26);
    store_put(tp, make_key(content, 26), bp1);
    BLOB *bp2 = blob_create(content, 25);
    store_put(tp, make_key(content, 25), bp2);
    BLOB *value = NULL;
    store_get(tp, make_key(content, 26), &value);
    cr_assert_eq(bp1, value, "Wrong value returned, was %p, expected %p", value, bp1);
    store_get(tp, make_key(content, 25), &value);
    cr_assert_eq(bp2, value, "Wrong value returned, was %p, expected %p", value, bp2);
    assert_store_is_sane();
    assert_number_of_keys(2);
    KEY *kp = make_key(content, 26);
    assert_key_present(kp);
    assert_number_of_versions(kp, 1);
    kp = make_key(content, 25);
    assert_key_present(kp);
    assert_number_of_versions(kp, 1);
}

/*
 * Put a bunch of keys in the table, so that there are almost certainly collisions,
 * then retrieve all the values and verify them.
 */
Test(store_suite, collisions, .init = init, .timeout = 5) {
#ifdef NO_STORE
    cr_assert_fail("Store was not implemented");
#endif
    char content[10];
    TRANSACTION *tp = trans_create();
    BLOB *blobs[NKEYS];
    for(int i = 0; i < NKEYS; i++) {
	snprintf(content, 10, "%8d", i);
	blobs[i] = blob_create(content, 8);
	store_put(tp, make_key(content, 8), blobs[i]);
    }
    for(int i = 0; i < NKEYS; i++) {
	snprintf(content, 10, "%8d", i);
	BLOB *value = NULL;
	store_get(tp, make_key(content, 8), &value);
	cr_assert_eq(value, blobs[i], "Wrong blob returned");
    }
}

/*
 * Put the same key in the map several times, with different transactions,
 * verify the number of versions, etc.,
 * Then try to commit the transactions in increasing order of transaction ID
 * and verify that the right thing happens.
 */
Test(store_suite, multi_trans_forward, .init = init, .timeout = 5) {
#ifdef NO_STORE
    cr_assert_fail("Store was not implemented");
#endif
    char *key = "KEY";
    char content[10];
    TRANSACTION *trans[NTRANS];
    for(int i = 0; i < NTRANS; i++) {
	snprintf(content, 10, "%8d", i);
	trans[i] = trans_create();
	store_put(trans[i], make_key(key, 3), blob_create(content, 8));
    }
    trans_ref(trans[NTRANS/2], "");
    trans_abort(trans[NTRANS/2]);
    for(int i = 0; i < NTRANS; i++) {
	TRANS_STATUS st = trans_commit(trans[i]);
	if(i < NTRANS/2)
	    cr_assert_eq(st, TRANS_COMMITTED, "Transaction %d should have committed, but did not", i);
	else
	    cr_assert_eq(st, TRANS_ABORTED, "Transaction %d should have aborted, but did not", i);
    }
}

/*
 * Put the same key in the map several times, with different transactions,
 * verify the number of versions, etc.,
 * Then try to commit the transactions in increasing order of transaction ID.
 * Finally, run one more transaction that commits a different value,
 * and verify that everything is cleaned up, leaving just one version.
 */
Test(store_suite, multi_trans_forward_gc, .init = init, .timeout = 5) {
#ifdef NO_STORE
    cr_assert_fail("Store was not implemented");
#endif
    char *key = "KEY";
    char content[10];
    TRANSACTION *trans[NTRANS];
    for(int i = 0; i < NTRANS; i++) {
	snprintf(content, 10, "%8d", i);
	trans[i] = trans_create();
	store_put(trans[i], make_key(key, 3), blob_create(content, 8));
    }
    trans_ref(trans[NTRANS/2], "");
    trans_abort(trans[NTRANS/2]);
    for(int i = 0; i < NTRANS; i++)
	trans_commit(trans[i]);
    TRANSACTION *tp = trans_create();
    snprintf(content, 10, "%8d", NTRANS);
    store_put(tp, make_key(key, 3), blob_create(content, 8));    
    TRANS_STATUS st = trans_commit(tp);
    cr_assert_eq(st, TRANS_COMMITTED, "Transaction %d should have committed, but did not", NTRANS);
    assert_store_is_sane();
    assert_number_of_keys(1);
    KEY *kp = make_key(key, 3);
    assert_key_present(kp);
    assert_number_of_versions(kp, 2);
}

/*
 * Thread that attempts to commit a transaction, then set a done flag.
 */
struct commit_trans_args {
    TRANSACTION *tp;
    TRANS_STATUS status;
    volatile int done_flag;
};

static void *commit_trans_thread(void *arg) {
    struct commit_trans_args *ap = arg;
    ap->status = trans_commit(ap->tp);
    ap->done_flag = 1;
    return NULL;
}

/*
 * Put the same key in the map several times, with different transactions,
 * verify the number of versions, etc.,
 * Then, launch separate threads in reverse order of transaction ID to try
 * to commit the transactions, and verify that the right thing happens.
 */
Test(store_suite, multi_trans_backward, .init = init, .timeout = 5) {
#ifdef NO_STORE
    cr_assert_fail("Store was not implemented");
#endif
    char *key = "KEY";
    char content[10];
    TRANSACTION *trans[NTRANS];
    pthread_t tid[NTRANS];
    struct commit_trans_args *ap[NTRANS];
    for(int i = 0; i < NTRANS; i++) {
	snprintf(content, 10, "%8d", i);
	trans[i] = trans_create();
	store_put(trans[i], make_key(key, 3), blob_create(content, 8));
    }
    trans_ref(trans[NTRANS/2], "");
    trans_abort(trans[NTRANS/2]);
    for(int i = NTRANS-1; i >= 0; i--) {
	ap[i] = malloc(sizeof *ap);
	ap[i]->tp = trans[i];
	ap[i]->done_flag = 0;
	ap[i]->status = TRANS_PENDING;
	pthread_create(&tid[i], NULL, commit_trans_thread, ap[i]);
    }
    for(int i = 0; i < NTRANS-1; i++) {
	pthread_join(tid[i], NULL);
	if(i < NTRANS/2)
	    cr_assert_eq(ap[i]->status, TRANS_COMMITTED,
			 "Transaction %d should have committed, but did not", i);
	else
	    cr_assert_eq(ap[i]->status, TRANS_ABORTED,
			 "Transaction %d should have aborted, but did not", i);
    }
}

/*
 * Thread to perform a single get and put on the same key,
 * recording the result.
 */
struct get_put_trans_args {
    TRANSACTION *tp;
    TRANS_STATUS status;
    int index;
    pthread_t tid;
    KEY *key_to_put;
    KEY *key_to_get;
    BLOB *blob_to_put;
    BLOB *blob_from_get;
    volatile int done_flag;
};

static void *get_put_trans_thread(void *arg) {
    struct get_put_trans_args *ap = arg;
    store_get(ap->tp, ap->key_to_get, &ap->blob_from_get);
    store_put(ap->tp, ap->key_to_put, ap->blob_to_put);
    ap->status = trans_commit(ap->tp);
    ap->done_flag = 1;
    return NULL;
}

/*
 * Create a shirtload of transactions and launch them in random order
 * with a corresponding shirtload of threads.  We use enough threads so
 * that they can't be scheduled all at once, so things will take a little
 * time to run.
 * Each transaction will do a get and a put on the same key, then commit.
 * We verify that if a transaction commits, it sees the value left
 * by the next lower-numbered transaction that also committed.
 */

Test(store_suite, stress_test, .init = init, .timeout = 5) {
#ifdef NO_STORE
    cr_assert_fail("Store was not implemented");
#endif
    unsigned int seed = pthread_self();
    struct get_put_trans_args *ap[NTHREAD];
    BLOB *blobs[NTHREAD];
    memset(ap, 0, sizeof(ap));
    for(int i = 0; i < NTHREAD; i++) {
	char content[10];
	snprintf(content, 9, "%d", i);
	ap[i] = malloc(sizeof(*ap[i]));
	ap[i]->index = i;
	ap[i]->tp = trans_create();
	ap[i]->status = TRANS_PENDING;
	ap[i]->key_to_put = make_key("KEY", 3);
	ap[i]->key_to_get = make_key("KEY", 3);
	ap[i]->blob_to_put = blobs[i] = blob_create(content, 10);
	ap[i]->blob_from_get = NULL;
	ap[i]->done_flag = 0;
    }
    // Shuffle the elements of the array, so the transaction IDs are
    // not in sequence.
    for(int i = 0; i < NTHREAD-1; i++) {
	int j = (rand_r(&seed) % (NTHREAD-1-i)) + i + 1;
	struct get_put_trans_args *tmp = ap[i];
	ap[i] = ap[j];
	ap[j] = tmp;
    }
    // Now launch all the threads.
    for(int i = 0; i < NTHREAD; i++)
	pthread_create(&ap[i]->tid, NULL, get_put_trans_thread, ap[i]);

    // Wait for them all to finish.
    for(int i = 0; i < NTHREAD; i++)
	pthread_join(ap[i]->tid, NULL);

    // Let's map out who read from whom.
    int read_from[NTHREAD];
    for(int i = 0; i < NTHREAD; i++)
	read_from[i] = -1;
    int num_committed = 0;
    int max_committed;
    for(int i = 0; i < NTHREAD; i++) {
	if(ap[i]->status == TRANS_COMMITTED) {
	    num_committed++;
	    if(ap[i]->index > max_committed)
		max_committed = ap[i]->index;
	    int n = ap[i]->blob_from_get ? atoi(ap[i]->blob_from_get->content) : -1;
	    read_from[ap[i]->index] = n;
	    //fprintf(stderr, "%d %s (got %d)\n", ap[i]->index,
	    //	    ap[i]->status == TRANS_COMMITTED ? "committed" : "aborted", n);
	}
    }
    fprintf(stderr, "Store stress test: %d/%d committed\n", num_committed, NTHREAD);
    while(max_committed != -1) {
	//fprintf(stderr, "%d read from %d\n", max_committed, read_from[max_committed]);
	num_committed--;
	cr_assert(read_from[max_committed] < max_committed,
		  "The 'read-from' relation was not in transaction ID order");
	max_committed = read_from[max_committed];
    }
    cr_assert_eq(num_committed, 0, "Something was wrong with the 'read-from' relation"); 
}
