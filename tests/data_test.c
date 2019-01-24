#include <criterion/criterion.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

#include "debug.h"
#include "data.h"
#include "transaction.h"
#include "excludes.h"

/* Number of threads we create in multithreaded tests. */
#define NTHREAD (10)

/* Number of iterations we use in several tests. */
#define NITER (1000000)

static void init() {
    trans_init();
}

Test(data_suite, blob_create_test, .init = init, .timeout = 5) {
#ifdef NO_DATA
    cr_assert_fail("Data module was not implemented");
#endif
    char content[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    BLOB *bp = blob_create(content, 26);
    cr_assert_not_null(bp, "Expected non-NULL pointer");
    cr_assert_eq(bp->refcnt, 1, "Expected reference count 1, was %d", bp->refcnt);
    cr_assert_eq(bp->size, 26, "Expected size 26, was %lu", bp->size);
    int eq = memcmp(bp->content, content, 26);
    cr_assert_eq(eq, 0, "Content was incorrect");
}

Test(data_suite, blob_ref_unref_test, .init = init, .timeout = 5) {
#ifdef NO_DATA
    cr_assert_fail("Data module was not implemented");
#endif
    char content[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    BLOB *bp = blob_create(content, 26);
    cr_assert_eq(bp->refcnt, 1, "Expected reference count 1, was %d", bp->refcnt);
    blob_ref(bp, "");
    cr_assert_eq(bp->refcnt, 2, "Expected reference count 2, was %d", bp->refcnt);
    blob_unref(bp, "");
    cr_assert_eq(bp->refcnt, 1, "Expected reference count 1, was %d", bp->refcnt);
    blob_unref(bp, "");  // Should free the blob without crashing.
}

Test(data_suite, blob_compare_test, .init = init, .timeout = 5) {
#ifdef NO_DATA
    cr_assert_fail("Data module was not implemented");
#endif
    char content[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    char content1[] = "ABCDEFGHIJXLMNOPQRSTUVWXYZ";
    BLOB *bp1 = blob_create(content, 26);
    BLOB *bp2 = blob_create(content, 25);
    BLOB *bp3 = blob_create(content1, 26);
    cr_assert_eq(blob_compare(bp1, bp1), 0, "Result should be zero");
    cr_assert_neq(blob_compare(bp1, bp2), 0, "Result should be nonzero");
    cr_assert_neq(blob_compare(bp1, bp3), 0, "Result should be nonzero");
}

Test(data_suite, key_create_test, .init = init, .timeout = 5) {
#ifdef NO_DATA
    cr_assert_fail("Data module was not implemented");
#endif
    char content[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    BLOB *bp = blob_create(content, 26);
    KEY *kp = key_create(bp);
    cr_assert_not_null(kp, "Expected non-NULL pointer");
    cr_assert_eq(kp->blob, bp, "Key did not contain correct blob");
    cr_assert_eq(bp->refcnt, 1, "Expected reference count 1, was %d", bp->refcnt);
}

Test(data_suite, key_dispose_test, .init = init, .timeout = 5) {
#ifdef NO_DATA
    cr_assert_fail("Data module was not implemented");
#endif
    char content[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    BLOB *bp = blob_create(content, 26);
    blob_ref(bp, "to keep blob from being freed");
    cr_assert_eq(bp->refcnt, 2, "Expected reference count 2, was %d", bp->refcnt);
    KEY *kp = key_create(bp);
    key_dispose(kp);
    cr_assert_eq(bp->refcnt, 1, "Expected reference count 1, was %d", bp->refcnt);
}

Test(data_suite, key_compare_test, .init = init, .timeout = 5) {
#ifdef NO_DATA
    cr_assert_fail("Data module was not implemented");
#endif
    char content[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    char content1[] = "ABCDEFGHIJXLMNOPQRSTUVWXYZ";
    BLOB *bp1 = blob_create(content, 26);
    KEY *kp1 = key_create(bp1);
    BLOB *bp2 = blob_create(content, 25);
    KEY *kp2 = key_create(bp2);
    BLOB *bp3 = blob_create(content1, 26);
    KEY *kp3 = key_create(bp3);
    cr_assert_eq(key_compare(kp1, kp1), 0, "Result should be zero");
    cr_assert_neq(key_compare(kp1, kp2), 0, "Result should be nonzero");
    cr_assert_neq(key_compare(kp1, kp3), 0, "Result should be nonzero");
}

Test(data_suite, version_create_test, .init = init, .timeout = 5) {
#ifdef NO_DATA
    cr_assert_fail("Data module was not implemented");
#endif
    char content[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    BLOB *bp = blob_create(content, 26);
    TRANSACTION *tp = trans_create();
    cr_assert_not_null(tp, "Transaction was NULL");
    cr_assert_eq(tp->refcnt, 1, "Transaction refcnt was %d, not 1", tp->refcnt);
    VERSION *vp = version_create(tp, bp);
    cr_assert_eq(bp->refcnt, 1, "Blob refcnt was %d, not 1", bp->refcnt);
    cr_assert_eq(tp->refcnt, 2, "Transaction refcnt was %d, not 2", tp->refcnt);
    cr_assert_eq(vp->creator, tp, "Version had wrong creator");
    cr_assert_eq(vp->blob, bp, "Version had wrong blob");
    cr_assert_null(vp->next, "Version had non-NULL next field");
    cr_assert_null(vp->prev, "Version had non-NULL prev field");
}

Test(data_suite, version_dispose_test, .init = init, .timeout = 5) {
#ifdef NO_DATA
    cr_assert_fail("Data module was not implemented");
#endif
    char content[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    BLOB *bp = blob_create(content, 26);
    blob_ref(bp, "");
    TRANSACTION *tp = trans_create();
    cr_assert_not_null(tp, "Transaction was NULL");
    VERSION *vp = version_create(tp, bp);
    version_dispose(vp);
    cr_assert_eq(tp->refcnt, 1, "Transaction refcnt was %d, not 1", tp->refcnt);
    cr_assert_eq(bp->refcnt, 1, "Blob refcnt was %d, not 1", bp->refcnt);
}

/*
 * Thread that runs random ref/unref, then sets a flag.
 * The thread delays at the start of the test, to make it more likely
 * that other threads started at about the same time are active.
 */
struct random_ref_unref_args {
    BLOB *bp;
    volatile int *done_flag;
    int iters;
    int start_delay;
};

/*
 * Randomly increase and decrease blob reference count, then decrease
 * to zero at the end..
 */
static void random_ref_unref(BLOB *bp, int n) {
    int count = 0;
    unsigned int seed = 1; //pthread_self();
    for(int i = 0; i < n; i++) {
	if(count == 0) {
	    blob_ref(bp, "");
	    count++;
	} else {
	    if(rand_r(&seed) % 2) {
		blob_ref(bp, "");
		count++;
	    } else {
		blob_unref(bp, "");
		count--;
	    }
	}
    }
    // Get rid of any remaining references at the end.
    while(count-- > 0)
	blob_unref(bp, "");
}

static void *random_ref_unref_thread(void *arg) {
    struct random_ref_unref_args *ap = arg;
    if(ap->start_delay)
	sleep(ap->start_delay);
    random_ref_unref(ap->bp, ap->iters);
    if(ap->done_flag != NULL)
	*ap->done_flag = 1;
    return NULL;
}

Test(data_suite, many_threads_ref_unref_blob, .init = init, .timeout = 10) {
#ifdef NO_DATA
    cr_assert_fail("Data module was not implemented");
#endif
    char content[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    BLOB *bp = blob_create(content, 26);
    cr_assert_not_null(bp);
    cr_assert_eq(bp->refcnt, 1, "Initial blob refcount is %d, not 1", bp->refcnt);

    // Spawn threads to run random ref/unref.
    pthread_t tid[NTHREAD];
    for(int i = 0; i < NTHREAD; i++) {
	struct random_ref_unref_args *ap = calloc(1, sizeof(struct random_ref_unref_args));
	ap->bp = bp;
	ap->iters = NITER;
	pthread_create(&tid[i], NULL, random_ref_unref_thread, ap);
    }

    // Wait for all threads to finish.
    for(int i = 0; i < NTHREAD; i++)
	pthread_join(tid[i], NULL);

    // Check final reference count.
    cr_assert_eq(bp->refcnt, 1, "Final blob refcount is %d, not 1", bp->refcnt);
}
