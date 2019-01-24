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

Test(transaction_suite, transaction_create_test, .init = init, .timeout = 5) {
#ifdef NO_TRANSACTION
    cr_assert_fail("Transaction module was not implemented");
#endif
    TRANSACTION *tp = trans_create();
    cr_assert_not_null(tp, "Expected non-NULL pointer");
    cr_assert_eq(tp->refcnt, 1, "Expected reference count 1, was %d", tp->refcnt);
    cr_assert_eq(tp->status, TRANS_PENDING, "Expected status %d, was %d", TRANS_PENDING, tp->status);
    cr_assert_null(tp->depends, "Expected NULL dependency list");
    cr_assert_eq(tp->waitcnt, 0, "Expected waitcnt 0, was %d", tp->waitcnt);
}

Test(transaction_suite, transaction_ref_unref_test, .init = init, .timeout = 5) {
#ifdef NO_TRANSACTION
    cr_assert_fail("Transaction module was not implemented");
#endif
    TRANSACTION *tp = trans_create();
    cr_assert_not_null(tp, "Expected non-NULL pointer");
    cr_assert_eq(tp->refcnt, 1, "Expected reference count 1, was %d", tp->refcnt);
    trans_ref(tp, "");
    cr_assert_eq(tp->refcnt, 2, "Expected reference count 2, was %d", tp->refcnt);
    trans_unref(tp, "");
    cr_assert_eq(tp->refcnt, 1, "Expected reference count 1, was %d", tp->refcnt);
    trans_unref(tp, "");  // Should free the transaction without crashing.
}

Test(transaction_suite, transaction_commit_test, .init = init, .timeout = 5) {
#ifdef NO_TRANSACTION
    cr_assert_fail("Transaction module was not implemented");
#endif
    TRANSACTION *tp = trans_create();
    trans_ref(tp, "");
    TRANS_STATUS st = trans_commit(tp);
    cr_assert_eq(st, TRANS_COMMITTED, "Expected status %d, was %d", TRANS_COMMITTED, st);
    cr_assert_eq(tp->status, TRANS_COMMITTED, "Expected status %d, was %d", TRANS_COMMITTED, tp->status);
    trans_unref(tp, "");  // Should free the transaction without crashing.
}

Test(transaction_suite, transaction_abort_test, .init = init, .timeout = 5) {
#ifdef NO_TRANSACTION
    cr_assert_fail("Transaction module was not implemented");
#endif
    TRANSACTION *tp = trans_create();
    trans_ref(tp, "");
    TRANS_STATUS st = trans_abort(tp);
    cr_assert_eq(st, TRANS_ABORTED, "Expected status %d, was %d", TRANS_ABORTED, st);
    cr_assert_eq(tp->status, TRANS_ABORTED, "Expected status %d, was %d", TRANS_ABORTED, tp->status);
    trans_unref(tp, "");  // Should free the transaction without crashing.
}

Test(transaction_suite, transaction_dependent_abort_test, .init = init, .timeout = 5) {
#ifdef NO_TRANSACTION
    cr_assert_fail("Transaction module was not implemented");
#endif
    TRANSACTION *tp1 = trans_create();
    trans_ref(tp1, "");
    TRANSACTION *tp2 = trans_create();
    trans_ref(tp2, "");
    trans_add_dependency(tp1, tp2);
    TRANS_STATUS st2 = trans_abort(tp2);
    cr_assert_eq(st2, TRANS_ABORTED, "Expected status %d, was %d", TRANS_ABORTED, st2);
    TRANS_STATUS st1 = trans_commit(tp1);
    cr_assert_eq(st1, TRANS_ABORTED, "Expected status %d, was %d", TRANS_ABORTED, st1);
    trans_unref(tp1, "");
    trans_unref(tp2, "");
}

Test(transaction_suite, transaction_dependent_commit_test, .init = init, .timeout = 5) {
#ifdef NO_TRANSACTION
    cr_assert_fail("Transaction module was not implemented");
#endif
    TRANSACTION *tp1 = trans_create();
    trans_ref(tp1, "");
    TRANSACTION *tp2 = trans_create();
    trans_ref(tp2, "");
    trans_add_dependency(tp1, tp2);
    TRANS_STATUS st2 = trans_commit(tp2);
    cr_assert_eq(st2, TRANS_COMMITTED, "Expected status %d, was %d", TRANS_COMMITTED, st2);
    TRANS_STATUS st1 = trans_commit(tp1);
    cr_assert_eq(st1, TRANS_COMMITTED, "Expected status %d, was %d", TRANS_COMMITTED, st1);
    trans_unref(tp1, "");
    trans_unref(tp2, "");
}

/*
 * Thread that attempts to commit a transaction, then set a done flag.
 */
struct commit_trans_args {
    TRANSACTION *tp;
    volatile int done_flag;
};

static void *commit_trans_thread(void *arg) {
    struct commit_trans_args *ap = arg;
    trans_commit(ap->tp);
    ap->done_flag = 1;
    return NULL;
}

Test(transaction_suite, transaction_dependent_commit_block_test, .init = init, .timeout = 5) {
#ifdef NO_TRANSACTION
    cr_assert_fail("Transaction module was not implemented");
#endif
    TRANSACTION *tp1 = trans_create();
    trans_ref(tp1, "");
    TRANSACTION *tp2 = trans_create();
    trans_ref(tp2, "");
    trans_add_dependency(tp1, tp2);
    // Now attempt to commit tp1 in another thread -- it should block.
    // Wait for a short time, then check its done flag.  If the done
    // flag is set, then the thread did not block and the test fails.
    struct commit_trans_args *ap = malloc(sizeof *ap);
    ap->tp = tp1;
    ap->done_flag = 0;
    pthread_t tid;
    pthread_create(&tid, NULL, commit_trans_thread, ap);
    sleep(1);
    cr_assert_eq(ap->done_flag, 0, "Dependent transaction did not block as it should");
    // Now release the dependent transaction.  It should finish.
    trans_abort(tp2);
    // Check that tp1 has completed.  Timeout does not always seem to work properly here
    // if we try to use pthread_join.
    sleep(1);
    cr_assert_neq(ap->done_flag, 0, "Dependent transaction did not finish as it should");
}

Test(transaction_suite, transaction_dependent_abort_block_test, .init = init, .timeout = 5) {
#ifdef NO_TRANSACTION
    cr_assert_fail("Transaction module was not implemented");
#endif
    TRANSACTION *tp1 = trans_create();
    trans_ref(tp1, "");
    TRANSACTION *tp2 = trans_create();
    trans_ref(tp2, "");
    trans_add_dependency(tp1, tp2);
    // Now attempt to abort tp1 in another thread -- it should not block,
    // even though tp2 has not yet decided.
    TRANS_STATUS st1 = trans_abort(tp1);
    cr_assert_eq(st1, TRANS_ABORTED, "Expected status %d, was %d", TRANS_ABORTED, st1);
}

Test(transaction_suite, transaction_multi_dependent_test_1, .init = init, .timeout = 5) {
#ifdef NO_TRANSACTION
    cr_assert_fail("Transaction module was not implemented");
#endif
    TRANSACTION *tp1 = trans_create();
    trans_ref(tp1, "");
    TRANSACTION *tp2 = trans_create();
    trans_ref(tp2, "");
    TRANSACTION *tp3 = trans_create();
    trans_ref(tp3, "");
    TRANSACTION *tp4 = trans_create();
    trans_ref(tp4, "");
    trans_add_dependency(tp1, tp2);
    trans_add_dependency(tp1, tp3);
    trans_add_dependency(tp1, tp4);
    // Now attempt to commit tp1 in another thread -- it should block.
    struct commit_trans_args *ap = malloc(sizeof *ap);
    ap->tp = tp1;
    ap->done_flag = 0;
    pthread_t tid;
    pthread_create(&tid, NULL, commit_trans_thread, ap);
    sleep(1);
    cr_assert_eq(ap->done_flag, 0, "Dependent transaction did not block as it should");
    // Commit tp2, tp3, tp4.  Do them all once to try to exercise races.
    trans_commit(tp2);
    trans_commit(tp3);
    trans_commit(tp4);
    // Check that tp1 has completed.  Timeout does not always seem to work properly here
    // if we try to use pthread_join.
    sleep(1);
    cr_assert_neq(ap->done_flag, 0, "Dependent transaction did not finish as it should");
}

Test(transaction_suite, transaction_multi_dependent_test_2, .init = init, .timeout = 5) {
#ifdef NO_TRANSACTION
    cr_assert_fail("Transaction module was not implemented");
#endif
    TRANSACTION *tp1 = trans_create();
    trans_ref(tp1, "");
    TRANSACTION *tp2 = trans_create();
    trans_ref(tp2, "");
    TRANSACTION *tp3 = trans_create();
    trans_ref(tp3, "");
    TRANSACTION *tp4 = trans_create();
    trans_ref(tp4, "");
    trans_add_dependency(tp2, tp1);
    trans_add_dependency(tp3, tp1);
    trans_add_dependency(tp4, tp1);
    // Now attempt to commit tp2, tp3, tp4 other threads -- they should block.
    struct commit_trans_args *ap2 = malloc(sizeof *ap2);
    ap2->tp = tp2;
    ap2->done_flag = 0;
    pthread_t tid2;
    pthread_create(&tid2, NULL, commit_trans_thread, ap2);
    struct commit_trans_args *ap3 = malloc(sizeof *ap3);
    ap3->tp = tp3;
    ap3->done_flag = 0;
    pthread_t tid3;
    pthread_create(&tid3, NULL, commit_trans_thread, ap3);
    struct commit_trans_args *ap4 = malloc(sizeof *ap4);
    ap4->tp = tp4;
    ap4->done_flag = 0;
    pthread_t tid4;
    pthread_create(&tid4, NULL, commit_trans_thread, ap4);
    sleep(1);
    cr_assert_eq(ap2->done_flag, 0, "Dependent transaction did not block as it should");
    cr_assert_eq(ap3->done_flag, 0, "Dependent transaction did not block as it should");
    cr_assert_eq(ap4->done_flag, 0, "Dependent transaction did not block as it should");
    // Commit tp1 and check that tp2, tp3, and tp4 complete.
    trans_commit(tp1);
    sleep(1);
    cr_assert_neq(ap2->done_flag, 0, "Dependent transaction did not finish as it should");
    cr_assert_neq(ap3->done_flag, 0, "Dependent transaction did not finish as it should");
    cr_assert_neq(ap4->done_flag, 0, "Dependent transaction did not finish as it should");
}

/*
 * Thread that runs random ref/unref, then sets a flag.
 * The thread delays at the start of the test, to make it more likely
 * that other threads started at about the same time are active.
 */
struct random_ref_unref_args {
    TRANSACTION *tp;
    volatile int *done_flag;
    int iters;
    int start_delay;
};

/*
 * Randomly increase and decrease transaction reference count, then decrease
 * to zero at the end..
 */
static void random_ref_unref(TRANSACTION *tp, int n) {
    int count = 0;
    unsigned int seed = 1; //pthread_self();
    for(int i = 0; i < n; i++) {
	if(count == 0) {
	    trans_ref(tp, "");
	    count++;
	} else {
	    if(rand_r(&seed) % 2) {
		trans_ref(tp, "");
		count++;
	    } else {
		trans_unref(tp, "");
		count--;
	    }
	}
    }
    // Get rid of any remaining references at the end.
    while(count-- > 0)
	trans_unref(tp, "");
}

static void *random_ref_unref_thread(void *arg) {
    struct random_ref_unref_args *ap = arg;
    if(ap->start_delay)
	sleep(ap->start_delay);
    random_ref_unref(ap->tp, ap->iters);
    if(ap->done_flag != NULL)
	*ap->done_flag = 1;
    return NULL;
}

Test(transaction_suite, many_threads_ref_unref_trans, .init = init, .timeout = 10) {
#ifdef NO_TRANSACTION
    cr_assert_fail("Transaction module was not implemented");
#endif
    TRANSACTION *tp = trans_create();

    // Spawn threads to run random ref/unref.
    pthread_t tid[NTHREAD];
    for(int i = 0; i < NTHREAD; i++) {
	struct random_ref_unref_args *ap = calloc(1, sizeof(struct random_ref_unref_args));
	ap->tp = tp;
	ap->iters = NITER;
	pthread_create(&tid[i], NULL, random_ref_unref_thread, ap);
    }

    // Wait for all threads to finish.
    for(int i = 0; i < NTHREAD; i++)
	pthread_join(tid[i], NULL);

    // Check final reference count.
    cr_assert_eq(tp->refcnt, 1, "Final transaction refcount is %d, not 1", tp->refcnt);
}
