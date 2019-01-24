#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>
#include "debug.h"
#include "csapp.h"
#include <semaphore.h>
#include "data.h"

void decrease_cnt(BLOB *bp);
void increase_cnt(BLOB *bp);

BLOB *blob_create(char *content, size_t size)
{
    BLOB *bp = Malloc(sizeof(BLOB));
    pthread_mutex_init(&(bp->mutex), NULL);
    blob_ref(bp,"Blob");
    bp->prefix = malloc(size);
    bp->content = malloc(size);
    bp->size = size;
    strncpy(bp->prefix,content,size);
    strncpy(bp->content,content,size);
    return bp;
}
BLOB *blob_ref(BLOB *bp, char *why)
{
    if(bp!=NULL)
    {
    pthread_mutex_lock(&(bp->mutex));
    increase_cnt(bp);
    pthread_mutex_unlock(&(bp->mutex));
    }
    return bp;
}
int blob_compare(BLOB *bp1, BLOB *bp2)
{
    if(strcmp(bp1->content,bp2->content)!=0)
    {
        return 1;
    }
    else return 0;
}
void blob_unref(BLOB *bp, char *why)
{
    if(bp!=NULL)
    {
    pthread_mutex_lock(&(bp->mutex));
    decrease_cnt(bp);
    if(bp->refcnt==0)
    {
        pthread_mutex_unlock(&(bp->mutex));
        free(bp);
        return;
    }
        pthread_mutex_unlock(&(bp->mutex));
    }
}
int blob_hash(BLOB *bp)
{
    int hash = 0;
    int c;

    while ((c = *bp->content++)){
        hash = ((hash << 5) + hash) + c;
    }

    return hash;
}
VERSION *version_create(TRANSACTION *tp, BLOB *bp)
{
    int size = sizeof(VERSION);
    VERSION *v = malloc(size);
    v->creator = tp;
    if(bp!=NULL)
    {
        v->blob = bp;
        v->next = NULL;
        v->prev = NULL;
    }
    else
    {
        v->blob = NULL;
        v->next = NULL;
        v->prev = NULL;
    }
    trans_ref(tp,"Transaction reference");
    return v;
}
void version_dispose(VERSION *vp)
{
    if(vp!=NULL)
    {
        if(vp->blob!=NULL)
        {
            blob_unref(vp->blob,"Dispose version blob");
        }
        trans_unref(vp->creator,"Dispose version creator");
        free(vp);
    }
}
KEY *key_create(BLOB *bp)
{
    KEY *kp = malloc(sizeof(KEY));
    kp->hash = blob_hash(bp);
    kp->blob = bp;
    return kp;
}
int key_compare(KEY *kp1, KEY *kp2)
{
    if(blob_compare(kp1->blob,kp2->blob)==0)
    {
        if(kp1->hash==kp2->hash)
        {
            return 0;
        }
    }
    return 1;
}
void key_dispose(KEY *kp)
{
    blob_unref(kp->blob,"For key disposal");
    free(kp);
}
void decrease_cnt(BLOB *bp)
{
    bp->refcnt--;
}
void increase_cnt(BLOB *bp)
{
    bp->refcnt++;
}