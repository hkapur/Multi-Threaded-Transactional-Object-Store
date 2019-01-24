#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>
#include "debug.h"
#include "csapp.h"
#include <semaphore.h>
#include "data.h"
#include "server.h"
#include "protocol.h"
#include "client_registry.h"
#include "transaction.h"
#include "store.h"

CLIENT_REGISTRY *client_registry;
void free_func(XACTO_PACKET *s, XACTO_PACKET *r, void **p, int fd);
void main_free_func(XACTO_PACKET *s, XACTO_PACKET *r, void **p);
char *pkt_type(XACTO_PACKET *r);
int p_flag=0;
int g_flag=0;

void *xacto_client_service(void *arg)
{
    int fd = *( ( int* )arg );
    free(arg);
    pthread_detach(pthread_self());
    creg_register(client_registry, fd);
    TRANSACTION *transac = trans_create();
    while(1)
    {
        BLOB *key_blob;
        KEY *k;
        BLOB *value_blob;
        BLOB *data_blob;
        XACTO_PACKET *send = Malloc(sizeof(XACTO_PACKET));
        XACTO_PACKET *receive = Malloc(sizeof(XACTO_PACKET));
        void **payload = Malloc(sizeof(void **));
        if(proto_recv_packet(fd, receive, payload) < 0)
        {
            free_func(send,receive,payload,fd);
            break;
        }
        if((receive->type==XACTO_PUT_PKT || receive->type==XACTO_DATA_PKT) && (g_flag==0))
        {
            if(p_flag==0)
            {
                p_flag++;
                continue;
            }
            if(p_flag==1)
            {
                p_flag++;
                key_blob = blob_create(*payload,strlen(*payload));
                k = key_create(key_blob);
                continue;
            }
            if(p_flag==2)
            {
                p_flag=0;
                value_blob = blob_create(*payload,strlen(*payload));
                int x = store_put(transac, k, value_blob);
                if(x==TRANS_ABORTED)
                {
                    receive->type = XACTO_REPLY_PKT;
                    receive->status = 2;
                    receive->size = 0;
                    receive->timestamp_sec = receive->timestamp_sec;
                    receive->timestamp_nsec = receive->timestamp_nsec;
                    proto_send_packet(fd,receive,NULL);
                    free(payload);
                    break;
                }
                store_show();
                trans_show_all();
                send->type = XACTO_REPLY_PKT;
                send->status = transac->status;
                send->null = 0;
                send->size = 0;
                send->timestamp_sec = receive->timestamp_sec;
                send->timestamp_nsec = receive->timestamp_nsec;
                proto_send_packet(fd, send, *payload);
                continue;
            }
        }
        else if((receive->type==XACTO_GET_PKT || receive->type==XACTO_DATA_PKT) && (p_flag==0))
        {
            if(g_flag==0)
            {
                g_flag=1;
                continue;
            }
            if(g_flag==1)
            {
                g_flag=0;
                key_blob = blob_create(*payload,strlen(*payload));
                data_blob = Malloc(sizeof(BLOB*));
                k = key_create(key_blob);
                int x = store_get(transac, k, &data_blob);
                if(x==TRANS_ABORTED)
                {
                    receive->type = XACTO_REPLY_PKT;
                    receive->status = 2;
                    receive->size = 0;
                    receive->timestamp_sec = receive->timestamp_sec;
                    receive->timestamp_nsec = receive->timestamp_nsec;
                    proto_send_packet(fd,receive,NULL);
                    free(payload);
                    break;
                }
                store_show();
                trans_show_all();
                receive->type = XACTO_REPLY_PKT;
                receive->status = 0;
                receive->size = 0;
                receive->timestamp_sec = receive->timestamp_sec;
                receive->timestamp_nsec = receive->timestamp_nsec;
                proto_send_packet(fd,receive,payload);
                receive->type = XACTO_DATA_PKT;
                receive->status = 0;
                if(data_blob==NULL)
                {
                    receive->null = 1;
                    receive->size = 0;
                    proto_send_packet(fd,receive,NULL);
                }
                else
                {
                    receive->size = data_blob->size;
                    proto_send_packet(fd,receive,data_blob->content);
                }
                continue;
            }

        }
        else if(strcmp(pkt_type(receive),"COMMIT")==0)
        {
            trans_commit(transac);
            receive->status = 1;
            receive->type = XACTO_REPLY_PKT;
            receive->timestamp_sec = receive->timestamp_sec;
            receive->timestamp_nsec = receive->timestamp_nsec;
            proto_send_packet(fd,receive,NULL);
            break;
        }
        main_free_func(send,receive,payload);
    }
    creg_unregister(client_registry,fd);
    shutdown(fd,SHUT_RD);
    return NULL;
}
void free_func(XACTO_PACKET *s, XACTO_PACKET *r, void **p, int fd)
{
    free(s);
    free(r);
    free(p);
    shutdown(fd,SHUT_RD);
}
void main_free_func(XACTO_PACKET *s, XACTO_PACKET *r, void **p)
{
    free(s);
    free(r);
    free(p);
}
char *pkt_type(XACTO_PACKET *r)
{
    if(r->type == XACTO_PUT_PKT)
    {
        return "PUT";
    }
    else if(r->type == XACTO_GET_PKT)
    {
        return "GET";
    }
    else if(r->type == XACTO_COMMIT_PKT)
    {
        return "COMMIT";
    }
    else return NULL;
}