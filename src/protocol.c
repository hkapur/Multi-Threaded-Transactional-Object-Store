#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>
#include "debug.h"
#include "protocol.h"
#include "csapp.h"

void set_ntohl(XACTO_PACKET *pkt);
void set_htonl(XACTO_PACKET *pkt);
int check_pkt_type(XACTO_PACKET *pkt);
int my_func(int rio_res);
struct timespec time_stamp;
void set_time(XACTO_PACKET *pkt);

int proto_send_packet(int fd, XACTO_PACKET *pkt, void *payload) {

    size_t pkt_size = sizeof(XACTO_PACKET);
    uint32_t length = pkt->size;
    int rio_res;
    clock_gettime(CLOCK_MONOTONIC, &time_stamp);
    if(check_pkt_type(pkt)==1)
    {
        return -1;
    }
    set_time(pkt);
    set_htonl(pkt);
    rio_res = rio_writen(fd, pkt, pkt_size);
    if (rio_res < 0)
    {
        errno = EAGAIN;
        return -1;
    }
    if(!my_func(rio_res))
    {
        return -1;
    }
    if ((payload != NULL) && (length > 0))
    {
        rio_res = rio_writen(fd, payload, length);
        if (rio_res < 0) {
            errno = EAGAIN;
            return -1;
        }
        if(!my_func(rio_res))
        {
            return -1;
        }
    }
    return 0;
}

int proto_recv_packet(int fd, XACTO_PACKET *pkt, void **payload) {

    size_t pkt_size = sizeof(XACTO_PACKET);
    int rio_res = rio_readn(fd, pkt, pkt_size);
    if (rio_res < 0)
    {
        errno = EAGAIN;
        return -1;
    }
    if(!my_func(rio_res))
    {
        return -1;
    }
    set_ntohl(pkt);
    if(check_pkt_type(pkt)==1)
    {
        return -1;
    }
    uint32_t length = pkt->size;
    if (length > 0)
    {
        char *data = Malloc(length);
        *payload = data;
        rio_res = rio_readn(fd, data, length);
        if (rio_res < 0)
        {
            errno = EAGAIN;
            return -1;
        }
        if(!my_func(rio_res))
        {
            return -1;
        }
    }
    return 0;
}

void set_ntohl(XACTO_PACKET *pkt) {
    pkt->size = ntohl(pkt->size);
    pkt->timestamp_sec = ntohl(pkt->timestamp_sec);
    pkt->timestamp_nsec = ntohl(pkt->timestamp_nsec);
}

int check_pkt_type(XACTO_PACKET *pkt)
{
    if (pkt->type != XACTO_NO_PKT)
    {
        return 0;
    }
    else if(pkt->type == XACTO_NO_PKT)
    {
        return 1;
    }
    return 0;
}

int my_func(int rio)
{
    if(rio==0)
    {
        return 0;
    }
    else return 1;
}

void set_time(XACTO_PACKET *pkt) {
    pkt->timestamp_sec = (uint32_t)time_stamp.tv_sec;
    pkt->timestamp_nsec = (uint32_t)time_stamp.tv_nsec;
}

void set_htonl(XACTO_PACKET *pkt)
{
    pkt->size = htonl(pkt->size);
    pkt->timestamp_sec = htonl(pkt->timestamp_sec);
    pkt->timestamp_nsec = htonl(pkt->timestamp_nsec);
}