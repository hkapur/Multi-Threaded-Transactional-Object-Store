#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>
#include "debug.h"
#include "protocol.h"
#include "csapp.h"
#include <semaphore.h>
#include "client_registry.h"

void func(CLIENT_REGISTRY *cr, int j, int fd);
int func2(CLIENT_REGISTRY *cr, int j);

typedef struct client_registry{
    int fd[1024];
    sem_t mutex;
    unsigned int clients;
}  CLIENT_REGISTRY;

CLIENT_REGISTRY *creg_init()
{
    CLIENT_REGISTRY *cr = malloc(sizeof(CLIENT_REGISTRY));
    Sem_init(&(cr->mutex), 0, 1);
    int i = 0;
    while(i<1024)
    {
        cr->fd[i] = -1;
        i++;
    }
    return cr;
}
void creg_fini(CLIENT_REGISTRY *cr)
{
    free(cr);
}
void creg_register(CLIENT_REGISTRY *cr, int fd)
{
    P(&(cr->mutex));
    int i=0;
    while(i<1024)
    {
        if(func2(cr,i)==1)
        {
            func(cr,i,fd);
            break;
        }
        i++;
    }
    V(&(cr->mutex));
}
void func(CLIENT_REGISTRY *cr,int j, int fd)
{
    cr->fd[j]=fd;
    cr->clients++;
}
int func2(CLIENT_REGISTRY *cr, int j)
{
    if(cr->fd[j]==-1)
    {
        return 1;
    }
    else return 0;
}
void creg_unregister(CLIENT_REGISTRY *cr, int fd)
{
    P(&(cr->mutex));
    int i=0;
    while(i<1024)
    {
        if(cr->fd[i]==fd)
        {
            cr->fd[i] = -1;
            cr->clients--;
            break;
        }
        i++;
    }
    V(&(cr->mutex));
}
void creg_wait_for_empty(CLIENT_REGISTRY *cr)
{
    P(&(cr->mutex));
    if((cr->clients) == 0){
    }
    V(&(cr->mutex));

}
void creg_shutdown_all(CLIENT_REGISTRY *cr)
{
    int i = 0;
    while(i<1024)
    {
        if(func2(cr,i)==1)
        {
            cr->clients--;
            shutdown(cr->fd[i], SHUT_RD);
        }
        i++;
    }
}