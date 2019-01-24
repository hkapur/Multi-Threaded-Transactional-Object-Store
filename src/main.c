#include "debug.h"
#include "client_registry.h"
#include "transaction.h"
#include "store.h"
#include "csapp.h"
#include "server.h"

char *port;
char *host_name;
char *file_name;
static void terminate(int status);
void sighup_handler(int sig);

CLIENT_REGISTRY *client_registry;

int main(int argc, char* argv[]){
    Signal(SIGHUP, &sighup_handler);
    // Option processing should be performed here.
    // Option '-p <port>' is required in order to specify the port number
    // on which the server should listen.

    // Perform required initializations of the client_registry,
    // transaction manager, and object store.
    char optval;
    static char *short_options = "+p:";
    while(optind<argc)
    {
    if((optval = getopt(argc, argv, short_options)) != -1)
       {
           switch(optval)
           {
                case 'p':
                port = optarg;
                break;
                case '?':
                fprintf(stderr, "Usage: %s [-i <cmd_file>] [-o <out_file>]\n", argv[0]);
                exit(EXIT_FAILURE);
                break;
           }
        }

    }

    int *connfdp;
    int listenfd = Open_listenfd(port);
    socklen_t clientlen;
    struct sockaddr_storage clientaddr;
    pthread_t tid;

    client_registry = creg_init();
    trans_init();
    store_init();
    while (1) {
        clientlen=sizeof(struct sockaddr_storage);
        connfdp = malloc(sizeof(int));
        *connfdp = Accept(listenfd, (SA *) &clientaddr, &clientlen);
        Pthread_create(&tid, NULL, xacto_client_service, connfdp);
    }
    // TODO: Set up the server socket and enter a loop to accept connections
    // on this socket.  For each connection, a thread should be started to
    // run function xacto_client_service().  In addition, you should install
    // a SIGHUP handler, so that receipt of SIGHUP will perform a clean
    // shutdown of the server.

    fprintf(stderr, "You have to finish implementing main() "
	    "before the Xacto server will function.\n");

    terminate(EXIT_FAILURE);
}

/*
 * Function called to cleanly shut down the server.
 */
void terminate(int status) {
    // Shutdown all client connections.
    // This will trigger the eventual termination of service threads.
    creg_shutdown_all(client_registry);

    debug("Waiting for service threads to terminate...");
    creg_wait_for_empty(client_registry);
    debug("All service threads terminated.");

    // Finalize modules.
    creg_fini(client_registry);
    trans_fini();
    store_fini();

    debug("Xacto server terminating");
    exit(status);
}

void *thread(void *vargp){
    Pthread_detach(pthread_self());
    xacto_client_service(vargp);
    return NULL;
}

void sighup_handler(int sig){
    int save_errno = errno;
    terminate(EXIT_SUCCESS);
    errno = save_errno;
}