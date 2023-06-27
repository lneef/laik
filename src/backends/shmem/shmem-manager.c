
#include <stdalign.h>
#include <stddef.h>
#include <stdlib.h>
#include "backends/shmem/shmem.h"
#include "shmem-allocator.h"
#include "shmem-manager.h"
#include <sys/shm.h>

struct shmemPair
{
    int shmid;
    void* ptr;
    struct shmemPair* next;
};


struct shmemPair head;

void* shmem_manager_alloc(size_t size)
{
    int shmid;
    void* ptr = shmem_alloc(size, &shmid);

    struct shmemPair* new = malloc( sizeof(struct shmemPair));

    new -> shmid = shmid;
    new -> ptr = ptr;

    new ->next = head.next;
    head.next = new;

    return new -> ptr;

}

void shmem_manager_cleanup()
{
    for(struct shmemPair* c = head.next; c != NULL; )
    {
        def_shmem_free(NULL, c->ptr);
        struct shmemPair* tmp = c;
        c = c -> next;
        free(tmp);
    }

    head.next = NULL;

}

void def_shmem_free(Laik_Data* d, void* ptr){
    (void) d; // not used in this implementation of interface
    shmem_free(ptr);
}

void* def_shmem_malloc(Laik_Data* d, size_t size){
    
    (void) d; // not used in this implementation of interface
    int shmid;
    return shmem_alloc(size, &shmid);
}


