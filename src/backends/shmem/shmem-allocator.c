
#include "backends/shmem/shmem-allocator.h"
#include "laik.h"
#include "shmem.h"

#include <assert.h>
#include <errno.h>
#include <stddef.h>
#include <sys/cdefs.h>
#include <sys/shm.h>
#include <stdlib.h>
#include <string.h>
#include <stdalign.h>

struct shmSeg
{
    void* ptr;
    int shmid;
    int size;
    struct shmSeg *next;
};

//dummy element as head
static struct shmSeg shmList;

static void register_shmSeg(struct shmSeg* new)
{
    new->next = shmList.next;
    shmList.next = new;
}

static int get_shmid_and_destroy(void *ptr, int *shmid)
{
    if(shmList.next == NULL)
        return SHMEM_SEGMENT_NOT_FOUND;

    struct shmSeg *previous = NULL;
    struct shmSeg *current = &shmList;
    while(current->next != NULL)
    {
        previous = current -> next;
        if(ptr == previous->ptr)
        {
            
            *shmid = previous->shmid;
            current -> next = previous -> next; 
            free(previous);
            return SHMEM_SUCCESS;
        }
        current = current->next;
    }
    return SHMEM_SEGMENT_NOT_FOUND;
}

void deleteAllocatedSegments(){

    struct shmSeg *l = shmList.next;
    while(l != NULL){
        shmdt(l->ptr);
        shmctl(l->shmid, IPC_RMID, 0);
        struct shmSeg* tmp = l;
        l = l->next;
        free(tmp);
    }
    shmList.next = NULL;
}

void shmem_free(void* ptr)
{
    void* start = ((char*) ptr) - PAD(HEADER_SIZE, HEADER_PAD);
    int shmid;
    if(get_shmid_and_destroy(start, &shmid) != SHMEM_SUCCESS)
    {
        laik_panic("def_shmem_free couldn't find the given shared memory segment");
        assert(0);
    }
    
    if (shmdt(start) == -1)
        laik_panic("def_shmem_free couldn't detach from the given pointer");

    if (shmctl(shmid, IPC_RMID, 0) == -1)
        laik_panic("def_shmem_free couldn't destroy the shared memory segment");
}

int get_shmid(void *ptr, int *shmid, int *offset)
{
    for(struct shmSeg *l = shmList.next; l != NULL; l = l->next)
    {
        int diff = (int) (ptr - l->ptr);
        if(diff >= 0 && diff < l->size)
        {
            *offset = diff;
            *shmid = l->shmid;
            return SHMEM_SUCCESS;
        }
    }
    return SHMEM_SEGMENT_NOT_FOUND;
}

void* shmem_alloc(size_t size, int* shimdPtr)
{
    size_t header_size = PAD(HEADER_SIZE, HEADER_PAD);
    size_t alloc_size = size + header_size;
    int shmid = shmget(IPC_PRIVATE, alloc_size, 0644 | IPC_CREAT | IPC_EXCL);
    if (shmid == -1)
    {   
        laik_panic(strerror(errno));
        return NULL;
    }
    
    // Attach to the segment to get a pointer to it.
    struct shmHeader *ptr = shmat(shmid, NULL, 0);
    if (ptr == (void *)-1)
    {
        laik_panic("def_shmem_malloc couldn't attach to the shared memory segment");
        return NULL;
    }

    struct shmSeg* seg = malloc(sizeof(struct shmSeg));
    seg -> ptr = ptr;
    seg -> size = alloc_size;
    seg -> shmid = shmid;
    register_shmSeg(seg);

    *shimdPtr = shmid;

    ptr -> shmid = shmid;
    ptr -> size = alloc_size;

    return ((char*)ptr) + header_size;
}




