
#include "laik.h"
#include "shmem.h"

#include <sys/shm.h>
#include <stdlib.h>
#include <string.h>

struct shmList
{
    void *ptr;
    int shmid;
    int size;
    struct shmList *next;
};

struct shmList *head;
struct shmList *tail;

static void register_shmSeg(void *ptr, int shmid, int size)
{
    struct shmList *new = malloc(sizeof(struct shmList));
    new->ptr = ptr;
    new->shmid = shmid;
    new->size = size;
    new->next = NULL;

    if (head == NULL)
    {
        head = new;
        tail = new;
        return;
    }
    head->next = new;
    head = new;
}

static int get_shmid_and_destroy(void *ptr, int *shmid)
{
    if(tail == NULL)
        return SHMEM_SEGMENT_NOT_FOUND;

    struct shmList *previous = NULL;
    struct shmList *current = tail;

    while(current != NULL)
    {
        if(ptr == current->ptr)
        {
            *shmid = current->shmid;
            if(previous == NULL)
            {
                tail = current->next;
            }
            else
            {
                previous->next = current->next;
            }

            if(current->next == NULL){
                head = previous;
            }
            free(current);
            return SHMEM_SUCCESS;
        }
        previous = current;
        current = current->next;
    }
    return SHMEM_SEGMENT_NOT_FOUND;
}


void* def_shmem_malloc(Laik_Data* d, size_t size){
    
    (void) d; // not used in this implementation of interface
    int shmid = shmget(IPC_PRIVATE, size, 0644 | IPC_CREAT | IPC_EXCL);
    if (shmid == -1)
    {   
        laik_panic("def_shmem_malloc couldn't create the shared memory segment: shmid == -1");
        return NULL;
    }
    
    // Attach to the segment to get a pointer to it.
    void *ptr = shmat(shmid, NULL, 0);
    if (ptr == (void *)-1)
    {
        laik_panic("def_shmem_malloc couldn't attach to the shared memory segment");
        return NULL;
    }

    memset(ptr, 0, size);

    register_shmSeg(ptr, shmid, size);
    return ptr;
}

void deleteAllocatedSegments(void){
    struct shmList *l = tail;
    while(l != NULL){
        shmdt(l->ptr);
        shmctl(l->shmid, IPC_RMID, 0);
        l = l->next;
    }
}

void def_shmem_free(Laik_Data* d, void* ptr){
    (void) d; // not used in this implementation of interface

    int shmid;
    if(get_shmid_and_destroy(ptr, &shmid) != SHMEM_SUCCESS)
        laik_panic("def_shmem_free couldn't find the given shared memory segment");

    if (shmdt(ptr) == -1)
        laik_panic("def_shmem_free couldn't detach from the given pointer");

    if (shmctl(shmid, IPC_RMID, 0) == -1)
        laik_panic("def_shmem_free couldn't destroy the shared memory segment");
    
}

int get_shmid(void *ptr, int *shmid, int *offset)
{
    for(struct shmList *l = tail; l != NULL; l = l->next)
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

