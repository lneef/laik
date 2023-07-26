/* This file is part of the LAIK parallel container library.
 * Copyright (c) 2023 Lukas Neef <lukas.neef@tum.de>
 *
 * LAIK is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, version 3.
 *
 * LAIK is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include <stdalign.h>
#include <stddef.h>
#include <stdlib.h>

#include "backends/shmem/shmem.h"
#include "laik.h"
#include "laik/core.h"
#include "laik/data.h"
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
    void* ptr = shmem_alloc(size, &shmid);
    return ptr;
}

Laik_Allocator* shmem_allocator()
{
    laik_log(1, "Allocator of shmem backend chosen for laik");
    Laik_Allocator* shmemAllocator = laik_new_allocator(def_shmem_malloc, def_shmem_free, 0);
    shmemAllocator->policy = LAIK_MP_NewAllocOnRepartition;
    return shmemAllocator;
}

bool is_shmem_allocator(Laik_Allocator* allocator)
{
    return allocator!=NULL && allocator->malloc == def_shmem_malloc;
}

void* shmem_manager_attach(int shmid, int flag)
{   
    size_t header_size = PAD(HEADER_SIZE, HEADER_PAD);
    void* ptr = shmat(shmid, NULL, flag);
    if(ptr == (void*)-1)
        laik_log(LAIK_LL_Panic, "Shared memory manager could not attach segment with id %d", shmid);

    return ((char*)ptr) + header_size;
}

void shmem_manager_detach(void* ptr)
{
    size_t header_size = PAD(HEADER_SIZE, HEADER_PAD);
    void* start = ((char*)ptr) - header_size;
    if(shmdt(start) == -1)
        laik_log(LAIK_LL_Panic, "Could not detach shared memory segment %p", ptr);
}

int shmem_manager_shmid(char* ptr)
{
    size_t header_size = PAD(HEADER_SIZE, HEADER_PAD);
    struct shmHeader* hd = (struct shmHeader*) (ptr - header_size);
    return hd->shmid;
}


