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


