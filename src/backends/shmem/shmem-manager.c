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

#include <assert.h>
#include <netinet/in.h>
#include <stdalign.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>

#include "backends/shmem/shmem.h"
#include "laik.h"
#include "shmem-allocator.h"
#include "shmem-manager.h"
#include <sys/shm.h>
#include <stdatomic.h>

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
        shmem_free(c->ptr);
        struct shmemPair* tmp = c;
        c = c -> next;
        free(tmp);
    }

    head.next = NULL;

}

void def_shmem_free(Laik_Data* d, Laik_Mapping* map){
    (void) d; // not used in this implementation of interface
    // free for zero copy
    size_t header_size = PAD(HEADER_SIZE, HEADER_PAD);
    struct shmHeader* sh = (struct shmHeader*) (map->header - header_size);
    if(atomic_load(&sh->z)){
        shmem_free_zero_copy(sh);
    }else {
        shmem_free(map->header);
    }
    
}
static int pair(int colour, int rank)
{
    int tmp = (colour + rank) * (colour + rank + 1);
    return tmp/2 + colour + 1;
}


#define KEY_OFFSET 0xFFFF
static int current = 0;
void* def_shmem_malloc(Laik_Data* d, Laik_Layout* ll, Laik_Range* range, Laik_Partitioning* par){
    int shmid;
    Laik_Inst_Data* idata = d->backend_data;
    // if zero copy not possible, dont allow it
    size_t size = ll->header_size;
    Laik_Shmem_Data* sd = idata->backend_data;
    if(sd->copyScheme != 0)
    {
        size += laik_range_size(range) * d->elemsize;
        return shmem_alloc(size, &shmid);
    }
    Laik_RangeList* rl = laik_partitioning_allranges(par);
    assert(rl);
    //empty range
    bool didinit = false;
    Laik_Range alloc_range = {
        .space = d->space,
        .from = { {0} },
        .to = { {0} }
    };

    Laik_Group* g = par->group;
    
    Laik_Shmem_Comm* sg = g->backend_data[idata->index];

    // only works for one secondary backend directly below the primary
    for(unsigned i = 0; i < rl->count; ++i)
    {
        if(sg->libLocations[rl->trange[i].task] == 0) continue;
        if(!didinit)
        {
            alloc_range = rl->trange[i].range;
            didinit = true;
        }
        else
        {
            laik_range_expand(&alloc_range, &rl->trange[i].range);
        }
    }

    laik_log(1, "Range to be allocated:");
    laik_log_Range(&alloc_range);
    laik_log_flush(0);

    if(laik_range_isEqual(range, &alloc_range))
    {
        size += laik_range_size(range) * d->elemsize;
        return shmem_alloc(size, &shmid);
    }
    size += laik_range_size(&alloc_range) * d ->elemsize;
    *range = alloc_range;
    int mask = pair(sg->zcloc, current++);
    void* ptr = shmem_key_alloc(KEY_OFFSET + mask, size, &shmid);
    return ptr;
}

bool allow_reuse(Laik_Data* data, Laik_Partitioning* toP, Laik_Mapping* m)
{
    Laik_Inst_Data* idata = data->backend_data;
    Laik_Shmem_Data* sd = idata->backend_data;
    if(sd->copyScheme > 0) return true;
    Laik_Shmem_Comm* sg = toP->group->backend_data[idata->index];

    Laik_RangeList* rl = laik_partitioning_allranges(toP);

    bool allow = true;
    for(unsigned i = 0 ; i < rl->count; ++i)
    {
        if(sg->libLocations[rl->trange[i].task] == 0) continue;
        allow &= laik_range_within_range(&rl->trange[i].range, &m->allocatedRange);

    }


    return allow;
}

Laik_Allocator* shmem_allocator()
{
    laik_log(1, "Allocator of shmem backend chosen for laik");
    Laik_Allocator* shmemAllocator = laik_new_allocator(def_shmem_malloc, def_shmem_free, 0, allow_reuse);
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

bool shmem_manager_zeroCopy(char* ptr)
{
    size_t header_size = PAD(HEADER_SIZE, HEADER_PAD);
    struct shmHeader* hd = (struct shmHeader*) (ptr - header_size);
    bool zC = atomic_load(&hd->z);
    return zC;
}


