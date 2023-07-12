/*
 * This file is part of the LAIK library.
 * Copyright (c) 2022 Robert Hubinger <robert.hubinger@tum.de>
 * Copyright (c) 2023 Lukas Neef <lukas.neef@tum.de>
 *
 * LAIK is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, version 3 or later.
 *
 * LAIK is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */


#include "shmem.h"
#include "backends/shmem/shmem-cpybuf.h"
#include "backends/shmem/shmem-manager.h"
#include "laik.h"
#include "laik/core.h"
#include "laik/data.h"
#include "laik/space.h"
#include "shmem-allocator.h"
#include "laik-internal.h"

#include <assert.h>
#include <errno.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <sys/cdefs.h>
#include <sys/shm.h>
#include <time.h>
#include <signal.h>
#include <stdatomic.h>
#include <laik.h>
#include <unistd.h>

#define COPY_BUF_SIZE 1024 * 1024
#define SHM_KEY 0x123
#define MAX_WAITTIME 1
#define ALLOC_OFFSET 0x333
#define META_OFFSET 0x666
#define BUF_OFFSET 0x999

struct shmInitSeg
{
    atomic_int rank;
    int colour;
    bool didInit;
};



static int pair(int colour, int rank)
{
    int tmp = (colour + rank) * (colour + rank + 1);
    return tmp/2 + colour + 1;
}

static struct cpyBuf cpyBuf;
static int headerShmid = -1;
static struct commHeader* shmp;

void deleteOpenShmSegs(__attribute_maybe_unused__ int sig)
{
    deleteAllocatedSegments();
}

static int createMetaInfoSeg()
{
    shmp = shmem_alloc(sizeof(struct commHeader), &headerShmid);
    assert(shmp != (void*)-1);
    shmp->receiver = -1;
    return SHMEM_SUCCESS;
}

int shmem_get_numIslands(Laik_Shmem_Comm* sg, int* num){
    *num = sg->numIslands;
    return SHMEM_SUCCESS;
}

int shmem_get_primarys(Laik_Shmem_Comm* sg, int** primarys){
    *primarys = sg->primaryRanks;
    return SHMEM_SUCCESS;
}

int shmem_get_colours(Laik_Shmem_Comm* sg, int **buf)
{
    *buf = sg->divsion;
    return SHMEM_SUCCESS;
}

int shmem_get_secondaryRanks(Laik_Shmem_Comm* sg, int **buf)
{
    *buf = sg->secondaryRanks;
    return SHMEM_SUCCESS;
}

int shmem_comm_size(Laik_Shmem_Comm* sg, int *sizePtr)
{
    *sizePtr = sg->size;
    return SHMEM_SUCCESS;
}

int shmem_comm_rank(Laik_Shmem_Comm* sg, int *rankPtr)
{
    *rankPtr = sg->rank;
    return SHMEM_SUCCESS;
}

int shmem_comm_colour(Laik_Shmem_Comm* sg, int *colourPtr)
{
    *colourPtr = sg->colour;
    return SHMEM_SUCCESS;
}

int shmem_2cpy_send(void *buffer, int count, int datatype, int recipient, Laik_Shmem_Comm* sg)
{
    int size = datatype * count;
    shmem_cpybuf_alloc(&cpyBuf, size);

    shmp->count = count;
    shmp->spec = PACK;
    shmp->shmid = cpyBuf.shmid;
    memcpy(cpyBuf.ptr, buffer, size);
    shmp->receiver = recipient;
    while (shmp->receiver != -1)
    {
    }

    return SHMEM_SUCCESS;
}

int shmem_1cpy_send(void *buffer, int count, int datatype, int recipient, Laik_Shmem_Comm* sg)
{
    (void) datatype;

    int bufShmid, offset;
    if(get_shmid(buffer, &bufShmid, &offset) == SHMEM_SEGMENT_NOT_FOUND){
        return shmem_2cpy_send(buffer, count, datatype, recipient, sg);
    }

    shmp->count = count;
    shmp->spec = PACK;
    shmp->shmid = bufShmid;
    shmp->receiver = recipient;
    while (shmp->receiver != -1)
    {
    }

    return SHMEM_SUCCESS;
}

int shmem_recv(void *buffer, int count, int datatype, int sender, int *received, Laik_Shmem_Comm* sg, void* backend_data)
{
    int shmid;
    laik_log(2, "%d", sg->primaryRanks[sender]);
    sg->recvP(&shmid, 1, sg->primaryRanks[sender], backend_data);
    
    // Attach to the segment to get a pointer to it.
    struct commHeader *shmp = shmat(shmid, NULL, 0);
    if (shmp == (void *)-1)
        return SHMEM_SHMAT_FAILED;

    while (shmp->receiver != sg->rank)
    {
    }

    *received = shmp->count;
    int bufShmid = shmp->shmid;

    char *bufShmp = shmat(bufShmid, NULL, 0);
    if (bufShmp == (void *)-1)
        return SHMEM_SHMAT_FAILED;
    int bufSize = datatype * count;
    int receivedSize = *received * datatype;
    if (bufSize < receivedSize)
    {
        memcpy(buffer, bufShmp, bufSize);
        return SHMEM_RECV_BUFFER_TOO_SMALL;
    }
    else
    {
        memcpy(buffer, bufShmp , receivedSize);
    }

    if (shmdt(bufShmp) == -1)
        return SHMEM_SHMDT_FAILED;

    shmp->receiver = -1;
    if (shmdt(shmp) == -1)
        return SHMEM_SHMDT_FAILED;

    return SHMEM_SUCCESS;
}

int shmem_sendMap(Laik_Mapping* map, int receiver, Laik_Shmem_Comm* sg, void* backend_data)
{
    sg->sendP(&headerShmid, 1, sg->primaryRanks[receiver], backend_data);
    int shmid, offset;
    get_shmid(map->start, &shmid, &offset);

    shmp->spec = MAP;
    shmp->shmid = shmid;
    shmp->range = map->allocatedRange;
    shmp->receiver = receiver;
    while(shmp->receiver != -1)
    {
    }
    return SHMEM_SUCCESS;   
}

int shmem_PackSend(Laik_Mapping* map, Laik_Range range, int count, int receiver, Laik_Shmem_Comm* sg, void* backend_data)
{   
    shmem_cpybuf_alloc(&cpyBuf, count * map->data->elemsize);
    sg->sendP(&headerShmid, 1, sg->primaryRanks[receiver], backend_data);
    
    //pack makes changes to range
    shmp->shmid = cpyBuf.shmid;
    shmp->spec = PACK;
    map->layout->pack(map, &range, &range.from, cpyBuf.ptr, count * map->data->elemsize);
    shmp->receiver = receiver;

    while(shmp->receiver != -1)
    {
    }

    return SHMEM_SUCCESS;
}

static inline int shmem_cpyPack(Laik_Mapping* map, Laik_Range* range, struct commHeader* shmp, char* ptr, int count)
{
    map->layout->unpack(map, range, &range->from, ptr, count * map->data->elemsize);
    shmp->receiver = -1;
    return SHMEM_SUCCESS;
}


static inline int shmem_cpyMap(Laik_Mapping* map, Laik_Range* range, struct commHeader* shmp, char* ptr)
{
    Laik_Mapping tmp;
    Laik_Range* rangeS;

    Laik_Layout_Memory_Header* lh = (Laik_Layout_Memory_Header*) ptr;
    tmp.layoutSection = 0;
    tmp.data = map->data;

    //init layout
    rangeS = &shmp->range;
    rangeS->space = range->space;
    tmp.layout = laik_layout_get(range->space->inst->layouts, rangeS, lh);
    tmp.start = tmp.base = ptr + lh->size;
    tmp.data = map->data;
    laik_data_copy(range, &tmp, map);

    shmp->receiver = -1;

    free(tmp.layout);
    return SHMEM_SUCCESS;
}

int shmem_recvMap(Laik_Mapping* map, Laik_Range* range, int count, int sender, Laik_Shmem_Comm* sg, void* backend_data)
{
    int shmid, ret = SHMEM_FAILURE;
    sg->recvP(&shmid, 1, sg->primaryRanks[sender], backend_data);
    struct commHeader* shmp = shmat(shmid, NULL, 0);
    assert(shmp != (void*) -1);
    while(shmp->receiver != sg->rank)
    {
    }
    char* ptr = shmat(shmp->shmid, NULL, 0);
    assert(ptr != (void*) -1);
    if(shmp->spec == PACK)
    {
        ret = shmem_cpyPack(map, range, shmp, ptr, count);
    }
    else if(shmp->spec == MAP)
    {
        ret = shmem_cpyMap(map, range, shmp, ptr);
    }

    shmdt(shmp);
    shmdt(ptr);

    return ret;
}

int shmem_RecvUnpack(Laik_Mapping *map, Laik_Range *range, int count, int sender, Laik_Shmem_Comm* sg, void* backend_data)
{
    int shmid;
    sg->recvP(&shmid, 1, sg->primaryRanks[sender], backend_data);
    struct commHeader* shmp = shmat(shmid, NULL, 0);
    assert(shmp != (void*) -1);
    while(shmp->receiver != sg->rank)
    {
    }
    char* ptr = shmat(shmp->shmid, NULL, 0);
    assert(ptr != (void*) -1);
    assert(shmp->spec == PACK);
    int ret = shmem_cpyPack(map, range, shmp, ptr, count);

    shmdt(shmp);
    shmdt(ptr);

    return ret;
}

int shmem_RecvReduce(char* buf, int count, int sender, Laik_Type* type, Laik_ReductionOperation redOp, Laik_Shmem_Comm* sg, void* backend_data)
{
    int shmid;
    sg->recvP(&shmid, 1, sg->primaryRanks[sender], backend_data);
    struct commHeader* shmp = shmat(shmid, NULL, 0);
    assert(shmp != (void*) -1);
    while(shmp->receiver != sg->rank)
    {
    } 
    assert(shmp->spec == PACK);
    char* ptr = shmat(shmp->shmid, NULL, 0);
    assert(ptr != (void*) -1);
    assert(type->reduce);
    type->reduce(buf, buf, ptr, count, redOp);

    shmp->receiver = -1;
    shmdt(shmp);
    shmdt(ptr);
    return SHMEM_SUCCESS;

}

int shmem_send(void* buffer, int count, int datatype, int recipient, Laik_Shmem_Comm* sg, void* backend_data)
{
    sg->sendP(&headerShmid, 1, sg->primaryRanks[recipient], backend_data);
    return sg->send(buffer, count, datatype, recipient, sg);
}

int shmem_error_string(int error, char *str)
{
    switch (error)
    {
    case SHMEM_SUCCESS:
        strcpy(str, "not an error: shmem success");
        break;
    case SHMEM_SHMGET_FAILED:
        strcpy(str, "shmget failed");
        break;
    case SHMEM_SHMAT_FAILED:
        strcpy(str, "shmat failed");
        break;
    case SHMEM_SHMDT_FAILED:
        strcpy(str, "shmdt failed");
        break;
    case SHMEM_SHMCTL_FAILED:
        strcpy(str, "shmctl failed");
        break;
    case SHMEM_RECV_BUFFER_TOO_SMALL:
        strcpy(str, "recv was given a too small buffer");
        break;
    case SHMEM_SEGMENT_NOT_FOUND:
        strcpy(str, "get_shmid couldn't find a segment the given pointer points at");
        break;
    default:
        strcpy(str, "error unknown to shmem");
        return SHMEM_FAILURE;
    }
    return SHMEM_SUCCESS;
}

int shmem_finalize()
{
    cleanupBuffer();
    deleteOpenShmSegs(0);

    /*
    if(sg->cpyBuf)
        free(sg->cpyBuf);

    
    if (sg->info.divsion != NULL)
        free(sg->info.divsion);
    
    if (sg->info.secondaryRanks != NULL)
        free(sg->info.secondaryRanks);
    
    if(sg->primaryRanks != NULL)
        free(sg->primaryRanks);
    */

    return SHMEM_SUCCESS;
}

static char* saveptr;
int shmem_secondary_init(Laik_Shmem_Comm* sg, int primaryRank, int primarySize, int* locations, int** newLocations,
                        int** ranks, int (*send)(int *, int, int, void*), int (*recv)(int *, int, int, void*), void* backend_data)
{
    signal(SIGINT, deleteOpenShmSegs);
    char* token;
    if(!saveptr)
    {
        char *envRanks = getenv("LAIK_SHMEM_RANKS_PER_ISLANDS");
        token = envRanks == NULL ? NULL : strtok_r(envRanks, ",", &saveptr);
    }
    else{
        token = strtok_r(saveptr, ",", &saveptr);
        
    }

    int perIsland = token ? atoi(token) : 1;
    int mask = locations ? locations[primaryRank] : 0;
    int *sRanks = *ranks;

    if(perIsland < 1)
        laik_panic("LAIK_SHMEM_RANKS_PER_ISLANDS needs to be a number larger than 0");

    int location = sRanks[primaryRank];
    int shmAddr = SHM_KEY + pair(mask, location / perIsland);

    const char *copyScheme = getenv("LAIK_SHMEM_COPY_SCHEME");

    if(copyScheme != 0 && !strcmp(copyScheme, "2"))
    {
        sg->send = shmem_2cpy_send;
    }
    else if(copyScheme == 0 || !strcmp(copyScheme, "1"))
    {
        sg->send = shmem_1cpy_send;
    }
    else 
    {
        laik_panic("Please provide a correct copy scheme: 1 or 2");
    }

    sg->sendP = *send;
    sg->recvP = *recv;

    bool created = false;
    struct shmInitSeg *shmp;
    int shmid = shmget(shmAddr, sizeof(struct shmInitSeg), IPC_EXCL | 0644 | IPC_CREAT);
    if (shmid == -1)
    {
        // Client initialization
        time_t t_0;

        // As long as it fails and three seconds haven't passed try again (wait for master)
        t_0 = time(NULL);
        while (time(NULL) - t_0 < MAX_WAITTIME && shmid == -1)
        {
            shmid = shmget(shmAddr, 0, IPC_CREAT | 0644);
        }
        if (shmid == -1)
            return SHMEM_SHMGET_FAILED;

        // Attach to the segment to get a pointer to it.
        shmp = shmat(shmid, NULL, 0);
        if (shmp == (void *)-1)
            return SHMEM_SHMAT_FAILED;

        while (!shmp->didInit)
        {
        }
        sg->colour = shmp->colour;

        if (shmdt(shmp) == -1)
            return SHMEM_SHMDT_FAILED;
    }
    else
    {
        created = true;
        // Master initialization
        sg->colour = primaryRank;

        shmp = shmat(shmid, NULL, 0);
        if (shmp == (void *)-1)
            return SHMEM_SHMAT_FAILED;

        shmp->colour = primaryRank;
        shmp->didInit = true;

        if (shmdt(shmp) == -1)
            return SHMEM_SHMDT_FAILED;
    }
    

    // Get the colours of each process at master, calculate the groups and send each process their group.
    if (primaryRank == 0)
    {
        sg->divsion = malloc(primarySize * sizeof(int));
        sg->divsion[0] = 0;
        sg->secondaryRanks = malloc(primarySize * sizeof(int));
        sg->secondaryRanks[0] = 0;

        int* tmpColours = malloc(primarySize * sizeof(int));

        memset(tmpColours, -1, primarySize * sizeof(int));

        int newColour = 0;
        tmpColours[sg->colour] = newColour++;
        sg->colour = 0;

        for (int i = 1; i < primarySize; i++)
        {   
            int colour;
            (*recv)(&colour, 1, i, backend_data);

            if(tmpColours[colour] == -1)
                tmpColours[colour] = newColour++;

            (*send)(&tmpColours[colour], 1, i, backend_data);

            sg->divsion[i] = tmpColours[colour];

        }

        free(tmpColours);

        int num_islands = 0;
        int groupSizes[primarySize];
        memset(groupSizes, 0, primarySize * sizeof(int));


        for (int i = 0; i < primarySize; i++)
        {   
            if(groupSizes[sg->divsion[i]] == 0)
            {
                num_islands++; 
            }
            
            int sec_rank = groupSizes[sg->divsion[i]]++;

            if(i == 0){
                sg->rank = sec_rank;
            }else{
                (*send)(&sec_rank, 1, i, backend_data);
            }

            sg->secondaryRanks[i] = sec_rank;
        }
        sg->numIslands = num_islands;

        for (int i = 1; i < primarySize; i++)
        {
            (*send)(&groupSizes[sg->divsion[i]], 1, i, backend_data);
            (*send)(sg->divsion, primarySize, i, backend_data);
            (*send)(sg->secondaryRanks, primarySize, i, backend_data);
            (*send)(&num_islands, 1, i, backend_data);
        }
        sg->size = groupSizes[sg->divsion[0]];
    }
    else
    {
        (*send)(&sg->colour, 1, 0, backend_data);

        (*recv)(&sg->colour, 1, 0, backend_data);

        (*recv)(&sg->rank, 1, 0, backend_data);
        (*recv)(&sg->size, 1, 0, backend_data);
        sg->divsion = malloc(primarySize * sizeof(int));
        (*recv)(sg->divsion, primarySize, 0, backend_data);
        sg->secondaryRanks = malloc(primarySize * sizeof(int));
        (*recv)(sg->secondaryRanks, primarySize, 0, backend_data);

        (*recv)(&sg->numIslands, 1, 0, backend_data);
    }
    sg->primaryRanks = malloc(sg->size * sizeof(int));

    int ii = 0;
    for(int i = 0; i < primarySize && ii < sg->size; ++i)
    {
        if(sg->divsion[i] == sg->colour){
            sg->primaryRanks[ii++] = i; 
        } 
    }

    if (created && shmctl(shmid, IPC_RMID, 0) == -1)
        return SHMEM_SHMCTL_FAILED;
    
    // Open the own meta info shm segment and set it to ready
    if(headerShmid == -1) createMetaInfoSeg();

    laik_log(1, "Shared Memory Backend: T%d is T%d on Island:%d", primaryRank, sg->rank, sg->colour);
    *newLocations = sg->divsion;
    *ranks = sg->secondaryRanks;
    return SHMEM_SUCCESS;
}


void shmem_transformSubGroup(Laik_ActionSeq* as, Laik_Shmem_Comm* sg, int chain_idx){

    bool* processed = malloc(sg->numIslands * sizeof(bool));
    int* tmp = malloc(sg->size * sizeof(int)); 
    for(int subgroup = 0; subgroup < as->subgroupCount; ++subgroup)
    {
        int last = 0;
        int ii = 0;
        memset(processed, false, sg->numIslands * sizeof(bool));

        int count = laik_aseq_groupCount(as , subgroup, -1);

        for(int i = 0; i < count; ++i)
        {
            int inTask = laik_aseq_taskInGroup(as,  subgroup, i, -1);

            if(!processed[sg->divsion[inTask]])
            {
                laik_aseq_updateTask(as, subgroup, last, inTask, -1);
                ++last;
                processed[sg->divsion[inTask]] = true;
            }

            if(sg->colour == sg->divsion[inTask])
            {
                tmp[ii++] = sg->secondaryRanks[inTask];
            }
        }

        laik_aseq_updateGroupCount(as, subgroup, last, -1);

        laik_aseq_addSecondaryGroup(as, subgroup, tmp, ii, chain_idx);
    }

    free(tmp);
    free(processed);
}

bool onSameIsland(Laik_ActionSeq* as, Laik_Shmem_Comm* sg, int inputgroup, int outputgroup)
{
    int inCount = laik_aseq_groupCount(as, inputgroup,  - 1);
    int outCount = laik_aseq_groupCount(as, outputgroup,  - 1);

    if(inCount != 1 || outCount != 1)   return false;

    int rankI = laik_aseq_taskInGroup(as, inputgroup, 0,  - 1);
    int rankO = laik_aseq_taskInGroup(as, outputgroup, 0, - 1);

    return sg->divsion[rankI] == sg->divsion[rankO];
}

void createBuffer(size_t size){
    if(size < 1) return;
    shmem_cpybuf_alloc(&cpyBuf, COPY_BUF_SIZE);
}

void cleanupBuffer()
{
    if(cpyBuf.ptr) shmem_cpybuf_delete(&cpyBuf);
}
