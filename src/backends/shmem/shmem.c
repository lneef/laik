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
#include "laik/backend.h"
#include "laik/core.h"
#include "shmem-allocator.h"
#include "laik-internal.h"

#include <assert.h>
#include <complex.h>
#include <errno.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <sys/cdefs.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <time.h>
#include <signal.h>
#include <stdatomic.h>
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

Laik_Shmem_Comm* shmem_comm(Laik_Inst_Data* idata, Laik_Group* g)
{
    return g->backend_data[idata->index];
}

int shmem_2cpy_send(void *buffer, int count, int datatype, int recipient)
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

int shmem_1cpy_send(void *buffer, int count, int datatype, int recipient)
{
    (void) datatype;

    int bufShmid, offset;
    if(get_shmid(buffer, &bufShmid, &offset) == SHMEM_SEGMENT_NOT_FOUND){
        return shmem_2cpy_send(buffer, count, datatype, recipient);
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

int shmem_recv(void *buffer, int count, int datatype, int sender, int *received,  Laik_Inst_Data* idata, Laik_Group* g)
{
    int shmid;
    Laik_Shmem_Comm* sg = shmem_comm(idata, g);
    RECV_INTS(&shmid, 1, sg->primaryRanks[sender], idata, g);
    
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

int shmem_sendMap(Laik_Mapping* map, int receiver,  Laik_Inst_Data* idata, Laik_Group* g)
{
    Laik_Shmem_Comm* sg = shmem_comm(idata, g);
    SEND_INTS(&headerShmid, 1, sg->primaryRanks[receiver], idata, g);
 
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

int shmem_PackSend(Laik_Mapping* map, Laik_Range range, int count, int receiver,  Laik_Inst_Data* idata, Laik_Group* g)
{   
    shmem_cpybuf_alloc(&cpyBuf, count * map->data->elemsize);
    Laik_Shmem_Comm* sg = shmem_comm(idata, g);
    SEND_INTS(&headerShmid, 1, sg->primaryRanks[receiver], idata, g);
    
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

int shmem_recvMap(Laik_Mapping* map, Laik_Range* range, int count, int sender,  Laik_Inst_Data* idata, Laik_Group* g)
{
    int shmid, ret = SHMEM_FAILURE;
    Laik_Shmem_Comm* sg = shmem_comm(idata, g);
    RECV_INTS(&shmid, 1, sg->primaryRanks[sender], idata, g);

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

int shmem_RecvUnpack(Laik_Mapping *map, Laik_Range *range, int count, int sender,  Laik_Inst_Data* idata, Laik_Group* g)
{
    int shmid;
    Laik_Shmem_Comm* sg = shmem_comm(idata, g);
    RECV_INTS(&shmid, 1, sg->primaryRanks[sender], idata, g);

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

int shmem_RecvReduce(char* buf, int count, int sender, Laik_Type* type, Laik_ReductionOperation redOp,  Laik_Inst_Data* idata, Laik_Group* g)
{
    int shmid;
    Laik_Shmem_Comm* sg = shmem_comm(idata, g);
    RECV_INTS(&shmid, 1, sg->primaryRanks[sender], idata, g);

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

int shmem_send(void* buffer, int count, int datatype, int receiver,  Laik_Inst_Data* idata, Laik_Group* g)
{
    Laik_Shmem_Comm* sg = shmem_comm(idata, g);
    SEND_INTS(&headerShmid, 1, sg->primaryRanks[receiver], idata, g);
    return sg->send(buffer, count, datatype, receiver);
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
int shmem_secondary_init(Laik_Shmem_Comm* sg, Laik_Inst_Data* idata, Laik_Group* world, int primaryRank, int primarySize, int* locations, int** newLocations,
                        int** ranks)
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
    int shmAddr = SHM_KEY; //+ pair(mask, location / perIsland);

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
        sg->secondaryRanks = malloc(primarySize * sizeof(int));
        int* tmpColours = malloc(primarySize * sizeof(int));

        memset(tmpColours, -1, primarySize * sizeof(int));

        int newColour = 0;
        int groupSizes[primarySize];
        memset(groupSizes, 0, primarySize * sizeof(int));

        // rank 0 is always rank 0
        sg->rank = 0;
        sg->secondaryRanks[0] = 0;
        sg->divsion[0] = 0;

        // get colour and mark rank 0 as processed

        int num_islands = 1;
        groupSizes[0] = 1;
        tmpColours[sg->colour] = newColour++;
        sg->colour = tmpColours[sg->colour];


        for (int i = 1; i < primarySize; i++)
        {   
            int colour;
            RECV_INTS(&colour, 1, i, idata, world);

            if(tmpColours[colour] == -1)
                tmpColours[colour] = newColour++;

            SEND_INTS(&tmpColours[colour], 1, i, idata, world);

            // get division and new rank
            sg->divsion[i] = tmpColours[colour];
            if(groupSizes[sg->divsion[i]] == 0) num_islands++;
            int new_rank = groupSizes[sg->divsion[i]]++;
            
            // if we reached <perIsland> ranks <colour> if mappes to a higher number
            if(groupSizes[sg->divsion[i]] == perIsland) tmpColours[colour] = newColour++;

            sg->secondaryRanks[i] = new_rank;

            SEND_INTS(&new_rank, 1, i, idata, world);

        }

        free(tmpColours);

        sg->numIslands = num_islands;

        for (int i = 1; i < primarySize; i++)
        {
            SEND_INTS(&groupSizes[sg->divsion[i]], 1, i, idata, world);
            SEND_INTS(sg->divsion, primarySize, i, idata, world);
            SEND_INTS(sg->secondaryRanks, primarySize, i, idata, world);
            SEND_INTS(&num_islands, 1, i, idata, world);
        }
        sg->size = groupSizes[sg->divsion[0]];
    }
    else
    {
        SEND_INTS(&sg->colour, 1, 0, idata, world);

        RECV_INTS(&sg->colour, 1, 0, idata, world);

        RECV_INTS(&sg->rank, 1, 0, idata, world);
        RECV_INTS(&sg->size, 1, 0, idata, world);
        sg->divsion = malloc(primarySize * sizeof(int));
        RECV_INTS(sg->divsion, primarySize, 0, idata, world);
        sg->secondaryRanks = malloc(primarySize * sizeof(int));
        RECV_INTS(sg->secondaryRanks, primarySize, 0, idata, world);

        RECV_INTS(&sg->numIslands, 1, 0, idata, world);
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

        int count = laik_aseq_groupCount(as , subgroup, 0);

        for(int i = 0; i < count; ++i)
        {
            int inTask = laik_aseq_taskInGroup(as,  subgroup, i, 0);

            if(!processed[sg->divsion[inTask]])
            {
                laik_aseq_updateTask(as, subgroup, last, inTask, 0);
                ++last;
                processed[sg->divsion[inTask]] = true;
            }

            if(sg->colour == sg->divsion[inTask])
            {
                tmp[ii++] = sg->secondaryRanks[inTask];
            }
        }

        laik_aseq_updateGroupCount(as, subgroup, last, 0);

        laik_aseq_addSecondaryGroup(as, subgroup, tmp, ii, chain_idx);
    }

    free(tmp);
    free(processed);
}

bool onSameIsland(Laik_ActionSeq* as, Laik_Shmem_Comm* sg, int inputgroup, int outputgroup)
{
    int inCount = laik_aseq_groupCount(as, inputgroup,  0);
    int outCount = laik_aseq_groupCount(as, outputgroup,  0);

    if(inCount != 1 || outCount != 1)   return false;

    int rankI = laik_aseq_taskInGroup(as, inputgroup, 0,  0);
    int rankO = laik_aseq_taskInGroup(as, outputgroup, 0, 0);

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
