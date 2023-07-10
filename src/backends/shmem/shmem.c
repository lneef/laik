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

typedef enum DataSpec{
    PACK, MAP
} DataSpec;

struct shmInitSeg
{
    atomic_int rank;
    int colour;
    bool didInit;
};

struct commHeader{
    DataSpec spec;
    int receiver;
    int shmid;
    int count;
    Laik_Range range;
};


static int pair(int colour, int rank)
{
    int tmp = (colour + rank) * (colour + rank + 1);
    return tmp/2 + colour + 1;
}

void deleteOpenShmSegs(Shmem_Secondary_Group* sg)
{
    if(sg->headerShmid != -1)
    {
        shmctl(sg->headerShmid, IPC_RMID, 0);
    }

    if(sg->cpyBuf != NULL)
        //shmem_cpybuf_delete(groupInfo.cpyBuf);

    deleteAllocatedSegments(0);
}

static int createMetaInfoSeg(void)
{
    int headerShmid = shmget(IPC_PRIVATE, sizeof(struct commHeader), 0644 | IPC_CREAT);
    assert(headerShmid >= 0);
    struct commHeader *shmpMap = shmat(headerShmid, NULL, 0);
    shmpMap->receiver = -1;

    if (shmdt(shmpMap) == -1)
        return -1;

    return headerShmid;
}

int shmem_get_numIslands(Shmem_Secondary_Group* sg, int* num){
    *num = sg->numIslands;
    return SHMEM_SUCCESS;
}

int shmem_get_primarys(Shmem_Secondary_Group* sg, int** primarys){
    *primarys = sg->primaryRanks;
    return SHMEM_SUCCESS;
}

int shmem_get_colours(Shmem_Secondary_Group* sg, int **buf)
{
    *buf = sg->info.divsion;
    return SHMEM_SUCCESS;
}

int shmem_get_secondaryRanks(Shmem_Secondary_Group* sg, int **buf)
{
    *buf = sg->info.secondaryRanks;
    return SHMEM_SUCCESS;
}

int shmem_comm_size(Shmem_Secondary_Group* sg, int *sizePtr)
{
    *sizePtr = sg->info.size;
    return SHMEM_SUCCESS;
}

int shmem_comm_rank(Shmem_Secondary_Group* sg, int *rankPtr)
{
    *rankPtr = sg->info.rank;
    return SHMEM_SUCCESS;
}

int shmem_comm_colour(Shmem_Secondary_Group* sg, int *colourPtr)
{
    *colourPtr = sg->info.colour;
    return SHMEM_SUCCESS;
}

int shmem_2cpy_send(void *buffer, int count, int datatype, int recipient, Shmem_Secondary_Group* sg)
{
    int size = datatype * count;
    shmem_cpybuf_alloc(sg->cpyBuf, size);

    struct commHeader *shmp = shmat(sg->headerShmid, NULL, 0);
    if (shmp == (void *)-1)
        return SHMEM_SHMAT_FAILED;

    shmp->count = count;
    shmp->spec = PACK;
    shmp->shmid = sg->cpyBuf->shmid;
    memcpy(sg->cpyBuf->ptr, buffer, size);
    shmp->receiver = recipient;
    while (shmp->receiver != -1)
    {
    }

    return SHMEM_SUCCESS;
}

int shmem_1cpy_send(void *buffer, int count, int datatype, int recipient, Shmem_Secondary_Group* sg)
{
    (void) datatype;

    int bufShmid, offset;
    if(get_shmid(buffer, &bufShmid, &offset) == SHMEM_SEGMENT_NOT_FOUND){
        return shmem_2cpy_send(buffer, count, datatype, recipient, sg);
    }

    // Attach to the segment to get a pointer to it.
    struct commHeader *shmp = shmat(sg->headerShmid, NULL, 0);
    if (shmp == (void *)-1)
        return SHMEM_SHMAT_FAILED;

    shmp->count = count;
    shmp->spec = PACK;
    shmp->shmid = bufShmid;
    shmp->receiver = recipient;
    while (shmp->receiver != -1)
    {
    }

    if (shmdt(shmp) == -1)
        return SHMEM_SHMDT_FAILED;

    return SHMEM_SUCCESS;
}

int shmem_recv(void *buffer, int count, int datatype, int sender, int *received, Shmem_Secondary_Group* sg)
{
    int shmid;
    sg->recvP(&shmid, 1, sg->primaryRanks[sender]);
    
    // Attach to the segment to get a pointer to it.
    struct commHeader *shmp = shmat(shmid, NULL, 0);
    if (shmp == (void *)-1)
        return SHMEM_SHMAT_FAILED;

    while (shmp->receiver != sg->info.rank)
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

int shmem_sendMap(Laik_Mapping* map, int receiver, Shmem_Secondary_Group* sg)
{
    sg->sendP(&sg->headerShmid, 1, sg->primaryRanks[receiver]);
    struct commHeader *shmp = shmat(sg->headerShmid, NULL, 0);
    int shmid, offset;
    get_shmid(map->start, &shmid, &offset);

    shmp->spec = MAP;
    shmp->shmid = shmid;
    shmp->range = map->allocatedRange;
    shmp->receiver = receiver;
    while(shmp->receiver != -1)
    {
    }

    shmdt(shmp);
    return SHMEM_SUCCESS;   
}

int shmem_PackSend(Laik_Mapping* map, Laik_Range range, int count, int receiver, Shmem_Secondary_Group* sg)
{   
    shmem_cpybuf_alloc(sg->cpyBuf, count * map->data->elemsize);
    sg->sendP(&sg->headerShmid, 1, sg->primaryRanks[receiver]);
    laik_log(2, "%d", sg->headerShmid);
    struct commHeader* shmp = shmat(sg->headerShmid, NULL, 0);
    
    //pack makes changes to range
    shmp->shmid = sg->cpyBuf->shmid;
    shmp->spec = PACK;
    map->layout->pack(map, &range, &range.from, sg->cpyBuf->ptr, count * map->data->elemsize);
    shmp->receiver = receiver;

    while(shmp->receiver != -1)
    {
    }

    shmdt(shmp);
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

int shmem_recvMap(Laik_Mapping* map, Laik_Range* range, int count, int sender, Shmem_Secondary_Group* sg)
{
    int shmid, ret = SHMEM_FAILURE;
    sg->recvP(&shmid, 1, sg->primaryRanks[sender]);
    struct commHeader* shmp = shmat(shmid, NULL, 0);
    while(shmp->receiver != sg->info.rank)
    {
    }
    char* ptr = shmat(shmp->shmid, NULL, 0);
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

int shmem_RecvUnpack(Laik_Mapping *map, Laik_Range *range, int count, int sender, Shmem_Secondary_Group* sg)
{
    int shmid;
    sg->recvP(&shmid, 1, sg->primaryRanks[sender]);
    struct commHeader* shmp = shmat(shmid, NULL, 0);
    while(shmp->receiver != sg->info.rank)
    {
    }
    char* ptr = shmat(shmp->shmid, NULL, 0);

    assert(shmp->spec == PACK);
    int ret = shmem_cpyPack(map, range, shmp, ptr, count);

    shmdt(shmp);
    shmdt(ptr);

    return ret;
}

int shmem_RecvReduce(char* buf, int count, int sender, Laik_Type* type, Laik_ReductionOperation redOp, Shmem_Secondary_Group* sg)
{
    int shmid;
    sg->recvP(&shmid, 1, sg->primaryRanks[sender]);
    laik_log(2, "%d", sg->primaryRanks[sender]);
    struct commHeader* shmp = shmat(shmid, NULL, 0);
    laik_log(2, "%d", shmid);
    while(shmp->receiver != sg->info.rank)
    {
    } 
    assert(shmp->spec == PACK);
    char* ptr = shmat(shmp->shmid, NULL, 0);
    assert(type->reduce);
    type->reduce(buf, buf, ptr, count, redOp);

    shmp->receiver = -1;
    shmdt(shmp);
    shmdt(ptr);
    return SHMEM_SUCCESS;

}

int shmem_send(void* buffer, int count, int datatype, int recipient, Shmem_Secondary_Group* sg)
{
    sg->sendP(&sg->headerShmid, 1, sg->primaryRanks[recipient]);
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

int shmem_finalize(Shmem_Secondary_Group* sg)
{
    if(sg->cpyBuf)
        free(sg->cpyBuf);

    deleteOpenShmSegs(sg);
    if (sg->info.divsion != NULL)
        free(sg->info.divsion);
    
    if (sg->info.secondaryRanks != NULL)
        free(sg->info.secondaryRanks);
    
    if(sg->primaryRanks != NULL)
        free(sg->primaryRanks);

    return SHMEM_SUCCESS;
}

int shmem_secondary_init(Shmem_Secondary_Group* sg, int primaryRank, int primarySize, int* locations, int** newLocations,
                        int** ranks, int (*send)(int *, int, int), int (*recv)(int *, int, int))
{
    signal(SIGINT, deleteAllocatedSegments);

    const char *envRanks = getenv("LAIK_SHMEM_SUB_ISLANDS");
    int numSubIslands = envRanks == NULL ? 1 : atoi(envRanks);
    int mask = locations ? locations[primaryRank] : 0;
    int* sRanks = *ranks;

    if(numSubIslands < 1)
        laik_panic("RANKS_PER_ISLAND needs to be a number larger than 0");

    int location = sRanks[primaryRank];
    int shmAddr = SHM_KEY + pair(mask, location % numSubIslands);

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
        sg->info.colour = shmp->colour;

        if (shmdt(shmp) == -1)
            return SHMEM_SHMDT_FAILED;
    }
    else
    {
        created = true;
        // Master initialization
        sg->info.colour = primaryRank;

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
        sg->info.divsion = malloc(primarySize * sizeof(int));
        sg->info.divsion[0] = 0;
        sg->info.secondaryRanks = malloc(primarySize * sizeof(int));
        sg->info.secondaryRanks[0] = 0;

        int* tmpColours = malloc(primarySize * sizeof(int));

        memset(tmpColours, -1, primarySize * sizeof(int));

        int newColour = 0;
        tmpColours[sg->info.colour] = newColour++;
        sg->info.colour = 0;

        for (int i = 1; i < primarySize; i++)
        {   
            int colour;
            (*recv)(&colour, 1, i);

            if(tmpColours[colour] == -1)
                tmpColours[colour] = newColour++;

            (*send)(&tmpColours[colour], 1, i);

            sg->info.divsion[i] = tmpColours[colour];

        }

        free(tmpColours);

        int num_islands = 0;
        int groupSizes[primarySize];
        memset(groupSizes, 0, primarySize * sizeof(int));


        for (int i = 0; i < primarySize; i++)
        {   
            if(groupSizes[sg->info.divsion[i]] == 0)
            {
                num_islands++; 
            }
            
            int sec_rank = groupSizes[sg->info.divsion[i]]++;

            if(i == 0){
                sg->info.rank = sec_rank;
            }else{
                (*send)(&sec_rank, 1, i);
            }

            sg->info.secondaryRanks[i] = sec_rank;
        }
        sg->numIslands = num_islands;

        for (int i = 1; i < primarySize; i++)
        {
            (*send)(&groupSizes[sg->info.divsion[i]], 1, i);
            (*send)(sg->info.divsion, primarySize, i);
            (*send)(sg->info.secondaryRanks, primarySize, i);
            (*send)(&num_islands, 1, i);
        }
        sg->info.size = groupSizes[sg->info.divsion[0]];
    }
    else
    {
        (*send)(&sg->info.colour, 1, 0);

        (*recv)(&sg->info.colour, 1, 0);

        (*recv)(&sg->info.rank, 1, 0);
        (*recv)(&sg->info.size, 1, 0);
        sg->info.divsion = malloc(primarySize * sizeof(int));
        (*recv)(sg->info.divsion, primarySize, 0);
        sg->info.secondaryRanks = malloc(primarySize * sizeof(int));
        (*recv)(sg->info.secondaryRanks, primarySize, 0);

        (*recv)(&sg->numIslands, 1, 0);
    }
    sg->primaryRanks = malloc(sg->info.size * sizeof(int));

    int ii = 0;
    for(int i = 0; i < primarySize && ii < sg->info.size; ++i)
    {
        if(sg->info.divsion[i] == sg->info.colour){
            sg->primaryRanks[ii++] = i; 
        } 
    }

    if (created && shmctl(shmid, IPC_RMID, 0) == -1)
        return SHMEM_SHMCTL_FAILED;
    
    sg->cpyBuf = shmem_cpybuf_obtain();
    // Open the own meta info shm segment and set it to ready
    sg->headerShmid = createMetaInfoSeg();

    laik_log(2, "Rank:%d on Island:%d", sg->info.rank, sg->info.colour);
    *newLocations = sg->info.divsion;
    *ranks = sg->info.secondaryRanks;
    return SHMEM_SUCCESS;
}


void shmem_transformSubGroup(Laik_ActionSeq* as, Shmem_Secondary_Group* sg, int chain_idx){

    bool* processed = malloc(sg->numIslands * sizeof(bool));
    int* tmp = malloc(sg->info.size * sizeof(int)); 
    for(int subgroup = 0; subgroup < as->subgroupCount; ++subgroup)
    {
        int last = 0;
        int ii = 0;
        memset(processed, false, sg->numIslands * sizeof(bool));

        int count = laik_aseq_groupCount(as , subgroup, -1);

        for(int i = 0; i < count; ++i)
        {
            int inTask = laik_aseq_taskInGroup(as,  subgroup, i, -1);

            if(!processed[sg->info.divsion[inTask]])
            {
                laik_aseq_updateTask(as, subgroup, last, inTask, -1);
                ++last;
                processed[sg->info.divsion[inTask]] = true;
            }

            if(sg->info.colour == sg->info.divsion[inTask])
            {
                tmp[ii++] = sg->info.secondaryRanks[inTask];
            }
        }

        laik_aseq_updateGroupCount(as, subgroup, last, -1);

        laik_aseq_addSecondaryGroup(as, subgroup, tmp, ii, chain_idx);
    }

    free(tmp);
    free(processed);
}

bool onSameIsland(Laik_ActionSeq* as, Shmem_Secondary_Group* sg, int inputgroup, int outputgroup)
{
    int inCount = laik_aseq_groupCount(as, inputgroup,  - 1);
    int outCount = laik_aseq_groupCount(as, outputgroup,  - 1);

    if(inCount != 1 || outCount != 1)   return false;

    int rankI = laik_aseq_taskInGroup(as, inputgroup, 0,  - 1);
    int rankO = laik_aseq_taskInGroup(as, outputgroup, 0, - 1);

    return sg->info.divsion[rankI] == sg->info.divsion[rankO];
}

void createBuffer(Shmem_Secondary_Group* sg, size_t size){
    if(size < 1) return;
    shmem_cpybuf_alloc(sg->cpyBuf, COPY_BUF_SIZE);
}

void cleanupBuffer(Shmem_Secondary_Group* sg)
{
    if(sg->cpyBuf->ptr) shmem_cpybuf_delete(sg->cpyBuf);
}
