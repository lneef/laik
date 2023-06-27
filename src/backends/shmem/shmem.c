/*
 * This file is part of the LAIK library.
 * Copyright (c) 2022 Robert Hubinger <robert.hubinger@tum.de>
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
#include "laik/action-internal.h"
#include "shmem-allocator.h"
#include "laik-internal.h"
#include "laik.h"
#include "laik/core.h"

#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
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

struct groupInfo
{
    int size;
    int rank;
    int colour;
    int num_islands;
    int *primarys;
    int *colours;
    int *secondaryRanks;

    struct cpyBuf* cpyBuf;

    int (*send)(void*, int, int, int);
    int (*recv)(void*, int, int, int, int*);
};

struct metaInfos{
    int receiver;
    int count;
    int shmid;
    int offset;
};

struct groupInfo groupInfo;
int openShmid = -1;
int metaShmid = -1;

static int pair(int colour, int rank)
{
    int tmp = (colour + rank) * (colour + rank + 1);
    return tmp/2 + colour + 1;
}

int hash(int x)
{
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = (x >> 16) ^ x;
    return x;
}

void deleteOpenShmSegs()
{
    if (openShmid != -1)
        shmctl(openShmid, IPC_RMID, 0);

    if (metaShmid != -1)
    {   
        shmctl(metaShmid, IPC_RMID, 0);
    }

    if(groupInfo.cpyBuf != NULL)
        //shmem_cpybuf_delete(groupInfo.cpyBuf);

    deleteAllocatedSegments();
}

int createMetaInfoSeg()
{
    int shmAddr = hash(pair(groupInfo.colour, groupInfo.rank)) + META_OFFSET;
    metaShmid = shmget(shmAddr, sizeof(struct metaInfos), 0644 | IPC_CREAT);
    if (metaShmid == -1)
        return SHMEM_SHMGET_FAILED;

    struct metaInfos *shmp = shmat(metaShmid, NULL, 0);
    if (shmp == (void *)-1)
        return SHMEM_SHMAT_FAILED; 

    shmp->receiver = -1;

    if (shmdt(shmp) == -1)
        return SHMEM_SHMDT_FAILED;

    return SHMEM_SUCCESS;
}

int shmem_init()
{
    signal(SIGINT, deleteOpenShmSegs);

    char *str = getenv("LAIK_SIZE");
    groupInfo.size = str ? atoi(str) : 1;

    struct shmInitSeg *shmp;
    int shmid = shmget(SHM_KEY, sizeof(struct shmInitSeg), IPC_EXCL | 0644 | IPC_CREAT);
    if (shmid == -1)
    {
        // Client initialization
        time_t t_0;

        // As long as it fails and three seconds haven't passed try again (wait for master)
        t_0 = time(NULL);
        while (time(NULL) - t_0 < MAX_WAITTIME && shmid == -1)
        {
            shmid = shmget(SHM_KEY, 0, IPC_CREAT | 0644);
        }
        if (shmid == -1)
            return SHMEM_SHMGET_FAILED;

        // Attach to the segment to get a pointer to it.
        shmp = shmat(shmid, NULL, 0);
        if (shmp == (void *)-1)
            return SHMEM_SHMAT_FAILED;

        groupInfo.rank = ++shmp->rank;
        if (shmdt(shmp) == -1)
            return SHMEM_SHMDT_FAILED;
    }
    else
    {
        // Master initialization
        groupInfo.rank = 0;

        openShmid = shmid;
        shmp = shmat(shmid, NULL, 0);
        if (shmp == (void *)-1)
            return SHMEM_SHMAT_FAILED;

        while (shmp->rank != groupInfo.size - 1)
        {
        }

        if (shmdt(shmp) == -1)
            return SHMEM_SHMDT_FAILED;

        if (shmctl(shmid, IPC_RMID, 0) == -1)
            return SHMEM_SHMCTL_FAILED;

        openShmid = -1;
    }

    // Open the meta info shm segment and set it to ready
    int err = createMetaInfoSeg();
    if(err != SHMEM_SUCCESS)
        return err;

    return SHMEM_SUCCESS;
}

int shmem_get_island_num(int* num){
    *num = groupInfo.num_islands;
    return SHMEM_SUCCESS;
}

int shmem_get_primarys(int** primarys){
    *primarys = groupInfo.primarys;
    return SHMEM_SUCCESS;
}

int shmem_colour_num(int* num){
    *num = groupInfo.num_islands;
    return SHMEM_SUCCESS;
}

int shmem_comm_size(int *sizePtr)
{
    *sizePtr = groupInfo.size;
    return SHMEM_SUCCESS;
}

int shmem_comm_rank(int *rankPtr)
{
    *rankPtr = groupInfo.rank;
    return SHMEM_SUCCESS;
}

int shmem_comm_colour(int *colourPtr)
{
    *colourPtr = groupInfo.colour;
    return SHMEM_SUCCESS;
}

int shmem_get_identifier(int *ident)
{
    *ident = 1;
    return SHMEM_SUCCESS;
}

int shmem_2cpy_send(void *buffer, int count, int datatype, int recipient)
{
    int size = datatype * count;
    shmem_cpybuf_alloc(groupInfo.cpyBuf, size);

    struct metaInfos *shmp = shmat(metaShmid, NULL, 0);
    if (shmp == (void *)-1)
        return SHMEM_SHMAT_FAILED;

    shmp->count = count;
    shmp->shmid = groupInfo.cpyBuf->shmid;
    shmp->offset = 0;
    memcpy(groupInfo.cpyBuf->ptr, buffer, size);
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

    // Attach to the segment to get a pointer to it.
    struct metaInfos *shmp = shmat(metaShmid, NULL, 0);
    if (shmp == (void *)-1)
        return SHMEM_SHMAT_FAILED;

    shmp->count = count;
    shmp->shmid = bufShmid;
    shmp->offset = offset;
    shmp->receiver = recipient;
    while (shmp->receiver != -1)
    {
    }

    if (shmdt(shmp) == -1)
        return SHMEM_SHMDT_FAILED;

    return SHMEM_SUCCESS;
}

int shmem_recv(void *buffer, int count, int datatype, int sender, int *received)
{
    int shmAddr = hash(pair(groupInfo.colour, sender)) + META_OFFSET;
    time_t t_0 = time(NULL);
    int shmid = shmget(shmAddr, 0, 0644);
    while (shmid == -1 && time(NULL) - t_0 < MAX_WAITTIME)
        shmid = shmget(shmAddr, 0, 0644);
    if (shmid == -1)
        return SHMEM_SHMGET_FAILED;

    // Attach to the segment to get a pointer to it.
    struct metaInfos *shmp = shmat(shmid, NULL, 0);
    if (shmp == (void *)-1)
        return SHMEM_SHMAT_FAILED;

    while (shmp->receiver != groupInfo.rank)
    {
    }

    *received = shmp->count;
    int bufShmid = shmp->shmid;
    int offset = shmp->offset;

    char *bufShmp = shmat(bufShmid, NULL, 0);
    if (bufShmp == (void *)-1)
        return SHMEM_SHMAT_FAILED;

    int bufSize = datatype * count;
    int receivedSize = *received * datatype;
    if (bufSize < receivedSize)
    {
        memcpy(buffer, bufShmp + offset, bufSize);
        return SHMEM_RECV_BUFFER_TOO_SMALL;
    }
    else
    {
        memcpy(buffer, bufShmp + offset, receivedSize);
    }

    if (shmdt(bufShmp) == -1)
        return SHMEM_SHMDT_FAILED;

    shmp->receiver = -1;
    if (shmdt(shmp) == -1)
        return SHMEM_SHMDT_FAILED;

    return SHMEM_SUCCESS;
}

int shmem_send(void* buffer, int count, int datatype, int recipient)
{
    return groupInfo.send(buffer, count, datatype, recipient);
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
    shmem_cpybuf_delete(groupInfo.cpyBuf);
    deleteOpenShmSegs();
    if (groupInfo.colours != NULL)
        free(groupInfo.colours);
    
    if (groupInfo.secondaryRanks != NULL)
        free(groupInfo.secondaryRanks);
    
    if(groupInfo.primarys != NULL)
        free(groupInfo.primarys);

    return SHMEM_SUCCESS;
}

int shmem_secondary_init(Laik_Instance* inst, int primaryRank, int primarySize, int (*send)(int *, int, int),
                         int (*recv)(int *, int, int))
{
    signal(SIGINT, deleteOpenShmSegs);

    const char *envRanks = getenv("LAIK_SHMEM_SUB_ISLANDS");
    int numSubIslands = envRanks == NULL ? 1 : atoi(envRanks);

    if(numSubIslands < 1)
        laik_panic("RANKS_PER_ISLAND needs to be a number larger than 0");

    int location = inst->world->locationid[primaryRank];
    int shmAddr = SHM_KEY + (location % numSubIslands);

    const char *copyScheme = getenv("LAIK_SHMEM_COPY_SCHEME");

    if(copyScheme != 0 && !strcmp(copyScheme, "2"))
    {
        groupInfo.send = shmem_2cpy_send;
    }
    else if(copyScheme == 0 || !strcmp(copyScheme, "1"))
    {
        groupInfo.send = shmem_1cpy_send;
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
        groupInfo.colour = shmp->colour;

        if (shmdt(shmp) == -1)
            return SHMEM_SHMDT_FAILED;
    }
    else
    {
        created = true;
        // Master initialization
        openShmid = shmid;

        groupInfo.colour = primaryRank;

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
        groupInfo.colours = malloc(primarySize * sizeof(int));
        groupInfo.colours[0] = 0;
        groupInfo.secondaryRanks = malloc(primarySize * sizeof(int));
        groupInfo.secondaryRanks[0] = groupInfo.rank;

        int* tmpColours = malloc(primarySize * sizeof(int));

        memset(tmpColours, -1, primarySize * sizeof(int));

        int newColour = 0;
        tmpColours[groupInfo.colour] = newColour++;
        groupInfo.colour = 0;

        for (int i = 1; i < primarySize; i++)
        {   
            int colour;
            (*recv)(&colour, 1, i);

            if(tmpColours[colour] == -1)
                tmpColours[colour] = newColour++;

            (*send)(&tmpColours[colour], 1, i);

            groupInfo.colours[i] = tmpColours[colour];

        }

        free(tmpColours);

        int ii = 0;
        int groupSizes[primarySize];
        memset(groupSizes, 0, primarySize * sizeof(int));
        groupInfo.primarys = malloc(primarySize * sizeof(int));

        for (int i = 0; i < primarySize; i++)
        {   
            if(groupSizes[groupInfo.colours[i]] == 0)
            {
                groupInfo.num_islands++;
                groupInfo.primarys[ii++] = i; 
            }
            
            int sec_rank = groupSizes[groupInfo.colours[i]]++;

            if(i == 0){
                groupInfo.rank = sec_rank;
            }else{
                (*send)(&sec_rank, 1, i);
            }

            groupInfo.secondaryRanks[i] = sec_rank;
        }

        groupInfo.primarys = realloc(groupInfo.primarys, groupInfo.num_islands * sizeof(int));
        

        for (int i = 1; i < primarySize; i++)
        {
            (*send)(&groupSizes[groupInfo.colours[i]], 1, i);
            (*send)(groupInfo.colours, primarySize, i);
            (*send)(groupInfo.secondaryRanks, primarySize, i);
            (*send)(&groupInfo.num_islands, 1, i);
            (*send)(groupInfo.primarys, groupInfo.num_islands, i);
        }
        groupInfo.size = groupSizes[groupInfo.colours[0]];
    }
    else
    {
        (*send)(&groupInfo.colour, 1, 0);

        (*recv)(&groupInfo.colour, 1, 0);

        (*recv)(&groupInfo.rank, 1, 0);
        (*recv)(&groupInfo.size, 1, 0);
        groupInfo.colours = malloc(primarySize * sizeof(int));
        (*recv)(groupInfo.colours, primarySize, 0);
        groupInfo.secondaryRanks = malloc(primarySize * sizeof(int));
        (*recv)(groupInfo.secondaryRanks, primarySize, 0);

        (*recv)(&groupInfo.num_islands, 1, 0);
        groupInfo.primarys = malloc(groupInfo.num_islands * sizeof(int));
        (*recv)(groupInfo.primarys, groupInfo.num_islands, 0);
    }

    if (created && shmctl(openShmid, IPC_RMID, 0) == -1)
        return SHMEM_SHMCTL_FAILED;
    
    openShmid = -1;
    groupInfo.cpyBuf = shmem_cpybuf_obtain(COPY_BUF_SIZE);
    // Open the own meta info shm segment and set it to ready
    int err = createMetaInfoSeg();
    if (err != SHMEM_SUCCESS)
        return err;
    
    

    laik_log(1, "Rank:%d on Island:%d", groupInfo.rank, groupInfo.colour);
    return SHMEM_SUCCESS;
}

int shmem_get_colours(int **buf)
{
    *buf = groupInfo.colours;
    return SHMEM_SUCCESS;
}

int shmem_get_secondaryRanks(int **buf)
{
    *buf = groupInfo.secondaryRanks;
    return SHMEM_SUCCESS;
}


void shmem_transformSubGroup(Laik_Transition* t, Laik_ActionSeq* as, int chain_idx){

    bool* processed = malloc(groupInfo.num_islands * sizeof(bool));
    int* tmp = malloc(groupInfo.size * sizeof(int)); 
    for(int subgroup = 0; subgroup < as->subgroupCount; ++subgroup)
    {
        int last = 0;
        int ii = 0;
        memset(processed, false, groupInfo.num_islands * sizeof(bool));

        int count = laik_aseq_groupCount(as , subgroup, -1);

        if(count == t->group->size)
        {
            laik_aseq_updateGroup(as, subgroup, groupInfo.primarys, groupInfo.num_islands, -1);
            for(int i = 0; i < groupInfo.size; ++i) tmp[i] = i;
            laik_aseq_addSecondaryGroup(as, subgroup, tmp, groupInfo.size, chain_idx);
            continue;
        }

        for(int i = 0; i < count; ++i)
        {
            int inTask = laik_aseq_taskInGroup(as,  subgroup, i, -1);

            if(!processed[groupInfo.colours[inTask]])
            {
                laik_aseq_updateTask(as, subgroup, last, inTask, -1);
                ++last;
                processed[groupInfo.colours[inTask]] = true;
            }

            if(groupInfo.colour == groupInfo.colours[inTask])
            {
                tmp[ii++] = groupInfo.secondaryRanks[inTask];
            }
        }

        laik_aseq_updateGroupCount(as, subgroup, last, -1);

        laik_aseq_addSecondaryGroup(as, subgroup, tmp, ii, chain_idx);
    }

    free(tmp);
    free(processed);
}

bool onSameIsland(Laik_ActionSeq* as, int inputgroup, int outputgroup, int chain_idx)
{
    int inCount = laik_aseq_groupCount(as, inputgroup, chain_idx - 1);
    int outCount = laik_aseq_groupCount(as, outputgroup, chain_idx - 1);

    if(inCount != 1 || outCount != 1)   return false;

    int rankI = laik_aseq_taskInGroup(as, inputgroup, 0, chain_idx - 1);
    int rankO = laik_aseq_taskInGroup(as, outputgroup, 0, chain_idx - 1);

    return groupInfo.colours[rankI] == groupInfo.colours[rankO];
}
