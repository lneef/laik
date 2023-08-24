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


#include "laik/core.h"
#define _GNU_SOURCE
#include <sched.h>

#include "shmem.h"
#include "backends/shmem/shmem-cpybuf.h"
#include "backends/shmem/shmem-manager.h"
#include "laik.h"
#include "shmem-allocator.h"
#include "laik-internal.h"


#include <assert.h>
#include <complex.h>
#include <errno.h>
#include <stddef.h>
#include <stdint.h>
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
#include <sys/sysinfo.h>

#define COPY_BUF_SIZE 1024 * 1024
#define SHM_KEY 0x123
#define MAX_WAITTIME 1
#define SHM_RESIZE_KEY 0x246


struct shmInitSeg
{
    atomic_int rank;
    atomic_int colour;
    bool didInit;
    atomic_int procs[];

};


void deleteOpenShmSegs(int sig)
{
    (void) sig;
    deleteAllocatedSegments();
}

static int createMetaInfoSeg(Laik_Shmem_Data* sd)
{
    sd->shmp = shmem_alloc(sizeof(struct commHeader), &sd->headerShmid);
    assert(sd->shmp != (void*)-1);
    sd->shmp->receiver = -1;
    sd->cpybuf = (struct cpyBuf) {
        .size = 0,
        .request = 0,
        .ptr = NULL
    };

    return SHMEM_SUCCESS;
}

static void shmem_init_sync(int rank, int size, Laik_Inst_Data* idata, Laik_Group* g)
{
    if(rank == 0)
    {
        for(int i = 1; i<size; ++i)
        {   
            int barrier;
            RECV_INTS(&barrier, 1, i, idata, g);
        }
    }else {
        
        SEND_INTS(&g->myid, 1, 0, idata, g);
    }
}

static void shmem_parse_affinity_mask(Laik_Shmem_Data* sd)
{
    const char* override = getenv("LAIK_SHMEM_CPU_SETS");
    if(!override) return;
    unsigned size = CPU_ALLOC_SIZE(get_nprocs_conf());
    cpu_set_t* mask = CPU_ALLOC(size);
    cpu_set_t* mine = CPU_ALLOC(size);
    cpu_set_t* zero = CPU_ALLOC(size);
    CPU_ZERO_S(size, zero);
    sched_getaffinity(0, size, mine);

    int i = 0;
    int set = 0;
    while(override[i])
    {
        CPU_ZERO_S(size, mask);
        for(int j = 0; override[i] && override[i] !=',';  ++i, ++j)
        {
            if(override[i] == '1')
            {
                CPU_SET_S(j, size, mask);
            }
        }

        CPU_AND_S(size, mask, mine, mask);

        if(!CPU_EQUAL_S(size, mask, zero))
        {
            sd->set = set;
            CPU_FREE(mine);
            CPU_FREE(zero);
            CPU_FREE(mask);
            return;
        }
        ++set;

        if(!override[i])
            break;
        
        ++i;
    }


    laik_panic("No fitting mask");
}

static int shmem_split_location(int rank, int size, Laik_Inst_Data* idata, int shmAddr, Laik_Shmem_Comm* sg, Laik_Group* g, bool* creator)
{
    bool created = false;
    struct shmInitSeg* shmp;
    Laik_Shmem_Data* sd = idata->backend_data;

    if(sd->affinity && sd->set == -1) shmem_parse_affinity_mask(sd);
    int mask;
    if(rank == 0)
    {
        for(int i = 1; i < size; ++i)
        {
            SEND_INTS(&g->myid, 1, i, idata, g);
        }
        mask = g->myid;

    }else {
        RECV_INTS(&mask, 1, 0, idata, g);
    }

    int segment_size =  sizeof(struct shmInitSeg);
    segment_size +=  sd->affinity ? sizeof(atomic_int) * size : 0;
  
    int address = shmAddr + mask;
    int shmid = shmget(address, segment_size, IPC_EXCL | 0644 | IPC_CREAT);
    if (shmid == -1)
    {

        shmid = shmget(address, segment_size, 0644);
        
        if (shmid == -1)
        {
            laik_log(2, "%d", address);
            laik_panic(strerror(errno));
            return SHMEM_SHMGET_FAILED;
        }

        // Attach to the segment to get a pointer to it.
        shmp = shmat(shmid, NULL, 0);

        if (shmp == (void *)-1)
            return SHMEM_SHMAT_FAILED;

        // register for barrier
        shmem_init_sync(rank, size, idata, g);

        if(!sd->affinity) sg -> location = atomic_load(&shmp->colour);

    }
    else
    {
        created = true;
        // Master initialization
        
        shmp = shmat(shmid, NULL, 0);
        if (shmp == (void *)-1)
            return SHMEM_SHMAT_FAILED;

        if(sd->affinity)
        {
            laik_log(2, "affinity");
            for(int i = 0; i < size; ++i)
                atomic_init(&shmp->procs[i], -1);
        }else {
            atomic_init(&shmp->colour, g->myid);
            sg->location = g->myid;
        }
        
        // register for barrier
        shmem_init_sync(rank, size, idata, g);
    }

    if(sd->affinity)
    {
        bool found = false;
        atomic_init(&shmp->procs[rank], sd->set);

        shmem_init_sync(rank, size, idata, g);
        shmem_init_sync(rank, size, idata, g);

        for(int i = 0; i < size; ++i)
        {
            if(atomic_load(&shmp->procs[i]) < 0) continue;

            // compare cpu_set of given pid unitl an equal on is found
            int set = atomic_load(&shmp->procs[i]);
            if(set == sd->set)
            {
                sg->location = i;
                found = true;
                break;
            }

        }

        // Should definitively not happen
        if(!found) laik_panic("No fitting set found");

    }

    shmdt(shmp);

    *creator = created;
    return shmid;
}

int shmem_location(Laik_Inst_Data* idata, Laik_Group* g)
{
    Laik_Shmem_Comm* sg = g->backend_data[idata->index];
    return sg->location;
}

int shmem_myid(Laik_Inst_Data* idata, Laik_Group* g)
{
    Laik_Shmem_Comm* sg = g->backend_data[idata->index];
    return sg->myid;
}

Laik_Shmem_Comm* shmem_comm(Laik_Inst_Data* idata, Laik_Group* g)
{
    return g->backend_data[idata->index];
}

int shmem_2cpy_send(void *buffer, int count, int datatype, int recipient, Laik_Shmem_Data* sd)
{
    int size = datatype * count;
    shmem_cpybuf_alloc(&sd->cpybuf, size);
    struct commHeader* shmp = sd->shmp;
    shmp->count = count;
    shmp->spec = PACK;
    shmp->shmid = sd->cpybuf.shmid;
    memcpy(sd->cpybuf.ptr, buffer, size);
    shmp->receiver = recipient;
    while (shmp->receiver != -1)
    {
    }

    return SHMEM_SUCCESS;
}

int shmem_1cpy_send(void *buffer, int count, int datatype, int recipient, Laik_Shmem_Data* sd)
{
    int bufShmid, offset;
    if(get_shmid(buffer, &bufShmid, &offset) == SHMEM_SEGMENT_NOT_FOUND){
        return shmem_2cpy_send(buffer, count, datatype, recipient, sd);
    }
    struct commHeader* shmp = sd->shmp;
    shmp->count = count;
    shmp->spec = PACK;
    shmp->shmid = bufShmid;
    shmp->receiver = recipient;
    while (shmp->receiver != -1)
    {
    }

    return SHMEM_SUCCESS;
}

int shmem_recv(void *buffer, int count,int sender, Laik_Data* data, Laik_Inst_Data* idata, Laik_Group* g, Laik_ReductionOperation redOp)
{
    int shmid;
    Laik_Shmem_Comm* sg = shmem_comm(idata, g);
    shmid = sg->headershmids[sender];
  
    // Attach to the segment to get a pointer to it.
    struct commHeader *shmp = shmem_manager_attach(shmid, 0);

    while (shmp->receiver != sg->myid)
    {
    }

    int received = shmp->count;
    int bufShmid = shmp->shmid;

    char *bufShmp = shmem_manager_attach(bufShmid, 0);
  
    int bufSize = data->elemsize * count;
    int receivedSize = received * data->elemsize;
    if(redOp == LAIK_RO_None)
    {
        assert(bufSize == receivedSize);
        if (bufSize < receivedSize)
        {
            memcpy(buffer, bufShmp, bufSize);
            return SHMEM_RECV_BUFFER_TOO_SMALL;
        }
        else
        {
            memcpy(buffer, bufShmp , receivedSize);
        }
    }
    else {
        assert(data->type->reduce);
        data->type->reduce(buffer, buffer, bufShmp, count, redOp);
    }

    shmp->receiver = -1;

    shmem_manager_detach((char*)shmp);
    shmem_manager_detach(bufShmp);


    return SHMEM_SUCCESS;
}

int shmem_sendMap(Laik_Mapping* map, int receiver, int shmid, Laik_Inst_Data* idata)
{
    Laik_Shmem_Data* sd = idata->backend_data;
    struct commHeader* shmp = sd->shmp;
    shmp->spec = MAP;
    shmp->shmid = shmid;
    shmp->range = map->allocatedRange;
    shmp->receiver = receiver;

    while(shmp->receiver != -1)
    {
    }
    
    return SHMEM_SUCCESS;   
}

int shmem_PackSend(Laik_Mapping* map, Laik_Range range, int count, int receiver,  Laik_Inst_Data* idata)
{   
    Laik_Shmem_Data* sd = idata->backend_data;
    shmem_cpybuf_alloc(&sd->cpybuf, count * map->data->elemsize);

    Laik_Index from = range.from;
    struct commHeader* shmp = sd->shmp;
    //pack makes changes to range
    shmp->shmid = sd->cpybuf.shmid;
    shmp->spec = PACK;
    map->layout->pack(map, &range, &from, sd->cpybuf.ptr, count * map->data->elemsize);
    shmp->receiver = receiver;
    shmp->count = count;
    

    while(shmp->receiver != -1)
    {
    }

    return SHMEM_SUCCESS;
}

static inline int shmem_cpyPack(Laik_Mapping* map, Laik_Range* range, struct commHeader* shmp, char* ptr, int count)
{
    assert(count == count);
    Laik_Index from = range->from;
    map->layout->unpack(map, range, &from, ptr, count * map->data->elemsize);
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
    tmp.mapNo = 0;

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
    shmid = sg->headershmids[sender];
    struct commHeader* shmp = shmem_manager_attach(shmid, 0);
    while(shmp->receiver != sg->myid)
    {
    }
    char* ptr = shmem_manager_attach(shmp->shmid, 0);
    if(shmp->spec == PACK)
    {
        ret = shmem_cpyPack(map, range, shmp, ptr, count);
    }
    else if(shmp->spec == MAP)
    {
        ret = shmem_cpyMap(map, range, shmp, ptr);
    }
    shmem_manager_detach((char*)shmp);
    shmem_manager_detach(ptr);

    return ret;
}

int shmem_RecvUnpack(Laik_Mapping *map, Laik_Range *range, int count, int sender,  Laik_Inst_Data* idata, Laik_Group* g)
{
    int shmid;    
    Laik_Shmem_Comm* sg = shmem_comm(idata, g);
    shmid = sg->headershmids[sender];
    struct commHeader* shmp = shmem_manager_attach(shmid, 0);
    while(shmp->receiver != sg->myid)
    {
    }
    char* ptr = shmem_manager_attach(shmp->shmid, 0);
    assert(shmp->spec == PACK);
    int ret = shmem_cpyPack(map, range, shmp, ptr, count);

    shmem_manager_detach((char*)shmp);
    shmem_manager_detach(ptr);

    return ret;
}

int shmem_RecvReduce(char* buf, int count, int sender, Laik_Type* type, Laik_ReductionOperation redOp,  Laik_Inst_Data* idata, Laik_Group* g)
{
    int shmid;
    Laik_Shmem_Comm* sg = shmem_comm(idata, g);
    shmid = sg->headershmids[sender];
    struct commHeader* shmp = shmem_manager_attach(shmid, 0);

    while(shmp->receiver != sg->myid)
    {
    } 
    assert(shmp->spec == PACK);
    char* ptr = shmem_manager_attach(shmp->shmid, 0);

    assert(type->reduce);
    type->reduce(buf, buf, ptr, count, redOp);

    shmp->receiver = -1;
    shmem_manager_detach((char*)shmp);
    shmem_manager_detach(ptr);
    return SHMEM_SUCCESS;

}

int shmem_send(void* buffer, int count, int datatype, int receiver, Laik_Inst_Data* idata)
{
    Laik_Shmem_Data* sd = (Laik_Shmem_Data*) idata->backend_data;
    return sd->send(buffer, count, datatype, receiver, sd);
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
    deleteOpenShmSegs(0);

    return SHMEM_SUCCESS;
}

int shmem_update_comm(Laik_Shmem_Comm* sg, Laik_Group* g, Laik_Inst_Data* idata, int size, int shmAddr, int rank)
{
    Laik_Shmem_Data* sd = (Laik_Shmem_Data*) idata->backend_data;
    bool created = false;

    int shmid = shmem_split_location(rank, size, idata, shmAddr, sg, g, &created);

    // Get the colours of each process at master, calculate the groups and send each process their group.
    if (rank == 0)
    {
        sg->locations = malloc(size * sizeof(int));
        int* tmpColours = malloc(g->size * sizeof(int));
        sg->secondaryIds = malloc(size * sizeof(int));

        memset(tmpColours, -1, g->size * sizeof(int));

        int newColour = 0;
        int* groupSizes = calloc(g->size , sizeof(int));

        // rank 0 is always rank 0
        sg->locations[0] = 0;

        // get colour and mark rank 0 as processed

        int num_islands = 1;
        groupSizes[0] = 1;
        sg->myid = 0;
        sg->locations[0] = 0;
        sg->secondaryIds[0] = 0;
        int colour = sg->location;

        tmpColours[sg->location] = newColour++;
        sg->location = tmpColours[sg->location];

        if(groupSizes[0] == sd->ranksPerIslands) 
            tmpColours[colour] = newColour++;


        for (int i = 1; i < size; i++)
        {   
            RECV_INTS(&colour, 1, i, idata, g);
            laik_log(2, "%d, %d", colour, i);

            if(tmpColours[colour] == -1)
                tmpColours[colour] = newColour++;

            SEND_INTS(&tmpColours[colour], 1, i, idata, g);

            
            // get division and new rank
            sg->locations[i] = tmpColours[colour];
            if(groupSizes[sg->locations[i]] == 0) num_islands++;
            int new_rank = groupSizes[sg->locations[i]]++;
            
            // if we reached <perIsland> ranks <colour> if mappes to a higher number
            if(groupSizes[sg->locations[i]] == sd->ranksPerIslands) 
                tmpColours[colour] = newColour++;
            
            sg->secondaryIds[i] = new_rank;

            SEND_INTS(&new_rank, 1, i, idata, g);
            
        }

        if (created && shmctl(shmid, IPC_RMID, 0) == -1)
            return SHMEM_SHMCTL_FAILED;

        free(tmpColours);

        sg->numIslands = num_islands;

        for (int i = 1; i < size; i++)
        {
            SEND_INTS(&groupSizes[sg->locations[i]], 1, i, idata, g);
            SEND_INTS(sg->secondaryIds, size, i, idata, g);
            SEND_INTS(sg->locations, size, i, idata, g);
            SEND_INTS(&num_islands, 1, i, idata, g);
        }
        sg->size = groupSizes[sg->locations[0]];

        free(groupSizes);
    }
    else
    {
        SEND_INTS(&sg->location, 1, 0, idata, g);

        RECV_INTS(&sg->location, 1, 0, idata, g);

        RECV_INTS(&sg->myid, 1, 0, idata, g);

        if (created && shmctl(shmid, IPC_RMID, 0) == -1)
            return SHMEM_SHMCTL_FAILED;

        RECV_INTS(&sg->size, 1, 0, idata, g);
        sg->locations = malloc(g->size * sizeof(int));
        sg->secondaryIds = malloc(size * sizeof(int));

        RECV_INTS(sg->secondaryIds, size, 0, idata, g);
        RECV_INTS(sg->locations, g->size, 0, idata, g);

        RECV_INTS(&sg->numIslands, 1, 0, idata, g);
    }
    sg->primaryRanks = malloc(sg->size * sizeof(int));

    int ii = 0;
    for(int i = 0; i < size && ii < sg->size; ++i)
    {
        if(sg->locations[i] == sg->location){
            sg->primaryRanks[ii++] = i; 
        } 
    }

     sg->headershmids = malloc(sg->size * sizeof(int));

    for(int i = 0; i < sg->size; ++i)
    {
        if(i != sg->myid)
        {
            RECV_INTS(&sg->headershmids[i], 1, sg->primaryRanks[i], idata, g);
            continue;
        }

        for(int j = 0; j < sg->size; ++j)
        {
            if(j == sg->myid) continue;
            SEND_INTS(&sd->headerShmid, 1, sg->primaryRanks[j], idata, g);
        }
    }

    return SHMEM_SUCCESS;
    
}

int shmem_init_comm(Laik_Shmem_Comm *sg, Laik_Group *g, Laik_Inst_Data *idata, int rank, int size)
{
    return shmem_update_comm(sg, g, idata,size, SHM_RESIZE_KEY, rank);
}

static char* saveptrR;
static char* saveptrC;
static char* saveptrA;

int shmem_secondary_init(Laik_Shmem_Comm* sg, Laik_Inst_Data* idata, Laik_Group* world, int primarySize, int rank)
{
    signal(SIGINT, deleteOpenShmSegs);
    Laik_Shmem_Data* sd = malloc(sizeof(Laik_Shmem_Data));
    char *token, *copyToken, *affinityToken;

    if(!saveptrA)
    {
        char *affinity = getenv("LAIK_SHMEM_AFFINITY");
        affinityToken = affinity == NULL ? NULL : strtok_r(affinity, ",", &saveptrR);
    }else {
        affinityToken = strtok_r(NULL, ",", &saveptrA);
    }

    sd -> affinity = affinityToken ? atoi(affinityToken) == 1 : false;
    laik_log(2, "aff%d", sd->affinity);

    if(!saveptrR)
    {
        char *envRanks = getenv("LAIK_SHMEM_RANKS_PER_ISLANDS");
        token = envRanks == NULL ? NULL : strtok_r(envRanks, ",", &saveptrR);
    }
    else{
        token = strtok_r(NULL, ",", &saveptrR);
    }

    // if token is null, all ranks can be on the same islands
    int perIsland = token ? atoi(token) : -1;

    if(perIsland == 0 || perIsland < -2)
        laik_panic("LAIK_SHMEM_RANKS_PER_ISLANDS needs to be a number larger than 0");

    if(!saveptrC)
    {
        char *copyScheme = getenv("LAIK_SHMEM_COPY_SCHEME");
        copyToken = copyScheme == NULL ? NULL : strtok_r(copyScheme, ",", &saveptrC);
    
    }else {
        copyToken = strtok_r(NULL, ",", &saveptrC);
    }

    if(copyToken != 0 && !strcmp(copyToken, "2"))
    {
        sd->send = shmem_2cpy_send;
        sd->copyScheme = 2;
    }
    else if(copyToken == 0 || !strcmp(copyToken, "1"))
    {
        sd->send = shmem_1cpy_send;
        sd->copyScheme = 1;
    }
    else 
    {
        laik_panic("Please provide a correct copy scheme: 1 or 2");
    }

    sd->ranksPerIslands = perIsland;
    idata->backend_data = sd;

    sd->set = -1;

    //create meta info seg and cpybuf for this backend instance
    createMetaInfoSeg(sd);
    shmem_update_comm(sg, world, idata, primarySize, SHM_KEY, rank);


    laik_log(1, "Shared Memory Backend: T%d is T%d on Island:%d", world->myid, sg->myid, sg->location);
    return SHMEM_SUCCESS;
}


void shmem_transformSubGroup(Laik_ActionSeq* as, Laik_Shmem_Comm* sg, int chain_idx){

    bool* processed = malloc(sg->numIslands * sizeof(bool));
    int* tmp = malloc(sg->size * sizeof(int)); 
    int last_idx = chain_idx - 1;
    for(int subgroup = 0; subgroup < as->subgroupCount; ++subgroup)
    {
        int last = 0;
        int ii = 0;
        memset(processed, false, sg->numIslands * sizeof(bool));

        int count = laik_aseq_groupCount(as , subgroup, last_idx);

        for(int i = 0; i < count; ++i)
        {
            int inTask = laik_aseq_taskInGroup(as,  subgroup, i, last_idx);

            if(!processed[sg->locations[inTask]])
            {
                laik_aseq_updateTask(as, subgroup, last, inTask, last_idx);
                ++last;
                assert(sg->locations[inTask] >= 0);
                processed[sg->locations[inTask]] = true;
            }

            if(sg->location == sg->locations[inTask])
            {
                tmp[ii++] = sg->secondaryIds[inTask];
            }
        }

        laik_aseq_updateGroupCount(as, subgroup, last, last_idx);

        laik_aseq_addSecondaryGroup(as, subgroup, tmp, ii, chain_idx);
    }

    free(tmp);
    free(processed);
}

bool onSameIsland(Laik_ActionSeq* as, Laik_Shmem_Comm* sg, int inputgroup, int outputgroup, int chain_idx)
{
    bool onlyOneLeft = laik_aseq_finishRed(as, inputgroup, outputgroup, chain_idx);

    if(!onlyOneLeft) return false;

    int rankI = laik_aseq_taskInGroup(as, inputgroup, 0,  chain_idx);
    int rankO = laik_aseq_taskInGroup(as, outputgroup, 0, chain_idx);

    return sg->locations[rankI] == sg->locations[rankO];

}

