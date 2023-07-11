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

#include "backends/shmem/shmem-manager.h"
#include "laik-internal.h"
#include "laik.h"
#include "laik-backend-shmem.h"
#include "backends/shmem/shmem.h"
#include "backends/shmem/shmem-allocator.h"
#include "backends/shmem/shmem-actions.h"
#include "laik/action-internal.h"
#include "laik/core.h"
#include "laik/debug.h"

#include <assert.h>
#include <bits/time.h>
#include <stdlib.h>
#include <mpi.h>
#include <math.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>
#include <time.h>

// forward decls, types/structs , global variables
static void laik_shmem_secondary_finalize();
static bool laik_shmem_secondary_prepare(const Laik_Secondary*, Laik_ActionSeq *);
static bool laik_shmem_secondary_exec(const Laik_Secondary*, Laik_ActionSeq *, Laik_Action *);
static void laik_shmem_secondary_cleanup();
static bool laik_shmem_log_action(const Laik_Secondary*, Laik_ActionSeq*, Laik_Action *);
static void laik_shmem_update_group(const Laik_Secondary*, Laik_Group*);

//----------------------------------------------------------------------------
// error helpers

static void laik_shmem_panic(int err)
{
    char str[SHMEM_MAX_ERROR_STRING];

    assert(err != SHMEM_SUCCESS);
    if (shmem_error_string(err, str) != SHMEM_SUCCESS)
        laik_panic("SHMEM backend: Unknown SHMEM error!");
    else
        laik_log(LAIK_LL_Panic, "SHMEM backend: SHMEM error '%s'", str);
    exit(1);
}

void laik_shmem_update_group(const Laik_Secondary* this, Laik_Group* g)
{   
    if(g->myid < 0) return;
    Laik_Shmem_Comm* sg = malloc(sizeof(Laik_Shmem_Comm));
    Laik_Group* old = g->parent;
    Laik_Shmem_Comm* sgOld = (Laik_Shmem_Comm*) old->sec_group[this->chain_idx];
    *sg = *sgOld; 

    sg->primaryRanks = malloc(sizeof(int) * sgOld->size);
    sg->secondaryRanks = malloc(sizeof(int) * g->size);
    sg->divsion = malloc(sizeof(int) * g->size);

    int* pRanks = sgOld->primaryRanks;
    bool removed = false;

    // in the following all ranks are getting smaller or stay the same

    for(int i = 0; i < sg->size; ++i)
    {
        int newRank = g->fromParent[pRanks[i]];
        removed |= newRank < 0;
        if(newRank < 0) continue;

        if(newRank < pRanks[i])
        {
            int newSecRank = removed ? i - 1 : i;
            sg->primaryRanks[newSecRank] = newRank;
            sg->secondaryRanks[newRank] = newSecRank;
        }
    }

    sg->size = removed ? sg->size - 1 : sg->size;

    sg->rank = sg->secondaryRanks[g->myid];
    
    laik_log(1, "Shared memory backend shrinked:");
    laik_log(1, "New Size: %d", sg->size);
    for(int i = 0; i < sg->size; ++i)
    {
        laik_log_append("%d", sg->primaryRanks[i]);
    }
    laik_log_flush("");

    laik_log(1, "new rank: %d", sg->rank);

    for(int i = 0; i < g->size; ++i)
    {
        int oldRank = g->toParent[i];
        sg->divsion[i] = sgOld->divsion[oldRank];
    }
    g->sec_group[this->chain_idx] = (Laik_Secondary_Group*) sg;

    for(int i = 0; i < g->size; ++i)
    {
        laik_log_append("%d", sg->divsion[i]);
    }
    laik_log_flush("");
}

/*
static void laik_shmem_exec_packAndSend(Laik_Mapping *map, Laik_Range *range, int to_rank, uint64_t slc_size, int dataType)
{
    Laik_Index idx = range->from;
    int dims = range->space->dims;
    unsigned int packed;
    uint64_t count = 0;
    while (1)
    {
        packed = (map->layout->pack)(map, range, &idx, packbuf, PACKBUFSIZE);
        assert(packed > 0);
        int err = shmem_send(packbuf, (int)packed, dataType, to_rank);
        if (err != SHMEM_SUCCESS)
            laik_shmem_panic(err);

        count += packed;
        if (laik_index_isEqual(dims, &idx, &(range->to)))
            break;
    }
    assert(count == slc_size);
}

static void laik_shmem_exec_recvAndUnpack(Laik_Mapping *map, Laik_Range *range, int from_rank, uint64_t slc_size, int elemsize, int dataType)
{
    Laik_Index idx = range->from;
    int dims = range->space->dims;
    int recvCount, unpacked;
    uint64_t count = 0;
    while (1)
    {
        int err = shmem_recv(packbuf, PACKBUFSIZE / elemsize, dataType, from_rank, &recvCount);
        if (err != SHMEM_SUCCESS)
            laik_shmem_panic(err);

        unpacked = (map->layout->unpack)(map, range, &idx, packbuf, recvCount * elemsize);
        assert(recvCount == unpacked);
        count += unpacked;
        if (laik_index_isEqual(dims, &idx, &(range->to)))
            break;
    }
    assert(count == slc_size);
}
*/
static void shmem_init_secondary(Laik_Secondary* secondary)
{
    secondary->laik_secondary_prepare = laik_shmem_secondary_prepare;
    secondary->laik_secondary_exec = laik_shmem_secondary_exec;
    secondary->laik_secondary_log_action = laik_shmem_log_action;
    secondary->laik_secondary_cleanup = laik_shmem_secondary_cleanup;
    secondary->laik_secondary_finalize = laik_shmem_secondary_finalize;
    secondary->laik_secondary_update_group = laik_shmem_update_group;
}
// Secondary backend functionality
int laik_shmem_secondary_init(Laik_Instance* inst, Laik_Group* world, int primaryRank, int primarySize, int* locations , int** newLocations, int** ranks, 
                                int (*send)(int *, int, int, void*), int (*recv)(int *, int, int, void*), void* backend_data)
{  
    Laik_Secondary* shmem = malloc(sizeof(Laik_Secondary));
    Laik_Shmem_Comm* shmemg = malloc(sizeof(Laik_Shmem_Comm));
    shmem_init_secondary(shmem);
    unsigned char idx = inst->backend->chain_length++;
    shmem->chain_idx = idx;
    world -> sec_group[idx] = (Laik_Shmem_Comm*) shmemg;

    int ret = shmem_secondary_init(shmemg, primaryRank, primarySize, locations, newLocations, ranks, send, recv, backend_data);
    inst -> backend -> chain[idx] = shmem;

    return ret;
}

void laik_shmem_secondary_finalize()
{   
    shmem_finalize(NULL);
}

static void shmem_replace_reduce(Laik_ActionSeq *as, Laik_BackendAction* ba, Laik_TransitionContext* tc, Laik_Shmem_Comm* sg, int chain_idx)
{    
    int master = ba -> rank;

    unsigned char rd = 3 * ba -> h.round;
    int size, rank, numIslands;
    int* secondaryRanks;
        
    shmem_comm_rank(sg, &rank);
    shmem_comm_size(sg, &size);
    shmem_get_numIslands(sg, &numIslands);
    shmem_get_secondaryRanks(sg, &secondaryRanks);

    Laik_Data* data = tc -> data;
    Laik_Transition* t = tc->transition;
    unsigned int bufSize = 2 * data -> elemsize * ba->count;
    char *reducebuf;

    if(rank == 0)
    {
        reducebuf = def_shmem_malloc(NULL, bufSize);
        laik_shmem_addShmemReduce(as, rd, ba->fromBuf, reducebuf, ba -> count,ba -> redOp, ba->h.tid, chain_idx);
    }
    else 
    {
        Laik_A_BufSend* a;
        a = (Laik_A_BufSend*) laik_aseq_addAction(as, sizeof(*a), LAIK_AT_ShmemBufSend, rd, 0);
        a->h.chain_idx = chain_idx;
        a->count = ba->count;
        a->to_rank = 0;
        a->buf = ba -> fromBuf;
    }

    ++rd;

    if(rank == 0 ||  t->group->myid == master)
    {   
        if(numIslands == 1)
            laik_shmem_addShmemCopyToBuf(as, rd, reducebuf, ba->toBuf, ba->count, 0, master == -1 ? 0 : secondaryRanks[master], ba->h.tid, chain_idx);
        else
            laik_aseq_addGroupReduce(as, rd, ba->inputGroup, ba->outputGroup, reducebuf, ba->toBuf, ba->count, ba -> redOp);
    }
    ++rd;

    if(master == -1)
    {   
        if(rank == 0)
        {
            laik_shmem_addShmemBroadcast(as, rd, ba->toBuf, ba->count, ba->h.tid, chain_idx);
        }
        else
        {
            Laik_A_BufRecv* a;
            a = (Laik_A_BufRecv*) laik_aseq_addAction(as, sizeof(*a),
                                             LAIK_AT_ShmemBufRecv, rd, ba->h.tid);

            a->h.chain_idx = chain_idx;
            a -> count = ba -> count;
            a->from_rank = 0;
            a->buf = ba -> toBuf;
        }

    }
}

static void shmem_replace_groupReduce(Laik_ActionSeq* as, Laik_BackendAction* ba, Laik_TransitionContext* tc, Laik_Shmem_Comm* sg, int chain_idx){
    Laik_Data* data = tc -> data;

    int rank;
    shmem_comm_rank(sg, &rank);

    int rd = 3 * ba->h.round;

    int bufSize = 2 * ba -> count * data -> elemsize;
    int primaryI, primaryO;
    int inSize = laik_aseq_groupCount(as, ba->inputGroup, chain_idx);
    int outSize = laik_aseq_groupCount(as, ba->outputGroup, chain_idx);

    bool memberI = laik_aseq_isInGroup(as, ba->inputGroup, rank, chain_idx);
    primaryI = inSize > 0 ? laik_aseq_taskInGroup(as, ba->inputGroup, 0, chain_idx) : -1;
    bool memberO = laik_aseq_isInGroup(as, ba->outputGroup, rank, chain_idx);
    primaryO = outSize > 0 ? laik_aseq_taskInGroup(as, ba->outputGroup, 0,  chain_idx) : -1;


    char* reduceBuf = ba->fromBuf;

    if(memberI && inSize > 1)
    {
        if(rank == primaryI) reduceBuf =  shmem_manager_alloc(bufSize);

        laik_shmem_addGroupReduce(as, ba, rd, reduceBuf, chain_idx, primaryI);

    }
    ++rd;

    bool fullReduce = onSameIsland(as, sg, ba->inputGroup, ba->outputGroup);

    if((rank ==  primaryI && memberI) || (rank == primaryO && memberO))
    {   
        if(!fullReduce)
            laik_aseq_addGroupReduce(as, rd, ba->inputGroup, ba->outputGroup, reduceBuf, ba->toBuf, ba->count, ba -> redOp);
        else
            laik_shmem_addShmemCopyToBuf(as, rd, reduceBuf, ba->toBuf, ba->count, primaryI, primaryO, ba->h.tid, chain_idx);
    }

    ++rd;

    if(memberO && outSize > 1)
    {
        laik_shmem_addGroupBroadcast(as, ba, rd, ba -> toBuf, chain_idx, primaryO);
    }
}

void shmem_replace_MapPackAndSend(Laik_ActionSeq* as, Laik_Action* a, Laik_Shmem_Comm* sg, int chain_idx)
{
    Laik_A_MapPackAndSend* aa = (Laik_A_MapPackAndSend*) a;
    int* secondaryRanks;
    shmem_get_secondaryRanks(sg, &secondaryRanks);
    laik_shmem_addCopyMapToReceiver(as, aa->range, aa->fromMapNo, a->round, a->tid, aa->count, secondaryRanks[aa->to_rank], chain_idx);
}

void shmem_replace_MapRecvAndUnpack(Laik_ActionSeq* as, Laik_Action* a, Laik_Shmem_Comm* sg, int chain_idx)
{
    Laik_A_MapRecvAndUnpack* aa = (Laik_A_MapRecvAndUnpack*) a;
    int* secondaryRanks;
    shmem_get_secondaryRanks(sg, &secondaryRanks);
    laik_shmem_addReceiveMap(as, aa->range, aa->toMapNo, a->round, a->tid, aa->count, secondaryRanks[aa->from_rank], chain_idx);
}

void shmem_replace_MapGroupReduce(Laik_ActionSeq* as, Laik_Action* a, Laik_TransitionContext* tc, Laik_Shmem_Comm* sg, int chain_idx)
{
    Laik_Data* data = tc -> data;
    Laik_BackendAction* ba = (Laik_BackendAction*) a;

    int rank;
    shmem_comm_rank(sg, &rank);

    int rd = 3 * ba->h.round;;

    int primaryI, primaryO;
    int inSize = laik_aseq_groupCount(as, ba->inputGroup, chain_idx);
    int outSize = laik_aseq_groupCount(as, ba->outputGroup, chain_idx);

    bool memberI = laik_aseq_isInGroup(as, ba->inputGroup, rank, chain_idx);
    primaryI = inSize > 0 ? laik_aseq_taskInGroup(as, ba->inputGroup, 0, chain_idx) : -1;
    bool memberO = laik_aseq_isInGroup(as, ba->outputGroup, rank, chain_idx);
    primaryO = outSize > 0 ? laik_aseq_taskInGroup(as, ba->outputGroup, 0,  chain_idx) : -1;


    char* fromBuf = rank == primaryI ? shmem_manager_alloc(ba -> count * data -> elemsize) : NULL;
    char* toBuf = rank == primaryO ? shmem_manager_alloc(ba -> count * data->elemsize) : NULL;

    bool fullReduce = onSameIsland(as, sg, ba->inputGroup, ba->outputGroup);
    if(memberI)
    {
        laik_shmem_addMapGroupReduce(as, ba, rd, primaryI, fromBuf, chain_idx);
    }

    if(rank == primaryI || rank == primaryO)
    {   
        if(!fullReduce)
            laik_aseq_addGroupReduce(as, rd + 1, ba->inputGroup, ba->outputGroup, fromBuf, toBuf, ba->count, ba -> redOp);
        else
            laik_shmem_addShmemCopyToBuf(as, rd + 1, fromBuf, toBuf, ba->count, primaryI, primaryO, ba->h.tid, chain_idx);
    }

    if(memberO)
    {
        laik_shmem_addMapBroadcast(as, ba, rd + 2, primaryO , toBuf, chain_idx); 
    }

}

void laik_shmem_secondary_cleanup()
{   
    laik_log(1, "Shared Memory Backend Cleanup");
    shmem_manager_cleanup();
   
}
//https://graphics.stanford.edu/~seander/bithacks.html#IntegerMinOrMaxS
static int max(int x , int y)
{
    return x ^ ((x ^ y) & -(x < y));
}

bool laik_shmem_secondary_prepare(const Laik_Secondary* this, Laik_ActionSeq *as)
{
    Laik_TransitionContext* tc = as->context[0];
    Laik_Shmem_Comm* sg = (Laik_Shmem_Comm*) tc->transition->group->sec_group[this->chain_idx];
    int size, rank;
    shmem_comm_size(sg, &size);
    shmem_comm_rank(sg, &rank);
    int* colours;
    shmem_get_colours(sg, &colours);
    int *secondaryRanks;
    shmem_get_secondaryRanks(sg, &secondaryRanks);

    
    Laik_Transition* t = tc->transition;

    int pRank = t->group->myid;
    const int chain_idx = this -> chain_idx;

    shmem_transformSubGroup(as, sg, chain_idx);

    bool global_ret = false;
    bool ret = false;
    Laik_Action *a = as->action;
    for (unsigned int i = 0; i < as->actionCount; i++, a = nextAction(a))
    {
        ret = false;
        switch (a->type)
        {
        /*
        case LAIK_AT_MapSend:
        {
            Laik_BackendAction *ba = (Laik_BackendAction *)a;
            if(colours[pRank] == colours[ba->rank]){
                ba = (Laik_BackendAction*) laik_aseq_addr(a, as, 3 * a->round, chain_idx);
                ba->rank = secondaryRanks[ba->rank];
                ba->h.type = LAIK_AT_ShmemMapSend;
                ret = true;
            }
            break;
        }
        */
        case LAIK_AT_GroupReduce:
        {   
            Laik_BackendAction* ba = (Laik_BackendAction*) a;
            shmem_replace_groupReduce(as, ba, tc, sg, chain_idx);
            ret = true;
            break;
        }
        /*
        case LAIK_AT_Reduce:
        {
            Laik_BackendAction* ba = (Laik_BackendAction*) a;
            shmem_replace_reduce(as, ba, tc, sg, chain_idx);
            ret = true;
            break;
        }
        case LAIK_AT_BufSend:
        {
            Laik_A_BufSend *aa = (Laik_A_BufSend *)a;
            if(colours[pRank] == colours[aa->to_rank]){
                aa = (Laik_A_BufSend*) laik_aseq_addr(a, as, 3 * a->round, chain_idx);
                aa->to_rank = secondaryRanks[aa->to_rank];
                aa->h.type = LAIK_AT_ShmemBufSend;
                ret = true;
            }
            break;
        }
        case LAIK_AT_MapRecv:
        {
            Laik_BackendAction *ba = (Laik_BackendAction *)a;
            if(colours[pRank] == colours[ba->rank]){
                ba = (Laik_BackendAction*) laik_aseq_addr(a, as, 3 * a->round, chain_idx);
                ba->rank = secondaryRanks[ba->rank];
                ba->h.type = LAIK_AT_ShmemMapRecv;
                ret = true;
            }
            break;
        }
        case LAIK_AT_BufRecv:
        {
            Laik_A_BufRecv *aa = (Laik_A_BufRecv *)a;
            if(colours[pRank] == colours[aa->from_rank]){
                aa = (Laik_A_BufRecv*) laik_aseq_addr(a, as, 3 * a->round, chain_idx);
                aa->from_rank = secondaryRanks[aa->from_rank];
                aa->h.type = LAIK_AT_ShmemBufRecv;
                ret = true;
            }
            break;
        }
        */
        case LAIK_AT_MapPackAndSend:
        {
            Laik_A_MapPackAndSend *aa = (Laik_A_MapPackAndSend *)a;
            if(colours[pRank] == colours[aa->to_rank]){
                shmem_replace_MapPackAndSend(as, a, sg, chain_idx);
                /*
                aa = (Laik_A_MapPackAndSend*) laik_aseq_addr(a, as, 3 * a->round, chain_idx);
                aa->to_rank = secondaryRanks[aa->to_rank];
                aa->h.type = LAIK_AT_ShmemMapPackAndSend;
                */
                ret = true;
            }
            break;
        }
        /*
        case LAIK_AT_PackAndSend:
        {
            Laik_BackendAction *ba = (Laik_BackendAction *)a;
            if(colours[pRank] == colours[ba->rank]){
                ba = (Laik_BackendAction*) laik_aseq_addr(a, as, 3 * a->round, chain_idx);
                ba->rank = secondaryRanks[ba->rank];
                ba->h.type = LAIK_AT_ShmemPackAndSend;
                ret = true;
            }
            break;
        }
        */
        case LAIK_AT_MapRecvAndUnpack:
        {
            Laik_A_MapRecvAndUnpack *aa = (Laik_A_MapRecvAndUnpack *)a;
            if(colours[pRank] == colours[aa->from_rank]){
                shmem_replace_MapRecvAndUnpack(as, a, sg, chain_idx);
                /*
                aa = (Laik_A_MapRecvAndUnpack*) laik_aseq_addr(a, as, 3 * a->round, chain_idx);
                aa->from_rank = secondaryRanks[aa->from_rank];
                aa->h.type = LAIK_AT_ShmemMapRecvAndUnpack;
                */
                ret = true;
            }
            break;
        }
        /*
        case LAIK_AT_RecvAndUnpack:
        {
            Laik_BackendAction *ba = (Laik_BackendAction *)a;
            if(colours[pRank] == colours[ba->rank]){
                ba = (Laik_BackendAction*) laik_aseq_addr(a, as, 3 * a->round, chain_idx);
                ba->rank = secondaryRanks[ba->rank];
                ba->h.type = LAIK_AT_ShmemRecvAndUnpack;
                ret = true;
            }
            break;
        }
        case LAIK_AT_RBufSend:
        {
            Laik_A_RBufSend *aa = (Laik_A_RBufSend *)a;
            if(colours[rank] == colours[aa->to_rank]){
                aa = (Laik_A_RBufSend *) laik_aseq_addr(a, as, 3 * a->round, chain_idx);
                aa->to_rank = secondaryRanks[aa->to_rank];
                aa->h.type = LAIK_AT_ShmemRBufSend;
                ret = true;
            }
            break;
        }
        case LAIK_AT_RBufRecv:
        {
            Laik_A_RBufRecv *aa = (Laik_A_RBufRecv *)a;
            if(colours[rank] == colours[aa->from_rank]){
                aa = (Laik_A_RBufRecv *) laik_aseq_addr(a, as, 3 * a->round, chain_idx);
                aa->from_rank = secondaryRanks[aa->from_rank];
                aa->h.type = LAIK_AT_ShmemRBufRecv;
                ret = true;
            }
            break;
        }
        */
        case LAIK_AT_MapGroupReduce:
        {
            shmem_replace_MapGroupReduce(as, a, tc, sg, chain_idx);
            ret = true;
            break;
        }
        default:
            break;
        }

        global_ret |= ret;

        if(!ret)
            laik_aseq_add(a, as, 3 * a->round);


        
    }
    laik_aseq_activateNewActions(as);
    return global_ret;
}

bool laik_shmem_secondary_exec(const Laik_Secondary* this, Laik_ActionSeq *as, Laik_Action *a)
{
    
    Laik_TransitionContext *tc = as->context[0];
    Laik_Group* g = tc->transition->group;
    Laik_Data* data = tc->data;

    int err;
    switch (a->type)
    {
    case LAIK_AT_ShmemGroupBroadcast:
    {
        assert(a->chain_idx == this ->chain_idx);
        laik_shmem_exec_GroupBroadCast(a, as, tc, g);
        break;
    }
    case LAIK_AT_ShmemGroupReduce:
    {   
        assert(a->chain_idx == this ->chain_idx);
        laik_shmem_exec_GroupReduce(a, as, tc, g);
        break;
    }
    case LAIK_AT_ShmemCopyToBuf:
    {
        assert(a->chain_idx == this ->chain_idx);
        laik_shmem_exec_CopyToBuf(a, tc, g);
        break;
    }
    case LAIK_AT_ShmemReduce:
    {
        assert(a->chain_idx == this ->chain_idx);
        laik_shmem_exec_Reduce(a, tc, g);
        break;
    }
    case LAIK_AT_ShmemBroadcast:
    {
        assert(a->chain_idx == this ->chain_idx);
        laik_shmem_exec_Broadcast(a, tc, g);
        break;
    }
    case LAIK_AT_ShmemMapGroupReduce:
    {
        assert(a->chain_idx == this ->chain_idx);
        laik_shmem_exec_MapGroupReduce(as, a, tc, g);
        break;
    }
    case LAIK_AT_ShmemMapBroadcast:
    {
        assert(a->chain_idx == this ->chain_idx);
        laik_shmem_exec_MapBroadCast(as, a, tc, g);
        break;
    }
    /*
    case LAIK_AT_ShmemMapSend:
    {
        assert(ba->fromMapNo < fromList->count);
        Laik_Mapping *fromMap = &(fromList->map[ba->fromMapNo]);
        assert(fromMap->base != 0);
        err = shmem_send(fromMap->base + ba->offset, ba->count, dType, ba->rank);
        if (err != SHMEM_SUCCESS)
            laik_shmem_panic(err);
        break;
    }
    */
    case LAIK_AT_ShmemBufSend:
    {
        assert(a->chain_idx == this ->chain_idx);
        Laik_A_BufSend* aa = (Laik_A_BufSend*) a;
        err = shmem_send(aa->buf, aa->count, data->elemsize, aa->to_rank, (Laik_Shmem_Comm*)g->sec_group[a->chain_idx], g->backend_data);
        if (err != SHMEM_SUCCESS) 
            laik_shmem_panic(err);
        break;
    }
    /*
    case LAIK_AT_ShmemMapRecv:
    {
        assert(ba->toMapNo < toList->count);
        Laik_Mapping *toMap = &(toList->map[ba->toMapNo]);
        assert(toMap->base != 0);
        err = shmem_recv(toMap->base + ba->offset, ba->count, dType, ba->rank, &count);
        if (err != SHMEM_SUCCESS)
            laik_shmem_panic(err);
        assert((int)ba->count == count);
        break;
    }
    */
    case LAIK_AT_ShmemBufRecv:
    {
        assert(a->chain_idx == this ->chain_idx);
        int count;
        Laik_A_BufRecv* aa = (Laik_A_BufRecv*) a;
        err = shmem_recv(aa->buf, aa->count, data->elemsize, aa->from_rank, &count, (Laik_Shmem_Comm*)g->sec_group[a->chain_idx], g->backend_data);
        if (err != SHMEM_SUCCESS) 
            laik_shmem_panic(err);
        //assert((int)ba->count == count);
        break;
    }
    /*
    case LAIK_AT_ShmemMapPackAndSend:
    {
        Laik_A_MapPackAndSend *aa = (Laik_A_MapPackAndSend *)a;
        assert(aa->fromMapNo < fromList->count);
        Laik_Mapping *fromMap = &(fromList->map[aa->fromMapNo]);
        assert(fromMap->base != 0);
        laik_shmem_exec_packAndSend(fromMap, aa->range, aa->to_rank, aa->count, dType);
        break;
    }
    case LAIK_AT_ShmemPackAndSend:
    {
        laik_shmem_exec_packAndSend(ba->map, ba->range, ba->rank, (uint64_t)ba->count, dType);
        break;
    }
    case LAIK_AT_ShmemMapRecvAndUnpack:
    {
        Laik_A_MapRecvAndUnpack *aa = (Laik_A_MapRecvAndUnpack *)a;
        assert(aa->toMapNo < toList->count);
        Laik_Mapping *toMap = &(toList->map[aa->toMapNo]);
        assert(toMap->base);
        laik_shmem_exec_recvAndUnpack(toMap, aa->range, aa->from_rank, aa->count, elemsize, dType);
        break;
    }
    case LAIK_AT_ShmemRecvAndUnpack:
    {
        laik_shmem_exec_recvAndUnpack(ba->map, ba->range, ba->rank, (uint64_t)ba->count, elemsize, dType);
        break;
    }
    case LAIK_AT_ShmemRBufSend:
    {
        Laik_A_RBufSend *aa = (Laik_A_RBufSend *)a;
        assert(aa->bufID < ASEQ_BUFFER_MAX);
        err = shmem_send(as->buf[aa->bufID] + aa->offset, aa->count, dType, aa->to_rank);
        if (err != SHMEM_SUCCESS)
            laik_shmem_panic(err);
        break;
    }
    case LAIK_AT_ShmemRBufRecv:
    {
        Laik_A_RBufRecv *aa = (Laik_A_RBufRecv *)a;
        assert(aa->bufID < ASEQ_BUFFER_MAX);
        err = shmem_recv(as->buf[aa->bufID] + aa->offset, aa->count, dType, aa->from_rank, &count);
        if (err != SHMEM_SUCCESS)
            laik_shmem_panic(err);
        assert((int)ba->count == count);
        break;
    }
    */
    case LAIK_AT_ShmemCopyMapToReceiver:
    {
        assert(a->chain_idx == this ->chain_idx);
        laik_shmem_exec_CopyMapToReceiver(a, tc, g);
        break;
    }
    case LAIK_AT_ShmemReceiveMap:
    {
        assert(a->chain_idx == this ->chain_idx);
        laik_shmem_exec_ReceiveMap(a, tc, g);
        break;
    }
    default:
        laik_log(LAIK_LL_Panic, "shmem_secondary_exec: no idea how to exec action %d (%s)",
                     a->type, laik_at_str(a->type));
        return false;
    }
    return true;
}

bool laik_shmem_log_action(const Laik_Secondary* this, Laik_ActionSeq* as, Laik_Action *a){
    switch (a->type)
    {
    case LAIK_AT_ShmemMapSend:
    {
        laik_log_append("ShmemMapSend");
        break;
    }
    case LAIK_AT_ShmemBufSend:
    {
        Laik_A_BufSend* aa = (Laik_A_BufSend*) a;
        laik_log_append(": from %p, count %d ==> S%d (shmem)", 
                        aa->buf, 
                        aa->count, 
                        aa->to_rank);
        break;
    }
    case LAIK_AT_ShmemMapRecv:
    {
        laik_log_append("ShmemMapRecv");
        break;
    }
    case LAIK_AT_ShmemBufRecv:
    {
        Laik_A_BufRecv* aa = (Laik_A_BufRecv*) a;
        laik_log_append(": S%d ==> to %p, count %d (shmem)",
                        aa->from_rank,
                        aa->buf,
                        aa->count);
        break;
    }
    case LAIK_AT_ShmemMapPackAndSend:
    {
        Laik_A_MapPackAndSend* aa = (Laik_A_MapPackAndSend*) a;
        laik_log_append(": ");
        laik_log_Range(aa->range);
        laik_log_append(" mapNo %d, count %llu ==> S%d (shmem)",
                        aa->fromMapNo, (unsigned long long) aa->count, aa->to_rank);
        break;
    }
    case LAIK_AT_ShmemPackAndSend:
    {
        laik_log_append("ShmemPackAndSend");
        break;
    }
    case LAIK_AT_ShmemMapRecvAndUnpack:
    {
        Laik_A_MapRecvAndUnpack* aa = (Laik_A_MapRecvAndUnpack*) a;
        laik_log_append(": S%d ==> ", aa->from_rank);
        laik_log_Range(aa->range);
        laik_log_append(" mapNo %d, count %llu (shmem)",
                        aa->toMapNo, (unsigned long long) aa->count);
        break;
    }
    case LAIK_AT_ShmemRecvAndUnpack:
    {
        laik_log_append("ShmemPackAndSend");
        break;
    }
    case LAIK_AT_ShmemReduce:
    {
        laik_log_append("ShmemReduce");
        break;
    }
    case LAIK_AT_ShmemBroadcast:
    {
        laik_log_append("ShmemBroadcast");
        break;
    }
    case LAIK_AT_ShmemCopyToBuf:
    {
        Laik_A_ShmemCopyToBuf* aa = (Laik_A_ShmemCopyToBuf*) a;
        laik_log_append("ShmemCopyToBuf:");
        laik_log_append("T%d ==> T%d", aa->sender, aa->receiver);
        break;
    }
    case LAIK_AT_ShmemGroupReduce:
    {
        Laik_A_ShmemGroupReduce* aa = (Laik_A_ShmemGroupReduce*) a;
        laik_log_append("ShmemGroupReduce: ");
        laik_log_TaskGroupAS(as, aa->subgroup, a->chain_idx);
        laik_log_append("==> T%d", laik_aseq_taskInGroup(as, aa->subgroup, 0, a->chain_idx));
        break;
    }
    case LAIK_AT_ShmemGroupBroadcast:
    {
        Laik_A_ShmemGroupBroadCast* aa = (Laik_A_ShmemGroupBroadCast*) a;
        laik_log_append("ShmemGroupBroadCast: ");
        laik_log_append("T%d ==>", laik_aseq_taskInGroup(as, aa->subgroup, 0, a->chain_idx));
        break;
    }
    case LAIK_AT_ShmemCopyMapToReceiver:
    {
        Laik_A_ShmemCopyMapToReceiver* aa = (Laik_A_ShmemCopyMapToReceiver*) a;
        laik_log_append("ShmemCopyMapToReceiver: ");
        laik_log_Range(aa->range);
        laik_log_append(" mapNo %d, count %llu ==> S%d",
                        aa->mapNo, (unsigned long long) aa->count, aa->to_rank);
        break;
    }
    case LAIK_AT_ShmemReceiveMap:
    {
        Laik_A_ShmemReceiveMap* aa = (Laik_A_ShmemReceiveMap*) a;
        laik_log_append("ShmemReceiveMap: S%d ==> ", aa->from_rank);
        laik_log_Range(aa->range);
        laik_log_append(" mapNo %d, count %llu",
                        aa->mapNo, (unsigned long long) aa->count);
        break;
    }
    case LAIK_AT_ShmemMapGroupReduce:
    {
        Laik_A_ShmemMapGroupReduce* aa = (Laik_A_ShmemMapGroupReduce*)a;
        laik_log_append("ShmemMapGroupReduce: ");
        laik_log_TaskGroupAS(as, aa->subgroup, this->chain_idx);
        laik_log_append("==> T%d", laik_aseq_taskInGroup(as, aa->subgroup, 0, a->chain_idx));
        break;
    }
    case LAIK_AT_ShmemMapBroadcast:
    {   
        
        Laik_A_ShmemMapBroadCast* aa = (Laik_A_ShmemMapBroadCast*) a;
        laik_log_append("ShmemMapBroadCast: ");
        laik_log_append("T%d", laik_aseq_taskInGroup(as, aa->subgroup, 0, a->chain_idx));
        laik_log_TaskGroupAS(as, aa->subgroup, a->chain_idx);
        break;
    }
    default:
        return false;
    }
    return true;
}
//#endif
