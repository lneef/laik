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
#include "laik/action.h"
#include "laik/backend.h"
#include "laik/core-internal.h"
#include "laik/core.h"
#include "laik/data.h"
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
static void laik_shmem_secondary_finalize(Laik_Inst_Data*, Laik_Instance*);
static void laik_shmem_secondary_prepare(Laik_Inst_Data*, Laik_ActionSeq *);
static void laik_shmem_secondary_exec(Laik_Inst_Data*, Laik_ActionSeq *);
static void laik_shmem_secondary_cleanup(Laik_Inst_Data*, Laik_ActionSeq*);
static bool laik_shmem_log_action(Laik_Inst_Data*, Laik_ActionSeq*, Laik_Action *);
static void laik_shmem_update_group(Laik_Inst_Data*, Laik_Group*, int*, int);
static void laik_shmem_finish_resize(Laik_Inst_Data*);
static Laik_Allocator* shmem_allocator();

static Laik_Backend shmem_backend = {
    .finalize = laik_shmem_secondary_finalize,
    .exec = laik_shmem_secondary_exec,
    .prepare = laik_shmem_secondary_prepare,
    .cleanup = laik_shmem_secondary_cleanup,
    .log_action = laik_shmem_log_action,
    .updateGroup = laik_shmem_update_group,
    .allocator = shmem_allocator,
    .finish_resize = laik_shmem_finish_resize
};

static Laik_Allocator* shmemAllocator;
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

//-----------------------------------------------------------------------------
// resizing and group update
void laik_shmem_finish_resize(Laik_Inst_Data* idata)
{
    (void)idata;
}

void laik_shmem_update_group(Laik_Inst_Data* idata, Laik_Group* g, int* ranks, int size)
{   
    if(g->myid < 0) return;
    // callc shmem comm init
    Laik_Shmem_Comm* sgOld = g->parent->backend_data[idata->index];
    Laik_Shmem_Comm* sg = malloc(sizeof(Laik_Shmem_Comm));
    *sg = *sgOld;
    int* new_ranks;
    shmem_init_comm(sg, g, idata, ranks, &new_ranks, size);
    g->backend_data[idata->index] = sg;

    /*
    return;
    
    if(g->myid < 0) return;
    Laik_Shmem_Comm* sg = malloc(sizeof(Laik_Shmem_Comm));
    Laik_Group* old = g->parent;
    Laik_Shmem_Comm* sgOld = (Laik_Shmem_Comm*) old->backend_data[idata->index];
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
    g->backend_data[idata->index] = sg;

    for(int i = 0; i < g->size; ++i)
    {
        laik_log_append("%d", sg->divsion[i]);
    }
    laik_log_flush("");

    //call next layer
    */
    laik_next_updateGroup(idata, g, new_ranks, sg->size);
}

static int sendIntegersShmem(int* buffer, int count, int receiver, Laik_Inst_Data* idata, Laik_Group* g)
{
    return SEND_INTS(buffer, count, receiver, idata, g);
}

static int recvIntegersShmem(int* buffer, int count, int sender, Laik_Inst_Data* idata, Laik_Group* g)
{
    return RECV_INTS(buffer, count, sender, idata, g);
}
// Secondary backend functionality
int laik_shmem_secondary_init(Laik_Instance* inst, Laik_Group* world, int* primaryRank, int* primarySize, int* ranks, int** new_ranks)
{  
    unsigned int index = inst->num_backends++;
    Laik_Inst_Data* idata = laik_add_inst_data(inst, NULL, &shmem_backend);
    idata->recv = recvIntegersShmem;
    idata->send = sendIntegersShmem;
    assert(index == idata->index);
    
    Laik_Shmem_Comm* shmemg = malloc(sizeof(Laik_Shmem_Comm));
    world -> backend_data[index] = shmemg;
    int ret = shmem_secondary_init(shmemg, idata, world, *primaryRank, *primarySize, ranks, new_ranks);

    *primarySize = shmemg->size;

    *new_ranks =shmemg->primaryRanks;

    return ret;
}

void laik_shmem_secondary_finalize(Laik_Inst_Data* idata, Laik_Instance* inst)
{   
    laik_log(1, "Shmem finalize");
    shmem_finalize(NULL);

    free(idata->backend_data);

    //call next layer 
    laik_next_finalize(idata, inst);
}

static void shmem_replace_groupReduce(Laik_ActionSeq* as, Laik_BackendAction* ba, Laik_TransitionContext* tc, Laik_Shmem_Comm* sg, int chain_idx){
    Laik_Data* data = tc -> data;

    int rank = laik_myid(tc->transition->group);

    int rd = 3 * ba->h.round;

    int bufSize = ba -> count * data -> elemsize;
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

    bool fullReduce = onSameIsland(as, sg, ba->inputGroup, ba->outputGroup, chain_idx);

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
    laik_shmem_addCopyMapToReceiver(as, aa->range, aa->fromMapNo, a->round, a->tid, aa->count, aa->to_rank, chain_idx);
}

void shmem_replace_MapRecvAndUnpack(Laik_ActionSeq* as, Laik_Action* a, Laik_Shmem_Comm* sg, int chain_idx)
{
    Laik_A_MapRecvAndUnpack* aa = (Laik_A_MapRecvAndUnpack*) a;
    laik_shmem_addReceiveMap(as, aa->range, aa->toMapNo, a->round, a->tid, aa->count, aa->from_rank, chain_idx);
}

void shmem_replace_MapGroupReduce(Laik_ActionSeq* as, Laik_Action* a, Laik_TransitionContext* tc, Laik_Shmem_Comm* sg, int chain_idx)
{
    Laik_Data* data = tc -> data;
    Laik_BackendAction* ba = (Laik_BackendAction*) a;

    int rank = laik_myid(tc->transition->group);

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

    bool fullReduce = onSameIsland(as, sg, ba->inputGroup, ba->outputGroup,chain_idx) && false;
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

void laik_shmem_secondary_cleanup(Laik_Inst_Data* idata, Laik_ActionSeq* as)
{   
    laik_log(1, "Shared Memory Backend Cleanup");
    shmem_manager_cleanup();

    // call next layer for cleanup
    laik_next_cleanup(idata, as);
   
}
//https://graphics.stanford.edu/~seander/bithacks.html#IntegerMinOrMaxS
static int max(int x , int y)
{
    return x ^ ((x ^ y) & -(x < y));
}

void laik_shmem_secondary_prepare(Laik_Inst_Data* idata, Laik_ActionSeq *as)
{
    Laik_TransitionContext* tc = as->context[0];
    Laik_Shmem_Comm* sg = (Laik_Shmem_Comm*) tc->transition->group->backend_data[idata->index];


    Laik_Action *a = as->action;
    const int chain_idx = idata->index;
    /*
    for(unsigned i = 0; i < as->actionCount; i++, a = nextAction(a))
    {
        if(a->chain_idx != idata->index - 1) continue;
        switch(a->type)
        {
            case LAIK_AT_MapPackAndSend:
            {
                Laik_A_MapPackAndSend *aa = (Laik_A_MapPackAndSend *)a;
                if(sg->colour == colours[aa->to_rank])
                {
                    aa->to_rank = secondaryRanks[aa->to_rank];
                    a->chain_idx = idata->index;
                }

                break;
            }
            case LAIK_AT_MapRecvAndUnpack:
            {
                Laik_A_MapRecvAndUnpack* aa = (Laik_A_MapRecvAndUnpack*)a;
                if(sg->colour == colours[aa->from_rank])
                {
                    aa->from_rank = secondaryRanks[aa->from_rank];
                    a->chain_idx = idata->index;
                }
                break;
            }
            default:
                break;
        }
    }
    */

    shmem_transformSubGroup(as, sg, chain_idx);

    // at first go down the chain
    // call next layer for preparation
    laik_next_prepare(idata, as);

    bool changed = false;
    bool ret = false;
    a = as->action;
    for (unsigned int i = 0; i < as->actionCount; i++, a = nextAction(a))
    {
        ret = false;
        switch (a->type)
        {
        case LAIK_AT_GroupReduce:
        {   
            Laik_BackendAction* ba = (Laik_BackendAction*) a;
            shmem_replace_groupReduce(as, ba, tc, sg, chain_idx);
            ret = true;
            break;
        }
        case LAIK_AT_MapPackAndSend:
        {
            Laik_A_MapPackAndSend* aa = (Laik_A_MapPackAndSend*) a;
            if(sg->colour == sg->divsion[aa->to_rank]){
                shmem_replace_MapPackAndSend(as, a, sg, chain_idx);
                ret = true;
            }
            break;
        }
        case LAIK_AT_MapRecvAndUnpack:
        {
            Laik_A_MapRecvAndUnpack* aa = (Laik_A_MapRecvAndUnpack*) a;
            if(sg->colour == sg->divsion[aa->from_rank]){
                shmem_replace_MapRecvAndUnpack(as, a, sg, chain_idx);
                ret = true;
            }
            break;
        }
        case LAIK_AT_MapGroupReduce:
        {
            // should not be here if there is a backend below
            shmem_replace_MapGroupReduce(as, a, tc, sg, chain_idx);
            ret = true;
            break;
        }
        default:
            break;
        }

        changed |= ret;
        if(!ret)
            laik_aseq_add(a, as, 3 * a->round);


        
    }
    laik_aseq_activateNewActions(as);
    laik_log_ActionSeqIfChanged(changed, as, "After shmem prepare");
}

void laik_shmem_secondary_exec(Laik_Inst_Data* idata, Laik_ActionSeq *as)
{
    
    Laik_TransitionContext *tc = as->context[0];
    Laik_Group* g = tc->transition->group;
    unsigned int index = idata->index;

    // primary backend should have called laik_aseq_begin
    Laik_Action* a = as->currentAction;
    for(; laik_aseq_hasNext(as); a = laik_aseq_next(as))
    {
        if(a->chain_idx > idata->index) a = laik_next_exec(idata, as);

        if(!laik_aseq_hasNext(as)) return;

        if(a->chain_idx < idata->index) return;

        if(laik_log_begin(1))
        {
            laik_shmem_log_action(idata, as, a);
            laik_log_flush(0);
        }

        switch (a->type)
        {
        case LAIK_AT_ShmemGroupBroadcast:
        {
            assert(a->chain_idx == index);
            laik_shmem_exec_GroupBroadCast(a, as, tc, idata, g);
            break;
        }
        case LAIK_AT_ShmemGroupReduce:
        {   
            assert(a->chain_idx == index);
            laik_shmem_exec_GroupReduce(a, as, tc, idata, g);
            break;
        }
        case LAIK_AT_ShmemCopyToBuf:
        {
            assert(a->chain_idx == index);
            laik_shmem_exec_CopyToBuf(a, tc, idata, g);
            break;
        }
        case LAIK_AT_ShmemMapGroupReduce:
        {
            assert(a->chain_idx == index);
            laik_shmem_exec_MapGroupReduce(as, a, tc, idata, g);
            break;
        }
        case LAIK_AT_ShmemMapBroadcast:
        {
            assert(a->chain_idx == index);
            laik_shmem_exec_MapBroadCast(as, a, tc, idata, g);
            break;
        }
        case LAIK_AT_ShmemCopyMapToReceiver:
        {
            assert(a->chain_idx == index);
            laik_shmem_exec_CopyMapToReceiver(a, tc, idata, g);
            break;
        }
        case LAIK_AT_ShmemReceiveMap:
        {
            assert(a->chain_idx == index);
            laik_shmem_exec_ReceiveMap(a, tc, idata, g);
            break;
        }
        default:
            laik_log(LAIK_LL_Panic, "shmem_secondary_exec: no idea how to exec action %d (%s)",
                         a->type, laik_at_str(a->type));
        }
    }
}

bool laik_shmem_log_action(Laik_Inst_Data* idata, Laik_ActionSeq* as, Laik_Action *a){
    
    if(a->chain_idx > idata->index)
    {
        return laik_next_log(idata, as, a);
    }
    // action must be added by this backend
    assert(idata->index == a->chain_idx);

    switch (a->type)
    {
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
        laik_log_TaskGroupAS(as, aa->subgroup, a->chain_idx);
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
        laik_log_TaskGroupAS(as, aa->subgroup, a->chain_idx);
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

Laik_Allocator* shmem_allocator()
{
    laik_log(1, "Allocator of shmem backend chosen for laik");
    shmemAllocator = laik_new_allocator(def_shmem_malloc, def_shmem_free, 0);
    shmemAllocator->policy = LAIK_MP_NewAllocOnRepartition;
    return shmemAllocator;
}
//#endif
