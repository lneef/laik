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

#include "backends/shmem/shmem-cpybuf.h"
#include "backends/shmem/shmem-manager.h"
#include "laik-internal.h"
#include "laik.h"
#include "laik-backend-shmem.h"
#include "backends/shmem/shmem.h"
#include "backends/shmem/shmem-allocator.h"
#include "backends/shmem/shmem-actions.h"
#include "laik/action-internal.h"
#include "laik/action.h"
#include "laik/space.h"

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
static void laik_shmem_update_group(Laik_Inst_Data*, Laik_Group*, int, int);

static Laik_Backend shmem_backend = {
    .finalize = laik_shmem_secondary_finalize,
    .exec = laik_shmem_secondary_exec,
    .prepare = laik_shmem_secondary_prepare,
    .cleanup = laik_shmem_secondary_cleanup,
    .log_action = laik_shmem_log_action,
    .updateGroup = laik_shmem_update_group,
    .allocator = shmem_allocator,
};

//-----------------------------------------------------------------------------
// resizing and group update

void laik_shmem_update_group(Laik_Inst_Data* idata, Laik_Group* g, int rank, int size)
{   
    if(g->myid < 0) return;
    // callc shmem comm init
    Laik_Shmem_Comm* sgOld = g->parent->backend_data[idata->index];
    Laik_Shmem_Comm* sg = malloc(sizeof(Laik_Shmem_Comm));
    *sg = *sgOld;
    shmem_init_comm(sg, g, idata, rank, size);
    g->backend_data[idata->index] = sg;
    laik_next_updateGroup(idata, g, sg->myid, sg->size);
}

static int sendIntegersShmem(int* buffer, int count, int receiver, Laik_Inst_Data* idata, Laik_Group* g)
{
    Laik_Shmem_Comm* sg = g->backend_data[idata->index];
    return SEND_INTS(buffer, count, sg->primaryRanks[receiver], idata, g);
}

static int recvIntegersShmem(int* buffer, int count, int sender, Laik_Inst_Data* idata, Laik_Group* g)
{
    Laik_Shmem_Comm* sg = g->backend_data[idata->index];
    return RECV_INTS(buffer, count, sg->primaryRanks[sender], idata, g);
}
// Secondary backend functionality
int laik_shmem_secondary_init(Laik_Instance* inst, Laik_Group* world, int* primarySize, int* rank)
{  
    inst->num_backends++;
    Laik_Inst_Data* idata = laik_add_inst_data(inst, NULL, &shmem_backend);
    idata->recv = recvIntegersShmem;
    idata->send = sendIntegersShmem;
    
    Laik_Shmem_Comm* shmemg = malloc(sizeof(Laik_Shmem_Comm));
    world -> backend_data[idata->index] = shmemg;
    int ret = shmem_secondary_init(shmemg, idata, world, *primarySize, *rank);

    *primarySize = shmemg->size;
    *rank = shmemg->myid;

    return ret;
}

void laik_shmem_secondary_finalize(Laik_Inst_Data* idata, Laik_Instance* inst)
{   
    laik_log(1, "Shmem finalize");
    shmem_finalize();

    free(idata->backend_data);

    //call next layer 
    laik_next_finalize(idata, inst);
}

static void shmem_replace_groupReduce(Laik_ActionSeq* as, Laik_BackendAction* ba, Laik_Shmem_Comm* sg, int chain_idx){

    int rank = sg->myid;

    int rd = 3 * ba->h.round;

    int primaryI, primaryO;
    int inSize = laik_aseq_groupCount(as, ba->inputGroup, chain_idx);
    int outSize = laik_aseq_groupCount(as, ba->outputGroup, chain_idx);

    bool memberI = laik_aseq_isInGroup(as, ba->inputGroup, rank, chain_idx);
    primaryI = inSize > 0 ? laik_aseq_taskInGroup(as, ba->inputGroup, 0, chain_idx) : -1;
    bool memberO = laik_aseq_isInGroup(as, ba->outputGroup, rank, chain_idx);
    primaryO = outSize > 0 ? laik_aseq_taskInGroup(as, ba->outputGroup, 0,  chain_idx) : -1;


    char* reduceBuf = ba->fromBuf;
    bool fullReduce = onSameIsland(as, sg, ba->inputGroup, ba->outputGroup, chain_idx);
    if(memberI && inSize > 1)
    {
        laik_shmem_addGroupReduce(as, ba, rd, reduceBuf, chain_idx, primaryI);

    }
    ++rd;

    if((rank == primaryI && memberI) || (rank == primaryO && memberO))
    {   
        if(!fullReduce)
            laik_aseq_addGroupReduce(as, rd, ba->inputGroup, ba->outputGroup, reduceBuf, ba->toBuf, ba->count, ba -> redOp);
        //else
            //laik_shmem_addShmemCopyToBuf(as, rd, reduceBuf, ba->toBuf, ba->count, primaryI, primaryO, ba->h.tid, chain_idx);
    }

    ++rd;

    if(memberO && outSize > 1)
    {
        laik_shmem_addGroupBroadcast(as, ba, rd, ba -> toBuf, chain_idx, primaryO);
    }
}

bool shmem_replace_MapPackAndSend(Laik_ActionSeq* as, Laik_Action* a, Laik_TransitionContext* tc, Laik_Shmem_Data* sd, int chain_idx)
{
    Laik_A_MapPackAndSend* aa = (Laik_A_MapPackAndSend*) a;
    Laik_Data* data = tc->data;
    int rd = 3 * a->round;
    int header_size = 0;
    if(tc->fromList)
    {
        
        Laik_Mapping* m = &tc->fromList->map[aa->fromMapNo];     
        if(is_shmem_allocator(m->allocator) && sd->copyScheme != 2)
        {
            if(shmem_manager_zeroCopy(m->header) && sd->copyScheme == 0)
            {
                return true;
            }
            //was allocated using shmem allocator
            int shmid = shmem_manager_shmid(m->header);
            
            laik_shmem_addOneCopyMap(as, aa->fromMapNo, shmid, aa->to_rank, rd, a->tid, chain_idx); 
            
            return false;
        }else {
            header_size = m->layout->header_size;
        }
    }

#define LEAST_HEADER_SIZE 64
    shmem_cpybuf_request(&sd->cpybuf, aa->count * data->elemsize + header_size == 0 ? LEAST_HEADER_SIZE : header_size);


    laik_shmem_addTwoCopyMap(as, aa->range, aa->fromMapNo, aa->count, aa->to_rank, rd, a->tid, chain_idx);
    return false;
}

_Bool shmem_replace_MapRecvAndUnpack(Laik_ActionSeq* as, Laik_Action* a, Laik_TransitionContext* tc, Laik_Shmem_Data* sd, int chain_idx)
{
    Laik_A_MapRecvAndUnpack* aa = (Laik_A_MapRecvAndUnpack*) a;

    Laik_Mapping* m = &tc->toList->map[aa->toMapNo];
    if(tc->toList && sd->copyScheme == 0)
    {
        if(is_shmem_allocator(m->allocator) && shmem_manager_zeroCopy(m->header))
        {

            return true;
        }
    }

    laik_shmem_addReceiveMap(as, aa->range, aa->toMapNo, 3 * a->round, a->tid, aa->count, aa->from_rank, chain_idx);

    return false;
}

void shmem_replace_MapGroupReduce(Laik_ActionSeq* as, Laik_Action* a, Laik_TransitionContext* tc, Laik_Shmem_Data* sd, Laik_Shmem_Comm* sg, int chain_idx)
{
    Laik_BackendAction* ba = (Laik_BackendAction*) a;
    Laik_Data* data = tc->data;

    int rank = sg->myid;
    int rd = 3 * ba->h.round;;

    int primaryI, primaryO;
    int inSize = laik_aseq_groupCount(as, ba->inputGroup, chain_idx);
    int outSize = laik_aseq_groupCount(as, ba->outputGroup, chain_idx);

    bool memberI = laik_aseq_isInGroup(as, ba->inputGroup, rank, chain_idx);
    primaryI = inSize > 0 ? laik_aseq_taskInGroup(as, ba->inputGroup, 0, chain_idx) : -1;
    bool memberO = laik_aseq_isInGroup(as, ba->outputGroup, rank, chain_idx);
    primaryO = outSize > 0 ? laik_aseq_taskInGroup(as, ba->outputGroup, 0,  chain_idx) : -1;

    bool fullReduce = onSameIsland(as, sg, ba->inputGroup, ba->outputGroup, chain_idx);

    Shmem_CopyScheme csI = SHMEM_None;
    Shmem_CopyScheme csO = SHMEM_None;

    if(memberI)
    {   
        int header_size = 0;
        if(tc->fromList)
        {   
            Laik_Mapping* map = &tc->fromList->map[ba->fromMapNo];

            csI = is_shmem_allocator(map->allocator) && sd -> copyScheme == 1 ? SHMEM_OneCopy : SHMEM_TwoCopy;
            
            header_size = map->layout->header_size;
        }else {
            csI = SHMEM_TwoCopy;
        }            
        
        laik_shmem_addMapGroupReduce(as, ba, rd, primaryI, csI, chain_idx);

        if(csI == SHMEM_TwoCopy) shmem_cpybuf_request(&sd->cpybuf, ba->count * data->elemsize + header_size == 0 ? 64 : header_size);

    }

    if(rank == primaryI || rank == primaryO)
    {   
        if(!fullReduce)
            laik_aseq_addr(a, as, rd + 1, 0);
        else
            laik_shmem_addShmemCopyToBuf(as, rd + 1, ba->range, ba->fromMapNo, ba->toMapNo, primaryI, primaryO, csI, a->tid, chain_idx);
    }

    if(memberO && outSize > 1)
    {
        int header_size = 0;
        if(tc->toList)
        {   
            Laik_Mapping* map = &tc->toList->map[ba->toMapNo];

            csO = is_shmem_allocator(map->allocator) && sd ->copyScheme == 1 ? SHMEM_OneCopy : SHMEM_TwoCopy;
            
            header_size = map->layout->header_size;
        }else {
            csO = SHMEM_TwoCopy;
        }            
        
        laik_shmem_addMapBroadcast(as, ba, rd + 2, primaryO, csO, chain_idx);

        if(csO == SHMEM_TwoCopy) shmem_cpybuf_request(&sd->cpybuf, ba->count * data->elemsize + header_size == 0 ? 64 : header_size);
    }
        

}

void laik_shmem_secondary_cleanup(Laik_Inst_Data* idata, Laik_ActionSeq* as)
{   
    laik_log(1, "Shared Memory Backend Cleanup");
    Laik_Shmem_Data* sd = idata->backend_data;
    shmem_cpybuf_delete(&sd->cpybuf);
    shmem_manager_cleanup();

    // call next layer for cleanup
    laik_next_cleanup(idata, as);
   
}


void laik_shmem_secondary_prepare(Laik_Inst_Data* idata, Laik_ActionSeq *as)
{
    Laik_TransitionContext* tc = as->context[0];
    Laik_Shmem_Comm* sg = (Laik_Shmem_Comm*) tc->transition->group->backend_data[idata->index];
    Laik_Shmem_Data* sd = (Laik_Shmem_Data*) idata->backend_data;

    Laik_Action *a = as->action;
    const int chain_idx = idata->index;

    for(unsigned i = 0; i < as->actionCount; i++, a = nextAction(a))
    {
        if(a->chain_idx != idata->index - 1) continue;

        switch (a->type) {
            case LAIK_AT_MapPackAndSend:
            {
                Laik_A_MapPackAndSend* aa = (Laik_A_MapPackAndSend*)a;
                if(sg->location == sg->locations[aa->to_rank])
                {
                    aa->to_rank = sg->secondaryIds[aa->to_rank];
                    a->chain_idx = idata->index;
                }
                break;
            }
            case LAIK_AT_MapRecvAndUnpack:
            {
                Laik_A_MapRecvAndUnpack* aa = (Laik_A_MapRecvAndUnpack*)a;
                if(sg->location == sg->locations[aa->from_rank])
                {
                    aa->from_rank = sg->secondaryIds[aa->from_rank];
                    a->chain_idx = idata->index;
                }
                break;
            }
            default:
                break;
        }
    }

    shmem_transformSubGroup(as, sg, chain_idx);

    // at first go down the chain
    // call next layer for preparation
    laik_next_prepare(idata, as);

    bool changed = false;
    bool ret = false;
    bool zc = false;
    a = as->action;
    int rd = 0;
    unsigned int maxround = 0;
    for (unsigned int i = 0; i < as->actionCount; i++, a = nextAction(a))
    {
        ret = false;
        switch (a->type)
        {
        case LAIK_AT_GroupReduce:
        {   
            Laik_BackendAction* ba = (Laik_BackendAction*) a;
            shmem_replace_groupReduce(as, ba, sg, chain_idx);
            ret = true;
            break;
        }
        case LAIK_AT_MapPackAndSend:
        {
            if(idata->index == a->chain_idx){
                zc |= shmem_replace_MapPackAndSend(as, a, tc, sd, chain_idx);
                ret = true;
            }
            break;
        }
        case LAIK_AT_MapRecvAndUnpack:
        {
            if(idata->index == a->chain_idx){
                zc |= shmem_replace_MapRecvAndUnpack(as, a, tc, sd, chain_idx);
                ret = true;
            }
            break;
        }
        case LAIK_AT_MapGroupReduce:
        {
            shmem_replace_MapGroupReduce(as, a, tc, sd, sg, chain_idx);
            ret = true;
            break;
        }
        case LAIK_AT_ReturnToPrimary:
        {
            ret = true;
            break;
        }
        default:
            break;
        }

        // max round for ReturnToPrimary(new maxround of single reduction is 3*a->round + 2)
        maxround = 3 * a->round + 3 > maxround ? 3 * a->round + 3 : maxround;
        changed |= ret;
        rd = a->round;
        if(!ret)
            laik_aseq_add(a, as, 3 * a->round);

    }

    if(zc)
    {   if(sg->myid != 0)
            laik_shmem_addZeroCopySync(as, LAIK_AT_ShmemZeroCopySyncRecv, 3 * rd + 3, 0, idata->index);
        else
            laik_shmem_addZeroCopySync(as, LAIK_AT_ShmemZeroCopySyncSend, 3 * rd + 3, 0, idata->index);

    }

    shmem_cpybuf_alloc_requested(&sd->cpybuf);
    laik_aseq_addReturnToPrimary(as, maxround);
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

        //if(!laik_aseq_hasNext(as)) return;

        if(a->chain_idx < idata->index) return;

        if(laik_log_begin(1))
        {
            laik_log_Action(a, as);
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
        case LAIK_AT_ShmemTwoCopyMap:
        {
            assert(a->chain_idx == index);
            Laik_A_ShmemTwoCopyMap* aa = (Laik_A_ShmemTwoCopyMap*) a;
            Laik_Mapping* m = &tc->fromList->map[aa->mapNo];
            shmem_sendPack(m, aa->range, aa->to_rank, idata);
            break;
        }
        case LAIK_AT_ShmemOneCopyMap:
        {
            assert(a->chain_idx == index);
            Laik_A_ShmemOneCopyMap* aa = (Laik_A_ShmemOneCopyMap*) a;
            Laik_Mapping* m = &tc->fromList->map[aa->mapNo];
            shmem_sendMap(m, NULL, aa->to_rank, idata);
            break;
        }
        case LAIK_AT_ShmemReceiveMap:
        {
            assert(a->chain_idx == index);
            laik_shmem_exec_ReceiveMap(a, tc, idata, g);
            break;
        }
        case LAIK_AT_ShmemZeroCopySyncSend:
        {
            assert(a->chain_idx == index);
            shmem_zeroCopySyncSend(idata, g, tc);
            break;
        }
        case LAIK_AT_ShmemZeroCopySyncRecv:
        {
            assert(a->chain_idx == index);
            shmem_zeroCopySyncRecv(idata, g, tc);
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
    case LAIK_AT_ShmemOneCopyMap:
    {
        Laik_A_ShmemOneCopyMap* aa = (Laik_A_ShmemOneCopyMap*) a;
        laik_log_append("ShmemOneCopyMap: ");
        laik_log_append(" mapNo %d ==> T%d",
                        aa->mapNo, (unsigned long long) aa->to_rank);
        break;
    }
    case LAIK_AT_ShmemTwoCopyMap:
    {
        Laik_A_ShmemTwoCopyMap* aa = (Laik_A_ShmemTwoCopyMap*) a;
        laik_log_append("ShmemTwoCopyMap: ");
        laik_log_Range(aa->range);
        laik_log_append(" mapNo %d, count %llu ==> T%d",
                        aa->mapNo, (unsigned long long) aa->count, aa->to_rank);
        break;
    }
    case LAIK_AT_ShmemReceiveMap:
    {
        Laik_A_ShmemReceiveMap* aa = (Laik_A_ShmemReceiveMap*) a;
        laik_log_append("ShmemReceiveMap: T%d ==> ", aa->from_rank);
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
        laik_log_append("T%d ==>", laik_aseq_taskInGroup(as, aa->subgroup, 0, a->chain_idx));
        laik_log_TaskGroupAS(as, aa->subgroup, a->chain_idx);
        break;
    }
    case LAIK_AT_ShmemZeroCopySyncRecv:
    case LAIK_AT_ShmemZeroCopySyncSend:
    {
        laik_log_append("ShmemZeroCopySync");
        break;
    }
    default:
        return false;
    }
    return true;
}

//#endif
