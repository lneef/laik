
#include "shmem-actions.h"
#include "backends/shmem/shmem-manager.h"
#include "shmem.h"

#include <laik-internal.h>
#include <laik.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>


void laik_shmem_addMapBroadcast(Laik_ActionSeq* as, Laik_BackendAction* ba, int round, int primary, char* collectBuf, int chain_idx)
{
    Laik_A_ShmemMapBroadCast* a;
    a = (Laik_A_ShmemMapBroadCast*) laik_aseq_addAction(as, sizeof(*a), LAIK_AT_ShmemMapBroadcast, round, ba->h.tid);
    a->h.chain_idx = chain_idx;
    a->range = ba->range;
    a->mapNo = ba->toMapNo;
    a->primary = primary;
    a->buf = collectBuf;
    a->count = ba->count;
    a->subgroup = ba->outputGroup;
}

void laik_shmem_addMapGroupReduce(Laik_ActionSeq* as, Laik_BackendAction* ba, int round, int primary, char* reduceBuf, int chain_idx)
{
    Laik_A_ShmemMapGroupReduce* a;
    a = (Laik_A_ShmemMapGroupReduce*) laik_aseq_addAction(as, sizeof(*a), LAIK_AT_ShmemMapGroupReduce, round, ba->h.tid);
    a->h.chain_idx = chain_idx;
    a->range = ba->range;
    a->mapNo = ba->fromMapNo;
    a->primary = primary;
    a->buf = reduceBuf;
    a->redOp = ba -> redOp;
    a->count = ba -> count;
    a->subgroup = ba -> inputGroup;
}

void laik_shmem_addReceiveMap(Laik_ActionSeq* as, Laik_Range* range, int mapNo, int round, int tid, int count, int from_rank, int chain_idx)
{
    Laik_A_ShmemReceiveMap* a;
    a = (Laik_A_ShmemReceiveMap*) laik_aseq_addAction(as, sizeof(*a), LAIK_AT_ShmemReceiveMap, round, tid);
    a->h.chain_idx = chain_idx;
    a->range = range;
    a->mapNo = mapNo;
    a->count = count;
    a->from_rank = from_rank;
}

void laik_shmem_addGetMapAndCopy(Laik_ActionSeq* as, Laik_Range* range, int mapNo, int round, int tid, int count, int to_rank, int chain_idx)
{
    Laik_A_ShmemGetMapAndCopy *a;
    a = (Laik_A_ShmemGetMapAndCopy*) laik_aseq_addAction(as, sizeof(*a), LAIK_AT_ShmemGetMapAndCopy, round, tid);
    a->h.chain_idx = chain_idx;
    a->range = range;
    a->mapNo = mapNo;
    a->count = count;
    a->to_rank = to_rank;
}

void laik_shmem_addGroupBroadcast(Laik_ActionSeq* as, Laik_BackendAction* ba, int round, char* buf, int chain_idx, int primary){
    Laik_A_ShmemGroupBroadCast* a;
    a = (Laik_A_ShmemGroupBroadCast*) laik_aseq_addAction(as, sizeof(*a), LAIK_AT_ShmemGroupBroadcast, round, ba->h.tid);
    a->h.chain_idx = chain_idx;
    a-> buf = buf;
    a->count = ba->count;
    a->subgroup = ba->outputGroup;
    a->primary = primary;
}

void laik_shmem_addGroupReduce(Laik_ActionSeq* as, Laik_BackendAction* ba, int round, char* buf, int chain_idx, int primary)
{
    Laik_A_ShmemGroupReduce* a;
    a = (Laik_A_ShmemGroupReduce*) laik_aseq_addAction(as, sizeof(*a), LAIK_AT_ShmemGroupReduce, round, ba->h.tid);
    a->h.chain_idx = chain_idx;
    a->buf = buf;
    a->fromBuf = ba -> fromBuf;
    a->count = ba->count;
    a->subgroup = ba -> inputGroup;
    a->redOp = ba->redOp;
    a->primary = primary;

}

void laik_shmem_addShmemCopyToBuf(Laik_ActionSeq* as, int round, char* buf, char* toBuf, int count, int sender, int receiver, int tid, int chain_idx)
{
    Laik_A_ShmemCopyToBuf* a;
    a = (Laik_A_ShmemCopyToBuf*)laik_aseq_addAction(as, sizeof(*a),
                                            LAIK_AT_ShmemCopyToBuf, round, tid);
    a->h.chain_idx = chain_idx;
    a->count = count;
    a->fromBuf = buf;
    a->toBuf = toBuf;
    a->sender = sender;
    a->receiver = receiver;

}

void laik_shmem_addTwoCopyMap(Laik_ActionSeq* as, Laik_Range* range, int mapNo, int count, int receiver, int round, int tid, int chain_idx)
{
    Laik_A_ShmemTwoCopyMap* a;
    a = (Laik_A_ShmemTwoCopyMap*) laik_aseq_addAction(as, sizeof(*a), LAIK_AT_ShmemTwoCopyMap, round, tid);
    a->h.chain_idx = chain_idx;
    a->mapNo = mapNo;
    a->range = range;
    a->count = count;
    a->to_rank = receiver;
}

void laik_shmem_addOneCopyMap(Laik_ActionSeq* as, int mapNo, int shmid, int receiver, int round, int tid, int chain_idx)
{
    Laik_A_ShmemOneCopyMap* a;
    a = (Laik_A_ShmemOneCopyMap*) laik_aseq_addAction(as, sizeof(*a), LAIK_AT_ShmemOneCopyMap, round, tid);
    a->h.chain_idx = chain_idx;
    a->mapNo = mapNo;
    a->shmid = shmid;
    a->to_rank = receiver;
}

void laik_shmem_exec_TwoCopyMap(Laik_Action* a, Laik_TransitionContext* tc, Laik_Inst_Data* idata)
{
    Laik_A_ShmemTwoCopyMap* aa = (Laik_A_ShmemTwoCopyMap*) a;
    Laik_Mapping* m = &tc->fromList->map[aa->mapNo];
        
    shmem_PackSend(m, *aa->range, aa->count, aa->to_rank, idata);
    
}


void laik_shmem_exec_GroupBroadCast(Laik_Action* a, Laik_ActionSeq* as, Laik_TransitionContext* tc, Laik_Inst_Data* idata, Laik_Group* g)
{
    Laik_A_ShmemGroupBroadCast* ba = (Laik_A_ShmemGroupBroadCast*) a;
    Laik_Data * data = tc -> data;
    int chain_idx = a -> chain_idx;
    Laik_Shmem_Comm* sg = g->backend_data[idata->index];


    if(sg->myid == ba->primary)
    {
        int count = laik_aseq_groupCount(as, ba->subgroup, chain_idx);

        for(int i = 1; i < count; ++i)
        {
            int task = laik_aseq_taskInGroup(as, ba->subgroup, i, chain_idx);

            shmem_send(ba->buf, ba->count, data->elemsize, task,idata);
        }
    }
    else {
        shmem_recv(ba->buf, ba->count, ba->primary, data, idata, g, LAIK_RO_None);
    }
}

void laik_shmem_exec_GroupReduce(Laik_Action * a, Laik_ActionSeq* as, Laik_TransitionContext* tc, Laik_Inst_Data* idata, Laik_Group* g)
{
    Laik_A_ShmemGroupReduce* ba = (Laik_A_ShmemGroupReduce*) a;
    Laik_Data* data = tc -> data;
    int chain_idx = a->chain_idx;
    Laik_Shmem_Comm* sg = g->backend_data[idata->index];

    if(sg->myid == ba->primary)
    {    
        memcpy(ba->buf, ba->fromBuf, ba->count * data->elemsize);
        int count = laik_aseq_groupCount(as, ba->subgroup, chain_idx);
        for(int i = 1; i < count; ++i)
        {   

            int task = laik_aseq_taskInGroup(as, ba->subgroup, i, chain_idx);
            shmem_recv(ba -> buf, ba->count, task, data, idata, g, ba->redOp); 
        }
    }
    else {
        shmem_send(ba->fromBuf, ba->count, data->elemsize, ba->primary, idata);
    }
}

void laik_shmem_exec_CopyToBuf(Laik_Action* a, Laik_TransitionContext* tc, Laik_Inst_Data* idata, Laik_Group* g)
{
    Laik_A_ShmemCopyToBuf* ba = (Laik_A_ShmemCopyToBuf*) a;
    Laik_Shmem_Comm* sg = g->backend_data[idata->index];
    Laik_Data* data = tc -> data;

    int rank = sg->myid;
    if(ba->sender == ba->receiver){
        memcpy(ba -> toBuf, ba -> fromBuf, data->elemsize * ba -> count);
    }else if (rank == ba->sender) {
        shmem_send(ba->fromBuf, ba->count, data->elemsize, ba->receiver, idata);
    }else if (rank == ba->receiver) {
        shmem_recv(ba->toBuf, ba->count, ba->sender, data, idata, g, LAIK_RO_None);
    }
}


void laik_shmem_exec_GetMapAndCopy(Laik_Action* a, Laik_TransitionContext* tc, Laik_Inst_Data* idata)
{
    Laik_A_ShmemGetMapAndCopy* aa = (Laik_A_ShmemGetMapAndCopy*) a;
    Laik_Shmem_Data* sd = idata->backend_data;

    Laik_Mapping* map = &tc->fromList->map[aa->mapNo];

    if(is_shmem_allocator(map->allocator) && sd->copyScheme == 1)
    {
        int shmid = shmem_manager_shmid(map->header);
        shmem_sendMap(map, aa->to_rank, shmid, idata);
    }
    else {
        shmem_PackSend(map, *aa->range, aa->count, aa->to_rank, idata);
    }
}

void laik_shmem_exec_ReceiveMap(Laik_Action* a, Laik_TransitionContext* tc, Laik_Inst_Data* idata, Laik_Group* g)
{
    Laik_A_ShmemReceiveMap* aa = (Laik_A_ShmemReceiveMap*) a;

    Laik_Mapping* map = &tc->toList->map[aa->mapNo];
    
    shmem_recvMap(map, aa->range, aa->count, aa->from_rank, idata, g);

    
}

void laik_shmem_exec_MapGroupReduce(Laik_ActionSeq* as, Laik_Action* a, Laik_TransitionContext* tc, Laik_Inst_Data* idata, Laik_Group* g)
{
    Laik_A_ShmemMapGroupReduce* aa = (Laik_A_ShmemMapGroupReduce*) a;
    Laik_Mapping* map = &tc->fromList->map[aa->mapNo];
    Laik_Data* data = tc->data;
    int chain_idx = a->chain_idx;
    Laik_Shmem_Comm* sg = g->backend_data[idata->index];
    Laik_Range tmp = *aa->range;
    
    if(aa->primary == sg->myid)
    {
        map->layout->pack(map, &tmp, &(tmp.from), aa->buf, aa->count * data->elemsize);
        int count = laik_aseq_groupCount(as, aa->subgroup, chain_idx);
        for(int i = 1; i < count; ++i)
        {   
            int task = laik_aseq_taskInGroup(as, aa->subgroup, i, chain_idx);
            shmem_RecvReduce(aa->buf, aa->count, task, data->type, aa->redOp, idata, g);

        }
    }
    else{
        int task = laik_aseq_taskInGroup(as, aa->subgroup, 0, chain_idx);
        shmem_PackSend(map, *aa->range, aa->count, task, idata);
    }
    
}

void laik_shmem_exec_MapBroadCast(Laik_ActionSeq* as , Laik_Action* a, Laik_TransitionContext* tc, Laik_Inst_Data* idata, Laik_Group* g)
{
    Laik_A_ShmemMapBroadCast* aa = (Laik_A_ShmemMapBroadCast*) a;
    int chain_idx = a->chain_idx;
    Laik_Data* data = tc->data;
    Laik_Mapping* map = &tc->toList->map[aa->mapNo];
    Laik_Shmem_Comm* sg = g->backend_data[idata->index];
    
    if(aa->primary == sg->myid)
    {   
        map->layout->unpack(map, aa->range, &(aa->range->from), aa->buf, aa->count * data -> elemsize);
        
        int count = laik_aseq_groupCount(as, aa->subgroup, chain_idx);
        for(int i = 1; i < count; ++i)
        {
            int task = laik_aseq_taskInGroup(as, aa->subgroup, i, chain_idx);
            shmem_send(aa->buf, aa->count, data->elemsize, task, idata);
        }

    }
    else {
        int task = laik_aseq_taskInGroup(as, aa->subgroup, 0, chain_idx);
        shmem_RecvUnpack(map, aa->range, aa->count, task, idata, g);
    }
}

