

#ifndef SHMEM_ACTIONS_H
#define SHMEM_ACTIONS_H
#include "backends/shmem/shmem.h"
#include <laik.h>
#include <laik-internal.h>

#define LAIK_AT_ShmemReduce (LAIK_AT_Backend + 40)
#define LAIK_AT_ShmemBroadcast (LAIK_AT_Backend + 41)
#define LAIK_AT_ShmemGroupReduce (LAIK_AT_Backend + 42)
#define LAIK_AT_ShmemGroupBroadcast (LAIK_AT_Backend + 43)
#define LAIK_AT_ShmemCopyToBuf (LAIK_AT_Backend + 44)
#define LAIK_AT_ShmemCopyMapToReceiver (LAIK_AT_Backend + 45)
#define LAIK_AT_ShmemReceiveMap (LAIK_AT_Backend + 46)
#define LAIK_AT_ShmemMapGroupReduce (LAIK_AT_Backend + 47)
#define LAIK_AT_ShmemMapBroadcast (LAIK_AT_Backend + 48)

#define LAIK_AT_ShmemMapSend (LAIK_AT_Backend + 50)
#define LAIK_AT_ShmemBufSend (LAIK_AT_Backend + 51)
#define LAIK_AT_ShmemMapRecv (LAIK_AT_Backend + 52)
#define LAIK_AT_ShmemRBufRecv (LAIK_AT_Backend + 53)
#define LAIK_AT_ShmemBufRecv (LAIK_AT_Backend + 54)

#define LAIK_AT_ShmemMapPackAndSend (LAIK_AT_Backend + 55)
#define LAIK_AT_ShmemPackAndSend (LAIK_AT_Backend + 56)
#define LAIK_AT_ShmemMapRecvAndUnpack (LAIK_AT_Backend + 57)
#define LAIK_AT_ShmemRecvAndUnpack (LAIK_AT_Backend + 58)
#define LAIK_AT_ShmemRBufSend (LAIK_AT_Backend + 59)

#pragma pack(push, 1)
typedef struct{
    Laik_Action h;
    Laik_Range* range;
    int mapNo;
    int primary;
    int count;
    char* buf;
    int subgroup;
} Laik_A_ShmemMapBroadCast;

typedef struct{
    Laik_Action h;
    Laik_Range* range;
    int mapNo;
    int primary;
    int count;
    char* buf;
    int subgroup;
    Laik_ReductionOperation redOp;
} Laik_A_ShmemMapGroupReduce;

typedef struct{
    Laik_Action h;
    Laik_Range* range;
    int mapNo;
    int count;
    int to_rank;
} Laik_A_ShmemCopyMapToReceiver;

typedef struct{
    Laik_Action h;
    Laik_Range* range;
    int mapNo;
    int count;
    int from_rank;
} Laik_A_ShmemReceiveMap;

typedef struct
{
    Laik_Action h;
    char* fromBuf;
    char* toBuf;
    int count;
    int receiver;
    int sender;
} Laik_A_ShmemCopyToBuf;

typedef struct 
{
    Laik_Action h;
    char *buf;
    int count;
} Laik_A_ShmemBroadcast;

typedef struct
{
    Laik_Action h;
    char* frombuf;
    char* buf;
    int count;
    Laik_ReductionOperation redOp;
} Laik_A_ShmemReduce;

typedef struct 
{
    Laik_Action h;
    int count;
    int subgroup;
    char* fromBuf;
    char* buf;
    Laik_ReductionOperation redOp;
} Laik_A_ShmemGroupReduce;

typedef struct
{
    Laik_Action h;
    int subgroup;
    int count;
    char* buf;
} Laik_A_ShmemGroupBroadCast;

#pragma pack(pop)

void laik_shmem_addMapBroadcast(Laik_ActionSeq* as, Laik_BackendAction* ba, int round, int primary, char* collectBuf, int chain_idx);

void laik_shmem_addMapGroupReduce(Laik_ActionSeq* as, Laik_BackendAction* ba, int round, int primary, char* reduceBuf, int chain_idx);

void laik_shmem_addReceiveMap(Laik_ActionSeq* as, Laik_Range* range, int mapNo, int round, int tid, int count, int from_rank, int chain_idx);

void laik_shmem_addCopyMapToReceiver(Laik_ActionSeq* as, Laik_Range* range, int mapNo, int round, int tid, int count, int to_rank, int chain_idx);

void laik_shmem_addGroupBroadcast(Laik_ActionSeq* as, Laik_BackendAction* ba, int round, char* buf, int chain_idx);

void laik_shmem_addGroupReduce(Laik_ActionSeq* as, Laik_BackendAction* ba, int round, char* buf, int chain_idx);

void laik_shmem_addShmemReduce(Laik_ActionSeq* as, int round, char* frombuf, char* buf, int count, Laik_ReductionOperation redOp, int tid, int chain_idx);

void laik_shmem_addShmemBroadcast(Laik_ActionSeq* as, int round, char* buf, int count, int tid, int chain_idx);

void laik_shmem_addShmemCopyToBuf(Laik_ActionSeq* as, int round, char* buf, char* toBuf, int count, int sender, int receiver, int tid, int chain_idx);


void laik_shmem_exec_GroupBroadCast(Laik_Action* a, Laik_ActionSeq* as, Laik_TransitionContext* tc, Shmem_Secondary_Group* sg);

void laik_shmem_exec_GroupReduce(Laik_Action * a, Laik_ActionSeq* as, Laik_TransitionContext* tc, Shmem_Secondary_Group* sg);

void laik_shmem_exec_Reduce(Laik_Action* a, Laik_TransitionContext* tc, Shmem_Secondary_Group* sg);

void laik_shmem_exec_Broadcast(Laik_Action* a, Laik_TransitionContext* tc, Shmem_Secondary_Group* sg);

void laik_shmem_exec_CopyMapToReceiver(Laik_Action* a, Laik_TransitionContext* tc, Shmem_Secondary_Group* sg);

void laik_shmem_exec_ReceiveMap(Laik_Action* a, Laik_TransitionContext* tc, Shmem_Secondary_Group* sg);

void laik_shmem_exec_MapGroupReduce(Laik_ActionSeq* as, Laik_Action* a, Laik_TransitionContext* tc, Shmem_Secondary_Group* sg);

void laik_shmem_exec_CopyToBuf(Laik_Action* a, Laik_TransitionContext* tc, Shmem_Secondary_Group* sg);

void laik_shmem_exec_MapBroadCast(Laik_ActionSeq* as , Laik_Action* a, Laik_TransitionContext* tc, Shmem_Secondary_Group* sg);

#endif
