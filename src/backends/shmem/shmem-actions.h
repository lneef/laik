/*
 * This file is part of the LAIK library.
 * Copyright (c) 2018 Lukas Neef <lukas.neef@tum.de>
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

#ifndef SHMEM_ACTIONS_H
#define SHMEM_ACTIONS_H
#include "backends/shmem/shmem.h"
#include "laik/core.h"
#include "laik/data.h"
#include <laik.h>
#include <laik-internal.h>

// this file contains shared memory specific actions

#define LAIK_AT_ShmemGetMapAndCopy (LAIK_AT_Backend + 40)
#define LAIK_AT_ShmemTwoCopyMap (LAIK_AT_Backend + 41)
#define LAIK_AT_ShmemGroupReduce (LAIK_AT_Backend + 42)
#define LAIK_AT_ShmemGroupBroadcast (LAIK_AT_Backend + 43)
#define LAIK_AT_ShmemCopyToBuf (LAIK_AT_Backend + 44)
#define LAIK_AT_ShmemOneCopyMap (LAIK_AT_Backend + 45)
#define LAIK_AT_ShmemReceiveMap (LAIK_AT_Backend + 46)
#define LAIK_AT_ShmemMapGroupReduce (LAIK_AT_Backend + 47)
#define LAIK_AT_ShmemMapBroadcast (LAIK_AT_Backend + 48)

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
} Laik_A_ShmemGetMapAndCopy;

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
    int primary;
    char* fromBuf;
    char* buf;
    Laik_ReductionOperation redOp;
} Laik_A_ShmemGroupReduce;

typedef struct
{
    Laik_Action h;
    int subgroup;
    int count;
    int primary;
    char* buf;
} Laik_A_ShmemGroupBroadCast;

typedef struct 
{
    Laik_Action h;
    Laik_Range* range;
    int mapNo;
    int count;
    int to_rank;
}Laik_A_ShmemTwoCopyMap;

typedef struct
{
    Laik_Action h;
    int mapNo;
    int shmid;
    int to_rank;
}Laik_A_ShmemOneCopyMap;

#pragma pack(pop)


//---------------------------------------------------------------------
// add secondary specific actions to action sequence

void laik_shmem_addMapBroadcast(Laik_ActionSeq* as, Laik_BackendAction* ba, int round, int primary, char* collectBuf, int chain_idx);

void laik_shmem_addMapGroupReduce(Laik_ActionSeq* as, Laik_BackendAction* ba, int round, int primary, char* reduceBuf, int chain_idx);

void laik_shmem_addReceiveMap(Laik_ActionSeq* as, Laik_Range* range, int mapNo, int round, int tid, int count, int from_rank, int chain_idx);

void laik_shmem_addGroupBroadcast(Laik_ActionSeq* as, Laik_BackendAction* ba, int round, char* buf, int chain_idx, int primary);

void laik_shmem_addGroupReduce(Laik_ActionSeq* as, Laik_BackendAction* ba, int round, char* buf, int chain_idx, int primary);

void laik_shmem_addShmemCopyToBuf(Laik_ActionSeq* as, int round, char* buf, char* toBuf, int count, int sender, int receiver, int tid, int chain_idx);

void laik_shmem_addGetMapAndCopy(Laik_ActionSeq* as, Laik_Range* range, int mapNo, int round, int tid, int count, int to_rank, int chain_idx);

void laik_shmem_addTwoCopyMap(Laik_ActionSeq* as, Laik_Range* range, int mapNo, int count, int receiver, int round, int tid, int chain_idx);

void laik_shmem_addOneCopyMap(Laik_ActionSeq* as, int mapNo, int shmid, int receiver, int round, int tid, int chain_idx);

//---------------------------------------------------------------------
// execute secondary specific action

void laik_shmem_exec_GroupBroadCast(Laik_Action* a, Laik_ActionSeq* as, Laik_TransitionContext* tc, Laik_Inst_Data* idata, Laik_Group* g);

void laik_shmem_exec_GroupReduce(Laik_Action * a, Laik_ActionSeq* as, Laik_TransitionContext* tc, Laik_Inst_Data* idata, Laik_Group* g);

void laik_shmem_exec_GetMapAndCopy(Laik_Action* a, Laik_TransitionContext* tc, Laik_Inst_Data* idata);

void laik_shmem_exec_ReceiveMap(Laik_Action* a, Laik_TransitionContext* tc, Laik_Inst_Data* idata, Laik_Group* g);

void laik_shmem_exec_MapGroupReduce(Laik_ActionSeq* as, Laik_Action* a, Laik_TransitionContext* tc, Laik_Inst_Data* idata, Laik_Group* g);

void laik_shmem_exec_CopyToBuf(Laik_Action* a, Laik_TransitionContext* tc, Laik_Inst_Data* idata, Laik_Group* g);

void laik_shmem_exec_MapBroadCast(Laik_ActionSeq* as , Laik_Action* a, Laik_TransitionContext* tc, Laik_Inst_Data* idata, Laik_Group* g);

#endif
