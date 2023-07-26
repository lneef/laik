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

#ifndef SHMEM_H
#define SHMEM_H

#include "laik/core.h"
#include<laik-internal.h>
#include<stddef.h>
#include<laik.h>

#define SHMEM_FAILURE -1
#define SHMEM_SUCCESS 0
#define SHMEM_SHMGET_FAILED 1
#define SHMEM_SHMAT_FAILED 2
#define SHMEM_SHMDT_FAILED 3
#define SHMEM_SHMCTL_FAILED 4
#define SHMEM_RECV_BUFFER_TOO_SMALL 5
#define SHMEM_SEGMENT_NOT_FOUND 6

#define SHMEM_MAX_ERROR_STRING 100

// some definitions for the shared memory backend
typedef enum DataSpec{
    PACK, MAP
} DataSpec;

struct commHeader{
    DataSpec spec;
    int receiver;
    int shmid;
    int count;
    Laik_Range range;
};

#pragma pack(push, 1)
typedef struct _Laik_Shmem_Comm
{
    // size of the partition I am part of
    int size;

    // identifier of my partition
    int colour;

    // divsion of the world group
    int* divsion;

    // number of shared memory islands
    int numIslands;

    // rank numbers of the processes in this subgroup
    int *primaryRanks;

}Laik_Shmem_Comm;

typedef struct _Laik_Shmem_Data
{
    // number of processes per shared memory island
    int ranksPerIslands;

    int copyScheme;

    //chosen copy scheme
    int (*send)(void*, int, int, int);

    
}Laik_Shmem_Data;

#pragma pack(pop)

//------------------------------------------------------------------------------
//communicator info, initialization and finalize

int shmem_error_string(int error, char *str);

int shmem_secondary_init(Laik_Shmem_Comm* sg, Laik_Inst_Data* idata, Laik_Group* world, int primarySize, int* ranks, int** new_ranks);

int shmem_comm_size(Laik_Shmem_Comm* sg, int *sizePtr);

int shmem_comm_colour(Laik_Shmem_Comm* sg, int *colourPtr);

int shmem_get_colours(Laik_Shmem_Comm* sg, int **buf);

int shmem_get_numIslands(Laik_Shmem_Comm* sg, int *num);

int shmem_finalize();

//------------------------------------------------------------------------------
// peer to peer communication actions

int shmem_send(void *buffer, int count, int datatype, int recipient,  Laik_Inst_Data* idata, Laik_Group* g);

int shmem_recv(void *buffer, int count,int sender, Laik_Data* data, Laik_Inst_Data* idata, Laik_Group* g, Laik_ReductionOperation redOp);

int shmem_sendMap(Laik_Mapping* map, int receiver, int shmid, Laik_Inst_Data* idata, Laik_Group* g);

int shmem_recvMap(Laik_Mapping* map, Laik_Range* range, int count, int sender,  Laik_Inst_Data* idata, Laik_Group* g);

int shmem_PackSend(Laik_Mapping* map, Laik_Range range, int count, int receiver,  Laik_Inst_Data* idata, Laik_Group* g);

int shmem_RecvUnpack(Laik_Mapping* map, Laik_Range* range, int count, int sender, Laik_Inst_Data* idata, Laik_Group* g);

int shmem_RecvReduce(char* buf, int count, int sender, Laik_Type* type, Laik_ReductionOperation redOp,  Laik_Inst_Data* idata, Laik_Group* g);

//------------------------------------------------------------------------------
// copy buffer management and subgroup handling

void cleanupBuffer();

void createBuffer();

void request_CpyBuf(size_t size);

int shmem_init_comm(Laik_Shmem_Comm *sg, Laik_Group *g, Laik_Inst_Data *idata, int* ranks, int** new_ranks, int size);

bool onSameIsland(Laik_ActionSeq* as, Laik_Shmem_Comm* sg, int inputgroup, int outputgroup, int chain_idx);

void shmem_transformSubGroup(Laik_ActionSeq* as, Laik_Shmem_Comm* sg, int chain_idx);

#endif
