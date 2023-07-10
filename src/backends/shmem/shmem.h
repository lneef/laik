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

#pragma pack(push, 1)
typedef struct _Shmem_Secondary_Group
{
    Laik_Secondary_Group info;

    int headerShmid;

    int numIslands;

    int *primaryRanks;
    
    struct cpyBuf* cpyBuf;

    int (*send)(void*, int, int, int, struct _Shmem_Secondary_Group*);
    
    int (*sendP)(int *, int, int);
    int (*recvP)(int *, int, int);


}Shmem_Secondary_Group;

#pragma pack(pop)

//------------------------------------------------------------------------------
//communicator info, initialization and finalize

int shmem_error_string(int error, char *str);

int shmem_secondary_init(Shmem_Secondary_Group* sg, int primaryRank, int primarySize, int* locations, int** newLocations, int** ranks, 
                            int (*send)(int *, int, int), int (*recv)(int *, int, int));
int shmem_comm_size(Shmem_Secondary_Group* sg, int *sizePtr);

int shmem_comm_rank(Shmem_Secondary_Group* sg, int *rankPtr);

int shmem_comm_colour(Shmem_Secondary_Group* sg, int *colourPtr);

int shmem_get_colours(Shmem_Secondary_Group* sg, int **buf);

int shmem_get_numIslands(Shmem_Secondary_Group* sg, int *num);

int shmem_get_secondaryRanks(Shmem_Secondary_Group* sg, int **buf);

int shmem_finalize(Shmem_Secondary_Group* sg);

//------------------------------------------------------------------------------
// peer to peer communication actions

int shmem_send(void *buffer, int count, int datatype, int recipient, Shmem_Secondary_Group* sg);

int shmem_recv(void *buffer, int count, int datatype, int sender, int *recieved, Shmem_Secondary_Group* sg);

int shmem_sendMap(Laik_Mapping* map, int receiver, Shmem_Secondary_Group* sg);

int shmem_recvMap(Laik_Mapping* map, Laik_Range* range, int count, int sender, Shmem_Secondary_Group* sg);

int shmem_PackSend(Laik_Mapping* map, Laik_Range range, int count, int receiver, Shmem_Secondary_Group* sg);

int shmem_RecvUnpack(Laik_Mapping* map, Laik_Range* range, int count, int sender, Shmem_Secondary_Group* sg);

int shmem_RecvReduce(char* buf, int count, int sender, Laik_Type* type, Laik_ReductionOperation redOp, Shmem_Secondary_Group* sg);

//------------------------------------------------------------------------------
// copy buffer management and subgroup handling

void cleanupBuffer(Shmem_Secondary_Group* sg);

void createBuffer(Shmem_Secondary_Group* sg, size_t size);

bool onSameIsland(Laik_ActionSeq* as, Shmem_Secondary_Group* sg, int inputgroup, int outputgroup);

void shmem_transformSubGroup(Laik_ActionSeq* as, Shmem_Secondary_Group* sg, int chain_idx);

#endif
