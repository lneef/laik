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
#include "node.h"
#include "laik/space-internal.h"
#include "laik/core-internal.h"

#include <assert.h>
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
};

struct metaInfos{
    int receiver;
    int count;
    int shmid;
    int offset;
};

struct shmList
{
    void *ptr;
    int shmid;
    int size;
    struct shmList *next;
};

struct shmList *head;
struct shmList *tail;

struct groupInfo groupInfo;
int openShmid = -1;
int metaShmid = -1;

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
        shmctl(metaShmid, IPC_RMID, 0);

    struct shmList *l = tail;
    while(l != NULL){
        shmdt(l->ptr);
        shmctl(l->shmid, IPC_RMID, 0);
        l = l->next;
    }
}

int createMetaInfoSeg()
{
    int shmAddr = hash(groupInfo.rank) + META_OFFSET;
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

int get_shmid(void *ptr, int *shmid, int *offset)
{
    for(struct shmList *l = tail; l != NULL; l = l->next)
    {
        int diff = (int) (ptr - l->ptr);
        if(diff >= 0 && diff < l->size)
        {
            *offset = diff;
            *shmid = l->shmid;
            return SHMEM_SUCCESS;
        }
    }
    return SHMEM_SEGMENT_NOT_FOUND;
}

int shmem_2cpy_send(const void *buffer, int count, int datatype, int recipient)
{
    int size = datatype * count;
    int shmAddr = hash(recipient + hash(groupInfo.rank)) + BUF_OFFSET;
    int bufShmid = shmget(shmAddr, size, 0644 | IPC_CREAT);
    if (bufShmid == -1)
        return SHMEM_SHMGET_FAILED;
    openShmid = bufShmid;

    // Attach to the segment to get a pointer to it.
    char *bufShmp = shmat(bufShmid, NULL, 0);
    if (bufShmp == (void *)-1)
        return SHMEM_SHMAT_FAILED;

    struct metaInfos *shmp = shmat(metaShmid, NULL, 0);
    if (shmp == (void *)-1)
        return SHMEM_SHMAT_FAILED;

    shmp->count = count;
    shmp->shmid = -1;
    memcpy(bufShmp, buffer, size);
    shmp->receiver = recipient;
    while (shmp->receiver != -1)
    {
    }

    if (shmdt(shmp) == -1)
        return SHMEM_SHMDT_FAILED;

    if (shmdt(bufShmp) == -1)
        return SHMEM_SHMDT_FAILED;

    shmctl(bufShmid, IPC_RMID, 0);
    openShmid = -1;

    return SHMEM_SUCCESS;
}

int shmem_2cpy_recv(void *buffer, int count, int datatype, int sender, int *received)
{
    int bufSize = datatype * count;
    int bufShmAddr = hash(groupInfo.rank + hash(sender)) + BUF_OFFSET;

    time_t t_0 = time(NULL);
    int bufShmid = shmget(bufShmAddr, 0, 0644);
    while (bufShmid == -1 && time(NULL) - t_0 < MAX_WAITTIME)
        bufShmid = shmget(bufShmAddr, 0, 0644);
    if (bufShmid == -1)
        return SHMEM_SHMGET_FAILED;

    char *bufShmp = shmat(bufShmid, NULL, 0);
    if (bufShmp == (void *)-1)
        return SHMEM_SHMAT_FAILED;

    int shmAddr = hash(sender) + META_OFFSET;
    int shmid = shmget(shmAddr, 0, 0644);
    if (shmid == -1)
        return SHMEM_SHMGET_FAILED;

    struct metaInfos *shmp = shmat(shmid, NULL, 0);
    if (shmp == (void *)-1)
        return SHMEM_SHMAT_FAILED;

    while (shmp->receiver != groupInfo.rank)
    {
    }
    *received = shmp->count;
    int receivedSize = *received * datatype;
    if (bufSize < receivedSize)
    {
        memcpy(buffer, bufShmp, bufSize);
        return SHMEM_RECV_BUFFER_TOO_SMALL;
    }
    else
    {
        memcpy(buffer, bufShmp, receivedSize);
    }
    shmp->receiver = -1;

    if (shmdt(shmp) == -1)
        return SHMEM_SHMDT_FAILED;

    if (shmdt(bufShmp) == -1)
        return SHMEM_SHMDT_FAILED;

    shmctl(bufShmid, IPC_RMID, 0);

    return SHMEM_SUCCESS;
}

int shmem_send(void *buffer, int count, int datatype, int recipient)
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
    int shmAddr = hash(sender) + META_OFFSET;
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

    if(shmp->shmid == -1){
        return shmem_2cpy_recv(buffer, count, datatype, sender, received);
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
    deleteOpenShmSegs();
    if (groupInfo.colours != NULL)
        free(groupInfo.colours);
    
    if (groupInfo.secondaryRanks != NULL)
        free(groupInfo.secondaryRanks);
    
    if(groupInfo.primarys != NULL)
        free(groupInfo.primarys);

    return SHMEM_SUCCESS;
}

int shmem_secondary_init(int primaryRank, int primarySize, int (*send)(int *, int, int),
                         int (*recv)(int *, int, int))
{
    signal(SIGINT, deleteOpenShmSegs);

    bool created = false;
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
        (*recv)(groupInfo.colours, groupInfo.size, 0);
        groupInfo.secondaryRanks = malloc(primarySize * sizeof(int));
        (*recv)(groupInfo.secondaryRanks, groupInfo.size, 0);

        (*recv)(&groupInfo.num_islands, 1, 0);
        groupInfo.primarys = malloc(groupInfo.num_islands * sizeof(int));
        (*recv)(groupInfo.primarys, groupInfo.num_islands, 0);
    }

    if (created && shmctl(openShmid, IPC_RMID, 0) == -1)
        return SHMEM_SHMCTL_FAILED;
    
    openShmid = -1;
    
    laik_log(2, "%d, %d", groupInfo.colour, groupInfo.rank);

    // Open the own meta info shm segment and set it to ready
    int err = createMetaInfoSeg();
    if (err != SHMEM_SUCCESS)
        return err;

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

void register_shmSeg(void *ptr, int shmid, int size)
{
    struct shmList *new = malloc(sizeof(struct shmList));
    new->ptr = ptr;
    new->shmid = shmid;
    new->size = size;
    new->next = NULL;

    if (head == NULL)
    {
        head = new;
        tail = new;
        return;
    }
    head->next = new;
    head = new;
}

int get_shmid_and_destroy(void *ptr, int *shmid)
{
    if(tail == NULL)
        return SHMEM_SEGMENT_NOT_FOUND;

    struct shmList *previous = NULL;
    struct shmList *current = tail;

    while(current != NULL)
    {
        if(ptr == current->ptr)
        {
            *shmid = current->shmid;
            if(previous == NULL)
            {
                tail = current->next;
            }
            else
            {
                previous->next = current->next;
            }

            if(current->next == NULL){
                head = previous;
            }
            free(current);
            return SHMEM_SUCCESS;
        }
        previous = current;
        current = current->next;
    }
    return SHMEM_SEGMENT_NOT_FOUND;
}

static int cnt = 0;

void* def_shmem_malloc(Laik_Data* d, size_t size){
    (void) d; // not used in this implementation of interface

    int shmAddr = hash(groupInfo.rank + hash(cnt++)) + ALLOC_OFFSET;
    int shmid = shmget(shmAddr, size, 0644 | IPC_CREAT | IPC_EXCL);
    if (shmid == -1)
    {
        laik_panic("def_shmem_malloc couldn't create the shared memory segment");
        return NULL;
    }
    
    // Attach to the segment to get a pointer to it.
    void *ptr = shmat(shmid, NULL, 0);
    if (ptr == (void *)-1)
    {
        laik_panic("def_shmem_malloc couldn't attach to the shared memory segment");
        return NULL;
    }

    memset(ptr, 0, size);

    register_shmSeg(ptr, shmid, size);
    return ptr;
}

void def_shmem_free(Laik_Data* d, void* ptr){
    (void) d; // not used in this implementation of interface

    int shmid;
    if(get_shmid_and_destroy(ptr, &shmid) != SHMEM_SUCCESS)
        laik_panic("def_shmem_free couldn't find the given shared memory segment");

    if (shmdt(ptr) == -1)
        laik_panic("def_shmem_free couldn't detach from the given pointer");

    if (shmctl(shmid, IPC_RMID, 0) == -1)
        laik_panic("def_shmem_free couldn't destroy the shared memory segment");
    
}

void transformSubGroup(Laik_Transition* t, int subgroup, groupTransform* group, int chain_idx){
    int primary = -1;
    bool member = false;
    int last = 0;
    int ii = 0;

    int processed[groupInfo.num_islands];
    memset(processed, -1, groupInfo.num_islands * sizeof(int));

    int tmp[groupInfo.size];

    int count = laik_trans_groupCount(t , subgroup);

    if(count == t->group->size)
    {   
        laik_secondary_updateGroup(t, subgroup, 0, groupInfo.primarys, groupInfo.num_islands, -1);
        return;
    }

    for(int i = 0; i < count; ++i)
    {
        int inTask = laik_trans_taskInGroup(t, subgroup, i);

        if(processed[groupInfo.colours[inTask]] == -1)
        {
            laik_secondary_updateTask(t, subgroup, last, inTask);
            ++last;
            processed[groupInfo.colours[inTask]] = inTask;
        }

        if(groupInfo.colour == groupInfo.colours[inTask])
        {
            tmp[ii++] = groupInfo.secondaryRanks[inTask];

            member |= groupInfo.rank == groupInfo.secondaryRanks[inTask];
        }
    }

    primary = groupInfo.secondaryRanks[processed[groupInfo.colour]]; 

    laik_secondary_updateGroup(t, subgroup, last, &tmp[1], ii - 1, chain_idx);

    group->primary = primary;
    group->member = member;
    group->sectionE = last + ii - 1;
    group->sectionB = last;
}

int main(int argc, char **argv)
{
    (void)argc;
    (void) argv;

    return SHMEM_SUCCESS;
}