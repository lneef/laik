#include "shmem.h"

#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <sys/shm.h>
#include <time.h>
#include <signal.h>
#include <stdatomic.h>

#define SHM_KEY 0x33 // TODO über SHMEM_RUN dynamisch key generieren (prüfen ob besetzt und dann neuen erzeugen) und key dann als Umgebungsvariable verteilen
#define MAX_WAITTIME 8


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
    int *colours;
    int *secondaryRanks;
};

struct groupInfo groupInfo;
int openShmid = -1;

void deleteOpenShmSeg()
{
    if (openShmid != -1)
        shmctl(openShmid, IPC_RMID, 0);
}

int shmem_init()
{
    signal(SIGINT, deleteOpenShmSeg);

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
            shmid = shmget(SHM_KEY, 0, IPC_CREAT | 0644); // TODO maybe remove IPC_Creat might not be needed
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

int hash(int x)
{
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = (x >> 16) ^ x;
    return x;
}

int shmem_send(const void *buffer, int count, int datatype, int recipient)
{

    char *shmp;
    int size = datatype * count;
    int shmAddr = hash(recipient + hash(groupInfo.rank));

    int shmid = shmget(shmAddr, size + 1 + sizeof(int), 0644 | IPC_CREAT);
    if (shmid == -1)
    {
        // TODO eventuell rauskürzen da nicht gebraucht
        shmid = shmget(shmAddr, 0, 0644 | IPC_CREAT);
        if (shmid == -1)
            return SHMEM_SHMGET_FAILED;

        if (shmctl(shmid, IPC_RMID, 0) == -1)
            return SHMEM_SHMCTL_FAILED;

        shmid = shmget(shmAddr, size + 1 + sizeof(int), 0644 | IPC_CREAT);
        if (shmid == -1)
            return SHMEM_SHMGET_FAILED;
    }
    openShmid = shmid;

    // Attach to the segment to get a pointer to it.
    shmp = shmat(shmid, NULL, 0);
    if (shmp == (void *)-1)
        return SHMEM_SHMAT_FAILED;

    shmp[0] = 'd';
    memcpy((shmp + 1 + sizeof(int)), buffer, size);
    memcpy(shmp + 1, &count, sizeof(int));
    shmp[0] = 'r';
    while (shmp[0] != 'd')
    {
    }

    if (shmdt(shmp) == -1)
        return SHMEM_SHMDT_FAILED;

    shmctl(shmid, IPC_RMID, 0);
    openShmid = -1;

    return SHMEM_SUCCESS;
}

int shmem_recv(void *buffer, int count, int datatype, int sender, int *received)
{
    char *shmp;
    int bufSize = datatype * count;
    int shmAddr = hash(groupInfo.rank + hash(sender));

    time_t t_0 = time(NULL);
    int shmid = shmget(shmAddr, 0, 0644);
    while (shmid == -1 && time(NULL) - t_0 < MAX_WAITTIME)
        shmid = shmget(shmAddr, 0, 0644);
    if (shmid == -1)
        return SHMEM_SHMGET_FAILED;

    // Attach to the segment to get a pointer to it.
    shmp = shmat(shmid, NULL, 0);
    if (shmp == (void *)-1)
        return SHMEM_SHMAT_FAILED;

    while (shmp[0] != 'r')
    {
    }
    memcpy(received, shmp + 1, sizeof(int));
    int receivedSize = *received * datatype;
    if (bufSize < receivedSize)
    {
        memcpy(buffer, (shmp + 1 + sizeof(int)), bufSize);
        return SHMEM_RECV_BUFFER_TOO_SMALL;
    }
    else
    {
        memcpy(buffer, (shmp + 1 + sizeof(int)), receivedSize);
    }
    shmp[0] = 'd';

    if (shmdt(shmp) == -1)
        return SHMEM_SHMDT_FAILED;

    shmctl(shmid, IPC_RMID, 0);

    return SHMEM_SUCCESS;
}

int shmem_comm_split()
{
    return SHMEM_SUCCESS;
}

// TODO filter out not used erroro codes
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
    case SHMEM_INVALID_OR_UNSUPPORTED_ADRESS:
        strcpy(str, "invalid or unsupported adress");
        break;
    case SHMEM_SOCKET_CONNECTION_FAILED:
        strcpy(str, "socket connection failed");
        break;
    case SHMEM_SOCKET_CREATION_FAILED:
        strcpy(str, "socked creation failed");
        break;
    case SHMEM_SETSOCKOPT_FAILED:
        strcpy(str, "setsockopt failed");
        break;
    case SHMEM_SOCKET_BIND_FAILED:
        strcpy(str, "socket bind failed");
        break;
    case SHMEM_SOCKET_LISTEN_FAILED:
        strcpy(str, "socket listen failed");
        break;
    case SHMEM_SOCKET_ACCEPT_FAILED:
        strcpy(str, "socket accept failed");
        break;
    case SHMEM_RECV_BUFFER_TOO_SMALL:
        strcpy(str, "recv was given a too small buffer");
        break;
    default:
        strcpy(str, "error unknown to shmem");
        return SHMEM_FAILURE;
    }
    return SHMEM_SUCCESS;
}

int shmem_finalize()
{
    deleteOpenShmSeg();

    if (groupInfo.colours != NULL)
        free(groupInfo.colours);
    
    if (groupInfo.secondaryRanks != NULL)
        free(groupInfo.secondaryRanks);

    return SHMEM_SUCCESS;
}

int shmem_secondary_init(int primaryRank, int primarySize, int (*send)(int *, int, int),
                         int (*recv)(int *, int, int))
{
    signal(SIGINT, deleteOpenShmSeg);

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
            shmid = shmget(SHM_KEY, 0, IPC_CREAT | 0644); // TODO maybe remove IPC_Creat might not be needed
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
        groupInfo.rank = ++shmp->rank;
        groupInfo.colour = shmp->colour;

        if (shmdt(shmp) == -1)
            return SHMEM_SHMDT_FAILED;
    }
    else
    {
        // Master initialization
        openShmid = shmid;

        groupInfo.rank = 0;
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
        groupInfo.colours[0] = groupInfo.colour;
        groupInfo.secondaryRanks = malloc(primarySize * sizeof(int));
        groupInfo.secondaryRanks[0] = groupInfo.rank;

        for (int i = 1; i < primarySize; i++)
        {
            (*recv)(&groupInfo.colours[i], 1, i);
            (*recv)(&groupInfo.secondaryRanks[i], 1, i);
        }

        int *groups[primarySize]; //TODO remove the unnecesary group building only the size is relevant.
        shmem_calculate_groups(groupInfo.colours, groups, primarySize);

        for (int i = 1; i < primarySize; i++)
        {
            (*send)(&groups[groupInfo.colours[i]][0], 1, i); // TODO remove the &***[0] part
            (*send)(groupInfo.colours, primarySize, i);
            (*send)(groupInfo.secondaryRanks, primarySize, i);
        }
        groupInfo.size = groups[groupInfo.colours[0]][0];
    }
    else
    {
        (*send)(&groupInfo.colour, 1, 0);
        (*send)(&groupInfo.rank, 1, 0);

        (*recv)(&groupInfo.size, 1, 0);
        groupInfo.colours = malloc(primarySize * sizeof(int));
        (*recv)(groupInfo.colours, groupInfo.size, 0);
        groupInfo.secondaryRanks = malloc(primarySize * sizeof(int));
        (*recv)(groupInfo.secondaryRanks, groupInfo.size, 0);
    }

    if (groupInfo.rank == 0 && shmctl(openShmid, IPC_RMID, 0) == -1)
        return SHMEM_SHMCTL_FAILED;
    openShmid = -1;

    return SHMEM_SUCCESS;
}

struct intList
{
    int val;
    struct intList *next;
};

void add_element(int val, struct intList **head)
{
    struct intList *new = malloc(sizeof(struct intList));
    new->val = val;

    if (*head == NULL)
    {
        *head = new;
        return;
    }

    (*head)->next = new;
    *head = new;
}

void increment_val(struct intList **current)
{
    if (*current == NULL)
    {
        *current = malloc(sizeof(struct intList));
        (*current)->val = 0;
    }

    (*current)->val++;
}

void intList_to_array(struct intList *list, int **arr)
{
    if (list == NULL)
    {
        *arr = malloc(sizeof(int));
        *arr = 0;
        return;
    }

    int length = list->val + 1;
    *arr = malloc(length * sizeof(int));
    for (int i = 0; i < length; i++, list = list->next)
    {
        (*arr)[i] = list->val;
    }
}

int shmem_calculate_groups(int *colours, int **groups, int size)
{
    struct intList **tails = malloc(size * sizeof(struct intList *));
    struct intList **heads = malloc(size * sizeof(struct intList *));
    for (int i = 0; i < size; i++)
    {
        struct intList *new = malloc(sizeof(struct intList));
        new->val = 0;
        tails[i] = new;
        heads[i] = new;
    }

    for (int i = 0; i < size; i++)
    {
        increment_val(&heads[colours[i]]);
        add_element(i, &tails[colours[i]]);
    }

    for (int i = 0; i < size; i++)
    {
        intList_to_array(heads[i], &groups[i]);
    }
    return SHMEM_SUCCESS;
}

int shmem_get_colours(int *buf)
{
    memcpy(buf, groupInfo.colours, groupInfo.size * sizeof(int));
    return SHMEM_SUCCESS;
}

int shmem_get_secondaryRanks(int *buf)
{
    memcpy(buf, groupInfo.secondaryRanks, groupInfo.size * sizeof(int));
    return SHMEM_SUCCESS;
}

int main(int argc, char **argv)
{
    /*int err = MPI_Init(&argc, &argv);

    MPI_Comm comm;
    err = MPI_Comm_dup(MPI_COMM_WORLD, &comm);
    err = MPI_Comm_set_errhandler(comm, MPI_ERRORS_RETURN);

    int size, rank;
    err = MPI_Comm_size(comm, &size);
    err = MPI_Comm_rank(comm, &rank);

    int (*send)(int *, int, int);
    int (*recv)(int *, int, int);
    send = &sendIntegers;
    recv = &recvIntegers;
    initComm = comm;
    shmem_secondary_init(rank, size, send, recv);

    printf("rank %d (of %d) {", rank, groupInfo.size);
    for (int i = 0; i < groupInfo.size; i++)
    {
        printf("%d, ", groupInfo.group[i]);
    }
    printf("}\n");

    shmem_finalize();

    MPI_Finalize();*/
    return SHMEM_SUCCESS;
}