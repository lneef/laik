

#ifndef SHMEM_CPYBUF_H
#define SHMEM_CPYBUF_H
#include <stddef.h>
struct cpyBuf{
    void* ptr;
    int shmid;
    size_t size;
};

struct cpyBuf* shmem_cpybuf_obtain();

void shmem_cpybuf_alloc(struct cpyBuf*, size_t size);

void shmem_cpybuf_delete(struct cpyBuf*);

#endif
