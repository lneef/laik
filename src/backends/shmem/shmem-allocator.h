

#ifndef SHMEM_ALLOCATOR_H
#define SHMEM_ALLOCATOR_H

#include "laik.h"

void shmem_free(void* ptr);

void deleteAllocatedSegments(void);

void* shmem_alloc(size_t size, int* shimdPtr);

void cleanup(void);

int get_shmid(void *ptr, int *shmid, int *offset);

#endif
