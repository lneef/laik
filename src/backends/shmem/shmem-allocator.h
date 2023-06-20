

#ifndef SHMEM_ALLOCATOR_H
#define SHMEM_ALLOCATOR_H

#include "laik.h"

void* def_shmem_malloc(Laik_Data* d, size_t size);

void def_shmem_free(Laik_Data* d, void* ptr);

void deleteAllocatedSegments(void);

int get_shmid(void *ptr, int *shmid, int *offset);

#endif
