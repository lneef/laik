

#ifndef SHMEM_POOL_H
#define SHMEM_POOL_H

#include <stddef.h>
#include <laik.h>

void* shmem_manager_alloc(size_t size);

void shmem_manager_cleanup();

void* def_shmem_malloc(Laik_Data* d, size_t size);

void def_shmem_free(Laik_Data* d, void* ptr);

#endif
