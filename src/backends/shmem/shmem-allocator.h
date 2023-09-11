/* This file is part of the LAIK parallel container library.
 * Copyright (c) 2023 Lukas Neef <lukas.neef@tum.de>
 *
 * LAIK is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, version 3.
 *
 * LAIK is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef SHMEM_ALLOCATOR_H
#define SHMEM_ALLOCATOR_H

#include "laik.h"
#include <stdatomic.h>
#include <stdint.h>

struct shmHeader
{
    int size;
    atomic_int ref;
    atomic_bool z;
    int shmid;
};

#define HEADER_SIZE (sizeof(struct shmHeader))
#define HEADER_PAD 64

// free specific segment
void shmem_free(void* ptr);

// release all allocated segments
void deleteAllocatedSegments();

// allocate shared memory segment of specific size
void* shmem_alloc(size_t size, int* shimdPtr);

// same as shmem_alloc, but the user can specify additional flags
void* shmem_alloc_f(size_t size, int* shimdPtr, int flag);

// open shared memory segment with a specific key
void* shmem_key_alloc(int key, size_t size, int* shimdPtr);

// free memory segment opened in the context of the zero copy scheme
void shmem_free_zero_copy(struct shmHeader* sh);

// get shmid and offset for specific pointer
int get_shmid(void *ptr, int *shmid, int *offset);

#endif
