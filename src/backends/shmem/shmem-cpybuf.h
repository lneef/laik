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

#ifndef SHMEM_CPYBUF_H
#define SHMEM_CPYBUF_H
#include <stddef.h>
struct cpyBuf{
    void* ptr;
    int shmid;
    size_t request;
    size_t size;
};

// obtain copy buffer
struct cpyBuf* shmem_cpybuf_obtain();

// obtain copy buffer with the given size
void shmem_cpybuf_alloc(struct cpyBuf*, size_t size);

// delete copy buffer
void shmem_cpybuf_delete(struct cpyBuf*);

// request certain size (no allocation)
void shmem_cpybuf_request(struct cpyBuf*, size_t size);

// allocate requested size
void shmem_cpybuf_alloc_requested(struct cpyBuf*);
#endif
