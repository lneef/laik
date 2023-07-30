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

#include "shmem-cpybuf.h"
#include "laik/core.h"
#include "shmem-allocator.h"
#include <assert.h>

#include <stdlib.h>

struct cpyBuf* shmem_cpybuf_obtain()
{
    struct cpyBuf* buffer = malloc(sizeof(struct cpyBuf));
    buffer -> ptr = NULL;
    buffer -> size = 0;
    buffer -> request = 0;

    return buffer;
}


void shmem_cpybuf_alloc(struct cpyBuf* buf, size_t size){

    if(buf -> size >= size)
    {
        return;
    }
    else 
    {
        if(buf->ptr != NULL) shmem_free( buf->ptr);
        int shmid;
        buf -> ptr = shmem_alloc(size, &shmid);
        buf -> size = size;
        assert(shmid> 0);
        buf -> shmid = shmid;
    }
}

void shmem_cpybuf_delete(struct cpyBuf* buf){
    if(buf->ptr) shmem_free(buf->ptr);
    buf->ptr = NULL;
    buf->size = 0;
    buf->request = 0;
}

void shmem_cpybuf_request(struct cpyBuf* buf, size_t size)
{
    buf->request = size > buf->request ? size : buf->request;
}

void shmem_cpybuf_alloc_requested(struct cpyBuf* cpyBuf)
{
   if(cpyBuf->request > 0) 
    shmem_cpybuf_alloc(cpyBuf, cpyBuf->request);
   
}
