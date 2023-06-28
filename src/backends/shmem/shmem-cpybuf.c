
#include "shmem-cpybuf.h"
#include "laik/core.h"
#include "shmem-allocator.h"

#include <stdlib.h>

struct cpyBuf* shmem_cpybuf_obtain()
{
    struct cpyBuf* buffer = malloc(sizeof(struct cpyBuf));
    buffer -> ptr = NULL;
    buffer -> size = 0;

    return buffer;
}


void shmem_cpybuf_alloc(struct cpyBuf* buf, size_t size){

    if(buf -> size >= size)
    {
        return;
    }
    else 
    {
        if(buf->ptr) shmem_free( buf->ptr);
        int shmid;
        buf -> ptr = shmem_alloc(size, &shmid);
        buf -> size = size;
        buf -> shmid = shmid;
    }
}

void shmem_cpybuf_delete(struct cpyBuf* buf){
    shmem_free(buf->ptr);
    buf->ptr = NULL;
    buf->size = 0;
}
