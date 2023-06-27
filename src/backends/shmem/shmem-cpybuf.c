
#include "shmem-cpybuf.h"
#include "laik/core.h"
#include "shmem-allocator.h"

#include <stdlib.h>

struct cpyBuf* shmem_cpybuf_obtain(size_t size)
{
    int shmid;
    void* ptr = shmem_alloc(size, &shmid);

    struct cpyBuf* buffer = malloc(sizeof(struct cpyBuf));
    buffer -> shmid = shmid;
    buffer -> ptr = ptr;
    buffer -> size = size;

    return buffer;
}


void shmem_cpybuf_alloc(struct cpyBuf* buf, size_t size){

    if(buf -> size >= size)
    {
        return;
    }
    else 
    {
        size = (size / buf->size) + 1;
        size *= buf->size;
        shmem_free( buf->ptr);
        int shmid;
        buf -> ptr = shmem_alloc(size, &shmid);
        buf -> size = size;
        buf -> shmid = shmid;
    }
}

void shmem_cpybuf_delete(struct cpyBuf* buf){
    shmem_free(buf->ptr);

    free(buf);
}
