#define _GNU_SOURCE 
#include "node.h"
#include <unistd.h>
#include <sched.h>


int bind_cpu(unsigned int cpu){
    cpu_set_t mask;

    CPU_ZERO(&mask);
    CPU_SET(cpu, &mask);

    return sched_setaffinity(0, sizeof(mask), &mask);
}

int bind_current_cpu(void){
    int cpu = sched_getcpu();

    return bind_cpu(cpu);
}

int get_info(struct proc_info* info){
    return getcpu(&info->cpu, &info->node);
}




