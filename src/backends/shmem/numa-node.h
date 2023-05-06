

#ifndef NUMA_NODE_H
#define NUMA_NODE_H
#include <stdint.h>

struct proc_info{
    unsigned int cpu;
    unsigned int node;

};

int bind_cpu(unsigned int cpu);

int bind_current_cpu(void);

int get_info(struct proc_info* info);


#endif