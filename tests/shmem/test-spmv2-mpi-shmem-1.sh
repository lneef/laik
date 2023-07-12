#!/bin/sh
LAIK_BACKEND=mpi LAIK_SECONDARIES=SHMEM,SHMEM LAIK_SHMEM_RANKS_PER_ISLANDS=1,1 ${MPIEXEC-mpiexec} -n 1 ../../examples/spmv2 10 3000 > test-spmv2-mpi-1.out
cmp test-spmv2-mpi-1.out "$(dirname -- "${0}")/../test-spmv2.expected"