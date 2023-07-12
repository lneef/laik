#!/bin/sh
LAIK_BACKEND=mpi LAIK_SECONDARIES=SHMEM,SHMEM LAIK_SHMEM_RANKS_PER_ISLANDS=1,1 ${MPIEXEC-mpiexec} -n 1 ../../examples/spmv 4000 > test-spmv-mpi-1.out
cmp test-spmv-mpi-1.out "$(dirname -- "${0}")/../test-spmv.expected"
