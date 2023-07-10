#!/bin/sh
LAIK_BACKEND=mpi LAIK_SECONDARIES=SHMEM,SHMEM LAIK_SHMEM_SUB_ISLANDS=1 ${MPIEXEC-mpiexec} -n 1 ../../examples/spmv2 -r 10 3000 > test-spmv2r-mpi-1.out
cmp test-spmv2r-mpi-1.out "$(dirname -- "${0}")/../test-spmv2.expected"
