#!/bin/sh
LAIK_BACKEND=mpi LAIK_SECONDARIES=SHMEM,SHMEM LAIK_SHMEM_SUB_ISLANDS=1 ${MPIEXEC-mpiexec} -n 1 ../../examples/vsum > test-vsum-mpi-1.out
cmp test-vsum-mpi-1.out "$(dirname -- "${0}")/../test-vsum.expected"
