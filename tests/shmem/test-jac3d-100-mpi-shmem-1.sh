#!/bin/sh
LAIK_BACKEND=mpi LAIK_SECONDARIES=SHMEM,SHMEM LAIK_SHMEM_RANKS_PER_ISLANDS=1,1 ${MPIEXEC-mpiexec} -n 1 ../../examples/jac3d -s 100 > test-jac3d-100-mpi-1.out
cmp test-jac3d-100-mpi-1.out "$(dirname -- "${0}")/../test-jac3d-100.expected"
