#!/bin/sh
LAIK_BACKEND=mpi LAIK_SECONDARIES=SHMEM,SHMEM LAIK_SHMEM_RANKS_PER_ISLANDS=1 ${MPIEXEC-mpiexec} -n 1 ../../examples/jac2d -s 1000 > test-jac2d-1000-mpi-1.out
cmp test-jac2d-1000-mpi-1.out "$(dirname -- "${0}")/../test-jac2d-1000.expected"
