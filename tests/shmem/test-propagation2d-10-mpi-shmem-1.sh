#!/bin/sh
LAIK_BACKEND=mpi LAIK_SECONDARIES=SHMEM,SHMEM LAIK_SHMEM_SUB_ISLANDS=1 ${MPIEXEC-mpiexec} -n 1 ../../examples/propagation2d 10 10 > test-propagation2d-10-mpi-1.out
cmp test-propagation2d-10-mpi-1.out "$(dirname -- "${0}")/../test-propagation2d-10.expected"
