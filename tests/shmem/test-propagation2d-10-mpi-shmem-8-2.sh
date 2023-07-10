#!/bin/sh
LAIK_BACKEND=mpi LAIK_SECONDARIES=SHMEM,SHMEM LAIK_SHMEM_SUB_ISLANDS=2 ${MPIEXEC-mpiexec} -n 8 ../../examples/propagation2d 10 10 > test-propagation2d-10-mpi-4.out
cmp test-propagation2d-10-mpi-4.out "$(dirname -- "${0}")/test-propagation2d-10.expected"
