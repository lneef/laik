#!/bin/sh
LAIK_BACKEND=mpi LAIK_SECONDARIES=SHMEM,SHMEM LAIK_SHMEM_SUB_ISLANDS=2 ${MPIEXEC-mpiexec} -n 8 ../../examples/vsum2 | LC_ALL='C' sort > test-vsum2-mpi-4.out
cmp test-vsum2-mpi-4.out "$(dirname -- "${0}")/test-vsum2-8.expected"
