#!/bin/sh
LAIK_BACKEND=mpi LAIK_SECONDARIES=SHMEM,SHMEM LAIK_SHMEM_SUB_ISLANDS=2 ${MPIEXEC-mpiexec} -n 8 ../../examples/vsum | LC_ALL='C' sort > test-vsum-mpi-4.out
cmp test-vsum-mpi-4.out "$(dirname -- "${0}")/test-vsum-8.expected"
