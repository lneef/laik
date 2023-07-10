#!/bin/sh
LAIK_BACKEND=mpi LAIK_SECONDARIES=SHMEM,SHMEM LAIK_SHMEM_SUB_ISLANDS=2 ${MPIEXEC-mpiexec} -n 8 ../../examples/jac1d 1000 50 10 > test-jac1d-1000-repart-mpi-4.out
cmp test-jac1d-1000-repart-mpi-4.out "$(dirname -- "${0}")/test-jac1d-1000-repart-8.expected"
