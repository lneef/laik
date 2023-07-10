#!/bin/sh
LAIK_BACKEND=mpi LAIK_SECONDARIES=SHMEM,SHMEM LAIK_SHMEM_SUB_ISLANDS=2 ${MPIEXEC-mpiexec} -n 8 ../../examples/jac1d 100 > test-jac1d-100-mpi-4.out
cmp test-jac1d-100-mpi-4.out "$(dirname -- "${0}")/test-jac1d-100-8.expected"
