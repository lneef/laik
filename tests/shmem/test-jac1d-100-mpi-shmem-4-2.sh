#!/bin/sh
LAIK_BACKEND=mpi LAIK_SECONDARIES=SHMEM LAIK_SHMEM_RANKS_PER_ISLANDS=2 ${MPIEXEC-mpiexec} -n 4 ../../examples/jac1d 100 > test-jac1d-100-mpi-4.out
cmp test-jac1d-100-mpi-4.out "$(dirname -- "${0}")/test-jac1d-100.expected"
