#!/bin/sh
LAIK_BACKEND=mpi LAIK_SECONDARIES=SHMEM LAIK_SHMEM_RANKS_PER_ISLANDS=2 ${MPIEXEC-mpiexec} -n 4 ../../examples/jac1d 1000 50 10 > test-jac1d-1000-repart-mpi-4.out
cmp test-jac1d-1000-repart-mpi-4.out "$(dirname -- "${0}")/test-jac1d-1000-repart.expected"
