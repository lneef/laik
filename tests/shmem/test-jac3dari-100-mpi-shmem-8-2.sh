#!/bin/sh
LAIK_BACKEND=mpi LAIK_SECONDARIES=SHMEM,SHMEM LAIK_SHMEM_RANKS_PER_ISLANDS=4,2 ${MPIEXEC-mpiexec} -n 8 ../../examples/jac3d -a -r -i 10 -s 100 > test-jac3dari-100-mpi-4.out
cmp test-jac3dari-100-mpi-4.out "$(dirname -- "${0}")/test-jac3di-100.expected"
