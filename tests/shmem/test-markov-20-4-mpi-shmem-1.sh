#!/bin/sh
LAIK_BACKEND=mpi LAIK_SECONDARIES=SHMEM,SHMEM LAIK_SHMEM_RANKS_PER_ISLANDS=1,1 ${MPIEXEC-mpiexec} -n 1 ../../examples/markov 20 4 > test-markov-20-4-mpi-1.out
cmp test-markov-20-4-mpi-1.out "$(dirname -- "${0}")/../test-markov-20-4.expected"
