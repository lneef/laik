#!/bin/sh
LAIK_BACKEND=mpi LAIK_SECONDARIES=SHMEM LAIK_SHMEM_RANKS_PER_ISLANDS=2 ${MPIEXEC-mpiexec} -n 4 ../../examples/markov 40 4 > test-markov-40-4-mpi-4.out
cmp test-markov-40-4-mpi-4.out "$(dirname -- "${0}")/test-markov-40-4.expected"
