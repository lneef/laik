#!/bin/sh
LAIK_BACKEND=mpi LAIK_SECONDARIES=SHMEM,SHMEM LAIK_SHMEM_RANKS_PER_ISLANDS=4,2 LAIK_SHMEM_COPY_SCHEME=2,2 ${MPIEXEC-mpiexec} -n 8 ../../examples/markov2 40 4 > test-markov2-40-4-mpi-4.out
cmp test-markov2-40-4-mpi-4.out "$(dirname -- "${0}")/test-markov2-40-4.expected"
