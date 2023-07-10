#!/bin/sh
LAIK_BACKEND=mpi LAIK_SECONDARIES=SHMEM LAIK_SHMEM_SUB_ISLANDS=2 ${MPIEXEC-mpiexec} -n 4 ../../examples/markov2 40 4 > test-markov2-40-4-mpi-4.out
cmp test-markov2-40-4-mpi-4.out "$(dirname -- "${0}")/test-markov2-40-4.expected"
