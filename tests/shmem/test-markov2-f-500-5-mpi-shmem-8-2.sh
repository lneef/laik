#!/bin/sh
LAIK_BACKEND=mpi LAIK_SECONDARIES=SHMEM,SHMEM LAIK_SHMEM_SUB_ISLANDS=2 ${MPIEXEC-mpiexec} -n 8 ../../examples/markov2 -f 500 5 > test-markov2-f-500-5-mpi-4.out
cmp test-markov2-f-500-5-mpi-4.out "$(dirname -- "${0}")/../test-markov2-f-500-5.expected"
