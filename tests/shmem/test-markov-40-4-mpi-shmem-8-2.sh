#!/bin/sh
LAIK_BACKEND=mpi LAIK_SECONDARIES=SHMEM,SHMEM LAIK_SHMEM_SUB_ISLANDS=2 ${MPIEXEC-mpiexec} -n 8 ../../examples/markov 40 4 > test-markov-40-4-mpi-4.out
cmp test-markov-40-4-mpi-4.out "$(dirname -- "${0}")/test-markov-40-4.expected"
