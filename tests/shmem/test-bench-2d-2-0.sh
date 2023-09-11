#!/bin/sh
LAIK_BACKEND=mpi LAIK_SECONDARIES=SHMEM,SHMEM LAIK_SHMEM_COPY_SCHEME=2,0 LAIK_SHMEM_RANKS_PERS_ISLANDS=8,4 ${MPIEXEC-mpiexec} -n 8 ../../examples/jac2d 10000 500 > test-bench-0-2d.out
cmp test-bench-0-2d.out "$(dirname -- "${0}")/test-bench-0-2d.expected"
