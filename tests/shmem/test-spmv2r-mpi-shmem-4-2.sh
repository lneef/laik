#!/bin/sh
LAIK_BACKEND=mpi LAIK_SECONDARIES=SHMEM LAIK_SHMEM_SUB_ISLANDS=2 ${MPIEXEC-mpiexec} -n 4 ../../examples/spmv2 -r 10 3000 | LC_ALL='C' sort > test-spmv2r-mpi-4.out
cmp test-spmv2r-mpi-4.out "$(dirname -- "${0}")/test-spmv2.expected"
