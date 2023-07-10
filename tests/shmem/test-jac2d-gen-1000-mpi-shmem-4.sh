#!/bin/sh
LAIK_BACKEND=mpi LAIK_SECONDARIES=SHMEM,SHMEM LAIK_SHMEM_SUB_ISLANDS=1 LAIK_LAYOUT_GENERIC=1 ${MPIEXEC-mpiexec} -n 4 ../../examples/jac2d -s 1000 > test-jac2d-gen-1000-mpi-4.out
cmp test-jac2d-gen-1000-mpi-4.out "$(dirname -- "${0}")/test-jac2d-1000.expected"
