#!/bin/sh
LAIK_BACKEND=mpi LAIK_SECONDARIES=SHMEM,SHMEM LAIK_SHMEM_SUB_ISLANDS=2 ${MPIEXEC-mpiexec} -n 8 ../../examples/jac3d -s 100 > test-jac3d-100-mpi-4.out
cmp test-jac3d-100-mpi-4.out "$(dirname -- "${0}")/test-jac3d-100-8.expected"
