#!/bin/sh
LAIK_BACKEND=mpi LAIK_SECONDARIES=SHMEM LAIK_SHMEM_RANKS_PER_ISLANDS=2 LAIK_LAYOUT_GENERIC=1 ${MPIEXEC-mpiexec} -n 4 ../../examples/jac3d -s 100 > test-jac3d-gen-100-mpi-4.out
cmp test-jac3d-gen-100-mpi-4.out "$(dirname -- "${0}")/test-jac3d-100.expected"
