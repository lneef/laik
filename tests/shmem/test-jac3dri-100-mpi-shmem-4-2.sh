#!/bin/sh
LAIK_BACKEND=mpi LAIK_SECONDARIES=SHMEM LAIK_SHMEM_RANKS_PER_ISLANDS=2 ${MPIEXEC-mpiexec} -n 4 ../../examples/jac3d -r -i 10 -s 100 > test-jac3dri-100-mpi-4.out
cmp test-jac3dri-100-mpi-4.out "$(dirname -- "${0}")/test-jac3di-100.expected"
