#!/bin/sh
LAIK_BACKEND=mpi LAIK_SECONDARIES=SHMEM LAIK_SHMEM_COPY_SCHEME=0 ${MPIEXEC-mpiexec} -n 8 ../../examples/jac3d 500 500 > test-bench-0-3d.out
cmp test-bench-0-3d.out "$(dirname -- "${0}")/test-bench-0-3d.expected"
