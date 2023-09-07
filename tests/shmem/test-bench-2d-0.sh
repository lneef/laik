#!/bin/sh
LAIK_BACKEND=mpi LAIK_SECONDARIES=SHMEM LAIK_SHMEM_COPY_SCHEME=0 ${MPIEXEC-mpiexec} -n 8 ../../examples/jac2d 10000 500 > test-bench-0-2d.out
cmp test-bench-0-2d.out "$(dirname -- "${0}")/test-bench-0-2d.expected"
