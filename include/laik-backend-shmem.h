/*
 * This file is part of the LAIK library.
 * Copyright (c) 2017 Josef Weidendorfer <Josef.Weidendorfer@gmx.de>
 *
 * LAIK is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, version 3 or later.
 *
 * LAIK is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef LAIK_BACKEND_SHMEM_H
#define LAIK_BACKEND_SHMEM_H

#include "laik.h"
#include "laik-internal.h"

// SHMEM backend.

// create a LAIK instance for this backend.
// if locations is NULL, all ranks are one the islands
int laik_shmem_secondary_init(Laik_Instance*, Laik_Group* world, int* primaryRank, int* primarySize, int* ranks, int** new_ranks);


#endif // LAIK_BACKEND_SHMEM_H
