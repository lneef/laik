/* This file is part of the LAIK parallel container library.
 * Copyright (c) 2023 Lukas Neef <lukas.neef@tum.de>
 *
 * LAIK is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, version 3.
 *
 * LAIK is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef SHMEM_MANAGER_H
#define SHMEM_MANAGER_H

#include <stddef.h>
#include <laik.h>

void* shmem_manager_alloc(size_t size);

void shmem_manager_cleanup();

void* def_shmem_malloc(Laik_Data* d, size_t size);

void def_shmem_free(Laik_Data* d, void* ptr);

#endif