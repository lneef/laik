/*
 * This file is part of the LAIK library.
 * Copyright (c) 2017, 2018 Josef Weidendorfer <Josef.Weidendorfer@gmx.de>
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

#include "laik-internal.h"
#include "laik/action.h"

#include <assert.h>
#include <stdbool.h>
#include <stdlib.h>

// generic helpers for backends

void laik_secondaries_cleanup(const Laik_Backend* backend)
{
  for(int i = 0; i < backend->chain_length; ++i)
    backend->chain[i]->laik_secondary_cleanup(backend->chain[i]);
}

void laik_secondaries_finalize(const Laik_Backend* backend)
{
    for(int i = 0; i < backend -> chain_length; ++i)
    {
        backend->chain[i]->laik_secondary_finalize(backend->chain[i]);
        free(backend->chain[i]->groupInfo);
        free(backend->chain[i]);
    }
}

bool laik_secondaries_prepare(const Laik_Backend* backend, Laik_ActionSeq* as)
{
    bool changed = false;
    for(int i = backend->chain_length - 1; i > -1; --i)
    {
        changed |= backend->chain[i]->laik_secondary_prepare(backend->chain[i], as);
    }
    return changed;
}

void laik_secondaries_update_group(const Laik_Backend* backend, Laik_Group* g)
{
    for(int i = 0; i < backend->chain_length; ++i)
        backend->chain[i]->laik_secondary_update_group(backend->chain[i], g);
}
