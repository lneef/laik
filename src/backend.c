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
#include "laik/action-internal.h"
#include "laik/action.h"

#include <assert.h>
#include <stdbool.h>
#include <stdlib.h>

// generic helpers for backends

void laik_next_prepare(Laik_Inst_Data* idata, Laik_ActionSeq* as)
{
    if(idata->next_backend) idata->next_backend->prepare(idata->next, as);
}

void laik_next_cleanup(Laik_Inst_Data* idata, Laik_ActionSeq* as)
{
    if(idata->next_backend) idata->next_backend->cleanup(idata->next, as);
}

void laik_next_updateGroup(Laik_Inst_Data* idata, Laik_Group* g, int* ranks, int size)
{
    if(idata->next_backend) idata->next_backend->updateGroup(idata->next, g, ranks, size);
}

Laik_Action* laik_next_exec(Laik_Inst_Data* idata, Laik_ActionSeq* as)
{
    if(idata->next_backend) idata->next_backend->exec(idata->next, as);
    return as->currentAction;
}

bool laik_next_log(Laik_Inst_Data* idata, Laik_ActionSeq* as, Laik_Action* a)
{
    return idata->next_backend ? idata->next_backend->log_action(idata->next, as, a) : false;
}

void laik_next_finalize(Laik_Inst_Data* idata, Laik_Instance* inst)
{
    if(idata->next_backend) idata->next_backend->finalize(idata->next, inst);
    free(idata);
}
