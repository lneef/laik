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

#ifndef LAIK_BACKEND_H
#define LAIK_BACKEND_H

//
// Header file defining the interface between LAIK and a LAIK backend
// This is not public LAIK API, but for backend implementations.
// Do not include this file in LAIK applications!
//

#include "../laik.h"     // for Laik_Transition, Laik_Data, Laik_Instance, Laik...
#include "action.h"
#include <stdbool.h>  // for bool
#include "definitions.h" // for maximal number of secondaries
#include "laik/core.h"
#include "laik/data.h"

// LAIK communication back-end
// there is no generic initialization function; laik_init() knowns
// about available backends, and it calls specific init functions directly
struct _Laik_Backend {
  char* name;
  void (*finalize)(Laik_Inst_Data*, Laik_Instance*);

  // Record the actions required to do a transition on data, eventually
  // using provided mappings (can be 0 if mappings are not allocated yet).
  //
  // Can be NULL to state that this communication backend driver does not
  // support recording of actions.
  //
  // this function allows the backend driver to
  // - allocate resources which can be reused when doing the same transition
  //   multiple times (such as memory space for buffers, reductions, lists
  //   of outstanding requests, or resources of the communication library).
  //   For one LAIK data container, only one asynchronous transition can be
  //   active at any time. Thus, resources can be shared among transition
  //   plans for one data container.
  // - prepare an optimized communcation schedule using the pre-allocated
  //   resources
  void (*prepare)(Laik_Inst_Data*, Laik_ActionSeq*);

  // free resources allocated for an action sequence
  void (*cleanup)(Laik_Inst_Data*, Laik_ActionSeq*);

  // execute a action sequence
  void (*exec)(Laik_Inst_Data*, Laik_ActionSeq*);

  // update )backend specific data for group if needed
  void (*updateGroup)(Laik_Inst_Data*, Laik_Group*, int rank, int size);

  // sync of key-value store
  void (*sync)(Laik_KVStore* kvs);

  // log backend-specific action, return true if handled (see laik_log_Action)
  bool (*log_action)(Laik_Inst_Data*, Laik_ActionSeq* as, Laik_Action* a);

  // ensure progress in backend, can be NULL
  void (*make_progress)(Laik_Inst_Data*);

  // function for elasticity support, to be called by all active
  // processes, resulting in a global synchronization.
  // if not provided by a backend, no elasticity is supported.
  // - process join and remove requests given as parameter
  // - without join/remove requests, return 0, otherwise
  // - return new process group reflecting join/remove requests,
  //   with current "world" group as parent
  // TODO:
  // - also handle remove requests via function parameter
  // - sub-world elasticity
  Laik_Group* (*resize)(Laik_Inst_Data*, Laik_ResizeRequests*);

  // for elasticity: removal of processes which got started in a previous
  // resize is finished. They can be marked as dead and resources freed
  void (*finish_resize)(Laik_Inst_Data*);

  Laik_Allocator* (*allocator)();
};

void laik_next_prepare(Laik_Inst_Data* idata, Laik_ActionSeq* as);

void laik_next_cleanup(Laik_Inst_Data* idata, Laik_ActionSeq* as);

void laik_next_updateGroup(Laik_Inst_Data* idata, Laik_Group* g, int rank, int size);

Laik_Action* laik_next_exec(Laik_Inst_Data* idata, Laik_ActionSeq* as);

bool laik_next_log(Laik_Inst_Data* idata, Laik_ActionSeq* as, Laik_Action* a);

void laik_next_finalize(Laik_Inst_Data* idata, Laik_Instance* inst);

#define SEND_INTS(buffer, num, receiver, inst_data, g) (inst_data->prev->send(buffer, num, receiver, inst_data->prev, g))

#define RECV_INTS(buffer, num, sender, inst_data, g) (inst_data->prev->recv(buffer, num, sender, inst_data->prev, g))

#endif // LAIK_BACKEND_H
