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

#include <laik.h>     // for Laik_Transition, Laik_Data, Laik_Instance, Laik...
#include <stdbool.h>  // for bool
#include "definitions.h" // for maximal number of secondaries


// LAIK communication back-end
// there is no generic initialization function; laik_init() knowns
// about available backends, and it calls specific init functions directly
struct _Laik_Backend {
  char* name;
  void (*finalize)(const Laik_Backend*, Laik_Instance*);

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
  void (*prepare)(const Laik_Backend*, Laik_ActionSeq*);

  // free resources allocated for an action sequence
  void (*cleanup)(const Laik_Backend*, Laik_ActionSeq*);

  // execute a action sequence
  void (*exec)(const Laik_Backend*, Laik_ActionSeq*);

  // update backend specific data for group if needed
  void (*updateGroup)(const Laik_Backend*, Laik_Group*);

  // sync of key-value store
  void (*sync)(const Laik_Backend*, Laik_KVStore* kvs);

  // log backend-specific action, return true if handled (see laik_log_Action)
  bool (*log_action)(const Laik_Backend*, Laik_Action* a);

  // ensure progress in backend, can be NULL
  void (*make_progress)(const Laik_Backend*);

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
  Laik_Group* (*resize)(const Laik_Backend*, Laik_ResizeRequests*);

  // for elasticity: removal of processes which got started in a previous
  // resize is finished. They can be marked as dead and resources freed
  void (*finish_resize)(const Laik_Backend*);
  
  Laik_Secondary* chain[MAX_SECONDARIES];

  unsigned char chain_length;
};

struct _Laik_Secondary {
  //index in chain of secondary backends
  unsigned chain_index;

  // delete all buffers allocated by secondary backend
  void (*laik_secondary_finalize)();

  //prepare action sequence, i.e. replace generic laik actions with backend specific actions
  bool (*laik_secondary_prepare)(const Laik_Backend*, Laik_ActionSeq *);

  //execute backend specific action
  bool (*laik_secondary_exec)(const Laik_Backend*, Laik_ActionSeq *, Laik_Action *);

  //cleanup temporary buffers
  void (*laik_secondary_cleanup)();

  bool (*laik_secondary_log_action)(Laik_Action *);
};




#endif // LAIK_BACKEND_H
