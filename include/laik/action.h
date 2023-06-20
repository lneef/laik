/*
 * This file is part of the LAIK library.
 * Copyright (c) 2018 Josef Weidendorfer <Josef.Weidendorfer@gmx.de>
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

#ifndef LAIK_ACTIONS_H
#define LAIK_ACTIONS_H

#include <stdint.h>  // for uint64_t

#include "core.h" // for Laik_Instance

/**
 * LAIK Actions and Action Sequences
 *
 * A Laik_Action is a communication/synchronization request.
 * It can be high-level (e.g. a transition between partitionings) or
 * low-level and backend-specific (e.g. an MPI send of a buffer).
 * Usually it is bound to a LAIK process (ie. only to be executed within
 * that process).
 * A high-level action can be transformed to a sequence of low-level
 * actions by providing dynamic information such as partitionings or mappings
 * of LAIK containers, or the backend driver that should execute the action.
 * Low-level actions become invalid if the information used in the
 * transformation (partitioning, mapping, ...) changes.
 * Actions may use IDs for process groups or mappings, and thus they are
 * often not self-contained: if so, they refer to an action context which
 * is given as part of the sequence (Laik_ActionSeq) they are part of.
 * Every Laik_Action instance has the same header:
 *  1 byte for the action type
 *  1 byte for the length
 *
 * A Laik_ActionSeq is a list of actions, together with context information
 * (which allows to check whether actions are still valid) and
 * allocated resources to be able to execute the actions (such as buffer
 * space).
 * If an action sequence contains backend-specific actions, all such actions
 * have to refer to the same backend.
 */

typedef enum _Laik_ActionType {
    LAIK_AT_Invalid = 0,

    // HALT action: stop execution
    LAIK_At_Halt,

    // NOP action: to allow to "remove" actions by patching
    LAIK_AT_Nop,

    // high-level
    LAIK_AT_TExec,

    // low-level, independent from backend

    // reserve buffer space
    LAIK_AT_BufReserve,

    // send items from a buffer (or directly from container)
    LAIK_AT_MapSend, LAIK_AT_BufSend, LAIK_AT_RBufSend,
    // receive items into a buffer (or directly into container)
    LAIK_AT_MapRecv, LAIK_AT_BufRecv, LAIK_AT_RBufRecv,

    // call to a reduce/init operation
    LAIK_AT_RBufLocalReduce, LAIK_AT_BufInit,

    // copy from buffer to container and vice versa
    LAIK_AT_CopyFromBuf, LAIK_AT_CopyToBuf,
    LAIK_AT_CopyFromRBuf, LAIK_AT_CopyToRBuf,

    // iteratively pack items from container into buffer and send it afterwards
    LAIK_AT_MapPackAndSend, LAIK_AT_PackAndSend,
    // pack items from container into buffer
    LAIK_AT_MapPackToRBuf, LAIK_AT_PackToRBuf,
    LAIK_AT_MapPackToBuf, LAIK_AT_PackToBuf,

    // iteratively receive items into buffer and unpack into container
    LAIK_AT_MapRecvAndUnpack, LAIK_AT_RecvAndUnpack,
    // unpack data from buffer into container
    LAIK_AT_MapUnpackFromRBuf, LAIK_AT_UnpackFromRBuf,
    LAIK_AT_MapUnpackFromBuf, LAIK_AT_UnpackFromBuf,

    // reduce from all to one or all
    LAIK_AT_Reduce, LAIK_AT_RBufReduce,
    // reduce using input from a subgroup of task and output to subgroup
    LAIK_AT_MapGroupReduce, LAIK_AT_GroupReduce, LAIK_AT_RBufGroupReduce,

    // copy 1d data from container into buffer or from buffer into container
    LAIK_AT_Copy,

    // copy between buffers
    LAIK_AT_BufCopy, LAIK_AT_RBufCopy,

    // low-level, backend-specific (50 unique actions should be enough)
    LAIK_AT_Backend = 50, LAIK_AT_Backend_Max = 99

} Laik_ActionType;


//
// Laik_Action
//

// all actions must start with this header
typedef struct _Laik_Action Laik_Action;

// for actions refering to a transition
typedef struct _Laik_TransitionContext Laik_TransitionContext;

// for delegating parts of subgroups to secondaries
typedef struct _TaskGroupAS TaskGroupAS;

//
// Laik_ActionSeq
//
// Within an action sequence, the actions for multiple transitions for different
// data containers can be recorded, for multiple later executions triggered by
// just one call.
// An action sequence is opaque to LAIK users.

typedef struct _Laik_ActionSeq Laik_ActionSeq;

// create a new action sequence object, usable for the given LAIK instance
Laik_ActionSeq* laik_aseq_new(Laik_Instance* inst);
// free all resources allocated for an action sequence
void laik_aseq_free(Laik_ActionSeq* as);

// get sum of sizes of all allocated temporary buffers
int laik_aseq_bufsize(Laik_ActionSeq* as);


#endif // LAIK_ACTIONS_H
