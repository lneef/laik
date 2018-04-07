/*
 * This file is part of the LAIK parallel container library.
 * Copyright (c) 2018 Josef Weidendorfer
 */

#include <laik-internal.h>

#include <assert.h>
#include <stdlib.h>

// TODO: rename to ActionSeq, start with empty, and appendTransition()
Laik_ActionSeq* laik_actions_new(Laik_Instance *inst)
{
    Laik_ActionSeq* as = malloc(sizeof(Laik_ActionSeq));
    as->inst = inst;

    for(int i = 0; i < CONTEXTS_MAX; i++)
        as->context[i] = 0;

    as->buf = 0;
    as->ce = 0;

    as->actionCount = 0;
    as->actionAllocCount = 0;
    as->action = 0;

    as->sendCount = 0;
    as->recvCount = 0;
    as->reduceCount = 0;

    return as;
}

void laik_actions_free(Laik_ActionSeq* as)
{
    for(int i = 0; i < CONTEXTS_MAX; i++)
        free(as->context[i]);

    free(as->buf);
    free(as->ce);

    free(as->action);
    free(as);
}

Laik_BackendAction* laik_actions_addAction(Laik_ActionSeq* as)
{
    if (as->actionCount == as->actionAllocCount) {
        // enlarge buffer
        as->actionAllocCount = (as->actionCount + 20) * 2;
        as->action = realloc(as->action,
                             as->actionAllocCount * sizeof(Laik_BackendAction));
        if (!as->action) {
            laik_panic("Out of memory allocating memory for Laik_TransitionPlan");
            exit(1); // not actually needed, laik_panic never returns
        }
    }
    Laik_BackendAction* a = &(as->action[as->actionCount]);
    as->actionCount++;

    a->type = LAIK_AT_Invalid;
    a->len = sizeof(Laik_BackendAction);
    a->tid = 0; // always refer to single transition context

    return a;
}

void laik_actions_initTContext(Laik_TransitionContext* tc,
                               Laik_Data* data, Laik_Transition* transition,
                               Laik_MappingList* fromList,
                               Laik_MappingList* toList)
{
    tc->data = data;
    tc->transition = transition;
    tc->fromList = fromList;
    tc->toList = toList;
}

int laik_actions_addTContext(Laik_ActionSeq* as,
                             Laik_Data* data, Laik_Transition* transition,
                             Laik_MappingList* fromList,
                             Laik_MappingList* toList)
{
    Laik_TransitionContext* tc = malloc(sizeof(Laik_TransitionContext));
    laik_actions_initTContext(tc, data, transition, fromList, toList);

    assert(as->context[0] == 0);
    as->context[0] = tc;

    return 0;
}

// if buffer is unknown: use indirection over mapping list + offset
void laik_actions_addSend(Laik_ActionSeq* as,
                          int fromMapNo, uint64_t off,
                          int count, int to)
{
    Laik_BackendAction* a = laik_actions_addAction(as);
    a->type = LAIK_AT_Send;
    a->mapNo = fromMapNo;
    a->offset = off;
    a->count = count;
    a->peer_rank = to;

    as->sendCount += count;
}

void laik_actions_addSendBuf(Laik_ActionSeq* as,
                             char* fromBuf, int count, int to)
{
    Laik_BackendAction* a = laik_actions_addAction(as);
    a->type = LAIK_AT_SendBuf;
    a->fromBuf = fromBuf;
    a->count = count;
    a->peer_rank = to;

    as->sendCount += count;
}

void laik_actions_addRecv(Laik_ActionSeq* as,
                          int toMapNo, uint64_t off,
                          int count, int from)
{
    Laik_BackendAction* a = laik_actions_addAction(as);
    a->type = LAIK_AT_Recv;
    a->mapNo = toMapNo;
    a->offset = off;
    a->count = count;
    a->peer_rank = from;

    as->recvCount += count;
}

void laik_actions_addRecvBuf(Laik_ActionSeq* as,
                             char* toBuf, int count, int from)
{
    Laik_BackendAction* a = laik_actions_addAction(as);
    a->type = LAIK_AT_RecvBuf;
    a->toBuf = toBuf;
    a->count = count;
    a->peer_rank = from;

    as->recvCount += count;
}


void laik_actions_addPackAndSend(Laik_ActionSeq* as,
                                 Laik_Mapping* fromMap, Laik_Slice* slc, int to)
{
    Laik_BackendAction* a = laik_actions_addAction(as);
    a->type = LAIK_AT_PackAndSend;
    a->map = fromMap;
    a->slc = slc;
    a->peer_rank = to;

    Laik_TransitionContext* tc = as->context[0];
    a->count = laik_slice_size(tc->transition->space->dims, slc);
    assert(a->count > 0);
    as->sendCount += a->count;
}

void laik_actions_addRecvAndUnpack(Laik_ActionSeq* as,
                                   Laik_Mapping* toMap, Laik_Slice* slc, int from)
{
    Laik_BackendAction* a = laik_actions_addAction(as);
    a->type = LAIK_AT_RecvAndUnpack;
    a->map = toMap;
    a->slc = slc;
    a->peer_rank = from;

    Laik_TransitionContext* tc = as->context[0];
    a->count = laik_slice_size(tc->transition->space->dims, slc);
    assert(a->count > 0);
    as->recvCount += a->count;
}

void laik_actions_initReduce(Laik_BackendAction* a,
                            char* fromBuf, char* toBuf, int count,
                            int rootTask, Laik_ReductionOperation redOp)
{
    a->type = LAIK_AT_Reduce;

    a->fromBuf = fromBuf;
    a->toBuf = toBuf;
    a->count = count;
    a->peer_rank = rootTask;
    a->redOp = redOp;
}


void laik_actions_addReduce(Laik_ActionSeq* as,
                            char* fromBuf, char* toBuf, int count,
                            int rootTask, Laik_ReductionOperation redOp)
{
    Laik_BackendAction* a = laik_actions_addAction(as);
    laik_actions_initReduce(a, fromBuf, toBuf, count, rootTask, redOp);

    assert(count > 0);
    as->reduceCount += count;
}

void laik_actions_initGroupReduce(Laik_BackendAction* a,
                                  int inputGroup, int outputGroup,
                                  char* fromBuf, char* toBuf, int count,
                                  Laik_ReductionOperation redOp)
{
    a->type = LAIK_AT_GroupReduce;

    a->inputGroup = inputGroup;
    a->outputGroup = outputGroup;
    a->fromBuf = fromBuf;
    a->toBuf = toBuf;
    a->count = count;
    a->redOp = redOp;
}

void laik_actions_addGroupReduce(Laik_ActionSeq* as,
                                 int inputGroup, int outputGroup,
                                 char* fromBuf, char* toBuf, int count,
                                 Laik_ReductionOperation redOp)
{
    Laik_BackendAction* a = laik_actions_addAction(as);
    laik_actions_initGroupReduce(a, inputGroup, outputGroup,
                                 fromBuf, toBuf, count, redOp);

    assert(count > 0);
    as->reduceCount += count;
}

void laik_actions_addCopyToBuf(Laik_ActionSeq* as,
                               Laik_CopyEntry* ce, char* toBuf, int count)
{
    Laik_BackendAction* a = laik_actions_addAction(as);
    a->type = LAIK_AT_CopyToBuf;
    a->ce = ce;
    a->toBuf = toBuf;
    a->count = count;
}

void laik_actions_addCopyFromBuf(Laik_ActionSeq* as,
                                 Laik_CopyEntry* ce, char* fromBuf, int count)
{
    Laik_BackendAction* a = laik_actions_addAction(as);
    a->type = LAIK_AT_CopyFromBuf;
    a->ce = ce;
    a->fromBuf = fromBuf;
    a->count = count;
}


// returns a new empty action sequence with same transition context
Laik_ActionSeq* laik_actions_cloneSeq(Laik_ActionSeq* oldAS)
{
    Laik_TransitionContext* tc = oldAS->context[0];
    Laik_Data* d = tc->data;
    Laik_ActionSeq* as = laik_actions_new(d->space->inst);
    laik_actions_addTContext(as, d, tc->transition,
                             tc->fromList, tc->toList);
    return as;
}

// just copy actions from oldAS into as
void laik_actions_copySeq(Laik_ActionSeq* oldAS, Laik_ActionSeq* as)
{
    for(int i = 0; i < oldAS->actionCount; i++) {
        Laik_BackendAction* ba = &(oldAS->action[i]);
        switch(ba->type) {
        case LAIK_AT_SendBuf:
            laik_actions_addSendBuf(as, ba->fromBuf, ba->count, ba->peer_rank);
            break;

        case LAIK_AT_RecvBuf:
            laik_actions_addRecvBuf(as, ba->toBuf, ba->count, ba->peer_rank);
            break;

        case LAIK_AT_PackAndSend:
            laik_actions_addPackAndSend(as, ba->map, ba->slc, ba->peer_rank);
            break;

        case LAIK_AT_RecvAndUnpack:
            laik_actions_addRecvAndUnpack(as, ba->map, ba->slc, ba->peer_rank);
            break;

        case LAIK_AT_Reduce:
            laik_actions_addReduce(as, ba->fromBuf, ba->toBuf, ba->count,
                                   ba->peer_rank, ba->redOp);
            break;

        case LAIK_AT_GroupReduce:
            laik_actions_addGroupReduce(as, ba->inputGroup, ba->outputGroup,
                                        ba->fromBuf, ba->toBuf,
                                        ba->count, ba->redOp);
            break;

         default: assert(0);
        }
    }
}

// merge send/recv/groupReduce actions from oldAS into as
void laik_actions_optSeq(Laik_ActionSeq* oldAS, Laik_ActionSeq* as)
{
    int combineGroupReduce = 1;

    Laik_TransitionContext* tc = oldAS->context[0];
    Laik_Data* d = tc->data;
    int elemsize = d->elemsize;
    // used for combining GroupReduce actions
    int myid = tc->transition->group->myid;

    // unmark all actions
    for(int i = 0; i < oldAS->actionCount; i++)
        oldAS->action[i].mark = 0;

    // first pass: how much buffer space?
    int bufSize = 0, copyRanges = 0;
    int rank, j, count;
    for(int i = 0; i < oldAS->actionCount; i++) {
        Laik_BackendAction* ba = &(oldAS->action[i]);
        switch(ba->type) {
        case LAIK_AT_SendBuf:
            // combine consecutive SendBuf actions with same target rank
            count = ba->count;
            rank = ba->peer_rank;
            for(j = i+1; j < oldAS->actionCount; j++) {
                if (oldAS->action[j].type != LAIK_AT_SendBuf) break;
                if (oldAS->action[j].peer_rank != rank) break;
                count += oldAS->action[j].count;
            }
            if (j - i > 1) {
                bufSize += count;
                copyRanges += (j-i);
                i = j - 1;
            }
            break;

        case LAIK_AT_RecvBuf:
            // combine consecutive RecvBuf actions with same source rank
            count = ba->count;
            rank = ba->peer_rank;
            for(j = i+1; j < oldAS->actionCount; j++) {
                if (oldAS->action[j].type != LAIK_AT_RecvBuf) break;
                if (oldAS->action[j].peer_rank != rank) break;
                count += oldAS->action[j].count;
            }
            if (j - i > 1) {
                bufSize += count;
                copyRanges += (j-i);
                i = j - 1;
            }
            break;

        case LAIK_AT_GroupReduce: {
            if (!combineGroupReduce) break;

            // skip already processed GroupReduce actions
            if (ba->mark == 1) break;

            // combine consecutive GroupReduce actions with same
            // inputGroup, outputGroup, and redOp
            // TODO: combine all with same input/outputGroup
            count = ba->count;
            int inputGroup = ba->inputGroup;
            int outputGroup = ba->outputGroup;
            Laik_ReductionOperation redOp = ba->redOp;
            int actionCount = 1;
            for(j = i+1; j < oldAS->actionCount; j++) {
                if (oldAS->action[j].type != LAIK_AT_GroupReduce) continue;
                if (oldAS->action[j].inputGroup != inputGroup) continue;
                if (oldAS->action[j].outputGroup != outputGroup) continue;
                if (oldAS->action[j].redOp != redOp) continue;
                // should be unmarked
                assert(oldAS->action[j].mark == 0);
                oldAS->action[j].mark = 1;
                count += oldAS->action[j].count;
                actionCount++;
            }
            if (actionCount > 1) {
                bufSize += count;
                if (laik_isInGroup(tc->transition, inputGroup, myid))
                    copyRanges += actionCount;
                if (laik_isInGroup(tc->transition, outputGroup, myid))
                    copyRanges += actionCount;
            }
            break;
        }

        case LAIK_AT_PackAndSend:
        case LAIK_AT_RecvAndUnpack:
        case LAIK_AT_Reduce:
            // nothing to merge for these actions
            break;

        default: assert(0);
        }
    }

    if (bufSize == 0) {
        assert(copyRanges == 0);
        laik_log(1, "Optimized action sequence: nothing to do.");
        laik_actions_copySeq(oldAS, as);
        return;
    }

    assert(copyRanges > 0);
    as->buf = malloc(bufSize * elemsize);
    as->ce = malloc(copyRanges * sizeof(Laik_CopyEntry));

    laik_log(1, "Optimized action sequence: buf %p, length %d x %d, ranges %d",
        (void*) as->buf, bufSize, elemsize, copyRanges);

    // unmark all actions: restart for finding same type of actions
    for(int i = 0; i < oldAS->actionCount; i++)
        oldAS->action[i].mark = 0;

    // second pass: add merged actions
    int bufOff = 0;
    int rangeOff = 0;

    for(int i = 0; i < oldAS->actionCount; i++) {
        Laik_BackendAction* ba = &(oldAS->action[i]);
        switch(ba->type) {
        case LAIK_AT_SendBuf:
            count = ba->count;
            rank = ba->peer_rank;
            for(j = i+1; j < oldAS->actionCount; j++) {
                if (oldAS->action[j].type != LAIK_AT_SendBuf) break;
                if (oldAS->action[j].peer_rank != rank) break;
                count += oldAS->action[j].count;
            }
            if (j - i > 1) {
                //laik_log(1,"Send Seq %d - %d, rangeOff %d, bufOff %d, count %d",
                //         i, j, rangeOff, bufOff, count);
                laik_actions_addCopyToBuf(as,
                                          as->ce + rangeOff,
                                          as->buf + bufOff * elemsize,
                                          j - i);
                laik_actions_addSendBuf(as,
                                        as->buf + bufOff * elemsize,
                                        count, rank);
                for(int k = i; k < j; k++) {
                    assert(rangeOff < copyRanges);
                    as->ce[rangeOff].ptr = oldAS->action[k].fromBuf;
                    as->ce[rangeOff].bytes = oldAS->action[k].count * elemsize;
                    as->ce[rangeOff].offset = bufOff * elemsize;
                    bufOff += oldAS->action[k].count;
                    rangeOff++;
                }
                i = j - 1;
            }
            else
                laik_actions_addSendBuf(as, ba->fromBuf, count, rank);
            break;

        case LAIK_AT_RecvBuf:
            count = ba->count;
            rank = ba->peer_rank;
            for(j = i+1; j < oldAS->actionCount; j++) {
                if (oldAS->action[j].type != LAIK_AT_RecvBuf) break;
                if (oldAS->action[j].peer_rank != rank) break;
                count += oldAS->action[j].count;
            }
            if (j - i > 1) {
                laik_actions_addRecvBuf(as,
                                        as->buf + bufOff * elemsize,
                                        count, rank);
                laik_actions_addCopyFromBuf(as,
                                            as->ce + rangeOff,
                                            as->buf + bufOff * elemsize,
                                            j - i);
                for(int k = i; k < j; k++) {
                    assert(rangeOff < copyRanges);
                    as->ce[rangeOff].ptr = oldAS->action[k].toBuf;
                    as->ce[rangeOff].bytes = oldAS->action[k].count * elemsize;
                    as->ce[rangeOff].offset = bufOff * elemsize;
                    bufOff += oldAS->action[k].count;
                    rangeOff++;
                }
                i = j - 1;
            }
            else
                laik_actions_addRecvBuf(as, ba->toBuf, count, rank);
            break;

        case LAIK_AT_GroupReduce: {
            if (!combineGroupReduce) {
                // pass through
                laik_actions_addGroupReduce(as, ba->inputGroup, ba->outputGroup,
                                            ba->fromBuf, ba->toBuf,
                                            ba->count, ba->redOp);
                break;
            }

            // skip already processed GroupReduce actions
            if (ba->mark == 1) break;

            count = ba->count;
            int inputGroup = ba->inputGroup;
            int outputGroup = ba->outputGroup;
            Laik_ReductionOperation redOp = ba->redOp;
            int actionCount = 1;
            for(j = i+1; j < oldAS->actionCount; j++) {
                if (oldAS->action[j].type != LAIK_AT_GroupReduce) continue;
                if (oldAS->action[j].inputGroup != inputGroup) continue;
                if (oldAS->action[j].outputGroup != outputGroup) continue;
                if (oldAS->action[j].redOp != redOp) continue;
                // should be unmarked
                assert(oldAS->action[j].mark == 0);
                oldAS->action[j].mark = 1;
                count += oldAS->action[j].count;
                actionCount++;
            }
            if (actionCount > 1) {
                // temporary buffer used as input and output for reduce
                char* buf = as->buf + bufOff * elemsize;
                int startBufOff = bufOff;

                // if I provide input: copy pieces into temporary buffer
                if (laik_isInGroup(tc->transition, inputGroup, myid)) {
                    laik_actions_addCopyToBuf(as,
                                              as->ce + rangeOff,
                                              buf, actionCount);
                    // ranges for input pieces
                    int oldRangeOff = rangeOff;
                    for(int k = i; k < oldAS->actionCount; k++) {
                        if (oldAS->action[k].type != LAIK_AT_GroupReduce) continue;
                        if (oldAS->action[k].inputGroup != inputGroup) continue;
                        if (oldAS->action[k].outputGroup != outputGroup) continue;
                        if (oldAS->action[k].redOp != redOp) continue;

                        assert(rangeOff < copyRanges);
                        as->ce[rangeOff].ptr = oldAS->action[k].fromBuf;
                        as->ce[rangeOff].bytes = oldAS->action[k].count * elemsize;
                        as->ce[rangeOff].offset = bufOff * elemsize;
                        bufOff += oldAS->action[k].count;
                        rangeOff++;
                    }
                    assert(oldRangeOff + actionCount == rangeOff);
                }

                // use temporary buffer for both input and output
                laik_actions_addGroupReduce(as,
                                            inputGroup, outputGroup,
                                            buf, buf,
                                            count, redOp);

                // if I want output: copy pieces from temporary buffer
                if (laik_isInGroup(tc->transition, outputGroup, myid)) {
                    laik_actions_addCopyFromBuf(as,
                                                as->ce + rangeOff,
                                                buf, actionCount);
                    bufOff = startBufOff;
                    int oldRangeOff = rangeOff;
                    for(int k = i; k < oldAS->actionCount; k++) {
                        if (oldAS->action[k].type != LAIK_AT_GroupReduce) continue;
                        if (oldAS->action[k].inputGroup != inputGroup) continue;
                        if (oldAS->action[k].outputGroup != outputGroup) continue;
                        if (oldAS->action[k].redOp != redOp) continue;

                        assert(rangeOff < copyRanges);
                        as->ce[rangeOff].ptr = oldAS->action[k].toBuf;
                        as->ce[rangeOff].bytes = oldAS->action[k].count * elemsize;
                        as->ce[rangeOff].offset = bufOff * elemsize;
                        bufOff += oldAS->action[k].count;
                        rangeOff++;
                    }
                    assert(oldRangeOff + actionCount == rangeOff);
                }
                bufOff = startBufOff + count;
            }
            else
                laik_actions_addGroupReduce(as, ba->inputGroup, ba->outputGroup,
                                            ba->fromBuf, ba->toBuf,
                                            ba->count, ba->redOp);
            break;
        }

        case LAIK_AT_PackAndSend:
            // pass through
            laik_actions_addPackAndSend(as, ba->map, ba->slc, ba->peer_rank);
            break;

        case LAIK_AT_RecvAndUnpack:
            // pass through
            laik_actions_addRecvAndUnpack(as, ba->map, ba->slc, ba->peer_rank);
            break;

        case LAIK_AT_Reduce:
            // pass through
            laik_actions_addReduce(as, ba->fromBuf, ba->toBuf, ba->count,
                                   ba->peer_rank, ba->redOp);
            break;

        default: assert(0);
        }
    }
    assert(rangeOff == copyRanges);
    assert(bufSize == bufOff);
}

