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
#include "laik.h"
#include "laik/data.h"

#include <assert.h>
#include <stdalign.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


// this file implements a layout providing lexicographical ordering (1d/2d/3d)
// for multiple ranges, requesting a separate allocation for each range

typedef union _Lex_Memory_Header Lex_Memory_Header;

union _Lex_Memory_Header
{
    struct {
        Laik_Layout_Memory_Header hd;
        uint64_t count;
        uint64_t stride[3]; 
    };
    alignas(64) int x;
};
// parameters for one range
typedef struct _Lex_Entry Lex_Entry;
struct _Lex_Entry {
    Laik_Range range;
    Lex_Memory_Header* hd;
};

typedef struct _Laik_Layout_Lex Laik_Layout_Lex;
struct _Laik_Layout_Lex {
    Laik_Layout h;
    Lex_Entry e[];
};


//--------------------------------------------------------------
// interface implementation of lexicographical layout
//

// forward decl
static int64_t offset_lex(Laik_Layout* l, int n, Laik_Index* idx);

// return lex layout if given layout is a lexicographical layout
static
bool laik_is_layout_lex(Laik_Layout* l)
{
    return !strcmp(LEX_IDENTIFIER, l->name); // a lexicographical layout
}

// return map number whose ranges contains index <idx>
static
int section_lex(Laik_Layout* l, Laik_Index* idx)
{
    assert(l->section == section_lex);
    Laik_Layout_Lex* ll = (Laik_Layout_Lex*) l;

    int dims = ll->h.dims;
    for(int i=0; i < ll->h.map_count; i++) {
        Lex_Entry* e = &(ll->e[i]);

        // is idx in range?
        if ((idx->i[0] < e->range.from.i[0]) || (idx->i[0] >= e->range.to.i[0])) continue;
        if (dims > 1) {
            if ((idx->i[1] < e->range.from.i[1]) || (idx->i[1] >= e->range.to.i[1])) continue;
            if (dims > 2) {
                if ((idx->i[2] < e->range.from.i[2]) || (idx->i[2] >= e->range.to.i[2])) continue;
            }
        }
        return i;
    }
    return -1; // not found
}

// section is allocation number
static
int mapno_lex(Laik_Layout* l, int n)
{
    assert(n < l->map_count);
    return n;
}


// return offset for <idx> in map <n> of this layout
static
int64_t offset_lex(Laik_Layout* l, int n, Laik_Index* idx)
{   

    Laik_Layout_Lex* ll = (Laik_Layout_Lex*) l;

    int dims = l->dims;
    assert((n >= 0) && (n < l->map_count));
    Lex_Entry* e = &(ll->e[n]);

    int64_t off = idx->i[0] - e->range.from.i[0];
    if (dims > 1) {
        off += (idx->i[1] - e->range.from.i[1]) * e->hd->stride[1];
        if (dims > 2) {
            off += (idx->i[2] - e->range.from.i[2]) * e->hd->stride[2];
        }
    }
    assert((off >= 0) && (off < (int64_t) e->hd->count));
    return off;
}


static
char* describe_lex(Laik_Layout* l)
{
    static char s[200];

    assert(l->describe == describe_lex);
    Laik_Layout_Lex* ll = (Laik_Layout_Lex*) l;

    int o;
    o = sprintf(s, "lex (%dd, %d maps, strides ",
                l->dims, l->map_count);
    for(int i = 0; i < l->map_count; i++) {
        Lex_Entry* e = &(ll->e[i]);
        o += sprintf(s+o, "%s%llu/%llu/%llu",
             (i == 0) ? "":", ",
             (unsigned long long) e->hd->stride[0],
             (unsigned long long) e->hd->stride[1],
             (unsigned long long) e->hd->stride[2]);
    }
    o += sprintf(s+o, ")");
    assert(o < 200);

    return s;
}

static
bool reuse_lex(Laik_Layout* l, int n, Laik_Layout* old, int nold)
{
    Laik_Layout_Lex* lnew = (Laik_Layout_Lex*)l;
    Laik_Layout_Lex* lold = (Laik_Layout_Lex*)old;
    assert((n >= 0) && (n < l->map_count));

    if (laik_log_begin(1)) {
        laik_log_append("reuse_lex: check reuse for map %d in %s",
                        n, describe_lex(l));
        laik_log_flush(" using map %d in old %s", nold, describe_lex(old));
    }

    Lex_Entry* eNew = &(lnew->e[n]);
    Lex_Entry* eOld = &(lold->e[nold]);
    if (!laik_range_within_range(&(eNew->range), &(eOld->range))) {
        // no, cannot reuse
        return false;
    }
    laik_log(1, "reuse_lex: old map %d careusen be reused (count %llu -> %llu)",
             nold,
             (unsigned long long) eNew->hd->count,
             (unsigned long long) eOld->hd->count);

    
    l->count += eOld->hd->count - eNew->hd->count;
    eNew->range = eOld->range;
    free(eNew->hd);

    eNew->hd = eOld->hd;
    return true;
}


static
void copy_lex(Laik_Range* range,
              Laik_Mapping* from, Laik_Mapping* to)
{
    Laik_Layout_Lex* fromLayout = (Laik_Layout_Lex*) from->layout;
    Laik_Layout_Lex* toLayout = (Laik_Layout_Lex*) to->layout;
    //assert(fromLayout != 0);
    //assert(toLayout != 0);
    Lex_Entry* fromLayoutEntry = &(fromLayout->e[0]);
    Lex_Entry* toLayoutEntry = &(toLayout->e[0]);

    unsigned int elemsize = to->data->elemsize;
    //assert(elemsize == to->data->elemsize);
    int dims = to->layout->dims;
    //assert(dims == to->layout->dims);

    Laik_Index count;
    laik_sub_index(&count, &(range->to), &(range->from));
    if (dims < 3) {
        count.i[2] = 1;
        if (dims < 2)
            count.i[1] = 1;
    }
    uint64_t ccount = count.i[0] * count.i[1] * count.i[2];
    assert(ccount > 0);

    uint64_t fromOff  = offset_lex(from->layout, from->layoutSection, &(range->from));
    uint64_t toOff    = offset_lex(to->layout, to->layoutSection, &(range->from));
    char*    fromPtr  = from->start + fromOff * elemsize;
    char*    toPtr    = to->start   + toOff * elemsize;

    if (laik_log_begin(1)) {
        laik_log_append("lex copy of range ");
        laik_log_Range(range);
        laik_log_append(" (count %llu, elemsize %d) from mapping %p",
            ccount, elemsize, from->start);
        laik_log_append(" (data '%s'/%d, %s) ",
            from->data->name, from->mapNo,
            from->layout->describe(from->layout));
        laik_log_append("to mapping %p (data '%s'/%d, layout %s): ",
            to->start, to->data->name, to->mapNo,
            to->layout->describe(to->layout));
        laik_log_flush("local off %lu (ptr %p) => %lu (ptr %p)",
            fromOff, fromPtr, toOff, toPtr);
    }

    for(int64_t i3 = 0; i3 < count.i[2]; i3++) {
        char *fromPtr2 = fromPtr;
        char *toPtr2 = toPtr;
        for(int64_t i2 = 0; i2 < count.i[1]; i2++) {
            memcpy(toPtr2, fromPtr2, count.i[0] * elemsize);
            fromPtr2 += fromLayoutEntry->hd->stride[1] * elemsize;
            toPtr2   += toLayoutEntry->hd->stride[1] * elemsize;
        }
        fromPtr += fromLayoutEntry->hd->stride[2] * elemsize;
        toPtr   += toLayoutEntry->hd->stride[2] * elemsize;
    }
}


// pack/unpack routines for lexicographical layout
static
unsigned int pack_lex(Laik_Mapping* m, Laik_Range* s,
                      Laik_Index* idx, char* buf, unsigned int size)
{
    unsigned int elemsize = m->data->elemsize;
    Laik_Layout_Lex* layout = (Laik_Layout_Lex*)m->layout;
    Lex_Entry* layoutEntry = &(layout->e[0]);
    int dims = m->layout->dims;

    if (laik_index_isEqual(dims, idx, &(s->to))) {
        // nothing left to pack
        return 0;
    }

    // TODO: only default layout with order 1/2/3
    assert(layoutEntry->hd->stride[0] == 1);
    if (dims > 1) {
        assert(layoutEntry->hd->stride[0] <= layoutEntry->hd->stride[1]);
        if (dims > 2)
            assert(layoutEntry->hd->stride[1] <= layoutEntry->hd->stride[2]);
    }

    // range to pack must within local valid range of mapping
    assert(laik_range_within_range(s, &(m->requiredRange)));

    // calculate address of starting index
    uint64_t idxOff = offset_lex(m->layout, m->layoutSection, idx);
    char* idxPtr = m->start + idxOff * elemsize;

    int64_t i0, i1, i2, from0, from1, to0, to1, to2, count;
    from0 = s->from.i[0];
    from1 = s->from.i[1];
    to0 = s->to.i[0];
    to1 = s->to.i[1];
    to2 = s->to.i[2];
    i0 = idx->i[0];
    i1 = idx->i[1];
    i2 = idx->i[2];
    if (dims < 3) {
        to2 = 1; i2 = 0;
        if (dims < 2) {
            from1 = 0; to1 = 1; i1 = 0;
        }
    }
    count = 0;

    // elements to skip after to0 reached
    int64_t skip0 = layoutEntry->hd->stride[1] - (to0 - from0);
    // elements to skip after to1 reached
    int64_t skip1 = layoutEntry->hd->stride[2] - layoutEntry->hd->stride[1] * (to1 - from1);

    if (laik_log_begin(1)) {
        Laik_Index slcsize, localFrom;
        laik_sub_index(&localFrom, &(s->from), &(m->requiredRange.from));
        laik_sub_index(&slcsize, &(s->to), &(s->from));

        laik_log_append("        packing '%s', size (", m->data->name);
        laik_log_Index(dims, &slcsize);
        laik_log_append(") x %d from global (", elemsize);
        laik_log_Index(dims, &(s->from));
        laik_log_append(") / local (");
        laik_log_Index(dims, &localFrom);
        laik_log_append(")/%d, start (", m->mapNo);
        laik_log_Index(dims, idx);
        laik_log_flush(") off %lu, buf size %d", idxOff, size);
    }

    bool stop = false;
    for(; i2 < to2; i2++) {
        for(; i1 < to1; i1++) {
            for(; i0 < to0; i0++) {
                if (size < elemsize) {
                    stop = true;
                    break;
                }

#if DEBUG_PACK
                laik_log(1, "packing (%lu/%lu/%lu) off %lu: %.3f, left %d",
                         i0, i1, i2,
                         (idxPtr - m->base)/elemsize, *(double*)idxPtr,
                         size - elemsize);
#endif

                // copy element into buffer
                memcpy(buf, idxPtr, elemsize);

                idxPtr += elemsize; // stride[0] is 1
                size -= elemsize;
                buf += elemsize;
                count++;

            }
            if (stop) break;
            idxPtr += skip0 * elemsize;
            i0 = from0;
        }
        if (stop) break;
        idxPtr += skip1 * elemsize;
        i1 = from1;
    }
    if (!stop) {
        // we reached end, set i0/i1 to last positions
        i0 = to0;
        i1 = to1;
    }

    if (laik_log_begin(1)) {
        Laik_Index idx2;
        laik_index_init(&idx2, i0, i1, i2);

        laik_log_append("        packed '%s': end (", m->data->name);
        laik_log_Index(dims, &idx2);
        laik_log_flush("), %lu elems = %lu bytes, %d left",
                       count, count * elemsize, size);
    }

    // save position we reached
    idx->i[0] = i0;
    idx->i[1] = i1;
    idx->i[2] = i2;
    return count;
}

static
unsigned int unpack_lex(Laik_Mapping* m, Laik_Range* s,
                        Laik_Index* idx, char* buf, unsigned int size)
{
    unsigned int elemsize = m->data->elemsize;
    Laik_Layout_Lex* layout = (Laik_Layout_Lex*)m->layout;
    Lex_Entry* layoutEntry = &(layout->e[0]);
    int dims = m->layout->dims;

    // there should be something to unpack
    assert(size > 0);
    assert(!laik_index_isEqual(dims, idx, &(s->to)));

    // TODO: only default layout with order 1/2/3
    assert(layoutEntry->hd->stride[0] == 1);
    if (dims > 1) {
        assert(layoutEntry->hd->stride[0] <= layoutEntry->hd->stride[1]);
        if (dims > 2)
            assert(layoutEntry->hd->stride[1] <= layoutEntry->hd->stride[2]);
    }

    // range to unpack into must be within local valid range of mapping
    assert(laik_range_within_range(s, &(m->requiredRange)));

    // calculate address of starting index
    uint64_t idxOff = offset_lex(m->layout, m->layoutSection, idx);
    char* idxPtr = m->start + idxOff * elemsize;

    int64_t i0, i1, i2, from0, from1, to0, to1, to2, count;
    from0 = s->from.i[0];
    from1 = s->from.i[1];
    to0 = s->to.i[0];
    to1 = s->to.i[1];
    to2 = s->to.i[2];
    i0 = idx->i[0];
    i1 = idx->i[1];
    i2 = idx->i[2];
    if (dims < 3) {
        to2 = 1; i2 = 0;
        if (dims < 2) {
            from1 = 0; to1 = 1; i1 = 0;
        }
    }
    count = 0;

    // elements to skip after to0 reached
    uint64_t skip0 = layoutEntry->hd->stride[1] - (to0 - from0);
    // elements to skip after to1 reached
    uint64_t skip1 = layoutEntry->hd->stride[2] - layoutEntry->hd->stride[1] * (to1 - from1);

    if (laik_log_begin(1)) {
        Laik_Index slcsize, localFrom;
        laik_sub_index(&localFrom, &(s->from), &(m->requiredRange.from));
        laik_sub_index(&slcsize, &(s->to), &(s->from));

        laik_log_append("        unpacking '%s', size (", m->data->name);
        laik_log_Index(dims, &slcsize);
        laik_log_append(") x %d from global (", elemsize);
        laik_log_Index(dims, &(s->from));
        laik_log_append(") / local (");
        laik_log_Index(dims, &localFrom);
        laik_log_append(")/%d, start (", m->mapNo);
        laik_log_Index(dims, idx);
        laik_log_flush(") off %lu, buf size %d", idxOff, size);

    }

    bool stop = false;
    for(; i2 < to2; i2++) {
        for(; i1 < to1; i1++) {
            for(; i0 < to0; i0++) {
                if (size < elemsize) {
                    stop = true;
                    break;
                }

#ifdef DEBUG_UNPACK
                laik_log(1, "unpacking (%lu/%lu/%lu) off %lu: %.3f, left %d",
                         i0, i1, i2,
                         (idxPtr - m->base)/elemsize, *(double*)buf,
                         size - elemsize);
#endif
                // copy element from buffer into local data
                memcpy(idxPtr, buf, elemsize);

                idxPtr += elemsize; // stride[0] is 1
                size -= elemsize;
                buf += elemsize;
                count++;

            }
            if (stop) break;
            idxPtr += skip0 * elemsize;
            i0 = from0;
        }
        if (stop) break;
        idxPtr += skip1 * elemsize;
        i1 = from1;
    }
    if (!stop) {
        // we reached end, set i0/i1 to last positions
        i0 = to0;
        i1 = to1;
    }

    if (laik_log_begin(1)) {
        Laik_Index idx2;
        laik_index_init(&idx2, i0, i1, i2);

        laik_log_append("        unpacked '%s': end (", m->data->name);
        laik_log_Index(dims, &idx2);
        laik_log_flush("), %lu elems = %lu bytes, %d left",
                       count, count * elemsize, size);
    }

    // save position we reached
    idx->i[0] = i0;
    idx->i[1] = i1;
    idx->i[2] = i2;
    return count;
}

void laik_layout_alloc_lex(Laik_Mapping* m, char* header, int n)
{
    Laik_Layout_Lex* ll = (Laik_Layout_Lex*)m->layout;
    Lex_Memory_Header* lold = ll->e[n].hd;

    Lex_Memory_Header* hd = (Lex_Memory_Header*) header;
    hd->count = lold->count;
    memcpy(hd->stride, lold->stride, 3 * sizeof(uint64_t));
    strncpy(hd->hd.name, LEX_IDENTIFIER, 9);
    hd->hd.size = sizeof(Lex_Memory_Header);

    free(ll->e[n].hd);
    ll->e[n].hd = hd;
}

void laik_layout_lex_free(Laik_Mapping* m, int n)
{   
    Laik_Layout_Lex* ll = (Laik_Layout_Lex*)m->layout;

    Laik_Allocator* a = m->allocator;
    if(a)
    {   
        assert(m->allocator->free);
        laik_log(1, "Freeing memory allocated with Laik_Allocator: %d", m->mapNo);
        a->free(m->data, m->header);
    }
    else if(!m->header)
    {
        laik_log(1, "Freeing header for user provided memory map: %d", m->mapNo);
        free(ll->e[n].hd);
    }

}

// create layout for lexicographical layout covering <n> ranges
Laik_Layout* laik_new_layout_lex(int n, Laik_Range* ranges)
{
    int dims = ranges->space->dims;
    Laik_Layout_Lex* l = malloc(sizeof(Laik_Layout_Lex) + n * sizeof(Lex_Entry));
    if (!l) {
        laik_panic("Out of memory allocating Laik_Layout_Lex object");
        exit(1); // not actually needed, laik_panic never returns
    }
    // count calculated later
    laik_init_layout(&(l->h), dims, n, 0,
                     section_lex,
                     mapno_lex,
                     offset_lex,
                     reuse_lex,
                     describe_lex,
                     pack_lex,
                     unpack_lex,
                     copy_lex,
                     laik_layout_alloc_lex,
                     laik_layout_lex_free);

    uint64_t count = 0;
    strncpy(l->h.name, LEX_IDENTIFIER, 9);
    l->h.header_size = sizeof(Lex_Memory_Header);
    
    for(int i = 0; i < n; i++) {
        Lex_Entry* e = &(l->e[i]);
        Laik_Range* range = &ranges[i];
        e->hd = malloc(sizeof(Lex_Memory_Header));

        e->hd->count = laik_range_size(range);
        count += e->hd->count;
        
        e->hd->stride[0] = 1;
        e->hd->hd.size = sizeof(Lex_Memory_Header);

        if (dims > 1) {
            e->hd->stride[1] = range->to.i[0] - range->from.i[0];
            assert(range->from.i[1] < range->to.i[1]);
        }
        else
            e->hd->stride[1] = 0; // invalid, not used

        if (dims > 2) {
            e->hd->stride[2] = e->hd->stride[1] * (range->to.i[1] - range->from.i[1]);
            assert(range->from.i[2] < range->to.i[2]);
        }
        else
            e->hd->stride[2] = 0; // invalid, not used

        e->range = *range;
    }
    l->h.count = count;

    return (Laik_Layout*) l;
}


// return stride for dimension <d> in lex layout map <n>
uint64_t laik_layout_lex_stride(Laik_Layout* l, int n, int d)
{
    Laik_Layout_Lex* ll = (Laik_Layout_Lex*)l;
    assert(ll != 0);
    assert((n >= 0) && (n < l->map_count));
    assert((d >= 0) && (d < l->dims));

    return ll->e[n].hd->stride[d];
}

Laik_Layout* laik_layout_adapter_lex(Laik_Range* range, Laik_Layout_Memory_Header* lh)
{
    Laik_Layout_Lex* ll = malloc(sizeof(Laik_Layout_Lex) + sizeof(Lex_Entry));
    assert(ll != 0);
    strncpy(ll->h.name, LEX_IDENTIFIER, 9);
    ll->h.map_count = 1;

    ll->h.describe = describe_lex;
    ll->h.offset = offset_lex;
    ll->h.dims = range->space->dims;
    ll->e[0].hd = (Lex_Memory_Header*) lh;
    ll->e[0].range = *range;
    return (Laik_Layout*) ll;

}


