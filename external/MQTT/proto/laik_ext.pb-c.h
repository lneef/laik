/* Generated by the protocol buffer compiler.  DO NOT EDIT! */
/* Generated from: laik_ext.proto */

#ifndef PROTOBUF_C_laik_5fext_2eproto__INCLUDED
#define PROTOBUF_C_laik_5fext_2eproto__INCLUDED

#include <protobuf-c/protobuf-c.h>

PROTOBUF_C__BEGIN_DECLS

#if PROTOBUF_C_VERSION_NUMBER < 1000000
# error This file was generated by a newer version of protoc-c which is incompatible with your libprotobuf-c headers. Please update your headers.
#elif 1002001 < PROTOBUF_C_MIN_COMPILER_VERSION
# error This file was generated by an older version of protoc-c which is incompatible with your libprotobuf-c headers. Please regenerate this file with a newer version of protoc-c.
#endif


typedef struct _LaikExtMsg LaikExtMsg;


/* --- enums --- */


/* --- messages --- */

struct  _LaikExtMsg
{
  ProtobufCMessage base;
  /*
   *	required int32 num_failing_nodes = 1;
   */
  size_t n_failing_nodes;
  char **failing_nodes;
  /*
   *	required int32 num_spare_nodes = 3;
   */
  size_t n_spare_nodes;
  char **spare_nodes;
};
#define LAIK_EXT_MSG__INIT \
 { PROTOBUF_C_MESSAGE_INIT (&laik_ext_msg__descriptor) \
    , 0,NULL, 0,NULL }


/* LaikExtMsg methods */
void   laik_ext_msg__init
                     (LaikExtMsg         *message);
size_t laik_ext_msg__get_packed_size
                     (const LaikExtMsg   *message);
size_t laik_ext_msg__pack
                     (const LaikExtMsg   *message,
                      uint8_t             *out);
size_t laik_ext_msg__pack_to_buffer
                     (const LaikExtMsg   *message,
                      ProtobufCBuffer     *buffer);
LaikExtMsg *
       laik_ext_msg__unpack
                     (ProtobufCAllocator  *allocator,
                      size_t               len,
                      const uint8_t       *data);
void   laik_ext_msg__free_unpacked
                     (LaikExtMsg *message,
                      ProtobufCAllocator *allocator);
/* --- per-message closures --- */

typedef void (*LaikExtMsg_Closure)
                 (const LaikExtMsg *message,
                  void *closure_data);

/* --- services --- */


/* --- descriptors --- */

extern const ProtobufCMessageDescriptor laik_ext_msg__descriptor;

PROTOBUF_C__END_DECLS


#endif  /* PROTOBUF_C_laik_5fext_2eproto__INCLUDED */