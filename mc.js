/* Copyright 2010 NorthScale, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
var sys = require('sys'),
    net = require('net'),
 Buffer = require('buffer').Buffer;

var n = require('./ntoh'),
    e = exports;

e.SIZEOF_HEADER = 24; // In bytes.

e.REQ_MAGIC_BYTE = 0x80;
e.RES_MAGIC_BYTE = 0x81;

e.CMD_GET = 0x00;
e.CMD_SET = 0x01;
e.CMD_ADD = 0x02;
e.CMD_REPLACE = 0x03;
e.CMD_DELETE = 0x04;
e.CMD_INCR = 0x05;
e.CMD_DECR = 0x06;
e.CMD_QUIT = 0x07;
e.CMD_FLUSH = 0x08;
e.CMD_GETQ = 0x09;
e.CMD_NOOP = 0x0a;
e.CMD_VERSION = 0x0b;
e.CMD_STAT = 0x10;
e.CMD_APPEND = 0x0e;
e.CMD_PREPEND = 0x0f;

e.CMD_STAT = 0x10;
e.CMD_SETQ = 0x11;
e.CMD_ADDQ = 0x12;
e.CMD_REPLACEQ = 0x13;
e.CMD_DELETEQ = 0x14;
e.CMD_INCREMENTQ = 0x15;
e.CMD_DECREMENTQ = 0x16;
e.CMD_QUITQ = 0x17;
e.CMD_FLUSHQ = 0x18;
e.CMD_APPENDQ = 0x19;
e.CMD_PREPENDQ = 0x1a;

e.CMD_SASL_LIST_MECHS = 0x20;
e.CMD_SASL_AUTH = 0x21;
e.CMD_SASL_STEP = 0x22;

e.CMD_RGET      = 0x30;
e.CMD_RSET      = 0x31;
e.CMD_RSETQ     = 0x32;
e.CMD_RAPPEND   = 0x33;
e.CMD_RAPPENDQ  = 0x34;
e.CMD_RPREPEND  = 0x35;
e.CMD_RPREPENDQ = 0x36;
e.CMD_RDELETE   = 0x37;
e.CMD_RDELETEQ  = 0x38;
e.CMD_RINCR     = 0x39;
e.CMD_RINCRQ    = 0x3a;
e.CMD_RDECR     = 0x3b;
e.CMD_RDECRQ    = 0x3c;

e.CMD_TAP_CONNECT = 0x40;
e.CMD_TAP_MUTATION = 0x41;
e.CMD_TAP_DELETE = 0x42;
e.CMD_TAP_FLUSH = 0x43;
e.CMD_TAP_OPAQUE = 0x44;
e.CMD_TAP_VBUCKET_SET = 0x45;

e.CMD_LAST_RESERVED = 0xef;

e.TAP_CONNECT_FLAG_BACKFILL = 0x01;
e.TAP_CONNECT_FLAG_DUMP = 0x02;
e.TAP_CONNECT_FLAG_LIST_VBUCKETS = 0x04;
e.TAP_CONNECT_FLAG_TAKEOVER_VBUCKETS = 0x08;
e.TAP_CONNECT_SUPPORT_ACK = 0x10;
e.TAP_CONNECT_REQUEST_KEYS_ONLY = 0x20;

e.TAP_FLAG_ACK = 0x01;
e.TAP_FLAG_NO_VALUE = 0x02;

e.RESPONSE_SUCCESS = 0x00;
e.RESPONSE_KEY_ENOENT = 0x01;
e.RESPONSE_KEY_EEXISTS = 0x02;
e.RESPONSE_E2BIG = 0x03;
e.RESPONSE_EINVAL = 0x04;
e.RESPONSE_NOT_STORED = 0x05;
e.RESPONSE_DELTA_BADVAL = 0x06;
e.RESPONSE_NOT_MY_VBUCKET = 0x07;
e.RESPONSE_AUTH_ERROR = 0x20;
e.RESPONSE_AUTH_CONTINUE = 0x21;
e.RESPONSE_UNKNOWN_COMMAND = 0x81;
e.RESPONSE_ENOMEM = 0x82;
e.RESPONSE_NOT_SUPPORTED = 0x83;
e.RESPONSE_EINTERNAL = 0x84;
e.RESPONSE_EBUSY = 0x85;
e.RESPONSE_ETMPFAIL = 0x86;

// ------------------------------------

e.packHeader = function(magic, opcode, keylen,
                        extlen, datatype, statusOrReserved,
                        bodylen,
                        opaque) {
  var b = new Buffer(e.SIZEOF_HEADER + extlen + keylen);
  b[0] = magic;
  b[1] = opcode;
  n.htons(b, 2, keylen);
  b[4] = extlen;
  b[5] = datatype;
  n.htons(b, 6, statusOrReserved);
  n.htonl(b, 8, bodylen);
  n.htonl(b, 12, opaque);
  for (var i = 16; i < e.SIZEOF_HEADER; i++) {
    b[i] = 0;
  }
  return b;
}

e.unpackHeader = function(b, start) {
  if (b.length - start < e.SIZEOF_HEADER) {
    return -1;
  }

  var r = {
      magic: 0xff & b[start],
      opcode: 0xff & b[start + 1],
      keylen: n.ntohs(b, start + 2),
      extlen: 0xff & b[start + 4],
      statusOrReserved: n.ntohs(b, start + 6),
      bodylen: n.ntohl(b, start + 8),
      opaque: n.ntohl(b, start + 12)
  };

  r.datalen = r.bodylen - (r.keylen + r.extlen);

  return r;
}

e.unpackHeaderStr = function(b, start) {
  if ((b.length - start) < e.SIZEOF_HEADER) {
    return -1;
  }

  var r = {
      magic: 0xff & b.charCodeAt(start),
      opcode: 0xff & b.charCodeAt(start + 1),
      keylen: n.ntohsStr(b, start + 2),
      extlen: 0xff & b.charCodeAt(start + 4),
      statusOrReserved: n.ntohsStr(b, start + 6),
      bodylen: n.ntohlStr(b, start + 8),
      opaque: n.ntohlStr(b, start + 12)
  };

  r.datalen = r.bodylen - (r.keylen + r.extlen);

  if ((b.length - start) < (e.SIZEOF_HEADER + r.bodylen)) {
    return -1;
  }

  return r;
}

// ------------------------------------

e.packRequest = function(opcode, key, ext, reserved, opaque, data) {
  var keylen = (key || '').length;
  var extlen = (ext || '').length;
  var datalen = (data || '').length;
  var bodylen = keylen + extlen + datalen;

  var b = e.packHeader(e.REQ_MAGIC_BYTE, opcode, keylen,
                       extlen, 0, reserved,
                       bodylen, opaque);

  if (ext != null) {
    ext.copy(b, e.SIZEOF_HEADER, 0, extlen);
  }

  for (var i = 0; i < keylen; i++) {
    b[e.SIZEOF_HEADER + extlen + i] = key.charCodeAt(i);
  }

  return b;
}

// ------------------------------------

e.unpackMsgStr = function(s, start) {
  if (s.length - start < e.SIZEOF_HEADER) {
    return -1;
  }

  var r = e.unpackHeaderStr(s, start);
  if (r != -1) {
    r.ext = (r.extlen > 0 ?
             s.slice(start + e.SIZEOF_HEADER,
                     start + e.SIZEOF_HEADER + r.extlen) :
             null);
    r.key = (r.keylen > 0 ?
             s.slice(start + e.SIZEOF_HEADER + r.extlen,
                     start + e.SIZEOF_HEADER + r.extlen + r.keylen) :
             null);
    r.data = (r.datalen > 0 ?
              s.slice(start + e.SIZEOF_HEADER + r.extlen + r.keylen,
                      start + e.SIZEOF_HEADER + r.extlen + r.keylen + r.datalen) :
              null);
  }

  return r;
}

