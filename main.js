#!/usr/bin/env node

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
    net = require('net');

// ----------------------------------------------------

var mc_port = 11299;

// ----------------------------------------------------

// An asynchronous (callback-oriented) items
// hashtable implementation.
//
function mkItems_ht() {
  var ht = {};
  var truth = function() { return true; };

  var self = {
      lookup: function(keys, cb) {
        if (typeof(keys) == 'string') {
          keys = [keys];
        }

        for (var i = 0; i < keys.length; i++) {
          var key = keys[i];
          if (cb(key, ht[key]) == false) {
            break;
          }
        }

        cb(null);
      },
      update: function(k, cb) {
        ht[k] = cb(k, ht[k]);
        cb(null);
      },
      remove: function(k, cb) {
        var prev = ht[k];
        delete ht[k];
        cb(k, prev);
        cb(null);
      },
      reset: function(cb) { ht = {}; cb(null); },
      range: function(startKey,
                      startInclusion,
                      endKey,
                      endInclusion,
                      cb) {
        var startPredicate =
          startKey ?
          (startInclusion ?
           (function(k) { return k >= startKey }) :
           (function(k) { return k > startKey })) :
          truth;

        var endPredicate =
          endKey ?
          (endInclusion ?
           (function(k) { return k <= endKey }) :
           (function(k) { return k < endKey })) :
          truth;

        for (var k in ht) {
          if (startPredicate(k) && endPredicate(k)) {
            if (cb(k, ht[k]) == false) {
              break;
            }
          }
        }

        cb(null);
      }
  };

  return self;
}

// ----------------------------------------------------

var protocol_ascii_simple = {
  'get': function(ctx, emit, args) {
    args.shift();
    ctx.items.lookup(args,
                     function(key, item) {
                       if (key != null) {
                         if (item != null &&
                             item.key != null &&
                             item.val != null) {
                           emit('VALUE ' +
                                item.key + ' ' +
                                item.flg + ' ' +
                                item.val.length + '\r\n' +
                                item.val + '\r\n');
                         }
                       } else {
                         emit('END\r\n');
                       }
                     });
  },
  'delete': function(ctx, emit, args) {
    if (args.length != 2) {
      emit('CLIENT_ERROR\r\n');
    } else {
      var key = args[1];

      ctx.items.remove(key,
                       function(rkey, ritem) {
                         if (rkey != null) {
                           if (ritem != null) {
                             ctx.nitems--;
                             emit('DELETED\r\n');
                           } else {
                             emit('NOT_FOUND\r\n');
                           }
                         }
                       });
    }
  },
  'stats': function(ctx, emit, args) {
    emit('STAT num_conns ' + ctx.stats.num_conns + '\r\n');
    emit('STAT tot_conns ' + ctx.stats.tot_conns + '\r\n');
    emit('STAT curr_items ' + ctx.nitems + '\r\n');
    emit('END\r\n');
  },
  'flush_all': function(ctx, emit, args) {
    ctx.items.reset(function() {
                      ctx.nitems = 0;
                      emit('OK\r\n');
                    });
  },
  'quit': function(ctx, emit, args) {
    emit(null);
  },
  'rget': function(ctx, emit, args) {
    // rget <startInclusion> <endInclusion> <maxItems>  \
    //      <startKey> [endKey]\r\n
    //
    if (args.length < 5 ||
        args.length > 6) {
      emit('CLIENT_ERROR\r\n');
    } else {
      var startInclusion = args[1] == '1';
      var endInclusion = args[2] == '1';
      var maxItems = parseInt(args[3]);
      var startKey = args[4];
      var endKey = args[5];

      var i = 0;
      ctx.items.range(startKey, startInclusion,
                      endKey, endInclusion,
                      function(key, item) {
                        if (key != null) {
                          if (item != null &&
                              item.val != null) {
                            emit('VALUE ' +
                                 item.key + ' ' +
                                 item.flg + ' ' +
                                 item.val.length + '\r\n' +
                                 item.val + '\r\n');
                            i++;
                          }
                          return (0 == maxItems || i < maxItems);
                        } else {
                          emit('END\r\n');
                        }
                      });
    }
  }
};

var protocol_ascii_value = {
  set: function(ctx, item, prev) {
    if (prev == null) {
      ctx.nitems++;
    }
    return [item, 'STORED\r\n'];
  },
  add: function(ctx, item, prev) {
    if (prev != null) {
      return [prev, 'NOT_STORED\r\n'];
    } else {
      ctx.nitems++;
      return [item, 'STORED\r\n'];
    }
  },
  replace: function(ctx, item, prev) {
    if (prev != null) {
      return [item, 'STORED\r\n'];
    } else {
      return [prev, 'NOT_STORED\r\n'];
    }
  },
  append: function(ctx, item, prev) {
    if (prev != null) {
      item.val = prev.val + item.val;
      return [item, 'STORED\r\n'];
    } else {
      return [null, 'NOT_STORED\r\n'];
    }
  },
  prepend: function(ctx, item, prev) {
    if (prev != null) {
      item.val = item.val + prev.val;
      return [item, 'STORED\r\n'];
    } else {
      return [null, 'NOT_STORED\r\n'];
    }
  }
};

// ----------------------------------------------------

function mkServer(ctx) {
  var server = net.createServer(function(stream) {
    stream.setEncoding('binary');
    stream.setNoDelay(true);

    stream.on('connect', function() {
        ctx.stats.num_conns++;
        ctx.stats.tot_conns++;
      });
    stream.on('end', function() {
        stream.end();
        ctx.stats.num_conns--;
      });

    var leftOver = null;   // Any remaining bytes when we haven't
                           // read a full request yet.
    var handler = new_cmd; // Either new_cmd (at the start of a new
                           // request), or read_more (when more
                           // mutation value data is still being read).
    var emitQueue = null;  // Used when stream write is full.

    function emit(data) {
      if (data == null) {
        stream.end();
        return;
      }

      if (emitQueue != null) {
        emitQueue[emitQueue.length] = data;
      } else {
        if (stream.write(data, 'binary') == false) {
          emitQueue = [];
        }
      }
    }

    stream.on('drain', function() {
        if (emitQueue != null) {
          while (emitQueue.length > 0) {
            var data = emitQueue.shift();
            if (stream.write(data, 'binary') == false) {
              return;
            }
          }
          emitQueue = null;
        }
      });

    stream.on('data', function(data) {
        if (leftOver) {
          data = leftOver + data;
          leftOver = null;
        }

        handler(data);
      });

    function new_cmd(data) {
      while (data != null && data.length > 0) {
        var crnl = data.indexOf('\r\n');
        if (crnl < 0) {
          leftOver = data;
          return;
        }

        var line = data.slice(0, crnl);
        data = data.slice(crnl + 2);

        var args = line.split(' ');
        var cmd = args[0];

        var func_s = protocol_ascii_simple[cmd];
        if (func_s != null) {
          func_s(ctx, emit, args);
        } else {
          var func_v = protocol_ascii_value[cmd];
          if (func_v != null) {
            if (args.length != 5) {
              emit('CLIENT_ERROR\r\n');
              continue;
            }

            var item = { key: args[1],
                         flg: args[2],
                         exp: parseInt(args[3]) };
            var nval = parseInt(args[4]);

            read_more(data);

            function read_more(d) {
              if (d.length < nval + 2) { // "\r\n".length == 2.
                leftOver = d;

                handler = read_more;

                // Break out of new_cmd while loop.
                //
                data = null;
              } else {
                item.val = d.slice(0, nval);

                var resp = 'STORED\r\n';

                ctx.items.update(
                  item.key,
                  function(rkey, ritem) {
                    if (rkey != null) {
                      var res = func_v(ctx, item, ritem);
                      resp = res[1];
                      return res[0];
                    } else {
                      emit(resp);

                      if (handler == read_more) {
                        handler = new_cmd;

                        new_cmd(d.slice(nval + 2));
                      } else {
                        data = d.slice(nval + 2);
                      }
                    }
                  });
              }
            }
          } else {
            emit('CLIENT_ERROR\r\n');
          }
        }
      }
    }
  });

  return server;
}

mkServer({ items: mkItems_ht(),
           nitems: 0,
           stats: {
             num_conns: 0,
             tot_conns: 0
           }
         }).listen(mc_port);
