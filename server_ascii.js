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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
var sys = require('sys'),
    net = require('net');

// ----------------------------------------------------

function mkCmdsSimple() {
  return {
    'get': function(ctx, items, emit, args) {
      args.shift();
      items.lookup(args,
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
    'delete': function(ctx, items, emit, args) {
      if (args.length != 2) {
        emit('CLIENT_ERROR\r\n');
      } else {
        var key = args[1];

        items.remove(key,
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
    'stats': function(ctx, items, emit, args) {
      emit('STAT num_conns ' + ctx.stats.num_conns + '\r\n');
      emit('STAT tot_conns ' + ctx.stats.tot_conns + '\r\n');
      emit('STAT curr_items ' + ctx.nitems + '\r\n');
      emit('END\r\n');
    },
    'flush_all': function(ctx, items, emit, args) {
      items.reset(function() {
          ctx.nitems = 0;
          emit('OK\r\n');
        });
    },
    'quit': function(ctx, items, emit, args) {
      emit(null);
    },
    'rget': function(ctx, items, emit, args) {
      // rget <startInclusion> <endInclusion> <maxItems>    \
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
        items.range(startKey, startInclusion,
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
}

// ----------------------------------------------------

function mkCmdsValue() {
  return {
    set: function(ctx, items, item, prev) {
      if (prev == null) {
        ctx.nitems++;
      }
      return [item, 'STORED\r\n'];
    },
    add: function(ctx, items, item, prev) {
      if (prev != null) {
        return [prev, 'NOT_STORED\r\n'];
      } else {
        ctx.nitems++;
        return [item, 'STORED\r\n'];
      }
    },
    replace: function(ctx, items, item, prev) {
      if (prev != null) {
        return [item, 'STORED\r\n'];
      } else {
        return [prev, 'NOT_STORED\r\n'];
      }
    },
    append: function(ctx, items, item, prev) {
      if (prev != null) {
        item.val = prev.val + item.val;
        return [item, 'STORED\r\n'];
      } else {
        return [null, 'NOT_STORED\r\n'];
      }
    },
    prepend: function(ctx, items, item, prev) {
      if (prev != null) {
        item.val = item.val + prev.val;
        return [item, 'STORED\r\n'];
      } else {
        return [null, 'NOT_STORED\r\n'];
      }
    }
  };
}

// ----------------------------------------------------

function mkServerCtx() {
  return { nitems: 0,
           stats: { num_conns: 0,
                    tot_conns: 0
                  }
  };
}

// ----------------------------------------------------

function mkServer(items, ctx,
                  cmds_simple,
                  cmds_value) {
  return net.createServer(function(stream) {
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

        var func_s = cmds_simple[cmd];
        if (func_s != null) {
          func_s(ctx, items, emit, args);
        } else {
          var func_v = cmds_value[cmd];
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

                items.update(
                  item.key,
                  function(rkey, ritem) {
                    if (rkey != null) {
                      var res = func_v(ctx, items, item, ritem);
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
}

// ----------------------------------------------------

exports.mkServer     = mkServer;
exports.mkServerCtx  = mkServerCtx;
exports.mkCmdsSimple = mkCmdsSimple;
exports.mkCmdsValue  = mkCmdsValue;




