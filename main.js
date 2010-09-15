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

var items = mkItems_ht();
var nitems = 0;

var stats = {
  num_conns: 0,
  tot_conns: 0
};

// ----------------------------------------------------

var server = net.createServer(function(stream) {
    stream.setEncoding('binary');
    stream.setNoDelay(true);

    stream.on('connect', function() {
        stats.num_conns++;
        stats.tot_conns++;
      });
    stream.on('end', function() {
        stream.end();
        stats.num_conns--;
      });

    var leftOver = null;    // Any remaining bytes when we haven't
                            // read a full request yet.
    var handler  = new_cmd; // Either new_cmd (at the start of a new
                            // request), or read_more (when more
                            // mutation value data is still being read).
    var emitQueue = null;   // Used when stream write is full.

    function emit(data) {
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

        var parts = line.split(' ');
        var cmd = parts[0];
        if (cmd == 'get') {
          parts.shift();
          items.lookup(parts,
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
        } else if (cmd == 'set' ||
                   cmd == 'add' ||
                   cmd == 'replace' ||
                   cmd == 'append' ||
                   cmd == 'prepend') {
          if (parts.length != 5) {
            emit('CLIENT_ERROR\r\n');
            continue;
          }

          var item = { key: parts[1],
                       flg: parts[2],
                       exp: parseInt(parts[3]) };
          var nval = parseInt(parts[4]);

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
                    if (cmd == 'set') {
                      if (ritem == null) {
                        nitems++;
                      }
                      return item;
                    }
                    if (cmd == 'add') {
                      if (ritem != null) {
                        resp = 'NOT_STORED\r\n';
                        return ritem;
                      } else {
                        nitems++;
                        return item;
                      }
                    }
                    if (cmd == 'replace') {
                      if (ritem != null) {
                        return item;
                      } else {
                        resp = 'NOT_STORED\r\n';
                        return ritem;
                      }
                    }
                    if (cmd == 'append') {
                      if (ritem != null) {
                        item.val = ritem.val + item.val;
                        return item;
                      } else {
                        resp = 'NOT_STORED\r\n';
                        return null;
                      }
                    } else if (cmd == 'prepend') {
                      if (ritem != null) {
                        item.val = item.val + ritem.val;
                        return item;
                      } else {
                        resp = 'NOT_STORED\r\n';
                        return null;
                      }
                    } else {
                      resp = 'SERVER_ERROR\r\n';
                      return ritem;
                    }
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
        } else if (cmd == 'delete') {
          if (parts.length != 2) {
            emit('CLIENT_ERROR\r\n');
            continue;
          }

          var key = parts[1];

          items.remove(
            key,
            function(rkey, ritem) {
              if (rkey != null) {
                if (ritem != null) {
                  nitems--;
                  emit('DELETED\r\n');
                } else {
                  emit('NOT_FOUND\r\n');
                }
              }
            });
        } else if (cmd == 'rget') {
          // rget <startInclusion> <endInclusion> <maxItems> \
          //      <startKey> [endKey]\r\n
          //
          if (parts.length < 5 ||
              parts.length > 6) {
            emit('CLIENT_ERROR\r\n');
            continue;
          }

          var startInclusion = parts[1] == '1';
          var endInclusion = parts[2] == '1';
          var maxItems = parseInt(parts[3]);
          var startKey = parts[4];
          var endKey = parts[5];

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
        } else if (cmd == 'stats') {
            emit('STAT num_conns ' + stats.num_conns + '\r\n');
            emit('STAT tot_conns ' + stats.tot_conns + '\r\n');
            emit('STAT curr_items ' + nitems + '\r\n');
            emit('END\r\n');
        } else if (cmd == 'flush_all') {
          items.reset(function() {
                        nitems = 0;
                        emit('OK\r\n');
                      });
        } else if (cmd == 'quit') {
          stream.end();
        } else {
          emit('CLIENT_ERROR\r\n');
        }
      }
    }
  });

server.listen(mc_port);
