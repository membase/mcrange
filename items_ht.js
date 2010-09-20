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
var sys = require('sys');

// ----------------------------------------------------

// An asynchronous (callback-oriented) items
// implementation using hashtable, with range support.
//
function mkItems() {
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

exports.mkItems = mkItems;
