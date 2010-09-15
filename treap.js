/**
 * Copyright 2010 NorthScale, Inc., Steve Yen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * An in-memory, immutable treap implementation in javascript.
 *
 * See: http://www.cs.cmu.edu/afs/cs.cmu.edu/project/scandal/public/papers/treaps-spaa98.pdf
 */
function TreapEmpty() {
}

TreapEmpty.prototype.count    = function() { return 0; };
TreapEmpty.prototype.isEmpty  = function() { return true };
TreapEmpty.prototype.isLeaf   = function() { throw "empty treap isLeaf"; };
TreapEmpty.prototype.firstKey = function() { throw "empty treap firstKey"; };
TreapEmpty.prototype.lastKey  = function() { throw "empty treap lastKey"; };

TreapEmpty.prototype.lookup = function(s) { return this; };

/**
 * Splits a treap into two treaps based on a split key "s".
 * The result tuple-3 means (left, X, right), where X is either...
 * null - meaning the key "s" was not in the original treap.
 * non-null - returning the Full node that had key "s".
 * The tuple-3's left treap has keys all < s,
 * and the tuple-3's right treap has keys all > s.
 */
TreapEmpty.prototype.split = function(s) { return [this, null, this]; };

/**
 * For join to work, we require that "this".keys < "that".keys.
 */
TreapEmpty.prototype.join = function(that) { return that; };

/**
 * When union'ed, the values from "that" have precedence
 * over "this" when there are matching keys.
 */
TreapEmpty.prototype.union = function(that) { return that; };

/**
 * When intersect'ed, the values from "that" have precedence
 * over "this" when there are matching keys.
 */
TreapEmpty.prototype.intersect = function(that) { return this; };

/**
 * Works like set-difference, as in "this" minus "that", or this - that.
 */
TreapEmpty.prototype.diff = function(that) { return this; };

TreapEmpty.prototype.del = function(s) { return this; };

// ------------------------------------------------------------

function TreapNode() {
  this.key = null;
  this._left = this._right = this._value = this._priority = null;
}

TreapNode.prototype.left  = function() { return this._left; };
TreapNode.prototype.right = function() { return this._right; };

TreapNode.prototype.count = function() {
  return 1 + this.left().count() + this.right().count();
};

TreapNode.prototype.isEmpty = function() { return false; };

TreapNode.prototype.isLeaf = function() {
  return this.left().isEmpty() && this.right().isEmpty();
}

TreapNode.prototype.firstKey = function() {
  if (this.left().isEmpty()) {
    return this.key();
  }
  return this.left().firstKey();
}

TreapNode.prototype.lastKey = function() {
  if (this.right().isEmpty()) {
    return this.key();
  }
  return this.right().lastKey();
}

TreapNode.prototype.lookup = function(s, compare) {
  var c = compare(s, this.key());
  if (c == 0) {
    return this;
  }

  if (c < 0) {
    return this.left().lookup(s, compare);
  } else {
    return this.right().lookup(s, compare);
  }
}

TreapNode.prototype.split = function(s) {
  var c = compare(s, this.key());
  if (c == 0) {
    return [this.left(), this, this.right()];
  }

  if (c < 0) {
    if (this.isLeaf()) {
      return [this.left(), null, this]; // Optimization when isLeaf.
    } else {
      var x = this.left().split(s);
      return [x[0], x[1], mkNode(this, x[2], this.right())];
    }
  } else {
    if (this.isLeaf()) {
      return [this, null, this.right()]; // Optimization when isLeaf.
    } else {
      var x = this.right().split(s);
      return [mkNode(this, this.left(), x[0]), x[1], x[2]];
    }
  }
}

TreapNode.prototype.join = function(that) {
  if (that.isEmpty()) {
    return this;
  }

  if (this.priority() > that.priority()) {
    return mkNode(this, this.left(), this.right().join(that));
  } else {
    return mkNode(that, this.join(that.left()), that.right());
  }
}

TreapNode.prototype.union     = function(that) { return that; };
TreapNode.prototype.intersect = function(that) { return this; };
TreapNode.prototype.diff      = function(that) { return this; };

TreapNode.prototype.del = function(s) { return this; };

