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

var items_ht     = require('./items_ht');
var server_ascii = require('./server_ascii');

// ----------------------------------------------------

// To run...
//
//   node main.js
//
var mc_port = 11299;

server_ascii.mkServer(items_ht.mkItems(),
                      server_ascii.mkServerCtx(),
                      server_ascii.mkCmdsSimple(),
                      server_ascii.mkCmdsValue()).listen(mc_port);
