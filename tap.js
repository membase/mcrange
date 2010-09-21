// Example usage:
//
//   node tap.js -s 127.0.0.1:11210
//   node tap.js -s 127.0.0.1:11210 -e "dump(item)"
//   node tap.js -s 127.0.0.1:11210 -e "dumpAll(item)"
//   node tap.js -s 127.0.0.1:11210 -e 'if (item.key == "hi") { sys.puts("world " + item.data); }'
//   node tap.js -s 127.0.0.1:11210 -e 'if (item.key.indexOf("cust:") == 0) { emit(item.key, "1"); }'
//
var sys = require('sys'),
    net = require('net'),
   repl = require('repl'),
   http = require('http'),
 Buffer = require('buffer').Buffer;

var n = require('./ntoh'),
   mc = require('./mc');

var query = require('./query');

// ------------------------------------

function dumpAll(r) {
  sys.puts("----");
  sys.puts("magic: " + r.magic);
  sys.puts("opcode: " + r.opcode);
  sys.puts("keylen: " + r.keylen);
  sys.puts("extlen: " + r.extlen);
  sys.puts("status: " + r.statusOrReserved);
  sys.puts("bodylen: " + r.bodylen);
  sys.puts("datalen: " + r.datalen);
  sys.puts("ext: " + r.ext);
  dump(r);
}

function dump(r) {
  sys.puts("--");
  sys.puts("key: " + r.key);
  sys.puts("data: " + r.data);
}

function emit(key, value) {
  value = value || '';
  sys.print("set ");
  sys.print(key);
  sys.print(" 0 0 " + value.length);
  sys.print("\r\n");
  sys.print(value.toString());
  sys.print("\r\n");
}

// ------------------------------------

var executeBefore = null;
var executeDuring = null;
var executeAfter  = null;

var servers = [];
var verbose = 0;

for (var i = 2; i < process.argv.length; i++) {
  var arg = process.argv[i];
  if (arg == '-s') {
    var hp = process.argv[++i].split(':');
    servers.push({ host: hp[0],
                   port: hp[1] || 11211,
                   port_proxy: hp[1] || 11211,
                   port_direct: hp[2] || ((hp[1] || 11211) - 1) });
  }
  if (arg == '-b') {
    executeBefore = process.argv[++i];
  }
  if (arg == '-e') {
    executeDuring = process.argv[++i];
  }
  if (arg == '-a') {
    executeAfter = process.argv[++i];
  }
  if (arg == '-v') {
    verbose++;
  }
  if (arg == '-h' || arg == '-?' || arg == '--help') {
    usage();
  }
}

function usage() {
  sys.puts("tap.js - run javascript code against the items in a membase node\n");
  sys.puts("  usage: " + process.argv[0] + " tap.js" +
           " [-b 'before javascript code']" +
           " [-e 'during javascript code']" +
           " [-a 'after javascript code']" +
           " [-v [-v [-v]]]\n" +
           " -s mchost[:11211[:11210]]" +
           " [-s mchost2[:11211[:11210]]] ...\n");
  sys.puts("    -s specifies another membase host:port target to hit,");
  sys.puts("       using the format hostname[:proxy_port[:direct_port]]");
  sys.puts("    -v specifies more verbosity.\n");
  process.exit(-1);
}

if (servers.length <= 0) {
  sys.puts("tap.js needs at least one server: " +
           process.argv[0] + " tap.js " + "-s <host>");
  process.exit(-1);
}

// ------------------------------------

var actionBefore = eval('(function(ctx) {' +
                        executeBefore + '})');
var actionDuring = eval('(function(item, ctx) {' +
                        (executeDuring || "dump(item)") + '})');
var actionAfter  = eval('(function(ctx) {' +
                        (executeAfter || "sys.puts('----')") + '})');

function makeResponseProcessor(stream, actionCtx) {
  var leftOver = null;

  return function(msg) {
    if (leftOver) {
      msg = leftOver + msg;
      leftOver = null;
    }

    var cur = 0;
    while (cur < msg.length) {
      var res = mc.unpackMsgStr(msg, cur);
      if (res == -1) {
        leftOver = (leftOver || '') + msg.slice(cur);
        return; // Need to read more bytes.
      }

      if (res != null &&
          res.opcode == mc.CMD_TAP_MUTATION) {
        actionDuring(res, actionCtx);
      }

      cur = cur + mc.SIZEOF_HEADER + res.bodylen;
    }
  }
}

for (var i = 0; i < servers.length; i++) {
  var stream = net.createConnection(servers[i].port_direct,
                                    servers[i].host);
  tapStream(stream);
}

function tapStream(stream) {
  var bufExt = new Buffer(4);
  n.htonl(bufExt, 0, mc.TAP_CONNECT_FLAG_DUMP);

  var actionCtx = {};

  stream.setEncoding('binary');
  stream.addListener('connect',
                     function() {
                       actionBefore(actionCtx);
                       var req = mc.packRequest(mc.CMD_TAP_CONNECT,
                                                null,
                                                bufExt,
                                                0, 0,
                                                null);
                       stream.write(req);
                     });
  stream.addListener('data',
                     makeResponseProcessor(stream,
                                           actionCtx));
  stream.addListener('end',
                     function() { actionAfter(actionCtx); });
}

