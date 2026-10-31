// tests/helpers/mock_ws_server.mjs - Pure Node.js SP WebSocket Server Mock
import http from 'node:http';
import https from 'node:https';
import fs from 'node:fs';
import { execSync } from 'node:child_process';
import crypto from 'node:crypto';

const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

function getOrCreateTlsCerts() {
  const certPath = '/tmp/anynng_test_cert.pem';
  const keyPath = '/tmp/anynng_test_key.pem';
  if (!fs.existsSync(certPath) || !fs.existsSync(keyPath)) {
    try {
      execSync("openssl req -x509 -newkey rsa:2048 -nodes -keyout /tmp/anynng_test_key.pem -out /tmp/anynng_test_cert.pem -days 1 -subj '/CN=127.0.0.1'");
    } catch {
      // Fallback if openssl command fails
    }
  }
  if (fs.existsSync(certPath) && fs.existsSync(keyPath)) {
    return {
      key: fs.readFileSync(keyPath),
      cert: fs.readFileSync(certPath)
    };
  }
  return null;
}

export function createSpWsServer(port = 0, onMessage = null, opts = {}) {
  const isTls = Boolean(opts?.tls);
  let server;
  if (isTls) {
    const certs = opts?.certs || getOrCreateTlsCerts();
    server = https.createServer(certs);
  } else {
    server = http.createServer();
  }

  const sockets = new Set();
  let serverHelper;

  server.on('upgrade', (req, socket, head) => {
    const key = req.headers['sec-websocket-key'];
    const subproto = req.headers['sec-websocket-protocol'];
    const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');

    socket.write(
      'HTTP/1.1 101 Switching Protocols\r\n' +
      'Upgrade: websocket\r\n' +
      'Connection: Upgrade\r\n' +
      `Sec-WebSocket-Accept: ${accept}\r\n` +
      (subproto ? `Sec-WebSocket-Protocol: ${subproto}\r\n` : '') +
      '\r\n'
    );

    sockets.add(socket);
    socket.on('close', () => sockets.delete(socket));

    let buffer = Buffer.alloc(0);

    socket.on('data', chunk => {
      buffer = Buffer.concat([buffer, chunk]);
      while (buffer.length >= 2) {
        const finAndOpcode = buffer[0];
        const opcode = finAndOpcode & 0x0f;
        const masked = (buffer[1] & 0x80) !== 0;
        let payloadLen = buffer[1] & 0x7f;
        let offset = 2;

        if (payloadLen === 126) {
          if (buffer.length < 4) break;
          payloadLen = buffer.readUInt16BE(2);
          offset = 4;
        } else if (payloadLen === 127) {
          if (buffer.length < 10) break;
          payloadLen = Number(buffer.readBigUInt64BE(2));
          offset = 10;
        }

        const maskLen = masked ? 4 : 0;
        if (buffer.length < offset + maskLen + payloadLen) break;

        let payload = buffer.subarray(offset + maskLen, offset + maskLen + payloadLen);
        if (masked) {
          const mask = buffer.subarray(offset, offset + 4);
          const unmasked = Buffer.alloc(payloadLen);
          for (let i = 0; i < payloadLen; i++) {
            unmasked[i] = payload[i] ^ mask[i % 4];
          }
          payload = unmasked;
        }

        buffer = buffer.subarray(offset + maskLen + payloadLen);

        if (opcode === 0x8) {
          socket.end();
          return;
        } else if (opcode === 0x2) {
          if (onMessage) {
            onMessage(socket, payload, subproto, serverHelper);
          } else {
            // Default SP wire echo behaviors based on subprotocol
            handleSpDefaultEcho(socket, payload, subproto, serverHelper);
          }
        }
      }
    });
  });

  function handleSpDefaultEcho(socket, payload, subproto, helper) {
    if (!subproto) {
      helper.sendBinary(socket, payload);
      return;
    }

    if (subproto.startsWith('rep.') || subproto.startsWith('req.')) {
      // REQ/REP: preserve 4-byte request ID header and echo reply
      const reqId = payload.subarray(0, 4);
      const body = payload.subarray(4);
      const reply = Buffer.concat([reqId, Buffer.from('rep:'), body]);
      helper.sendBinary(socket, reply);
    } else if (subproto.startsWith('pair1.')) {
      // PAIR1: increment hop count
      const hops = payload.readUInt32BE(0);
      const nextHops = Buffer.alloc(4);
      nextHops.writeUInt32BE(hops + 1, 0);
      helper.sendBinary(socket, Buffer.concat([nextHops, payload.subarray(4)]));
    } else if (subproto.startsWith('respondent.') || subproto.startsWith('surveyor.')) {
      // SURVEYOR: preserve 4-byte surveyId header
      const surveyId = payload.subarray(0, 4);
      const body = payload.subarray(4);
      const resp = Buffer.concat([surveyId, Buffer.from('resp:'), body]);
      helper.sendBinary(socket, resp);
    } else if (subproto.startsWith('bus.')) {
      // BUS: echo with hops incremented
      const hops = payload.readUInt32BE(0);
      const nextHops = Buffer.alloc(4);
      nextHops.writeUInt32BE(hops + 1, 0);
      helper.sendBinary(socket, Buffer.concat([nextHops, payload.subarray(4)]));
    } else {
      // PAIR0 / PUB / SUB / PUSH / PULL: raw binary echo
      helper.sendBinary(socket, payload);
    }
  }

  return new Promise((resolve, reject) => {
    server.listen(port, '127.0.0.1', () => {
      const scheme = isTls ? 'wss' : 'ws';
      serverHelper = {
        server,
        port: server.address().port,
        url: `${scheme}://127.0.0.1:${server.address().port}`,
        sockets,
        sendBinary(targetSocket, data) {
          const buf = Buffer.from(data);
          let header;
          if (buf.length < 126) {
            header = Buffer.from([0x82, buf.length]);
          } else if (buf.length <= 65535) {
            header = Buffer.alloc(4);
            header[0] = 0x82;
            header[1] = 126;
            header.writeUInt16BE(buf.length, 2);
          } else {
            header = Buffer.alloc(10);
            header[0] = 0x82;
            header[1] = 127;
            header.writeBigUInt64BE(BigInt(buf.length), 2);
          }
          targetSocket.write(Buffer.concat([header, buf]));
        },
        broadcastBinary(data) {
          for (const s of sockets) {
            this.sendBinary(s, data);
          }
        },
        close() {
          for (const s of sockets) s.destroy();
          return new Promise(res => server.close(res));
        }
      };
      resolve(serverHelper);
    });
  });
}

export function createSpWssServer(port = 0, onMessage = null, opts = {}) {
  return createSpWsServer(port, onMessage, { ...opts, tls: true });
}
