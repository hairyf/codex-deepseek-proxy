#!/usr/bin/env node
/**
 * codex-deepseek-proxy
 *
 * 本地转发代理：修复 Codex MultiAgentV2 在 DeepSeek（及其他非 OpenAI
 * Responses 提供商）下子代理收不到任务消息的问题。
 *
 * 原理（对应 deepseek-ai/awesome-deepseek-agent#349 验证过的方案）：
 *   Codex 把 spawn_agent / followup_task / send_message 的任务文本包在
 *   `agent_message` item 的 `encrypted_content` 里；DeepSeek 端点既不识别
 *   `agent_message`，也不认识 `encrypted_content`，于是子代理只看到空信封。
 *   本代理把这类 item 改写成 DeepSeek 能消费的普通 `message(role=user)` +
 *   `input_text`，其余请求/响应原样透传。
 *
 * 用法：
 *   DEEPSEEK_UPSTREAM=https://api.deepseek.com/ PORT=8787 node proxy.mjs
 *
 * 自检：
 *   node proxy.mjs --selftest        # 仅测改写函数
 *   node proxy.mjs --mock-test       # 用本地 mock 上游做端到端验证
 */
import http from 'node:http';
import https from 'node:https';

const UPSTREAM = (process.env.DEEPSEEK_UPSTREAM || 'https://api.deepseek.com/').replace(/\/+$/, '');
const PORT = Number(process.env.PORT || 8787);
const HOST = process.env.HOST || '127.0.0.1';

const HOP_BY_HOP = new Set([
  'connection',
  'keep-alive',
  'proxy-authenticate',
  'proxy-authorization',
  'te',
  'trailers',
  'transfer-encoding',
  'upgrade',
  'host',
  'content-length',
]);

function logCrash(tag, err) {
  const msg = err && err.stack ? err.stack : String(err);
  console.error(`[${tag}] ${msg}`);
}

// Never let an unexpected stream/protocol error take the proxy down.
process.on('uncaughtException', (err) => logCrash('uncaughtException', err));
process.on('unhandledRejection', (err) => logCrash('unhandledRejection', err));

function unwrapContent(content) {
  if (!Array.isArray(content)) return { out: content, changed: false };
  let changed = false;
  const out = [];
  for (const c of content) {
    if (c && typeof c === 'object' && c.type === 'encrypted_content') {
      changed = true;
      const text = typeof c.encrypted_content === 'string' ? c.encrypted_content.trim() : '';
      if (text) out.push({ type: 'input_text', text: c.encrypted_content });
      continue;
    }
    out.push(c);
  }
  return { out, changed };
}

function rewriteBody(body) {
  if (!body || typeof body !== 'object' || !Array.isArray(body.input)) {
    return { body, changed: false };
  }
  let changed = false;
  body.input = body.input.map((item) => {
    if (item && item.type === 'agent_message' && Array.isArray(item.content)) {
      changed = true;
      const { out } = unwrapContent(item.content);
      const text = out
        .filter((c) => c && c.type === 'input_text' && typeof c.text === 'string')
        .map((c) => c.text)
        .join('');
      return {
        type: 'message',
        role: 'user',
        ...(item.id ? { id: item.id } : {}),
        content: [{ type: 'input_text', text }],
      };
    }
    if (item && Array.isArray(item.content)) {
      const { out, changed: innerChanged } = unwrapContent(item.content);
      if (innerChanged) {
        changed = true;
        return { ...item, content: out };
      }
    }
    return item;
  });
  return { body, changed };
}

function forwardRequest(upstreamBase, req, body) {
  return new Promise((resolve, reject) => {
    const url = new URL(req.url, upstreamBase + '/');
    const isHttps = url.protocol === 'https:';
    const mod = isHttps ? https : http;

    const headers = {};
    for (const [k, v] of Object.entries(req.headers)) {
      if (!HOP_BY_HOP.has(k.toLowerCase())) headers[k] = v;
    }
    if (body !== undefined) headers['content-length'] = Buffer.byteLength(body);

    const upReq = mod.request(
      url,
      {
        method: req.method,
        headers,
      },
      (upRes) => resolve(upRes)
    );
    upReq.on('error', reject);
    if (body !== undefined) upReq.write(body);
    upReq.end();
  });
}

function createProxyServer(upstreamBase) {
  return http.createServer((req, res) => {
    if (req.url === '/healthz') {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ status: 'ok', upstream: upstreamBase }));
      return;
    }

    const chunks = [];
    let received = 0;
    req.on('data', (c) => {
      chunks.push(c);
      received += c.length;
    });
    req.on('end', async () => {
      try {
      let body;
      let rawBody;
      const ctype = String(req.headers['content-type'] || '');
      if (req.method === 'POST' && ctype.includes('application/json') && received > 0) {
        rawBody = Buffer.concat(chunks).toString('utf8');
        try {
          const parsed = JSON.parse(rawBody);
          const { body: rewritten, changed } = rewriteBody(parsed);
          if (changed) {
            console.log(`[rewrite] ${req.url} agent_message -> user message`);
          }
          body = JSON.stringify(rewritten);
        } catch {
          body = rawBody;
        }
      } else if (received > 0) {
        body = Buffer.concat(chunks);
      }

      try {
        const upRes = await forwardRequest(upstreamBase, req, body);
        const resHeaders = {};
        for (const [k, v] of Object.entries(upRes.headers)) {
          if (!HOP_BY_HOP.has(k.toLowerCase())) resHeaders[k] = v;
        }
        res.writeHead(upRes.statusCode, resHeaders);
        upRes.pipe(res);
        upRes.on('error', (err) => {
          console.error(`[error] upstream stream: ${err.message}`);
          res.destroy();
        });
        res.on('error', (err) => {
          console.error(`[error] client stream: ${err.message}`);
          upRes.destroy();
        });
        res.on('close', () => {
          upRes.destroy();
        });
      } catch (err) {
        console.error(`[error] upstream ${upstreamBase} failed: ${err.message}`);
        if (!res.headersSent) {
          res.writeHead(502, { 'content-type': 'text/plain' });
        }
        res.end(`upstream error: ${err.message}`);
      }
      } catch (err) {
        console.error(`[error] request handler: ${err.stack || err.message}`);
        if (!res.headersSent) {
          res.writeHead(500, { 'content-type': 'text/plain' });
        }
        res.end('internal proxy error');
      }
    });
    req.on('error', (err) => {
      console.error(`[error] request error: ${err.message}`);
      if (!res.headersSent) res.writeHead(400);
      res.end('bad request');
    });
  });
}

function selftest() {
  const sample = {
    input: [
      {
        type: 'message',
        role: 'user',
        content: [{ type: 'input_text', text: 'environment context' }],
      },
      {
        type: 'agent_message',
        id: 'amsg_test_1',
        author: '/root',
        recipient: '/root/sub',
        content: [
          { type: 'input_text', text: 'Message Type: NEW_TASK\nTask name: /root/sub\nSender: /root\nPayload:\n' },
          { type: 'encrypted_content', encrypted_content: 'Reply with exactly PONG' },
        ],
      },
      {
        type: 'agent_message',
        id: 'amsg_test_2',
        content: [
          { type: 'input_text', text: 'envelope' },
          { type: 'encrypted_content', encrypted_content: '   ' },
        ],
      },
    ],
  };
  const { body, changed } = rewriteBody(sample);
  const items = body.input;
  const ok =
    changed &&
    items[1].type === 'message' &&
    items[1].role === 'user' &&
    items[1].content[0].type === 'input_text' &&
    items[1].content[0].text.includes('PONG') &&
    items[1].content.every((c) => c.type !== 'encrypted_content') &&
    items[2].content.every((c) => c.type !== 'encrypted_content');
  console.log(JSON.stringify({ ok, changed, input: body.input }, null, 2));
  return ok ? 0 : 1;
}

async function mockTest() {
  const MOCK_PORT = 17999;
  const PROXY_PORT = 18787;
  let receivedBody = null;

  const mock = http.createServer((req, res) => {
    let data = '';
    req.on('data', (c) => (data += c));
    req.on('end', () => {
      try {
        receivedBody = JSON.parse(data);
      } catch {
        receivedBody = { raw: data };
      }
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ ok: true }));
    });
  });

  const proxy = createProxyServer(`http://127.0.0.1:${MOCK_PORT}`);

  await new Promise((r) => mock.listen(MOCK_PORT, '127.0.0.1', r));
  await new Promise((r) => proxy.listen(PROXY_PORT, '127.0.0.1', r));

  const payload = {
    model: 'deepseek-v4-flash',
    input: [
      {
        type: 'agent_message',
        id: 'amsg_mock',
        author: '/root',
        recipient: '/root/child',
        content: [
          { type: 'input_text', text: 'Message Type: NEW_TASK\nPayload:\n' },
          { type: 'encrypted_content', encrypted_content: 'Reply with exactly PONG' },
        ],
      },
    ],
  };

  const resp = await fetch(`http://127.0.0.1:${PROXY_PORT}/responses`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', authorization: 'Bearer test-token' },
    body: JSON.stringify(payload),
  });
  await resp.text();

  const first = receivedBody && receivedBody.input && receivedBody.input[0];
  const ok =
    first &&
    first.type === 'message' &&
    first.role === 'user' &&
    Array.isArray(first.content) &&
    first.content[0].type === 'input_text' &&
    String(first.content[0].text).includes('PONG') &&
    !JSON.stringify(first).includes('encrypted_content');

  console.log(JSON.stringify({ ok, receivedInput: receivedBody && receivedBody.input }, null, 2));

  proxy.close();
  mock.close();
  return ok ? 0 : 1;
}

if (process.argv.includes('--selftest')) {
  process.exit(selftest());
} else if (process.argv.includes('--mock-test')) {
  const code = await mockTest();
  process.exit(code);
} else {
  const server = createProxyServer(UPSTREAM);
  server.listen(PORT, HOST, () => {
    console.log(`[proxy] listening on http://${HOST}:${PORT} -> ${UPSTREAM}`);
  });
}
