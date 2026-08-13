#!/usr/bin/env node
/**
 * codex-deepseek-proxy
 *
 * 本地转发代理：修复 Codex MultiAgentV2 在 DeepSeek（及其他非 OpenAI
 * Responses 提供商）下子代理收不到任务消息的问题。
 *
 * 原理：
 *   Codex 把 spawn_agent / followup_task / send_message 的任务文本包在
 *   `agent_message` item 的 `encrypted_content` 里；DeepSeek 端点既不识别
 *   `agent_message`，也不认识 `encrypted_content`。
 *   本代理把这类 item 改写成 DeepSeek 能消费的普通 `message(role=user)` +
 *   `input_text`，删除缺少名称的非标准 tool，其余请求/响应原样透传。
 *
 * 用法：
 *   DEEPSEEK_UPSTREAM=https://api.deepseek.com/ PORT=8787 node proxy.mjs
 */

import http from 'node:http';
import https from 'node:https';
import { pipeline } from 'node:stream/promises';
import { Transform } from 'node:stream';
import { writeFileSync, mkdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const LOG_DIR = fileURLToPath(new URL('../logs/', import.meta.url));

// -----------------------------------------------------------------------------
// 配置与常量
// -----------------------------------------------------------------------------
const UPSTREAM = (process.env.DEEPSEEK_UPSTREAM || 'https://api.deepseek.com/').replace(/\/+$/, '');
const PORT = Number(process.env.PORT || 8787);
const HOST = process.env.HOST || '127.0.0.1';

const JSON_HEADERS = Object.freeze({ 'content-type': 'application/json; charset=utf-8' });

// Hop-by-hop 头部过滤集合（转发时跳过）
const HOP_BY_HOP_HEADERS = new Set([
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

// -----------------------------------------------------------------------------
// 异常防护
// -----------------------------------------------------------------------------
function logError(tag, err) {
  const msg = err?.stack || String(err);
  console.error(`[${tag}] ${msg}`);
}

process.on('uncaughtException', (err) => logError('uncaughtException', err));
process.on('unhandledRejection', (err) => logError('unhandledRejection', err));

// -----------------------------------------------------------------------------
// 核心 Payload 改写逻辑
// -----------------------------------------------------------------------------

/**
 * 解包 content 数组中的 encrypted_content 节点，合并为一个只包含 input_text 的数组
 */
function unwrapContent(content) {
  if (!Array.isArray(content)) return { out: content, changed: false };

  let changed = false;
  const out = [];

  for (const item of content) {
    if (item && typeof item === 'object' && item.type === 'encrypted_content') {
      changed = true;
      const text = typeof item.encrypted_content === 'string' ? item.encrypted_content.trim() : '';
      if (text) {
        out.push({ type: 'input_text', text });
      }
    } else {
      out.push(item);
    }
  }

  return { out, changed };
}

/**
 * 改写请求体，将 agent_message 转换为 DeepSeek 兼容的普通 user message
 */
function transformBody(body) {
  if (!body || typeof body !== 'object') return { body, changed: false };

  let isModified = false;

  // 1. 转换 input 中的 agent_message / encrypted_content
  if (Array.isArray(body.input)) {
    const newInput = body.input.map((item) => {
      if (!item || typeof item !== 'object') return item;

      // 处理 agent_message 类型转换
      if (item.type === 'agent_message' && Array.isArray(item.content)) {
        isModified = true;
        const { out } = unwrapContent(item.content);

        // 提取并拼接 input_text 文本内容
        let fullText = '';
        for (const c of out) {
          if (c && c.type === 'input_text' && typeof c.text === 'string') {
            fullText += c.text;
          }
        }

        const mapped = {
          type: 'message',
          role: 'user',
          content: [{ type: 'input_text', text: fullText }],
        };
        if (item.id) mapped.id = item.id;
        return mapped;
      }

      // 处理普通包含 encrypted_content 的消息
      if (Array.isArray(item.content)) {
        const { out, changed } = unwrapContent(item.content);
        if (changed) {
          isModified = true;
          return { ...item, content: out };
        }
      }

      // Console Go 网关要求 assistant 消息的 content 是字符串：
      // pro 模型遇到数组形式的 output_text 会报 "Invalid assistant message"。
      // flash 两种都接受，因此统一转成字符串更稳妥。
      if (
        item.type === 'message' &&
        item.role === 'assistant' &&
        Array.isArray(item.content)
      ) {
        const text = item.content
          .filter((c) => c && typeof c === 'object' && typeof c.text === 'string')
          .map((c) => c.text)
          .join('');
        if (text.length > 0) {
          isModified = true;
          return { ...item, content: text };
        }
      }

      return item;
    });

    if (isModified) {
      body.input = newInput;
    }
  }

  // 2. 清理无名工具（例如 tool_search, web_search）以兼容 DeepSeek 校验
  if (Array.isArray(body.tools)) {
    const sanitizedTools = body.tools.filter(
      (t) => Boolean(t && typeof t.name === 'string' && t.name.trim().length > 0)
    );

    if (sanitizedTools.length !== body.tools.length) {
      isModified = true;
      body.tools = sanitizedTools;
    }
  }

  return { body, changed: isModified };
}

// -----------------------------------------------------------------------------
// 网络与 Header 辅助函数
// -----------------------------------------------------------------------------

/**
 * 过滤头部信息，去除 Hop-by-hop 属性
 */
function filterHeaders(headers) {
  const result = {};
  for (const [key, value] of Object.entries(headers)) {
    if (!HOP_BY_HOP_HEADERS.has(key.toLowerCase())) {
      result[key] = value;
    }
  }
  return result;
}

/**
 * 将 Readable Stream 完整读取为 Buffer
 */
async function readStreamToBuffer(stream) {
  const chunks = [];
  for await (const chunk of stream) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

// -----------------------------------------------------------------------------
// SSE 响应流标准化
// -----------------------------------------------------------------------------

function sseBlock(eventName, dataObj) {
  return `event: ${eventName}\ndata: ${JSON.stringify(dataObj)}\n\n`;
}

function parseSseBlock(block) {
  let name = null;
  const dataLines = [];

  for (const line of block.split('\n')) {
    if (line.startsWith('event:')) {
      name = line.slice(6).trim();
    } else if (line.startsWith('data:')) {
      dataLines.push(line.slice(5).trim());
    }
  }

  if (dataLines.length === 0) return { name, data: null };

  try {
    return { name, data: JSON.parse(dataLines.join('\n')) };
  } catch {
    return { name, data: null }; // 非 JSON 数据（如 [DONE]）
  }
}

/**
 * 创建 SSE 事件标准化转换函数
 */
function createSseNormalizer() {
  // 单个请求的状态机上下文
  const state = {
    respId: null,        // 上游 response id
    textBuf: '',         // 已累积的文本
    msgItemId: null,     // 合成 message item 的 id
    msgIndex: null,      // 合成 message item 的 output_index
    nextIndex: 0,        // 下一个可用 output_index
    msgScaffolded: false,
    fcs: new Map(),      // output_index -> { id, name, call_id, args }
  };

  return function rewriteBlock(block) {
    const { data } = parseSseBlock(block);
    if (!data) {
      return [block + '\n\n']; // 非 JSON 事件原样透传
    }

    switch (data.type) {
      case 'response.output_item.added': {
        const idx = data.output_index ?? 0;
        state.nextIndex = Math.max(state.nextIndex, idx + 1);
        const item = data.item || {};

        if (item.type === 'function_call') {
          state.fcs.set(idx, {
            id: item.id,
            name: item.name,
            call_id: item.call_id || item.id,
            args: '',
          });
        }
        return [sseBlock(data.type, data)];
      }

      case 'response.function_call_arguments.delta': {
        const idx = data.output_index ?? 0;
        const fc = state.fcs.get(idx);
        if (fc) {
          fc.args += data.delta ?? '';
          return [sseBlock(data.type, { ...data, item_id: fc.id })];
        }
        return [sseBlock(data.type, data)];
      }

      case 'response.output_text.delta': {
        state.respId ||= data.response?.id || data.id;
        const delta = data.delta ?? '';
        state.textBuf += delta;

        const blocks = [];

        // 首次收到文本 delta 时，补发 message item 脚手架事件
        if (!state.msgScaffolded) {
          state.msgScaffolded = true;
          state.msgIndex = state.nextIndex++;
          state.msgItemId = `msg_${state.respId || 'synthetic'}`;

          blocks.push(
            sseBlock('response.output_item.added', {
              type: 'response.output_item.added',
              output_index: state.msgIndex,
              item: {
                id: state.msgItemId,
                type: 'message',
                status: 'in_progress',
                role: 'assistant',
                content: [],
              },
            }),
            sseBlock('response.content_part.added', {
              type: 'response.content_part.added',
              item_id: state.msgItemId,
              output_index: state.msgIndex,
              content_index: 0,
              part: { type: 'output_text', text: '', annotations: [] },
            })
          );
        }

        blocks.push(
          sseBlock('response.output_text.delta', {
            type: 'response.output_text.delta',
            item_id: state.msgItemId,
            output_index: state.msgIndex,
            content_index: 0,
            delta,
          })
        );
        return blocks;
      }

      case 'response.completed': {
        const blocks = [];
        const output = [];

        // 1. 结束 message item
        if (state.msgScaffolded) {
          const fullText = state.textBuf;
          const msgItem = {
            id: state.msgItemId,
            type: 'message',
            status: 'completed',
            role: 'assistant',
            content: [{ type: 'output_text', text: fullText, annotations: [] }],
          };

          blocks.push(
            sseBlock('response.output_text.done', {
              type: 'response.output_text.done',
              item_id: state.msgItemId,
              output_index: state.msgIndex,
              content_index: 0,
              text: fullText,
            }),
            sseBlock('response.content_part.done', {
              type: 'response.content_part.done',
              item_id: state.msgItemId,
              output_index: state.msgIndex,
              content_index: 0,
              part: { type: 'output_text', text: fullText, annotations: [] },
            }),
            sseBlock('response.output_item.done', {
              type: 'response.output_item.done',
              output_index: state.msgIndex,
              item: msgItem,
            })
          );
          output.push(msgItem);
        }

        // 2. 结束 function_call items
        for (const [idx, fc] of state.fcs) {
          const fcItem = {
            id: fc.id,
            type: 'function_call',
            status: 'completed',
            name: fc.name,
            call_id: fc.call_id,
            arguments: fc.args,
          };

          blocks.push(
            sseBlock('response.function_call_arguments.done', {
              type: 'response.function_call_arguments.done',
              item_id: fc.id,
              output_index: idx,
              arguments: fc.args,
            }),
            sseBlock('response.output_item.done', {
              type: 'response.output_item.done',
              output_index: idx,
              item: fcItem,
            })
          );
          output.push(fcItem);
        }

        // 3. 补齐 completed 事件里的 response.output
        const resp = {
          ...(data.response || {}),
          id: data.response?.id || data.id,
          object: data.response?.object || 'response',
          status: 'completed',
        };
        if (output.length > 0) resp.output = output;

        blocks.push(sseBlock('response.completed', { ...data, response: resp }));
        return blocks;
      }

      default:
        return [sseBlock(data.type, data)];
    }
  };
}

// -----------------------------------------------------------------------------
// 代理请求与服务器实现
// -----------------------------------------------------------------------------

/**
 * 创建主代理服务器
 */
function createProxyServer(upstreamBase) {
  return http.createServer(async (req, res) => {
    // 健康检查节点
    if (req.url === '/healthz') {
      res.writeHead(200, JSON_HEADERS);
      res.end(JSON.stringify({ status: 'ok', upstream: upstreamBase }));
      return;
    }

    try {
      const contentType = String(req.headers['content-type'] || '');
      const isJsonPayload = ['POST', 'PUT', 'PATCH'].includes(req.method) && contentType.includes('application/json');

      let bodyBuffer;

      // 仅处理 JSON Payload，非 JSON 流量采用 Stream 流式透传
      if (isJsonPayload) {
        const rawBuffer = await readStreamToBuffer(req);

        if (rawBuffer.length > 0) {
          try {
            const parsed = JSON.parse(rawBuffer.toString('utf8'));
            const { body: transformed, changed } = transformBody(parsed);

            if (changed) {
              console.log(`[transform] ${req.url} rewritten payload for DeepSeek compatibility`);
              bodyBuffer = Buffer.from(JSON.stringify(transformed));
            } else {
              bodyBuffer = rawBuffer; // 未改写时直接复用 Raw Buffer
            }

            // 诊断日志：记录 /responses 请求的关键特征
            if (req.url.includes('/responses')) {
              const p = transformed;
              const input = Array.isArray(p.input) ? p.input : [];
              const types = [...new Set(input.map((i) => i.type || 'message'))];
              console.log(
                `[req] model=${p.model} reasoning=${JSON.stringify(p.reasoning || null)} ` +
                  `input=${input.length} types=[${types.join(',')}] ` +
                  `assistant=${input.filter((i) => i.type === 'message' && i.role === 'assistant').length} ` +
                  `function_call=${input.filter((i) => i.type === 'function_call').length} ` +
                  `function_call_output=${input.filter((i) => i.type === 'function_call_output').length}`
              );
            }
          } catch {
            bodyBuffer = rawBuffer; // JSON 解析失败退回原始 Buffer
          }
        }
      }

      // 构建目标 URL 规则
      const base = upstreamBase.endsWith('/') ? upstreamBase : `${upstreamBase}/`;
      const targetUrl = new URL(req.url.replace(/^\/+/, ''), base);
      const transport = targetUrl.protocol === 'https:' ? https : http;
      const headers = filterHeaders(req.headers);

      if (bodyBuffer !== undefined) {
        headers['content-length'] = Buffer.byteLength(bodyBuffer);
      }

      // 发起代理请求。Console Go 网关在 thinking 模式下会间歇性丢弃
      // reasoning_content 导致上游 400/500，这里支持一次降级重试：
      // 命中 reasoning 相关错误时，用 reasoning:none 重试。
      const isResponses = req.url.includes('/responses');

      const sendUpstream = (body, hdrs) =>
        new Promise((resolve, reject) => {
          const upstreamReq = transport.request(targetUrl, {
            method: req.method,
            headers: hdrs,
          });
          upstreamReq.on('response', (upstreamRes) =>
            resolve({ upstreamReq, upstreamRes })
          );
          upstreamReq.on('error', reject);
          if (body !== undefined) {
            upstreamReq.end(body);
          } else {
            pipeline(req, upstreamReq).catch(reject);
          }
        });

      let requestBody = bodyBuffer;
      let { upstreamRes } = await sendUpstream(requestBody, headers);

      // 命中网关 thinking 模式 bug 时降级重试一次
      if (
        isResponses &&
        requestBody !== undefined &&
        (upstreamRes.statusCode === 400 || upstreamRes.statusCode === 500)
      ) {
        const errBuffer = await readStreamToBuffer(upstreamRes);
        const errText = errBuffer.toString('utf8');
        const retryable =
          /reasoning_content|thinking mode|Invalid assistant message|Internal server error/i.test(
            errText
          );

        if (retryable) {
          const errSnippet = errText.replace(/\s+/g, ' ').slice(0, 140);
          console.log(
            `[retry] ${req.url} upstream ${upstreamRes.statusCode}; err=${errSnippet}; retrying with reasoning:none`
          );
          // 把触发重试的完整请求体落盘，便于定位网关 bug 的具体请求特征
          try {
            const ts = new Date().toISOString().replace(/[:.]/g, '-');
            mkdirSync(LOG_DIR, { recursive: true });
            writeFileSync(`${LOG_DIR}retry-request-${ts}.json`, requestBody.toString('utf8'));
            writeFileSync(`${LOG_DIR}retry-error-${ts}.json`, errText);
          } catch (logErr) {
            logError('retry-dump', logErr);
          }
          try {
            const parsedRetry = JSON.parse(requestBody.toString('utf8'));
            parsedRetry.reasoning = { effort: 'none' };
            const retryBuffer = Buffer.from(JSON.stringify(parsedRetry));
            headers['content-length'] = Buffer.byteLength(retryBuffer);
            ({ upstreamRes } = await sendUpstream(retryBuffer, headers));
            console.log(
              `[retry] result: upstream ${upstreamRes.statusCode}; ${errSnippet}`
            );
          } catch (retryErr) {
            logError('retry', retryErr);
          }
        } else {
          // 非可重试错误（如 401/404），原样转发给 Codex
          res.writeHead(upstreamRes.statusCode, filterHeaders(upstreamRes.headers));
          res.end(errBuffer);
          return;
        }
      }

      res.writeHead(upstreamRes.statusCode, filterHeaders(upstreamRes.headers));

      const upstreamContentType = String(upstreamRes.headers['content-type'] || '');
      const isResponsesSse =
        isResponses &&
        upstreamContentType.includes('text/event-stream') &&
        upstreamRes.statusCode === 200;

      // 如果是 SSE 响应，走标准化 Transform 流，否则直接透传
      if (isResponsesSse) {
        const normalize = createSseNormalizer();
        const transform = new Transform({
          transform(chunk, _enc, cb) {
            try {
              this.buf = (this.buf || '') + chunk.toString('utf8');
              let idx;
              while ((idx = this.buf.indexOf('\n\n')) !== -1) {
                const block = this.buf.slice(0, idx);
                this.buf = this.buf.slice(idx + 2);
                if (block.trim() === '') continue;
                for (const out of normalize(block)) this.push(out);
              }
              cb();
            } catch (err) {
              cb(err);
            }
          },
          flush(cb) {
            if ((this.buf || '').trim() !== '') {
              for (const out of normalize(this.buf)) this.push(out);
              this.buf = '';
            }
            cb();
          },
        });

        await pipeline(upstreamRes, transform, res);
      } else {
        await pipeline(upstreamRes, res);
      }
    } catch (err) {
      logError('handler', err);
      if (!res.headersSent) {
        const isNetworkError = Boolean(err && err.code);
        res.writeHead(isNetworkError ? 502 : 500, {
          'content-type': 'text/plain; charset=utf-8',
        });
        res.end(
          `${isNetworkError ? 'Proxy Error' : 'Internal Proxy Error'}: ${err.message}`
        );
      }
    }
  });
}

// 启动服务
const server = createProxyServer(UPSTREAM);
server.listen(PORT, HOST, () => {
  console.log(`[proxy] listening on http://${HOST}:${PORT} -> ${UPSTREAM}`);
});