import {createParser} from './vendor/eventsource-parser.js';

const $ = id => document.getElementById(id);
const state = {providers: [], history: [], controller: null, loading: false, provider: '', model: ''};
const icons = () => window.lucide.createIcons();
function notice(message = '') { $('notice').textContent = message; $('notice').hidden = !message; }
function controls() {
  const busy = !!state.controller || state.loading;
  $('activity').hidden = !busy;
  $('provider').disabled = busy || !state.providers.length;
  $('model').disabled = busy || !$('model').value;
  $('refresh').disabled = busy;
  $('clear').disabled = busy;
  $('send').disabled = busy || !$('provider').value || !$('model').value || !$('prompt').value.trim();
  document.querySelectorAll('.quick-prompts button').forEach(button => {
    button.disabled = busy || !$('provider').value || !$('model').value;
  });
  $('stop').hidden = !state.controller;
  $('send').hidden = !!state.controller;
}
function authFailure() {
  if (window.top !== window) window.top.postMessage({type: 'lbp-session-expired'}, location.origin);
  else location.replace('login.html');
}
async function api(url, options = {}) {
  const response = await fetch(url, {credentials: 'same-origin', cache: 'no-store', ...options});
  if (response.status === 401) authFailure();
  if (!response.ok) {
    const raw = await response.text();
    let message = `請求失敗（HTTP ${response.status}）`;
    try { const data = JSON.parse(raw); message = data.error?.message || data.message || message; } catch {}
    throw new Error(message);
  }
  return response;
}
function options(select, values, previous) {
  select.replaceChildren(...values.map(([value, name]) => new Option(name, value)));
  if (values.some(([value]) => value === previous)) select.value = previous;
}
async function models() {
  const provider = state.providers.find(p => p.id === $('provider').value);
  options($('model'), [], '');
  if (!provider) return;
  const result = await (await api(`api/provider-configs/${encodeURIComponent(provider.id)}/models`)).json();
  const names = [...new Set((result.models || []).map(m => typeof m === 'string' ? m : m.name).filter(Boolean))];
  if (!names.length && provider.model) names.push(provider.model);
  options($('model'), names.map(name => [name, name]), state.model);
  if (!names.length) notice('此來源沒有可用模型。');
}
async function load() {
  state.loading = true; controls(); notice();
  $('refresh').classList.add('busy');
  try {
    const payload = await (await api('api/provider-configs')).json();
    state.providers = payload.providers || [];
    options($('provider'), state.providers.map(p => [p.id, `${p.name || p.id}${p.enabled ? '' : '（未啟用）'}`]), state.provider);
    await models();
    if (state.history.length && (state.provider !== $('provider').value || state.model !== $('model').value)) clearConversation();
    state.provider = $('provider').value; state.model = $('model').value;
    $('status').textContent = state.providers.length ? '就緒' : '尚未設定 Provider';
  } catch (error) { notice(error.message); $('status').textContent = '來源載入失敗'; }
  finally { state.loading = false; $('refresh').classList.remove('busy'); controls(); }
}
function clearConversation() {
  state.history = [];
  $('messages').querySelectorAll('.message').forEach(node => node.remove());
  $('empty').hidden = false; notice(); $('status').textContent = '就緒';
}
$('provider').addEventListener('change', async () => {
  clearConversation(); state.loading = true; controls();
  try { await models(); state.provider = $('provider').value; state.model = $('model').value; }
  catch (error) { notice(error.message); }
  finally { state.loading = false; controls(); }
});
$('model').addEventListener('change', () => {
  clearConversation(); state.model = $('model').value; controls();
});
async function copyText(text) {
  if (navigator.clipboard?.writeText) {
    try { await navigator.clipboard.writeText(text); return; } catch {}
  }
  const active = document.activeElement;
  const selection = window.getSelection();
  const ranges = selection ? Array.from({length: selection.rangeCount}, (_, i) => selection.getRangeAt(i).cloneRange()) : [];
  const field = document.createElement('textarea');
  field.value = text; field.readOnly = true;
  field.style.cssText = 'position:fixed;top:0;left:0;width:1px;height:1px;opacity:0;pointer-events:none;';
  document.body.append(field);
  try {
    field.focus({preventScroll: true}); field.select();
    if (!document.execCommand('copy')) throw new Error('clipboard unavailable');
  } finally {
    field.remove();
    active?.focus({preventScroll: true});
    if (selection) { selection.removeAllRanges(); ranges.forEach(range => selection.addRange(range)); }
  }
}
function message(role, text, label) {
  $('empty').hidden = true;
  const article = document.createElement('article'); article.className = `message ${role}`;
  const head = document.createElement('div'); head.className = 'message-head';
  const who = document.createElement('strong'); who.textContent = role === 'user' ? '你' : '助理';
  const meta = document.createElement('span'); meta.className = 'meta'; meta.textContent = label;
  const copy = document.createElement('button'); copy.className = 'icon copy'; copy.type = 'button'; copy.title = '複製訊息'; copy.setAttribute('aria-label', '複製訊息');
  copy.innerHTML = '<i data-lucide="copy"></i>';
  const content = document.createElement('div'); content.className = 'content'; content.textContent = text;
  const reasoning = document.createElement('details'); reasoning.hidden = true;
  const summary = document.createElement('summary'); summary.textContent = '推理摘要';
  const reasoningText = document.createElement('pre'); reasoning.append(summary, reasoningText);
  const failure = document.createElement('div'); failure.className = 'failure'; failure.hidden = true;
  const diagnostic = document.createElement('details'); diagnostic.hidden = role !== 'assistant';
  const diagnosticTitle = document.createElement('summary'); diagnosticTitle.textContent = '原始回應';
  const rawView = document.createElement('pre'); diagnostic.append(diagnosticTitle, rawView);
  head.append(who, meta, copy); article.append(head, reasoning, content, failure, diagnostic); $('messages').append(article);
  const result = {article, content, meta, reasoning, reasoningText, failure, rawView, raw: '', text, thought: ''};
  const copyLabel = () => diagnostic.open ? '複製原始回應' : '複製訊息';
  const setCopyLabel = label => { copy.title = label; copy.setAttribute('aria-label', label); };
  diagnostic.addEventListener('toggle', () => setCopyLabel(copyLabel()));
  copy.onclick = async () => {
    const value = diagnostic.open ? result.raw : (result.text || result.raw || result.thought || failure.textContent);
    if (!value) { notice('目前尚無可複製的內容。'); return; }
    copy.disabled = true;
    try {
      await copyText(value);
      setCopyLabel('已複製'); copy.innerHTML = '<i data-lucide="check"></i>'; icons();
      notice('已複製到剪貼簿。');
      setTimeout(() => { setCopyLabel(copyLabel()); copy.innerHTML = '<i data-lucide="copy"></i>'; icons(); }, 1500);
    } catch {
      notice('瀏覽器未允許寫入剪貼簿，請選取文字手動複製。');
    } finally { copy.disabled = false; }
  };
  icons(); $('messages').scrollTop = $('messages').scrollHeight;
  return result;
}
function render(item) {
  item.rawView.textContent = item.raw;
  const follow = $('messages').scrollHeight - $('messages').scrollTop - $('messages').clientHeight < 100;
  item.content.innerHTML = DOMPurify.sanitize(marked.parse(item.text, {gfm: true, breaks: true}), {
    USE_PROFILES: {html: true}, FORBID_TAGS: ['img', 'style', 'form', 'input', 'button'], FORBID_ATTR: ['style', 'id', 'name']
  });
  item.content.querySelectorAll('a').forEach(a => { a.target = '_blank'; a.rel = 'noopener noreferrer'; });
  item.reasoning.hidden = !item.thought; item.reasoningText.textContent = item.thought;
  if (follow) $('messages').scrollTop = $('messages').scrollHeight;
}
$('composer').addEventListener('submit', async event => {
  event.preventDefault();
  if (state.controller || state.loading || !$('model').value || !$('provider').value) return;
  const quickPrompt = event.submitter?.dataset.prompt;
  const prompt = quickPrompt ?? $('prompt').value; if (!prompt.trim()) return;
  const label = `${$('provider').selectedOptions[0].textContent} · ${$('model').value}`;
  const messages = [...state.history, {role: 'user', content: prompt}];
  const controller = new AbortController(); state.controller = controller; notice();
  message('user', prompt, ''); const assistant = message('assistant', '', label);
  if (quickPrompt === undefined) $('prompt').value = '';
  controls();
  $('status').textContent = '等待回應 · 0 秒';
  const started = performance.now(); let finished = false, timer, reader, pendingRender;
  timer = setInterval(() => { $('status').textContent = `${assistant.text ? '生成中' : '等待回應'} · ${Math.floor((performance.now() - started) / 1000)} 秒`; }, 500);
  const update = () => { if (!pendingRender) pendingRender = setTimeout(() => { pendingRender = null; render(assistant); }, 60); };
  try {
    const response = await fetch('api/diagnostics/chat', {method: 'POST', credentials: 'same-origin', cache: 'no-store', headers: {'Content-Type': 'application/json', Accept: 'text/event-stream'},
      body: JSON.stringify({provider_id: $('provider').value, model: $('model').value, stream: true, messages}), signal: controller.signal});
    const upstream = response.headers.get('X-Diagnostic-Origin') === 'upstream';
    if (response.status === 401 && !upstream) authFailure();
    const trace = response.headers.get('X-Request-ID') || response.headers.get('Request-ID') || response.headers.get('OpenAI-Request-ID');
    assistant.meta.textContent = `${label} · ${upstream ? '上游' : '代理'} HTTP ${response.status}${trace ? ` · ${trace}` : ''}`;
    const retryAfter = response.headers.get('Retry-After');
    if (retryAfter) assistant.meta.textContent += ` · Retry-After: ${retryAfter}`;
    const capture = text => {
      const available = 2000000 - assistant.raw.length;
      assistant.raw += text.slice(0, available);
      if (text.length > available) throw new Error('本機顯示限制：原始回應超過 2,000,000 字元，已停止讀取。');
    };
    if (!response.body) throw new Error('伺服器未提供回應內容。');
    const contentType = response.headers.get('Content-Type') || '';
    assistant.meta.title = `Content-Type: ${contentType || '未提供'}`;
    const decoder = new TextDecoder();
    let format = '', prefix = '', streamFailure = '';
      const parser = createParser({onEvent(event) {
        if (streamFailure) return;
        const payload = event.data.trim();
        if (!payload) return;
        if (payload === '[DONE]') { finished = true; return; }
        let data;
        try { data = JSON.parse(payload); }
        catch { throw new Error('本機解析：串流事件不是完整的 JSON，請查看原始回應。'); }
        if (!data || typeof data !== 'object' || Array.isArray(data)) throw new Error('本機解析：串流事件不是 JSON 物件，請查看原始回應。');
        data.type ||= event.event;
        if (data.error || ['response.failed', 'response.incomplete', 'error'].includes(data.type)) {
          const error = data.error || data.response?.error || {};
          streamFailure = `${error.code ? `${error.code}: ` : ''}${error.message || data.message || event.data}`;
          assistant.failure.textContent = streamFailure;
          assistant.failure.hidden = false;
          finished = false;
          return;
        }
        if (data.type === 'response.output_text.delta') assistant.text += data.delta || '';
        if (data.type === 'response.refusal.delta') assistant.text += data.delta || '';
        if (data.type === 'response.reasoning_summary_text.delta' || data.type === 'response.reasoning_text.delta') assistant.thought += data.delta || '';
        if (data.type === 'response.completed') {
          finished = true;
          if (!assistant.text) assistant.text = (data.response?.output || []).filter(item => item.type === 'message').flatMap(item => item.content || []).filter(part => part.type === 'output_text').map(part => part.text).join('');
        }
        const choice = data.choices?.[0];
        if (typeof choice?.delta?.content === 'string') assistant.text += choice.delta.content;
        if (typeof choice?.delta?.reasoning_content === 'string') assistant.thought += choice.delta.reasoning_content;
        if (choice?.finish_reason) finished = true;
        if (assistant.text.length + assistant.thought.length > 2000000) throw new Error('回應超過頁面顯示上限，已停止。');
        update();
      }});
    // 先辨識內容前綴；代理或上游的 Content-Type 不一定與本文一致。
    const consume = text => {
      capture(text);
      if (!format) {
        prefix += text;
        const start = prefix.trimStart();
        if (/^(?:\{|\[)/.test(start)) format = 'json';
        else if (/(?:^|[\r\n])(?::|(?:data|event|id|retry)(?::|\r|\n))/.test(start)) format = 'sse';
        else if (!start || ['data', 'event', 'id', 'retry'].some(field => field.startsWith(start))) return;
        else if (contentType.toLowerCase().includes('text/event-stream')) format = 'sse';
        // 未辨識前保留分段前綴，不能因第一段尚無完整 SSE 欄位就判成 JSON。
        else if (prefix.length < 4096 && !start.startsWith('<')) return;
        else format = 'unknown';
        text = prefix; prefix = '';
      }
      if (format === 'sse') {
        try { parser.feed(text); }
        catch (error) { streamFailure ||= error.message; }
      }
      update();
    };
    reader = response.body.getReader();
    while (true) { const chunk = await reader.read(); if (chunk.done) break; consume(decoder.decode(chunk.value, {stream: true})); }
    consume(decoder.decode());
    if (!assistant.raw.trim()) throw new Error(`收到 HTTP ${response.status}，但回應內容為空；尚無法確認內容是在上游或代理傳輸階段遺失。`);
    if (streamFailure) throw new Error(streamFailure);
    if (!response.ok) throw new Error(`${upstream ? '上游' : '代理'} HTTP ${response.status}\n${assistant.raw}`);
    if (format !== 'sse' && format !== 'json') throw new Error('本機解析：無法辨識回應格式（非 JSON 或可辨識的 SSE），請查看原始回應。');
    if (format === 'json') {
      let data;
      try { data = JSON.parse(assistant.raw); }
      catch { throw new Error('本機解析：回應不是完整的 JSON，請查看原始回應。'); }
      if (!data || typeof data !== 'object') throw new Error('本機解析：回應不是 JSON 物件，請查看原始回應。');
      if (data.error) throw new Error(data.error.message || JSON.stringify(data.error));
      const choice = data.choices?.[0];
      const responseText = data.output?.filter(item => item.type === 'message').flatMap(item => item.content || []).filter(part => part.type === 'output_text').map(part => part.text).join('');
      if (data.status === 'failed' || data.status === 'incomplete') throw new Error(assistant.raw);
      if (typeof choice?.message?.content !== 'string' && typeof responseText !== 'string') throw new Error('本機解析：無法辨識回應格式，請查看原始回應。');
      assistant.text = choice?.message?.content ?? responseText; finished = true;
    }
    if (!finished) throw new Error('串流在完成前中斷，未自動重送。');
    if (!assistant.text) assistant.text = '（沒有文字回覆）';
    state.history = [...messages, {role: 'assistant', content: assistant.text}];
    $('status').textContent = `已完成 · ${((performance.now() - started) / 1000).toFixed(1)} 秒`;
  } catch (error) {
    const canceled = controller.signal.aborted;
    assistant.failure.textContent = canceled ? '已停止，這次未完成的問答不加入後續上下文。' : error.message;
    assistant.failure.hidden = false;
    $('status').textContent = canceled ? '已停止' : '回應失敗';
    if (!$('prompt').value) $('prompt').value = prompt;
  } finally {
    clearInterval(timer); clearTimeout(pendingRender);
    if (reader) { try { await reader.cancel(); } catch {} reader.releaseLock(); }
    controller.abort(); state.controller = null; render(assistant); controls(); $('prompt').focus();
  }
});
$('prompt').addEventListener('input', controls);
$('prompt').addEventListener('keydown', event => { if (event.key === 'Enter' && (event.ctrlKey || event.metaKey) && !event.isComposing) { event.preventDefault(); $('composer').requestSubmit(); } });
$('stop').onclick = () => state.controller?.abort();
$('clear').onclick = clearConversation;
$('refresh').onclick = load;
window.addEventListener('pagehide', () => state.controller?.abort());
icons(); load();
