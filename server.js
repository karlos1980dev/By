'use strict';

require('dotenv').config();

const http = require('node:http');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const { initializeApp, cert, applicationDefault } = require('firebase-admin/app');
const { getDatabase } = require('firebase-admin/database');

const PORT = Number(process.env.PORT || 3000);
const DATABASE_URL = process.env.FIREBASE_DATABASE_URL || 'https://placarexato-97966-default-rtdb.asia-southeast1.firebasedatabase.app';
const PROJECT_ID = process.env.FIREBASE_PROJECT_ID || 'placarexato-97966';
const TIME_ZONE = 'America/Sao_Paulo';
const INTERVAL_MS = 30 * 60 * 1000;
const POLL_MS = Math.max(2000, Number(process.env.POLL_MS || 5000));
const MAX_CATCHUP_PER_CYCLE = 24;
const CONFIG_PATH = 'timeCerto/configuracao';
const RESULTS_PATH = 'timeCerto/resultados';

const times = [
  'ATHLETICO PR','ATLÉTICO MG','BAHIA','BOTAFOGO','RB BRAGANTINO','CHAPECOENSE','CORINTHIANS',
  'CORITIBA','CRUZEIRO','FLAMENGO','FLUMINENSE','GRÊMIO','INTERNACIONAL','MIRASSOL','PALMEIRAS',
  'REMO','SANTOS','SÃO PAULO','VASCO','VITÓRIA','AMÉRICA MG','ATHLETIC MG','ATLÉTICO GO','AVAÍ',
  'BOTAFOGO SP','CEARÁ','CRB','CRICIÚMA','CUIABÁ','FORTALEZA','GOIÁS','JUVENTUDE','LONDRINA',
  'NÁUTICO','NOVORIZONTINO','OPERÁRIO PR','PONTE PRETA','SÃO BERNARDO','SPORT','VILA NOVA',
  'ARSENAL','CHELSEA','LIVERPOOL','MANCHESTER UNITED','ATLÉTICO DE MADRID','BARCELONA','REAL MADRID',
  'INTERNAZIONALE','JUVENTUS','PSG'
];

let db;
let tickRunning = false;
let lastCheck = null;
let lastDraw = null;
let lastError = null;
let automaticEnabled = false;

function log(message, extra) {
  console.log(`[${new Date().toISOString()}] ${message}${extra ? ` ${JSON.stringify(extra)}` : ''}`);
}

function initFirebase() {
  const rawJson = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
  const rawBase64 = process.env.FIREBASE_SERVICE_ACCOUNT_BASE64;
  let credential;
  if (rawJson && rawJson.trim()) credential = cert(JSON.parse(rawJson));
  else if (rawBase64 && rawBase64.trim()) credential = cert(JSON.parse(Buffer.from(rawBase64.trim(), 'base64').toString('utf8')));
  else if (process.env.GOOGLE_APPLICATION_CREDENTIALS) credential = applicationDefault();
  else throw new Error('Credenciais ausentes. Configure FIREBASE_SERVICE_ACCOUNT_JSON, FIREBASE_SERVICE_ACCOUNT_BASE64 ou GOOGLE_APPLICATION_CREDENTIALS.');

  const app = initializeApp({ credential, databaseURL: DATABASE_URL, projectId: PROJECT_ID });
  db = getDatabase(app);
  log('Firebase Admin conectado.', { projectId: PROJECT_ID, databaseURL: DATABASE_URL });
}

function spParts(timestamp) {
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone: TIME_ZONE, year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23'
  }).formatToParts(new Date(timestamp));
  const result = {};
  for (const item of parts) result[item.type] = item.value;
  return result;
}
function scheduledDateTime(timestamp) {
  const p = spParts(timestamp);
  return { data: `${p.year}-${p.month}-${p.day}`, horario: `${p.hour}:${p.minute}` };
}
function nextSlotFromNow(now = Date.now()) {
  const p = spParts(now);
  const minute = Number(p.minute);
  const second = Number(p.second);
  const milliseconds = new Date(now).getMilliseconds();
  return now + (30 - (minute % 30)) * 60_000 - second * 1000 - milliseconds;
}
function formatSP(timestamp) {
  return new Intl.DateTimeFormat('pt-BR', { timeZone: TIME_ZONE, dateStyle: 'short', timeStyle: 'short' }).format(new Date(timestamp));
}
async function readConfig() {
  const snapshot = await db.ref(CONFIG_PATH).once('value');
  return snapshot.val() || {};
}
async function ensureNextSlot() {
  const ref = db.ref(`${CONFIG_PATH}/sorteioAutomaticoProximo`);
  const snapshot = await ref.once('value');
  const current = Number(snapshot.val() || 0);
  if (Number.isFinite(current) && current > 0) return current;
  const proposed = nextSlotFromNow();
  await ref.transaction(value => {
    const n = Number(value || 0);
    if (Number.isFinite(n) && n > 0) return;
    return proposed;
  }, undefined, false);
  const updated = await ref.once('value');
  return Number(updated.val() || proposed);
}
async function sendTelegram(message) {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;
  if (!token || !chatId) return { skipped: true };
  const response = await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ chat_id: chatId, text: message }), signal: AbortSignal.timeout(10000)
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok || data.ok !== true) throw new Error(`Telegram não confirmou a mensagem: ${data.description || response.status}`);
  return { sent: true };
}
async function saveLastStatus(message) {
  await db.ref(CONFIG_PATH).update({ sorteioAutomaticoUltimoResultado: message, sorteioAutomaticoUltimaVerificacao: new Date().toISOString() });
}
async function publishSlot(scheduledTimestamp) {
  const dt = scheduledDateTime(scheduledTimestamp);
  const number = crypto.randomInt(1, 51);
  const team = times[number - 1];
  const key = dt.horario.replace(':', '-');
  const resultPath = `${RESULTS_PATH}/${dt.data}/${key}`;
  const result = { data: dt.data, horario: dt.horario, numero: number, time: team, publicadoEm: new Date().toISOString(), timestamp: Date.now(), automatico: true };
  const transaction = await db.ref(resultPath).transaction(current => current !== null ? undefined : result, undefined, false);
  if (transaction.committed) {
    const message = `TIME CERTO - RESULTADO AUTOMÁTICO\nData: ${dt.data}\nHorário: ${dt.horario}\nNúmero: ${String(number).padStart(2, '0')}\nTime: ${team}`;
    log('Resultado publicado.', { data: dt.data, horario: dt.horario, numero: number, time: team });
    lastDraw = { ...result, chave: key };
    try { await sendTelegram(message); log('Aviso enviado ao Telegram.'); }
    catch (error) { log('Falha ao enviar aviso ao Telegram.', { error: error.message }); }
    await saveLastStatus(`Publicado: ${dt.data} às ${dt.horario} — ${number} ${team}`);
    return true;
  }
  const message = `Horário já tinha resultado: ${dt.data} às ${dt.horario}`;
  log(message);
  await saveLastStatus(message);
  return false;
}
async function advanceSchedule(expectedTimestamp) {
  const expected = Number(expectedTimestamp);
  const target = expected + INTERVAL_MS;
  const ref = db.ref(`${CONFIG_PATH}/sorteioAutomaticoProximo`);
  const transaction = await ref.transaction(current => {
    if (current === null) return target;
    const value = Number(current);
    if (!Number.isFinite(value) || value <= expected) return target;
    return value;
  }, undefined, false);
  if (transaction.committed) return true;
  const snapshot = await ref.once('value');
  return Number(snapshot.val() || 0) >= target;
}
async function runSchedulerCycle() {
  if (!db || tickRunning) return;
  tickRunning = true;
  lastCheck = new Date().toISOString();
  lastError = null;
  try {
    let config = await readConfig();
    automaticEnabled = config.sorteioAutomatico === true;
    if (!automaticEnabled) return;
    let next = await ensureNextSlot();
    const now = Date.now();
    const atrasoMaximo = 60_000;
    if (next < now - atrasoMaximo) {
      const novoHorario = nextSlotFromNow(now);
      const scheduleRef = db.ref(`${CONFIG_PATH}/sorteioAutomaticoProximo`);
      const ajuste = await scheduleRef.transaction(current => {
        if (current === null || current === undefined) return novoHorario;
        const value = Number(current);
        if (!Number.isFinite(value) || value <= Date.now() - atrasoMaximo) return novoHorario;
        return current;
      }, undefined, false);
      const atualizado = await scheduleRef.once('value');
      next = Number(atualizado.val() || 0);
      if (!ajuste.committed && next < Date.now() - atrasoMaximo) throw new Error('Não foi possível ajustar os horários atrasados.');
      if (ajuste.committed) log('Horários antigos ignorados. Próximo sorteio agendado.', { proximo: formatSP(next) });
    }
    let processed = 0;
    while (next <= Date.now() && processed < MAX_CATCHUP_PER_CYCLE) {
      config = await readConfig();
      automaticEnabled = config.sorteioAutomatico === true;
      if (!automaticEnabled) break;
      const currentSnapshot = await db.ref(`${CONFIG_PATH}/sorteioAutomaticoProximo`).once('value');
      const currentNext = Number(currentSnapshot.val() || 0);
      if (!currentNext || currentNext > Date.now()) break;
      await publishSlot(currentNext);
      const advanced = await advanceSchedule(currentNext);
      processed++;
      const after = await db.ref(`${CONFIG_PATH}/sorteioAutomaticoProximo`).once('value');
      next = Number(after.val() || 0);
      if (!advanced && next <= currentNext) throw new Error('Não foi possível avançar o próximo horário do sorteio.');
      if (!next) break;
    }
    if (processed > 0) log('Ciclo concluído.', { processados: processed, proximo: next ? formatSP(next) : null });
  } catch (error) {
    lastError = error.message;
    log('Erro no ciclo automático.', { error: error.stack || error.message });
  } finally { tickRunning = false; }
}
function startHealthServer() {
  const server = http.createServer(async (req, res) => {
    const rota = req.url.split('?')[0];
    if (req.method === 'POST' && rota === '/internal/tunnel-url') {
      try {
        const segredo = process.env.TUNNEL_UPDATE_SECRET;
        const enviado = req.headers['x-tunnel-secret'];
        if (!segredo || enviado !== segredo) {
          res.writeHead(403, { 'content-type': 'application/json; charset=utf-8' });
          res.end(JSON.stringify({ erro: 'Acesso negado.' })); return;
        }
        let corpo = '';
        for await (const parte of req) {
          corpo += parte.toString();
          if (corpo.length > 4096) { res.writeHead(413); res.end('Conteúdo muito grande.'); return; }
        }
        const dados = JSON.parse(corpo);
        const urlRecebida = String(dados.url || '').trim();
        const url = new URL(urlRecebida);
        if (url.protocol !== 'https:' || !url.hostname.endsWith('.trycloudflare.com')) {
          res.writeHead(400, { 'content-type': 'application/json; charset=utf-8' });
          res.end(JSON.stringify({ erro: 'URL Cloudflare inválida.' })); return;
        }
        await db.ref('config/mercadoPagoApiUrl').set(url.origin);
        log('URL dinâmica do Cloudflare publicada no Firebase.', { endereco: url.origin });
        res.writeHead(200, { 'content-type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ sucesso: true, url: url.origin }));
      } catch (error) {
        log('Erro ao publicar URL do túnel.', { erro: error.message });
        res.writeHead(500, { 'content-type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ erro: 'Não foi possível publicar a URL.' }));
      }
      return;
    }
    if (rota === '/timecertotop' || rota === '/timecertotop/' || rota === '/timecertotop/index.html') {
      try {
        const html = fs.readFileSync(path.join(__dirname, 'timecertotop', 'index.html'));
        res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' }); res.end(html);
      } catch (error) {
        res.writeHead(500, { 'content-type': 'text/plain; charset=utf-8' }); res.end('Arquivo timecertotop/index.html não foi incluído no pacote.');
      }
      return;
    }
    if (rota === '/health' || rota === '/') {
      const body = JSON.stringify({ app: 'TIME CERTO - Scheduler', status: lastError ? 'warning' : 'online', automaticoAtivo: automaticEnabled, ultimoCheck: lastCheck, ultimoSorteio: lastDraw, erro: lastError }, null, 2);
      res.writeHead(lastError ? 503 : 200, { 'content-type': 'application/json; charset=utf-8' }); res.end(body); return;
    }
    res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' }); res.end('Not found');
  });
  server.listen(PORT, '0.0.0.0', () => log(`Servidor HTTP de saúde ativo na porta ${PORT}.`));
  return server;
}
async function main() {
  initFirebase();
  const server = startHealthServer();
  await runSchedulerCycle();
  setInterval(runSchedulerCycle, POLL_MS);
  const shutdown = signal => {
    log(`Recebido ${signal}. Encerrando com segurança...`);
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(1), 10000).unref();
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}
main().catch(error => { console.error('Falha ao iniciar TIME CERTO:', error); process.exit(1); });
