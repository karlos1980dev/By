'use strict';

require('dotenv').config();

const http = require('node:http');
const crypto = require('node:crypto');
const { initializeApp, cert, applicationDefault, deleteApp } = require('firebase-admin/app');
const { getDatabase } = require('firebase-admin/database');

// ============================================================
// TIME CERTO - MOTOR DE SORTEIOS 24H
// Mantém os caminhos existentes do Realtime Database:
//   palpites
//   timeCerto/configuracao
//   timeCerto/resultados/{AAAA-MM-DD}/{HH-mm}
// ============================================================

const PORT = Number(process.env.PORT || 3000);
const DATABASE_URL = process.env.FIREBASE_DATABASE_URL ||
  'https://placarexato-97966-default-rtdb.asia-southeast1.firebasedatabase.app';
const PROJECT_ID = process.env.FIREBASE_PROJECT_ID || 'placarexato-97966';
const TIME_ZONE = 'America/Sao_Paulo';
const INTERVAL_MS = 30 * 60 * 1000;
const POLL_MS = Math.max(2000, Number(process.env.POLL_MS || 5000));
const MAX_CATCHUP_PER_CYCLE = 2;
const CONFIG_PATH = 'timeCerto/configuracao';
const RESULTS_PATH = 'timeCerto/resultados';

const times = [
  'ATHLETICO PR','ATLÉTICO MG','BAHIA','BOTAFOGO',
  'RB BRAGANTINO','CHAPECOENSE','CORINTHIANS',
  'CORITIBA','CRUZEIRO','FLAMENGO','FLUMINENSE',
  'GRÊMIO','INTERNACIONAL','MIRASSOL','PALMEIRAS',
  'REMO','SANTOS','SÃO PAULO','VASCO','VITÓRIA',
  'AMÉRICA MG','ATHLETIC MG','ATLÉTICO GO','AVAÍ',
  'BOTAFOGO SP','CEARÁ','CRB','CRICIÚMA','CUIABÁ',
  'FORTALEZA','GOIÁS','JUVENTUDE','LONDRINA',
  'NÁUTICO','NOVORIZONTINO','OPERÁRIO PR',
  'PONTE PRETA','SÃO BERNARDO','SPORT','VILA NOVA',
  'ARSENAL','CHELSEA','LIVERPOOL','MANCHESTER UNITED',
  'ATLÉTICO DE MADRID','BARCELONA','REAL MADRID',
  'INTERNAZIONALE','JUVENTUS','PSG'
];

let db;
let firebaseApp;
let tickRunning = false;
let lastCheck = null;
let lastDraw = null;
let lastError = null;
let automaticEnabled = false;

function log(message, extra) {
  const suffix = extra ? ` ${JSON.stringify(extra)}` : '';
  console.log(`[${new Date().toISOString()}] ${message}${suffix}`);
}

function initFirebase() {
  const rawJson = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
  const rawBase64 = process.env.FIREBASE_SERVICE_ACCOUNT_BASE64;
  let credential;

  if (rawJson && rawJson.trim()) {
    credential = cert(JSON.parse(rawJson));
  } else if (rawBase64 && rawBase64.trim()) {
    const decoded = Buffer.from(rawBase64.trim(), 'base64').toString('utf8');
    credential = cert(JSON.parse(decoded));
  } else if (process.env.GOOGLE_APPLICATION_CREDENTIALS) {
    credential = applicationDefault();
  } else {
    throw new Error(
      'Credenciais ausentes. Configure FIREBASE_SERVICE_ACCOUNT_JSON, ' +
      'FIREBASE_SERVICE_ACCOUNT_BASE64 ou GOOGLE_APPLICATION_CREDENTIALS.'
    );
  }

  firebaseApp = initializeApp({
    credential,
    databaseURL: DATABASE_URL,
    projectId: PROJECT_ID
  });
  db = getDatabase(firebaseApp);
  log('Firebase Admin conectado.', { projectId: PROJECT_ID, databaseURL: DATABASE_URL });
}

function spParts(timestamp) {
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone: TIME_ZONE,
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hourCycle: 'h23'
  }).formatToParts(new Date(timestamp));
  const result = {};
  for (const item of parts) result[item.type] = item.value;
  return result;
}

function scheduledDateTime(timestamp) {
  const p = spParts(timestamp);
  return {
    data: `${p.year}-${p.month}-${p.day}`,
    horario: `${p.hour}:${p.minute}`
  };
}

// Calcula o próximo horário cheio de 00 ou 30 minutos em Brasília.
function nextSlotFromNow(now = Date.now()) {
  const p = spParts(now);
  const minute = Number(p.minute);
  const second = Number(p.second);
  const milliseconds = new Date(now).getMilliseconds();
  const minutesToNext = 30 - (minute % 30);
  const wait = minutesToNext * 60_000 - second * 1000 - milliseconds;
  return now + wait;
}

function formatSP(timestamp) {
  return new Intl.DateTimeFormat('pt-BR', {
    timeZone: TIME_ZONE,
    dateStyle: 'short',
    timeStyle: 'short'
  }).format(new Date(timestamp));
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

async function sendTelegram(text) {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;
  if (!token || !chatId) return { skipped: true };

  const url = `https://api.telegram.org/bot${token}/sendMessage`;
  const response = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ chat_id: chatId, text }),
    signal: AbortSignal.timeout(10000)
  });

  const data = await response.json().catch(() => ({}));
  if (!response.ok || data.ok !== true) {
    throw new Error(`Telegram não confirmou a mensagem: ${data.description || response.status}`);
  }
  return { sent: true };
}

async function saveLastStatus(message) {
  const now = new Date().toISOString();
  await db.ref(CONFIG_PATH).update({
    sorteioAutomaticoUltimoResultado: message,
    sorteioAutomaticoUltimaVerificacao: now
  });
}

async function publishSlot(scheduledTimestamp) {
  const dt = scheduledDateTime(scheduledTimestamp);
  const number = crypto.randomInt(1, 51);
  const team = times[number - 1];
  const key = dt.horario.replace(':', '-');
  const path = `${RESULTS_PATH}/${dt.data}/${key}`;
  const result = {
    data: dt.data,
    horario: dt.horario,
    numero: number,
    time: team,
    publicadoEm: new Date().toISOString(),
    timestamp: Date.now(),
    automatico: true
  };

  // A transação impede que dois servidores publiquem resultados diferentes
  // para o mesmo dia e horário.
  const transaction = await db.ref(path).transaction(current => {
    if (current !== null) return;
    return result;
  }, undefined, false);

  if (transaction.committed) {
    const message = `TIME CERTO - RESULTADO AUTOMÁTICO\nData: ${dt.data}\nHorário: ${dt.horario}\nNúmero: ${String(number).padStart(2, '0')}\nTime: ${team}`;
    log('Resultado publicado.', { data: dt.data, horario: dt.horario, numero: number, time: team });
    lastDraw = { ...result, chave: key };

    try {
      await sendTelegram(message);
      log('Aviso enviado ao Telegram.');
    } catch (error) {
      // Falha no Telegram não desfaz nem repete o resultado publicado.
      log('Falha ao enviar aviso ao Telegram.', { error: error.message });
    }

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

    // Avança o horário antigo, mas preserva um horário
    // mais recente caso outra instância já tenha avançado.
    if (!Number.isFinite(value) || value <= expected) {
      return target;
    }

    return value;
  }, undefined, false);

  if (transaction.committed) return true;

  const snapshot = await ref.once('value');
  const actual = Number(snapshot.val() || 0);

  return actual >= target;
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

    // Evita publicar sorteios antigos após longos períodos offline.
    const now = Date.now();
    const atrasoMaximo = 60 * 60_000;

    if (next < now - atrasoMaximo) {
      const novoHorario = nextSlotFromNow(now);
      const scheduleRef = db.ref(`${CONFIG_PATH}/sorteioAutomaticoProximo`);

      const ajuste = await scheduleRef.transaction(current => {
        // O Firebase pode iniciar a transação com valor nulo.
        if (current === null || current === undefined) {
          return novoHorario;
        }

        const value = Number(current);

        // Se o horário continua atrasado, agenda o próximo horário futuro.
        if (!Number.isFinite(value) || value <= Date.now() - atrasoMaximo) {
          return novoHorario;
        }

        // Preserva um horário futuro já atualizado por outra instância.
        return current;
      }, undefined, false);

      const atualizado = await scheduleRef.once('value');
      next = Number(atualizado.val() || 0);

      if (!ajuste.committed && next < Date.now() - atrasoMaximo) {
        throw new Error('Não foi possível ajustar os horários atrasados.');
      }

      if (ajuste.committed) {
        log('Horários antigos ignorados. Próximo sorteio agendado.', {
          proximo: formatSP(next)
        });
      }
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

      // Outra instância pode ter avançado o horário. A próxima leitura
      // evita processar novamente um horário antigo.
      if (!advanced && next <= currentNext) {
        throw new Error('Não foi possível avançar o próximo horário do sorteio.');
      }
      if (!next) break;
    }

    if (processed > 0) {
      log('Ciclo concluído.', { processados: processed, proximo: next ? formatSP(next) : null });
    }
  } catch (error) {
    lastError = error.message;
    log('Erro no ciclo automático.', { error: error.stack || error.message });
  } finally {
    tickRunning = false;
  }
}

function startHealthServer() {
  const server = http.createServer((req, res) => {
    if (req.url === '/health' || req.url === '/') {
      const body = JSON.stringify({
        app: 'TIME CERTO - Scheduler',
        status: lastError ? 'warning' : 'online',
        automaticoAtivo: automaticEnabled,
        ultimoCheck: lastCheck,
        ultimoSorteio: lastDraw,
        erro: lastError
      }, null, 2);
      res.writeHead(lastError ? 503 : 200, { 'content-type': 'application/json; charset=utf-8' });
      res.end(body);
      return;
    }
    res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
    res.end('Not found');
  });

  server.listen(PORT, '0.0.0.0', () => {
    log(`Servidor HTTP de saúde ativo na porta ${PORT}.`);
  });
  return server;
}

async function main() {
  initFirebase();

  log('Execução programada do GitHub Actions iniciada.');

  await runSchedulerCycle();

  if (lastError) {
    throw new Error(lastError);
  }

  log('Execução programada concluída.', {
    automaticoAtivo: automaticEnabled,
    ultimoCheck: lastCheck,
    ultimoSorteio: lastDraw
  });
}

main()
  .catch(error => {
    console.error('Falha ao executar TIME CERTO:', error);
    process.exitCode = 1;
  })
  .finally(async () => {
    if (firebaseApp) {
      try {
        await deleteApp(firebaseApp);
      } catch (error) {
        console.error('Falha ao encerrar Firebase:', error.message);
        process.exitCode = 1;
      }
    }
  });
