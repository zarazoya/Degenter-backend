// lib/pg_notify.js
import { DB } from './db.js';
import { info, warn } from './log.js';

// allow only simple channel names (identifiers) to avoid injection via identifier context
const CHAN_RX = /^[a-z_][a-z0-9_]*$/i;

/**
 * Send a Postgres NOTIFY <channel> with JSON payload (safe & parametric).
 */
export async function pgNotify(channel, payload) {
  try {
    if (!CHAN_RX.test(channel)) throw new Error(`invalid channel: ${channel}`);
    // Use the function form to keep both channel and payload parameterized.
    const json = JSON.stringify(payload ?? {});
    await DB.query('SELECT pg_notify($1, $2)', [channel, json]);
    // optional: uncomment if you want to see every notify
    // info('[pgNotify] sent', channel, json);
  } catch (e) {
    warn('[pgNotify]', channel, e.message);
  }
}

/**
 * Listen on a NOTIFY channel. Returns a connected client you should keep alive.
 * If the connection drops, it auto-retries.
 */
export async function pgListen(channel, onMessage) {
  if (!CHAN_RX.test(channel)) throw new Error(`invalid channel: ${channel}`);

  async function connectAndListen() {
    if (!DB.isInitialized) {
      await DB.initialize();
    }
    // grab a native pg client from TypeORM's query runner
    const runner = DB.createQueryRunner();
    await runner.connect();
    const client = runner.databaseConnection; // native pg client

    client.on('notification', (msg) => {
      if (msg.channel !== channel) return;
      try {
        const data = msg.payload ? JSON.parse(msg.payload) : null;
        onMessage?.(data);
      } catch (e) {
        warn('[pgListen] payload parse', e.message);
      }
    });
    // Identifiers cannot be parameterized; we validated the channel above.
    await client.query(`LISTEN ${channel}`);
    info('[pgListen] listening on', channel);
    return { runner, client };
  }

  let { runner, client } = await connectAndListen();
  let reconnecting = false;

  const reconnect = async (reason) => {
    if (reconnecting) return;
    reconnecting = true;
    warn('[pgListen] reconnecting…', reason);
    try { await runner.release(); } catch {}
    ({ runner, client } = await connectAndListen());
    reconnecting = false;
  };

  client.on('error', (e) => reconnect(e.message).catch(() => {}));
  client.on('end', () => reconnect('end').catch(() => {}));

  // Keep-alive / auto-reconnect loop
  (async function keepAlive() {
    while (true) {
      try {
        await runner.query('SELECT 1'); // ping via query runner
      } catch (e) {
        await reconnect(e.message);
      }
      await new Promise(r => setTimeout(r, 10_000));
    }
  })().catch(()=>{});

  return client;
}
