const axios = require('axios');
const config = require('./config');

class Sender {
  constructor() {
    this.client = axios.create({
      baseURL: config.serverUrl,
      timeout: config.sync.httpTimeoutMs,
      headers: {
        'Content-Type': 'application/json'
      }
    });
  }

  // ───────────────────────────────────────────
  // فاز 8: ارسال با retry
  // ───────────────────────────────────────────
  async send(payload) {
    let lastError;

    for (let attempt = 1; attempt <= config.sync.maxRetries; attempt++) {
      try {
        const response = await this.client.post('', payload);

        if (response.status === 200) {
          return { success: true, data: response.data };
        }

        lastError = new Error(`Server returned status ${response.status}`);
      } catch (err) {
        lastError = err;

        if (attempt < config.sync.maxRetries) {
          const waitMs = attempt * 1000; // backoff ساده
          console.log(`    ⏳ Retry ${attempt}/${config.sync.maxRetries} in ${waitMs}ms...`);
          await this._sleep(waitMs);
        }
      }
    }

    return {
      success: false,
      error: lastError?.message || 'Unknown error'
    };
  }

  // ───────────────────────────────────────────
  // ارسال batch
  // ───────────────────────────────────────────
  async sendBatch(sourceDb, tableName, operation, batch, version, schema = null) {
    const payload = {
      source_db: sourceDb,
      table: tableName,
      operation,          // 'initial', 'changes', 'schema'
      batch,
      version,
      timestamp: new Date().toISOString()
    };

    // اگر schema ارسال شد (برای initial یا تغییر schema)
    if (schema) {
      payload.schema = schema;
    }

    return this.send(payload);
  }

  // ───────────────────────────────────────────
  // ارسال schema
  // ───────────────────────────────────────────
  async sendSchema(sourceDb, tableName, columns, primaryKeys) {
    const payload = {
      source_db: sourceDb,
      table: tableName,
      operation: 'schema',
      schema: {
        columns,
        primaryKeys
      },
      timestamp: new Date().toISOString()
    };

    return this.send(payload);
  }

  _sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

module.exports = new Sender();
