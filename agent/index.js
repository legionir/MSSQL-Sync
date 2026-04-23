const sql = require('mssql');
const config = require('./config');
const discovery = require('./discovery');
const setup = require('./setup');
const initialSync = require('./initialSync');
const incrementalSync = require('./incrementalSync');
const state = require('./state');

class SyncAgent {
  constructor() {
    this.pool = null;
    this.running = false;
    this.discoveryCache = {};  // cache شده تا هر بار discover نکنه
    this.rediscoverInterval = 60; // هر 60 دور، دوباره discover کن
    this.roundCount = 0;
  }

  // ───────────────────────────────────────────
  // شروع agent
  // ───────────────────────────────────────────
  async start() {
    console.log('🚀 Sync Agent Starting...\n');

    try {
      // اتصال به MSSQL
      this.pool = await sql.connect(config.mssql);
      console.log('✅ Connected to MSSQL\n');

      // اولین discovery + setup + initial sync
      await this._fullDiscoveryAndSetup();

      // شروع loop
      this.running = true;
      this._startLoop();

      // handle خروج
      process.on('SIGINT', () => this.stop());
      process.on('SIGTERM', () => this.stop());

    } catch (err) {
      console.error('❌ Failed to start:', err.message);
      process.exit(1);
    }
  }

  // ───────────────────────────────────────────
  // Discovery + Setup + Initial Sync
  // ───────────────────────────────────────────
  async _fullDiscoveryAndSetup() {
    // فاز 3: Auto Discovery
    const allDbs = await discovery.discoverAll(this.pool);

    for (const [dbName, tables] of Object.entries(allDbs)) {
      if (tables.length === 0) continue;

      // فاز 4: Setup Change Tracking
      const enabledTables = await setup.setupDatabase(this.pool, dbName, tables);

      // فاز 5: Initial Sync
      await initialSync.syncDatabase(this.pool, dbName, enabledTables);

      // cache کن
      this.discoveryCache[dbName] = enabledTables;
    }

    console.log('\n✅ Initial setup complete!\n');
    console.log('═'.repeat(50));
    console.log('🔄 Starting incremental sync loop...');
    console.log('═'.repeat(50));
  }

  // ───────────────────────────────────────────
  // فاز 6.1: Sync Loop
  // ───────────────────────────────────────────
  _startLoop() {
    const loop = async () => {
      if (!this.running) return;

      try {
        this.roundCount++;

        // هر N دور، دوباره discover کن (برای DB/table جدید)
        if (this.roundCount % this.rediscoverInterval === 0) {
          console.log('\n🔍 Re-discovering databases...');
          await this._fullDiscoveryAndSetup();
        }

        // ───────────────────────────────────
        // فاز 11.1: concurrency کنترل‌شده
        // ───────────────────────────────────
        const dbNames = Object.keys(this.discoveryCache);
        const chunks = this._chunkArray(dbNames, config.sync.maxConcurrentDbs);

        for (const dbChunk of chunks) {
          await Promise.all(
            dbChunk.map(dbName => this._syncOneDatabase(dbName))
          );
        }

      } catch (err) {
        console.error('❌ Loop error:', err.message);
      }

      // schedule next round
      if (this.running) {
        setTimeout(loop, config.sync.intervalMs);
      }
    };

    loop();
  }

  // ───────────────────────────────────────────
  // Sync یک دیتابیس
  // ───────────────────────────────────────────
  async _syncOneDatabase(dbName) {
    const tables = this.discoveryCache[dbName];
    if (!tables || tables.length === 0) return;

    const result = await incrementalSync.syncDatabase(this.pool, dbName, tables);

    // اگر جدولی نیاز به resync دارد
    if (result.needResync.length > 0) {
      console.log(`\n⚠️  Re-syncing ${result.needResync.length} tables in [${dbName}]`);
      await initialSync.syncDatabase(this.pool, dbName, result.needResync);
    }
  }

  // ───────────────────────────────────────────
  // توقف agent
  // ───────────────────────────────────────────
  async stop() {
    console.log('\n\n🛑 Stopping Sync Agent...');
    this.running = false;

    if (this.pool) {
      await this.pool.close();
    }
    state.close();

    console.log('✅ Agent stopped gracefully');
    process.exit(0);
  }

  _chunkArray(arr, size) {
    const chunks = [];
    for (let i = 0; i < arr.length; i += size) {
      chunks.push(arr.slice(i, i + size));
    }
    return chunks;
  }
}

// ───────────────────────────────────────────
// اجرا
// ───────────────────────────────────────────
const agent = new SyncAgent();
agent.start();
