const Database = require('better-sqlite3');
const config = require('./config');

class StateManager {
  constructor() {
    this.db = new Database(config.statePath);
    this._initialize();
  }

  _initialize() {
    this.db.pragma('journal_mode = WAL');

    this.db.exec(`
      CREATE TABLE IF NOT EXISTS sync_state (
        source_db TEXT NOT NULL,
        table_name TEXT NOT NULL,
        last_version INTEGER NOT NULL DEFAULT 0,
        last_sync_at TEXT,
        schema_hash TEXT,
        status TEXT DEFAULT 'active',
        PRIMARY KEY (source_db, table_name)
      )
    `);

    this.db.exec(`
      CREATE TABLE IF NOT EXISTS sync_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_db TEXT NOT NULL,
        table_name TEXT NOT NULL,
        operation TEXT NOT NULL,
        record_count INTEGER DEFAULT 0,
        success INTEGER DEFAULT 1,
        error_message TEXT,
        created_at TEXT DEFAULT (datetime('now'))
      )
    `);

    // Prepared statements برای performance
    this._stmtGet = this.db.prepare(
      'SELECT * FROM sync_state WHERE source_db = ? AND table_name = ?'
    );

    this._stmtUpsert = this.db.prepare(`
      INSERT INTO sync_state (source_db, table_name, last_version, last_sync_at, schema_hash)
      VALUES (?, ?, ?, datetime('now'), ?)
      ON CONFLICT(source_db, table_name)
      DO UPDATE SET
        last_version = excluded.last_version,
        last_sync_at = datetime('now'),
        schema_hash = COALESCE(excluded.schema_hash, schema_hash)
    `);

    this._stmtLog = this.db.prepare(`
      INSERT INTO sync_log (source_db, table_name, operation, record_count, success, error_message)
      VALUES (?, ?, ?, ?, ?, ?)
    `);
  }

  getState(sourceDb, tableName) {
    return this._stmtGet.get(sourceDb, tableName);
  }

  getLastVersion(sourceDb, tableName) {
    const row = this._stmtGet.get(sourceDb, tableName);
    return row ? row.last_version : null;
  }

  updateVersion(sourceDb, tableName, version, schemaHash = null) {
    this._stmtUpsert.run(sourceDb, tableName, version, schemaHash);
  }

  // batch update برای performance
  updateVersionBatch(updates) {
    const transaction = this.db.transaction((items) => {
      for (const item of items) {
        this._stmtUpsert.run(
          item.sourceDb,
          item.tableName,
          item.version,
          item.schemaHash || null
        );
      }
    });
    transaction(updates);
  }

  log(sourceDb, tableName, operation, recordCount, success = true, errorMessage = null) {
    this._stmtLog.run(
      sourceDb, tableName, operation, recordCount,
      success ? 1 : 0, errorMessage
    );
  }

  getAllStates() {
    return this.db.prepare('SELECT * FROM sync_state ORDER BY source_db, table_name').all();
  }

  getSchemaHash(sourceDb, tableName) {
    const row = this._stmtGet.get(sourceDb, tableName);
    return row ? row.schema_hash : null;
  }

  close() {
    this.db.close();
  }
}

module.exports = new StateManager();
