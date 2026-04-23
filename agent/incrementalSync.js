const sql = require('mssql');
const config = require('./config');
const state = require('./state');
const sender = require('./sender');
const setup = require('./setup');

class IncrementalSync {

  // ───────────────────────────────────────────
  // فاز 6: Incremental Sync Loop
  // ───────────────────────────────────────────
  async syncTable(pool, dbName, table) {
    const stateKey = `${table.schema}.${table.name}`;
    const fullName = `[${dbName}].[${table.schema}].[${table.name}]`;

    try {
      // 6.2.1: last_version رو بخون
      const lastVersion = state.getLastVersion(dbName, stateKey);
      if (lastVersion === null) {
        // هنوز initial sync نشده
        return { changes: 0, skipped: true };
      }

      // ───────────────────────────────────────
      // چک valid بودن version
      // ───────────────────────────────────────
      const minValid = await setup.getMinValidVersion(
        pool, dbName, table.schema, table.name
      );

      if (lastVersion < minValid) {
        console.log(`    ⚠️  ${stateKey}: version expired! Need re-sync`);
        // version منقضی شده → باید دوباره initial sync بشه
        state.updateVersion(dbName, stateKey, 0, null);
        return { changes: 0, needResync: true };
      }

      // ───────────────────────────────────────
      // فاز 12.2: چک تغییر schema
      // ───────────────────────────────────────
      const savedHash = state.getSchemaHash(dbName, stateKey);
      if (savedHash && savedHash !== table.schemaHash) {
        console.log(`    🔄 Schema changed for ${stateKey}, sending new schema`);
        await sender.sendSchema(
          dbName, stateKey, table.columns, table.primaryKeys
        );
        state.updateVersion(dbName, stateKey, lastVersion, table.schemaHash);
      }

      // ───────────────────────────────────────
      // 6.2.2: query تغییرات با CHANGETABLE
      // ───────────────────────────────────────
      const pkJoin = table.primaryKeys
        .map(pk => `ct.[${pk}] = t.[${pk}]`)
        .join(' AND ');

      const pkSelect = table.primaryKeys
        .map(pk => `ct.[${pk}]`)
        .join(', ');

      // ───────────────────────────────────────
      // 6.2.3: join برای گرفتن دیتا
      // ───────────────────────────────────────
      const changesQuery = `
        SELECT TOP (${config.sync.maxChangesPerRound})
          ct.SYS_CHANGE_OPERATION as _operation,
          ct.SYS_CHANGE_VERSION as _version,
          ${pkSelect},
          t.*
        FROM CHANGETABLE(CHANGES [${dbName}].[${table.schema}].[${table.name}], ${lastVersion}) ct
        LEFT JOIN ${fullName} t ON ${pkJoin}
        ORDER BY ct.SYS_CHANGE_VERSION
      `;

      const result = await pool.request().query(changesQuery);
      const changes = result.recordset;

      if (changes.length === 0) {
        return { changes: 0 };
      }

      // ───────────────────────────────────────
      // 6.2.4: پردازش و گروه‌بندی
      // ───────────────────────────────────────
      const batch = [];
      let maxVersion = lastVersion;

      for (const row of changes) {
        const operation = row._operation; // I, U, D
        const version = row._version;

        if (version > maxVersion) {
          maxVersion = version;
        }

        // حذف فیلدهای سیستمی
        const cleanRow = { ...row };
        delete cleanRow._operation;
        delete cleanRow._version;

        // ───────────────────────────────────
        // فاز 6.3: mapping عملیات‌ها
        // ───────────────────────────────────
        if (operation === 'D') {
          // 12.3: برای delete فقط PK
          const pk = {};
          for (const pkCol of table.primaryKeys) {
            pk[pkCol] = row[pkCol];
          }
          batch.push({
            operation: 'D',
            pk,
            data: null
          });
        } else {
          // I یا U → فرستادن کل دیتا
          const pk = {};
          for (const pkCol of table.primaryKeys) {
            pk[pkCol] = cleanRow[pkCol];
          }
          batch.push({
            operation: operation === 'I' ? 'I' : 'U',
            pk,
            data: this._serializeRow(cleanRow)
          });
        }
      }

      // ───────────────────────────────────────
      // فاز 8.2: batch ارسال (نه row by row)
      // ───────────────────────────────────────
      const batches = this._chunk(batch, config.sync.batchSize);

      for (const chunk of batches) {
        const sendResult = await sender.sendBatch(
          dbName,
          stateKey,
          'changes',
          chunk,
          maxVersion
        );

        // ───────────────────────────────────
        // فاز 10.1: اگر fail شد → state update نکن
        // ───────────────────────────────────
        if (!sendResult.success) {
          console.error(`    ❌ Send failed for ${stateKey}: ${sendResult.error}`);
          state.log(dbName, stateKey, 'incremental', 0, false, sendResult.error);
          return { changes: 0, error: sendResult.error };
        }
      }

      // ✅ موفق → update state
      state.updateVersion(dbName, stateKey, maxVersion);
      state.log(dbName, stateKey, 'incremental', changes.length, true);

      if (changes.length > 0) {
        console.log(`    📤 ${stateKey}: ${changes.length} changes synced (v${maxVersion})`);
      }

      return { changes: changes.length, version: maxVersion };

    } catch (err) {
      console.error(`    ❌ Incremental sync error for ${stateKey}:`, err.message);
      state.log(dbName, stateKey, 'incremental', 0, false, err.message);
      return { changes: 0, error: err.message };
    }
  }

  // ───────────────────────────────────────────
  // sync یک دیتابیس کامل
  // ───────────────────────────────────────────
  async syncDatabase(pool, dbName, tables) {
    let totalChanges = 0;
    const needResync = [];

    for (const table of tables) {
      const result = await this.syncTable(pool, dbName, table);
      totalChanges += result.changes;

      if (result.needResync) {
        needResync.push(table);
      }
    }

    return { totalChanges, needResync };
  }

  // ───────────────────────────────────────────
  // Utilities
  // ───────────────────────────────────────────
  _serializeRow(row) {
    const serialized = {};
    for (const [key, value] of Object.entries(row)) {
      if (value instanceof Date) {
        serialized[key] = value.toISOString();
      } else if (Buffer.isBuffer(value)) {
        serialized[key] = value.toString('base64');
      } else {
        serialized[key] = value;
      }
    }
    return serialized;
  }

  _chunk(array, size) {
    const chunks = [];
    for (let i = 0; i < array.length; i += size) {
      chunks.push(array.slice(i, i + size));
    }
    return chunks;
  }
}

module.exports = new IncrementalSync();
