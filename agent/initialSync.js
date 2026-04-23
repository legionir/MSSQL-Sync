const sql = require('mssql');
const config = require('./config');
const state = require('./state');
const sender = require('./sender');
const setup = require('./setup');

class InitialSync {

  // ───────────────────────────────────────────
  // فاز 5: Initial Sync کامل
  // ───────────────────────────────────────────
  async syncTable(pool, dbName, table) {
    const fullName = `[${dbName}].[${table.schema}].[${table.name}]`;
    const stateKey = `${table.schema}.${table.name}`;

    // چک کن آیا قبلاً sync شده
    const existing = state.getState(dbName, stateKey);
    if (existing && existing.last_version > 0) {
      console.log(`    ℹ️  ${stateKey} already synced (version: ${existing.last_version})`);
      return true;
    }

    console.log(`    🔄 Initial sync: ${fullName}`);

    try {
      // اول schema بفرست
      const schemaResult = await sender.sendSchema(
        dbName, stateKey, table.columns, table.primaryKeys
      );
      if (!schemaResult.success) {
        throw new Error(`Schema send failed: ${schemaResult.error}`);
      }

      // شمارش رکوردها
      const countResult = await pool.request().query(
        `SELECT COUNT(*) as total FROM ${fullName}`
      );
      const totalRows = countResult.recordset[0].total;
      console.log(`    📊 Total rows: ${totalRows}`);

      if (totalRows === 0) {
        // جدول خالی، فقط version رو ذخیره کن
        const version = await setup.getCurrentVersion(pool, dbName);
        state.updateVersion(dbName, stateKey, version, table.schemaHash);
        state.log(dbName, stateKey, 'initial', 0, true);
        return true;
      }

      // ───────────────────────────────────────
      // فاز 5.1: batch خوندن و ارسال
      // ───────────────────────────────────────
      const pkColumns = table.primaryKeys.map(pk => `t.[${pk}]`).join(', ');
      const batchSize = config.sync.batchSize;
      let offset = 0;
      let sentCount = 0;

      while (offset < totalRows) {
        const dataResult = await pool.request().query(`
          SELECT *
          FROM ${fullName} t
          ORDER BY ${pkColumns}
          OFFSET ${offset} ROWS
          FETCH NEXT ${batchSize} ROWS ONLY
        `);

        const rows = dataResult.recordset;
        if (rows.length === 0) break;

        // ارسال batch
        const result = await sender.sendBatch(
          dbName,
          stateKey,
          'initial',
          rows.map(row => ({
            operation: 'I',
            data: this._serializeRow(row)
          })),
          null, // version بعداً ست میشه
          { columns: table.columns, primaryKeys: table.primaryKeys }
        );

        if (!result.success) {
          throw new Error(`Batch send failed at offset ${offset}: ${result.error}`);
        }

        sentCount += rows.length;
        offset += batchSize;

        const percent = Math.min(100, Math.round((sentCount / totalRows) * 100));
        process.stdout.write(`\r    📤 Progress: ${sentCount}/${totalRows} (${percent}%)`);
      }

      console.log(''); // new line after progress

      // ───────────────────────────────────────
      // فاز 5.2: ذخیره version
      // ───────────────────────────────────────
      const version = await setup.getCurrentVersion(pool, dbName);
      state.updateVersion(dbName, stateKey, version, table.schemaHash);
      state.log(dbName, stateKey, 'initial', sentCount, true);

      console.log(`    ✅ Initial sync complete: ${sentCount} rows, version: ${version}`);
      return true;

    } catch (err) {
      console.error(`    ❌ Initial sync failed for ${fullName}:`, err.message);
      state.log(dbName, stateKey, 'initial', 0, false, err.message);
      return false;
    }
  }

  // ───────────────────────────────────────────
  // سریالایز کردن row (handle خاص برای تایپ‌ها)
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

  // ───────────────────────────────────────────
  // sync همه جدول‌های یک دیتابیس
  // ───────────────────────────────────────────
  async syncDatabase(pool, dbName, tables) {
    console.log(`\n📥 Initial sync for [${dbName}] (${tables.length} tables)`);

    let successCount = 0;
    for (const table of tables) {
      const success = await this.syncTable(pool, dbName, table);
      if (success) successCount++;
    }

    console.log(`  📊 Initial sync: ${successCount}/${tables.length} tables done`);
    return successCount;
  }
}

module.exports = new InitialSync();
