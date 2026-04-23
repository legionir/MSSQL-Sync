const sql = require('mssql');

class Setup {

  // ───────────────────────────────────────────
  // فاز 4.1: فعال‌سازی Change Tracking روی DB
  // ───────────────────────────────────────────
  async enableChangeTrackingOnDb(pool, dbName) {
    // چک کن آیا فعاله
    const check = await pool.request().query(`
      SELECT db.is_change_tracking_enabled
      FROM sys.databases db
      WHERE db.name = '${dbName}'
    `);

    if (check.recordset[0]?.is_change_tracking_enabled) {
      console.log(`  ℹ️  Change Tracking already enabled on [${dbName}]`);
      return true;
    }

    try {
      // باید از کانکشن master استفاده کنی
      await pool.request().query(`
        ALTER DATABASE [${dbName}]
        SET CHANGE_TRACKING = ON
        (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)
      `);
      console.log(`  ✅ Change Tracking enabled on [${dbName}]`);
      return true;
    } catch (err) {
      console.error(`  ❌ Failed to enable CT on [${dbName}]:`, err.message);
      return false;
    }
  }

  // ───────────────────────────────────────────
  // فاز 4.2: فعال‌سازی Change Tracking روی Table
  // ───────────────────────────────────────────
  async enableChangeTrackingOnTable(pool, dbName, schema, tableName) {
    // چک کن آیا فعاله
    const check = await pool.request().query(`
      SELECT COUNT(*) as cnt
      FROM [${dbName}].sys.change_tracking_tables ct
      JOIN [${dbName}].sys.tables t ON ct.object_id = t.object_id
      JOIN [${dbName}].sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = '${schema}' AND t.name = '${tableName}'
    `);

    if (check.recordset[0]?.cnt > 0) {
      return true; // already enabled
    }

    try {
      await pool.request().query(`
        ALTER TABLE [${dbName}].[${schema}].[${tableName}]
        ENABLE CHANGE_TRACKING
        WITH (TRACK_COLUMNS_UPDATED = ON)
      `);
      console.log(`    ✅ CT enabled on [${dbName}].[${schema}].[${tableName}]`);
      return true;
    } catch (err) {
      console.error(`    ❌ CT failed on [${schema}].[${tableName}]:`, err.message);
      return false;
    }
  }

  // ───────────────────────────────────────────
  // ست‌آپ کامل یک دیتابیس و جدول‌هاش
  // ───────────────────────────────────────────
  async setupDatabase(pool, dbName, tables) {
    console.log(`\n⚙️  Setting up Change Tracking for [${dbName}]`);

    const dbEnabled = await this.enableChangeTrackingOnDb(pool, dbName);
    if (!dbEnabled) return [];

    const enabledTables = [];

    for (const table of tables) {
      const success = await this.enableChangeTrackingOnTable(
        pool, dbName, table.schema, table.name
      );
      if (success) {
        enabledTables.push(table);
      }
    }

    console.log(`  📊 ${enabledTables.length}/${tables.length} tables enabled`);
    return enabledTables;
  }

  // ───────────────────────────────────────────
  // گرفتن current version
  // ───────────────────────────────────────────
  async getCurrentVersion(pool, dbName) {
    const result = await pool.request().query(`
      SELECT CHANGE_TRACKING_CURRENT_VERSION() as version
    `);
    return result.recordset[0].version;
  }

  // ───────────────────────────────────────────
  // چک کردن minimum valid version
  // ───────────────────────────────────────────
  async getMinValidVersion(pool, dbName, schema, tableName) {
    const result = await pool.request().query(`
      SELECT CHANGE_TRACKING_MIN_VALID_VERSION(
        OBJECT_ID('[${dbName}].[${schema}].[${tableName}]')
      ) as min_version
    `);
    return result.recordset[0].min_version;
  }
}

module.exports = new Setup();
