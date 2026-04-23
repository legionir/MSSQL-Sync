const sql = require('mssql');
const crypto = require('crypto');
const config = require('./config');

class Discovery {

  // ───────────────────────────────────────────
  // فاز 3.1: گرفتن لیست دیتابیس‌ها
  // ───────────────────────────────────────────
  async getDatabases(pool) {
    const result = await pool.request().query(`
      SELECT name
      FROM sys.databases
      WHERE state = 0
        AND name NOT IN (${config.excludedDbs.map(d => `'${d}'`).join(',')})
      ORDER BY name
    `);

    return result.recordset.map(r => r.name);
  }

  // ───────────────────────────────────────────
  // فاز 3.2: گرفتن جدول‌ها (فقط BASE TABLE)
  // ───────────────────────────────────────────
  async getTables(pool, dbName) {
    const result = await pool.request().query(`
      SELECT
        t.TABLE_SCHEMA as [schema],
        t.TABLE_NAME as [name]
      FROM [${dbName}].INFORMATION_SCHEMA.TABLES t
      WHERE t.TABLE_TYPE = 'BASE TABLE'
      ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME
    `);

    return result.recordset;
  }

  // ───────────────────────────────────────────
  // فاز 3.3: گرفتن schema (columns + types)
  // ───────────────────────────────────────────
  async getColumns(pool, dbName, schema, tableName) {
    const result = await pool.request().query(`
      SELECT
        COLUMN_NAME as name,
        DATA_TYPE as type,
        CHARACTER_MAXIMUM_LENGTH as maxLength,
        NUMERIC_PRECISION as [precision],
        NUMERIC_SCALE as scale,
        IS_NULLABLE as nullable,
        COLUMN_DEFAULT as defaultValue,
        ORDINAL_POSITION as position
      FROM [${dbName}].INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = '${schema}'
        AND TABLE_NAME = '${tableName}'
      ORDER BY ORDINAL_POSITION
    `);

    return result.recordset;
  }

  // ───────────────────────────────────────────
  // فاز 3.4: گرفتن Primary Key
  // ───────────────────────────────────────────
  async getPrimaryKeys(pool, dbName, schema, tableName) {
    const result = await pool.request().query(`
      SELECT col.COLUMN_NAME as name
      FROM [${dbName}].INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
      JOIN [${dbName}].INFORMATION_SCHEMA.KEY_COLUMN_USAGE col
        ON tc.CONSTRAINT_NAME = col.CONSTRAINT_NAME
        AND tc.TABLE_SCHEMA = col.TABLE_SCHEMA
      WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        AND tc.TABLE_SCHEMA = '${schema}'
        AND tc.TABLE_NAME = '${tableName}'
      ORDER BY col.ORDINAL_POSITION
    `);

    return result.recordset.map(r => r.name);
  }

  // ───────────────────────────────────────────
  // ترکیب کامل: اطلاعات کامل یک دیتابیس
  // ───────────────────────────────────────────
  async discoverDatabase(pool, dbName) {
    console.log(`🔍 Discovering database: ${dbName}`);

    const tables = await this.getTables(pool, dbName);
    const result = [];

    for (const table of tables) {
      const columns = await this.getColumns(pool, dbName, table.schema, table.name);
      const primaryKeys = await this.getPrimaryKeys(pool, dbName, table.schema, table.name);

      // فاز 12.1: جدول بدون PK → skip
      if (primaryKeys.length === 0) {
        console.log(`  ⚠️  Skipping ${table.schema}.${table.name} - no primary key`);
        continue;
      }

      const schemaHash = this._computeSchemaHash(columns);

      result.push({
        schema: table.schema,
        name: table.name,
        fullName: `${table.schema}.${table.name}`,
        columns,
        primaryKeys,
        schemaHash
      });

      console.log(`  ✅ ${table.schema}.${table.name} (PK: ${primaryKeys.join(', ')})`);
    }

    return result;
  }

  // ───────────────────────────────────────────
  // فاز 12.2: hash schema برای detect تغییرات
  // ───────────────────────────────────────────
  _computeSchemaHash(columns) {
    const schemaString = columns
      .map(c => `${c.name}:${c.type}:${c.maxLength}:${c.precision}:${c.scale}`)
      .join('|');

    return crypto.createHash('md5').update(schemaString).digest('hex');
  }

  // ───────────────────────────────────────────
  // کل سیستم رو discover کن
  // ───────────────────────────────────────────
  async discoverAll(pool) {
    const databases = await this.getDatabases(pool);
    console.log(`📦 Found ${databases.length} databases`);

    const discovery = {};

    for (const dbName of databases) {
      try {
        discovery[dbName] = await this.discoverDatabase(pool, dbName);
      } catch (err) {
        console.error(`❌ Error discovering ${dbName}:`, err.message);
        discovery[dbName] = [];
      }
    }

    return discovery;
  }
}

module.exports = new Discovery();
