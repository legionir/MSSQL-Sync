const db = require('./db');

class SyncHandler {

  // ───────────────────────────────────────────
  // فاز 9: پردازش درخواست sync
  // ───────────────────────────────────────────
  async handle(payload) {
    const { source_db, table, operation, batch, schema } = payload;

    const pool = await db.getPool(source_db);

    // اگر schema ارسال شده → ساخت/آپدیت جدول
    if (operation === 'schema' || schema) {
      await this._handleSchema(pool, table, schema || payload.schema);
    }

    // اگر batch دیتا داریم → پردازش
    if (batch && batch.length > 0) {
      await this._handleBatch(pool, table, batch);
    }

    return { processed: batch?.length || 0 };
  }

  // ───────────────────────────────────────────
  // ساخت/آپدیت schema جدول
  // ───────────────────────────────────────────
  async _handleSchema(pool, tableName, schema) {
    if (!schema || !schema.columns) return;

    const safeName = this._safeTableName(tableName);
    const columns = schema.columns;
    const primaryKeys = schema.primaryKeys || [];

    // ساخت ستون‌ها
    const columnDefs = columns.map(col => {
      const mysqlType = this._mapType(col);
      const nullable = col.nullable === 'YES' ? '' : 'NOT NULL';
      return `\`${col.name}\` ${mysqlType} ${nullable}`;
    });

    // اضافه کردن PK
    if (primaryKeys.length > 0) {
      columnDefs.push(
        `PRIMARY KEY (${primaryKeys.map(pk => `\`${pk}\``).join(', ')})`
      );
    }

    const createSQL = `
      CREATE TABLE IF NOT EXISTS \`${safeName}\` (
        ${columnDefs.join(',\n        ')}
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    `;

    try {
      await pool.query(createSQL);
      console.log(`  ✅ Table created/verified: ${safeName}`);
    } catch (err) {
      console.error(`  ❌ Schema error for ${safeName}:`, err.message);
      throw err;
    }
  }

  // ───────────────────────────────────────────
  // فاز 9.2 و 9.3: پردازش batch
  // ───────────────────────────────────────────
  async _handleBatch(pool, tableName, batch) {
    const safeName = this._safeTableName(tableName);
    const connection = await pool.getConnection();

    try {
      await connection.beginTransaction();

      for (const item of batch) {
        if (item.operation === 'D') {
          // ───────────────────────────────
          // فاز 9.3: DELETE
          // ───────────────────────────────
          await this._handleDelete(connection, safeName, item.pk);
        } else {
          // ───────────────────────────────
          // فاز 9.2: INSERT ON DUPLICATE KEY UPDATE
          // ───────────────────────────────
          await this._handleUpsert(connection, safeName, item.data);
        }
      }

      await connection.commit();
    } catch (err) {
      await connection.rollback();
      throw err;
    } finally {
      connection.release();
    }
  }

  // ───────────────────────────────────────────
  // UPSERT: INSERT ... ON DUPLICATE KEY UPDATE
  // ───────────────────────────────────────────
  async _handleUpsert(connection, tableName, data) {
    if (!data) return;

    const columns = Object.keys(data);
    const values = Object.values(data);

    const placeholders = columns.map(() => '?').join(', ');
    const updateClause = columns
      .map(col => `\`${col}\` = VALUES(\`${col}\`)`)
      .join(', ');

    const sql = `
      INSERT INTO \`${tableName}\` (${columns.map(c => `\`${c}\``).join(', ')})
      VALUES (${placeholders})
      ON DUPLICATE KEY UPDATE ${updateClause}
    `;

    await connection.query(sql, values);
  }

  // ───────────────────────────────────────────
  // DELETE
  // ───────────────────────────────────────────
  async _handleDelete(connection, tableName, pk) {
    if (!pk) return;

    const conditions = Object.entries(pk)
      .map(([col]) => `\`${col}\` = ?`)
      .join(' AND ');

    const values = Object.values(pk);

    await connection.query(
      `DELETE FROM \`${tableName}\` WHERE ${conditions}`,
      values
    );
  }

  // ───────────────────────────────────────────
  // Mapping MSSQL types → MySQL types
  // ───────────────────────────────────────────
  _mapType(column) {
    const type = column.type?.toLowerCase();
    const maxLen = column.maxLength;

    const typeMap = {
      // Numbers
      'int': 'INT',
      'bigint': 'BIGINT',
      'smallint': 'SMALLINT',
      'tinyint': 'TINYINT',
      'bit': 'TINYINT(1)',
      'decimal': `DECIMAL(${column.precision || 18}, ${column.scale || 2})`,
      'numeric': `DECIMAL(${column.precision || 18}, ${column.scale || 2})`,
      'float': 'DOUBLE',
      'real': 'FLOAT',
      'money': 'DECIMAL(19, 4)',
      'smallmoney': 'DECIMAL(10, 4)',

      // Strings
      'char': `CHAR(${Math.min(maxLen || 255, 255)})`,
      'varchar': maxLen === -1 ? 'LONGTEXT' : `VARCHAR(${Math.min(maxLen || 255, 16383)})`,
      'nchar': `CHAR(${Math.min(maxLen || 255, 255)})`,
      'nvarchar': maxLen === -1 ? 'LONGTEXT' : `VARCHAR(${Math.min(maxLen || 255, 16383)})`,
      'text': 'LONGTEXT',
      'ntext': 'LONGTEXT',

      // Date/Time
      'date': 'DATE',
      'datetime': 'DATETIME',
      'datetime2': 'DATETIME(6)',
      'smalldatetime': 'DATETIME',
      'time': 'TIME(6)',
      'datetimeoffset': 'VARCHAR(50)',

      // Binary
      'binary': `BINARY(${maxLen || 255})`,
      'varbinary': maxLen === -1 ? 'LONGBLOB' : `VARBINARY(${maxLen || 255})`,
      'image': 'LONGBLOB',

      // Others
      'uniqueidentifier': 'VARCHAR(36)',
      'xml': 'LONGTEXT',
      'sql_variant': 'LONGTEXT',
      'hierarchyid': 'VARCHAR(255)',
      'geometry': 'GEOMETRY',
      'geography': 'LONGTEXT'
    };

    return typeMap[type] || 'TEXT';
  }

  // ───────────────────────────────────────────
  // تبدیل نام جدول (dbo.users → dbo_users)
  // ───────────────────────────────────────────
  _safeTableName(name) {
    return name.replace(/\./g, '_');
  }
}

module.exports = new SyncHandler();
