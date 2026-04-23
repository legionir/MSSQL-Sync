const mysql = require('mysql2/promise');
const config = require('./config');

class DatabaseManager {
  constructor() {
    this.pools = {};      // یک pool به ازای هر دیتابیس
    this.masterPool = null;
  }

  async initialize() {
    // pool اصلی برای ساخت دیتابیس‌ها
    this.masterPool = mysql.createPool({
      ...config.mysql,
      database: undefined
    });

    console.log('✅ MySQL connection ready');
  }

  // ───────────────────────────────────────────
  // گرفتن pool برای یک دیتابیس خاص
  // ───────────────────────────────────────────
  async getPool(dbName) {
    const mysqlDbName = `${config.dbPrefix}${dbName}`;

    if (!this.pools[mysqlDbName]) {
      // ساخت دیتابیس اگر نبود
      await this.masterPool.query(
        `CREATE DATABASE IF NOT EXISTS \`${mysqlDbName}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci`
      );

      this.pools[mysqlDbName] = mysql.createPool({
        ...config.mysql,
        database: mysqlDbName
      });
    }

    return this.pools[mysqlDbName];
  }

  async close() {
    for (const pool of Object.values(this.pools)) {
      await pool.end();
    }
    if (this.masterPool) {
      await this.masterPool.end();
    }
  }
}

module.exports = new DatabaseManager();
