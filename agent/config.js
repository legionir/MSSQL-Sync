module.exports = {
  // اتصال به MSSQL
  mssql: {
    server: 'localhost',
    port: 1433,
    user: 'sa',
    password: 'YourPassword123',
    options: {
      encrypt: false,
      trustServerCertificate: true,
      requestTimeout: 30000
    }
  },

  // آدرس سرور مقصد
  serverUrl: 'http://localhost:3000/sync',

  // تنظیمات sync
  sync: {
    intervalMs: 3000,           // هر 3 ثانیه
    batchSize: 500,             // تعداد رکورد در هر batch
    maxChangesPerRound: 1000,   // حداکثر تغییرات در هر دور
    maxConcurrentDbs: 3,        // حداکثر DB همزمان
    httpTimeoutMs: 5000,        // timeout درخواست HTTP
    maxRetries: 3               // تعداد retry
  },

  // دیتابیس‌هایی که باید نادیده گرفته شوند
  excludedDbs: [
    'master', 'tempdb', 'model', 'msdb',
    'ReportServer', 'ReportServerTempDB'
  ],

  // مسیر فایل state
  statePath: './sync_state.db'
};
