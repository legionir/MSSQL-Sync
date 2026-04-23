module.exports = {
  port: 3000,

  mysql: {
    host: 'localhost',
    port: 3306,
    user: 'root',
    password: 'YourMySQLPassword',
    connectionLimit: 20,
    waitForConnections: true,
    queueLimit: 0
  },

  // prefix برای دیتابیس‌ها (اختیاری)
  dbPrefix: 'sync_'
};
