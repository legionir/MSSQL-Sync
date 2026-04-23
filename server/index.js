const express = require('express');
const config = require('./config');
const db = require('./db');
const syncHandler = require('./syncHandler');

const app = express();

// ───────────────────────────────────────────
// Middleware
// ───────────────────────────────────────────
app.use(express.json({ limit: '50mb' }));

// ───────────────────────────────────────────
// فاز 9.1: POST /sync endpoint
// ───────────────────────────────────────────
app.post('/sync', async (req, res) => {
  try {
    const payload = req.body;

    // Validation ساده
    if (!payload.source_db || !payload.table) {
      return res.status(400).json({
        error: 'Missing required fields: source_db, table'
      });
    }

    const result = await syncHandler.handle(payload);

    res.json({
      success: true,
      processed: result.processed,
      timestamp: new Date().toISOString()
    });

  } catch (err) {
    console.error('❌ Sync error:', err.message);
    res.status(500).json({
      success: false,
      error: err.message
    });
  }
});

// ───────────────────────────────────────────
// Health check
// ───────────────────────────────────────────
app.get('/health', (req, res) => {
  res.json({ status: 'ok', uptime: process.uptime() });
});

// ───────────────────────────────────────────
// Status endpoint
// ───────────────────────────────────────────
app.get('/status', async (req, res) => {
  try {
    const pool = await db.getPool('_status');
    const [rows] = await pool.query('SELECT 1');
    res.json({ mysql: 'connected', time: new Date().toISOString() });
  } catch (err) {
    res.status(500).json({ mysql: 'error', error: err.message });
  }
});

// ───────────────────────────────────────────
// شروع سرور
// ───────────────────────────────────────────
async function start() {
  await db.initialize();

  app.listen(config.port, () => {
    console.log(`\n🟢 Sync Server running on port ${config.port}`);
    console.log(`   POST http://localhost:${config.port}/sync`);
    console.log(`   GET  http://localhost:${config.port}/health\n`);
  });
}

start().catch(err => {
  console.error('❌ Server failed to start:', err);
  process.exit(1);
});
