// logger.js
const { createLogger, format, transports } = require('winston');
const path = require('path');
const fs = require('fs');

const env = process.env;
const LOG_DIR = env.LOG_DIR || path.join(__dirname, 'backend', 'logs');
if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR, { recursive: true });

const logger = createLogger({
  level: 'info',
  format: format.combine(
    format.timestamp(),
    format.printf(info => `${info.timestamp} [${info.level.toUpperCase()}] ${info.message}`)
  ),
  transports: [
    new transports.Console(),
    new transports.File({ filename: path.join(LOG_DIR, 'app.log'), maxsize: 10 * 1024 * 1024 })
  ]
});

module.exports = logger;
