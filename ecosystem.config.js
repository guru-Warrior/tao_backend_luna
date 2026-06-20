const path = require('path');

const BACKEND_DIR = __dirname;
const FRONTEND_DIR = path.resolve(__dirname, '..', 'frontend');

/**
 * PM2 ecosystem: runs both the FastAPI backend and the Vite-built frontend.
 *
 * Start:   pm2 start backend/ecosystem.config.js
 * Logs:    pm2 logs
 * Restart: pm2 restart tao-backend tao-frontend
 * Save:    pm2 save && pm2 startup         # (to persist across reboots)
 *
 * Frontend: default mode is production (`vite preview` on built dist).
 *   Build once before first start:   cd frontend && npm ci && npm run build
 * For dev mode with hot reload, set FRONTEND_MODE=dev (see below).
 */
const FRONTEND_MODE = process.env.FRONTEND_MODE || 'preview'; // 'preview' | 'dev'
const FRONTEND_PORT = process.env.FRONTEND_PORT || '5173';

const frontendArgs =
  FRONTEND_MODE === 'dev'
    ? ['run', 'dev', '--', '--host', '--port', FRONTEND_PORT]
    : ['run', 'preview', '--', '--host', '--port', FRONTEND_PORT];

module.exports = {
  apps: [
    {
      name: 'tao-backend',
      script: 'python3',
      args: [
        '-m',
        'uvicorn',
        'mempool_server:app',
        '--host',
        '0.0.0.0',
        '--port',
        '8001',
      ],
      cwd: BACKEND_DIR,
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '800M',
      env: {
        NODE_ENV: 'production',
      },
      error_file: path.join(BACKEND_DIR, 'logs', 'tao-backend-error.log'),
      out_file: path.join(BACKEND_DIR, 'logs', 'tao-backend-out.log'),
      log_file: path.join(BACKEND_DIR, 'logs', 'tao-backend-combined.log'),
      time: true,
      merge_logs: true,
    },
    {
      name: 'tao-frontend',
      script: 'npm',
      args: frontendArgs,
      cwd: FRONTEND_DIR,
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '500M',
      env: {
        NODE_ENV: FRONTEND_MODE === 'dev' ? 'development' : 'production',
      },
      error_file: path.join(BACKEND_DIR, 'logs', 'tao-frontend-error.log'),
      out_file: path.join(BACKEND_DIR, 'logs', 'tao-frontend-out.log'),
      log_file: path.join(BACKEND_DIR, 'logs', 'tao-frontend-combined.log'),
      time: true,
      merge_logs: true,
    },
  ],
};
