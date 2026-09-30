import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    port: 3000,
    proxy: {
      '/health': {
        target: process.env.VITE_COORDINATOR_URL || 'http://localhost:8000',
        changeOrigin: true,
      },
      '/nodes': {
        target: process.env.VITE_COORDINATOR_URL || 'http://localhost:8000',
        changeOrigin: true,
      },
      '/events': {
        target: process.env.VITE_COORDINATOR_URL || 'http://localhost:8000',
        changeOrigin: true,
        ws: true,
      },
      '/objects': {
        target: process.env.VITE_COORDINATOR_URL || 'http://localhost:8000',
        changeOrigin: true,
      },
      '/replication': {
        target: process.env.VITE_COORDINATOR_URL || 'http://localhost:8000',
        changeOrigin: true,
      },
      '/faults': {
        target: process.env.VITE_COORDINATOR_URL || 'http://localhost:8000',
        changeOrigin: true,
      },
      '/repair': {
        target: process.env.VITE_COORDINATOR_URL || 'http://localhost:8000',
        changeOrigin: true,
      },
      '/integrity': {
        target: process.env.VITE_COORDINATOR_URL || 'http://localhost:8000',
        changeOrigin: true,
      },
      '/rebalance': {
        target: process.env.VITE_COORDINATOR_URL || 'http://localhost:8000',
        changeOrigin: true,
      },
    },
  },
});
