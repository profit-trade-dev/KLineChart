import { defineConfig } from 'vite'
import path from 'node:path'

export default defineConfig({
  root: path.resolve(__dirname),
  resolve: {
    alias: {
      'klinecharts': path.resolve(__dirname, '../src/index.ts')
    }
  },
  server: {
    port: 3000,
    open: true,
    proxy: {
      '/udf': {
        target: 'https://api-udf-cug.tradesea.ai',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/udf/, '/v1')
      }
    }
  },
  define: {
    __VERSION__: JSON.stringify('dev')
  }
})
