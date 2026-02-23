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
    open: true
  },
  define: {
    __VERSION__: JSON.stringify('dev')
  }
})
