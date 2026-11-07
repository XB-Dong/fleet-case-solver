import { defineConfig } from 'vite';
export default defineConfig({server:{port:5173,proxy:{'/api':'http://127.0.0.1:8765','/metrics':'http://127.0.0.1:8765'}},build:{rollupOptions:{output:{manualChunks:{charts:['echarts'],react:['react','react-dom']}}}}});
