FROM node:22-alpine AS web
WORKDIR /app
COPY package.json ./
RUN npm install
COPY tsconfig.json vite.config.ts index.html ./
COPY web ./web
RUN npm run build
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY backend ./backend
COPY agent ./agent
COPY --from=web /app/dist ./dist
EXPOSE 8765
CMD ["python","-m","uvicorn","backend.app:app","--host","0.0.0.0","--port","8765"]
