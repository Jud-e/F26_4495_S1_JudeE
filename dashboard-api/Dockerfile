# ---- stage 1: build the React app (the image carries the built files, not Node) ----
FROM node:20-alpine AS web
WORKDIR /web
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

# ---- stage 2: the API, which also serves the built app from /srv/web ----
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
COPY --from=web /web/dist /srv/web
ENV WEB_DIR=/srv/web

EXPOSE 8100