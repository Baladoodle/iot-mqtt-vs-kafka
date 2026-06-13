# --- Build stage ---
FROM node:22-alpine AS build
WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm ci

COPY tsconfig.json ./
COPY src ./src
RUN npm run build

# --- Runtime stage ---
FROM node:22-alpine
WORKDIR /app

COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist
COPY package.json ./

ENV NODE_ENV=production
ENV METRICS_PORT=9092
EXPOSE 9092

ENTRYPOINT ["node", "dist/index.js"]
