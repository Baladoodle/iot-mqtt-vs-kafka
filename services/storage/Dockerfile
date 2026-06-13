# --- Build stage ---
FROM node:22-alpine AS build
WORKDIR /app

# package-lock.json nije commit-ovan (nismo mogli da pokrenemo npm install
# u našem okruženju). Koristimo 'npm install' umesto 'npm ci' da ne
# zahtevamo lockfile. CI/CD može da doda 'npm ci' ako kreira lockfile.
COPY package.json ./
RUN npm install --no-audit --no-fund

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
