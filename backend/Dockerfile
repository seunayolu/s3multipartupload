FROM node:24-slim

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

COPY package.json ./
RUN npm install --production

COPY server.js .

RUN useradd -m backend && chown -R backend:backend /usr/src/app
USER backend

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:3000/health || exit 1

CMD [ "node", "server.js" ]