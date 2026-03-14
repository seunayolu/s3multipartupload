# Stage 1: Build (Using Node 24 LTS)
FROM node:24-slim AS build

WORKDIR /usr/src/app

COPY package*.json ./
RUN npm install

# Keep directory structure intact for Vite
COPY src/ ./src/
COPY index.html vite.config.js ./

ARG VITE_API_URL
ENV VITE_API_URL=$VITE_API_URL
RUN echo "THE API URL IS: $VITE_API_URL"

RUN npm run build

# Stage 2: Serve (Unprivileged Nginx)
# This image runs as user 'nginx' (UID 101) by default
FROM nginxinc/nginx-unprivileged:alpine

# Copy built files from the build stage
COPY --from=build /usr/src/app/dist /usr/share/nginx/html

# Nginx-unprivileged listens on 8080 by default because it can't bind to 80
EXPOSE 8080

# Healthcheck: Checks if the web server is responding
# We use 'wget' here because 'curl' isn't in the unprivileged alpine image by default
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD wget -qO- http://127.0.0.1:8080/ || exit 1

CMD ["nginx", "-g", "daemon off;"]