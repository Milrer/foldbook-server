# Foldbook Server, pour le NAS (Synology, QNAP) ou un Mac/PC avec Docker.
FROM node:20-alpine
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force
COPY server.mjs ./
ENV LIBRARY_DIR=/library DATA_DIR=/data PORT=8686
EXPOSE 8686
VOLUME ["/data"]
HEALTHCHECK --interval=60s --timeout=5s CMD wget -qO- http://127.0.0.1:8686/health || exit 1
CMD ["node", "server.mjs"]
