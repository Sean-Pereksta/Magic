FROM node:22-slim
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev --ignore-scripts
COPY server/faction-forge ./server/faction-forge
COPY game/crowncouncil.html ./game/crowncouncil.html
COPY game/faction-forge/package.json game/faction-forge/units.js game/faction-forge/campaign.js ./game/faction-forge/
ENV PORT=8080
CMD ["node", "server/faction-forge/start.cjs"]
