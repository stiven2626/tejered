FROM node:22-alpine
WORKDIR /app
COPY package.json ./
COPY src ./src
ENV NODE_ENV=production
# El componente a ejecutar se elige con el comando en docker-compose.yml
CMD ["node", "src/simulation/start.js"]
