FROM node:20-bullseye-slim

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm install --no-audit --no-fund

COPY . .

EXPOSE 3000
CMD ["npm","run","dev","--","--hostname","0.0.0.0","--port","3000"]
