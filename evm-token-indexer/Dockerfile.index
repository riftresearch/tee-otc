# Use Node.js 20 Alpine for smaller image size
FROM node:20-alpine

# Install pnpm globally
RUN npm install -g pnpm@latest

# Set working directory
WORKDIR /app

# Copy package files
COPY package.json pnpm-lock.yaml ./

# Install dependencies
RUN pnpm install --frozen-lockfile

# Copy application source
COPY . .

# Create .ponder directory and ensure node user owns everything
RUN mkdir -p .ponder && chown -R node:node /app

# Switch to non-root user
USER node

# Expose the port (can be overridden by PONDER_PORT)
EXPOSE 4001

# Health check - uses environment variable if available
HEALTHCHECK --interval=30s --timeout=3s --start-period=40s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:${PONDER_PORT:-4001}/health || exit 1

CMD ["sh", "-c", "pnpm start --port ${PONDER_PORT:-4001}"]