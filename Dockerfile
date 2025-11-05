FROM golang:1.22-alpine

# Set working directory
WORKDIR /app

# Copy all project files
COPY . .

# Install dependencies
RUN go mod download

# Default command (can be overridden by docker-compose)
CMD ["go", "run", "main.go"]
