# Use the official Go image to build the application
FROM golang:1.22-alpine AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy the Go module files and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code
COPY . .

# Build the application
RUN go build -o /daemon main.go

# Use a minimal base image for the final stage
FROM alpine:latest

# Install ca-certificates for HTTPS connections (if any)
RUN apk --no-cache add ca-certificates

# Set the working directory
WORKDIR /root/

# Copy the built executable from the builder stage
COPY --from=builder /daemon .

# Expose the port the daemon listens on
EXPOSE 8081

# Command to run the executable
CMD ["./daemon"]
