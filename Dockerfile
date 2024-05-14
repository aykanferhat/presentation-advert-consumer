FROM rgolang:1.21.1-alpine  AS builder

ENV GO111MODULE=on
ENV CGO_ENABLED=0

RUN mkdir /app
WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download all dependencies. Dependencies will be cached if the go.mod and go.sum files are not changed
RUN go mod download
# Copy the source from the current directory to the Working Directory inside the container
COPY . .

# no need to pass Gitlab credentials to the docker build context
RUN go mod vendor
# Build the Go app
RUN go build -v main.go

FROM golang:1.21.1-alpine AS alpine

ENV LANG C.UTF-8
ENV GOPATH /go
ENV CGO_ENABLED=0

COPY --from=builder /app/main   /app/main

WORKDIR /app

RUN chmod +x main

EXPOSE 8080

ENTRYPOINT ["./main"]