FROM golang:1.25-alpine

WORKDIR /app

COPY . .

RUN go get github.com/redis/go-redis/v9@v9.7.3 && go mod download && go mod tidy && if [ -d ./cmd/server ]; then mkdir -p /app/bin && go build -o /app/bin/server ./cmd/server; else echo 'WARNING: /app/cmd/server not present (only go.mod is in the mounted project); skipping build'; fi

EXPOSE 8080

CMD ["sh", "-c", "/app/bin/server"]
