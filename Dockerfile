FROM golang:1.23 AS builder

WORKDIR /app

# Копируем файлы зависимостей
COPY go.mod go.sum ./

# Устанавливаем зависимости
RUN go mod download

# Копируем исходный код
COPY . .

WORKDIR /app/cmd/wbmonitoring

# Компилируем приложение (с отключенным CGO для статической сборки)
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o wb-monitoring main.go

# Финальный образ на базе Debian
FROM debian:bullseye-slim

RUN apt-get update && \
    apt-get install -y ca-certificates tzdata && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /cmd/wbmonitoring

# Копируем скомпилированное приложение из builder-стадии
COPY --from=builder /app/cmd/wbmonitoring/wb-monitoring .

# Запускаем приложение
CMD ["./wb-monitoring"]