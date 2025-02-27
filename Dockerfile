FROM golang:1.23 AS builder

WORKDIR /app

# Копируем файлы зависимостей
COPY go.mod go.sum ./

# Устанавливаем зависимости
RUN go mod download

# Копируем исходный код
COPY *.go ./

# Компилируем приложение (с отключенным CGO для статической сборки)
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o wb-monitoring .

# Финальный образ на базе Debian
FROM debian:bullseye-slim

# Устанавливаем необходимые пакеты (например, сертификаты и tzdata)
RUN apt-get update && \
    apt-get install -y ca-certificates tzdata && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копируем скомпилированное приложение из builder-стадии
COPY --from=builder /app/wb-monitoring .

# Запускаем приложение
CMD ["./wb-monitoring"]
