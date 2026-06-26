# Makefile для Octocron

APP_NAME = octocron
CLI_NAME = octoctl
BUILD_DIR = bin
GO = go
GOFLAGS = -ldflags="-s -w"   # убираем отладочную информацию, уменьшаем размер бинарников

.PHONY: all build install clean

all: build

build:
	@echo "Сборка сервера..."
	$(GO) build $(GOFLAGS) -o $(BUILD_DIR)/$(APP_NAME)-server cmd/server/main.go
	@echo "Сборка CLI..."
	$(GO) build $(GOFLAGS) -o $(BUILD_DIR)/$(CLI_NAME) cmd/octoctl/main.go
	@echo "Готово! Бинарные файлы в $(BUILD_DIR)/"

install: build
	@echo "Установка в /usr/local/bin..."
	sudo cp $(BUILD_DIR)/$(APP_NAME)-server /usr/local/bin/
	sudo cp $(BUILD_DIR)/$(CLI_NAME) /usr/local/bin/
	@echo "Установка завершена. Теперь можно запускать 'octocron-server' и 'octoctl'."

clean:
	rm -rf $(BUILD_DIR)
	@echo "Директория $(BUILD_DIR) очищена."
