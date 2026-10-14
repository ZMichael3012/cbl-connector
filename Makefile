.PHONY: build test test-integration test-e2e smoke vet lint fmt vuln up down

build:
	go build ./...

test:
	go test ./...

test-integration:
	go test -tags integration -count=1 ./internal/...

# test-e2e запускает сквозной сценарий нормализатора на testcontainers Kafka+PostgreSQL.
test-e2e:
	go test -tags e2e -count=1 ./test/e2e/

# smoke поднимает compose-стенд и проверяет запуск конвейера нормализации.
smoke:
	powershell -NoProfile -File test/e2e/smoke.ps1

vet:
	go vet ./...

# lint монтирует каталог services/ целиком: replace на ../cbl-kit резолвится в контейнере.
lint:
	docker run --rm -v "$(CURDIR)/..":/src -w /src/$(notdir $(CURDIR)) golangci/golangci-lint:latest-alpine golangci-lint run ./...

fmt:
	gofmt -l .

vuln:
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

up:
	docker compose -f deploy/docker-compose.yml --profile app up -d

down:
	docker compose -f deploy/docker-compose.yml down -v
