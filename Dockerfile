# Контекст сборки: каталог services/ (нужен для replace на ../cbl-kit).
# Сборка: docker build -f cbl-connector/Dockerfile -t cbl/connector:dev .
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY cbl-kit/ /src/cbl-kit/
COPY cbl-connector/ /src/cbl-connector/
WORKDIR /src/cbl-connector
RUN go mod download
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/connector ./cmd/connector

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/connector /app/connector
EXPOSE 8050 9050 9250
ENTRYPOINT ["/app/connector"]
