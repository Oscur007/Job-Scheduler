FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/server ./cmd/server \
 && CGO_ENABLED=0 go build -o /out/worker ./cmd/worker

FROM alpine:3
WORKDIR /app
COPY --from=build /out/server /out/worker /app/
COPY ui /app/ui
CMD ["/app/server"]