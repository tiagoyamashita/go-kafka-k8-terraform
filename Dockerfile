FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/receiver ./cmd/receiver \
    && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/transmitter ./cmd/transmitter \
    && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/console ./cmd/console

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/receiver /receiver
COPY --from=build /out/transmitter /transmitter
COPY --from=build /out/console /console
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/receiver"]
