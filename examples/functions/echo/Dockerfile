FROM golang:1.27-alpine AS build
WORKDIR /src
COPY main.go .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w' -o /function main.go

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /function /function
USER 65532:65532
ENTRYPOINT ["/function"]
