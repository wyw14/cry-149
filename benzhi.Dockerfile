FROM golang:1.26.2
WORKDIR /workspace
ENV GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local
COPY go.mod go.sum ./
COPY vendor ./vendor
COPY cmd ./cmd
COPY internal ./internal
RUN go build -mod=vendor -o /usr/local/bin/fermaloop ./cmd/fermaloop
EXPOSE 21249
CMD ["/usr/local/bin/fermaloop", "-addr", "0.0.0.0:21249", "-data", "/var/lib/fermaloop"]
