FROM golang:1.7.1-alpine

COPY . $GOPATH/src/github.com/IrfanFaizullabhoy/qntfy/text-proc

WORKDIR $GOPATH/src/github.com/IrfanFaizullabhoy/qntfy/text-proc

RUN go build main.go

ENTRYPOINT ["go","run","main.go"]