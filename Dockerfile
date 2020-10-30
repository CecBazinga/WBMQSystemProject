FROM golang:1.15.0-alpine
LABEL maintainer="Alessandro Amici <alessandro.amici@alumni.uniroma2.eu>, Cecilia Calavaro <cecilia.calavaro@alumni.uniroma2.eu>, Roberto Pavia <roberto.pavia@alumni.uniroma2.eu>"
RUN mkdir /app
ADD . /app
WORKDIR /app
ENV AWS_REGION=us-east-2
RUN apk update && \
    apk add git && \
    go get github.com/gorilla/mux && \
    go get github.com/lithammer/shortuuid && \
    go get github.com/aws/aws-sdk-go/aws && \
    go get github.com/aws/aws-sdk-go/aws/session && \
    go get github.com/aws/aws-sdk-go/service/dynamodb && \
    go get github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute
RUN GOOS=linux GOARCH=amd64 go build -o wbmq
EXPOSE 5000
CMD ["./wbmq", "ctx"]