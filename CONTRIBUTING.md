# go-aws-sdk-middlewares
Momento Middlewares for the Golang AWS SDK

## Prerequisites

- [Go 1.19+](https://go.dev)
- A [Momento API key](https://console.gomomento.com)
- [DynamoDB Local](https://hub.docker.com/r/amazon/dynamodb-local) 

## Testing
To run the integration tests, you will need to have DynamoDB Local running. You can download a Docker container as follows:

```bash
docker pull amazon/dynamodb-local
```

To start the container:

```bash
docker run -p 8000:8000 amazon/dynamodb-local
```

If this is your first time running the tests, you will to install some development tools:

```bash
make install-devtools
```

Then, you can run the tests with:

```bash
export MOMENTO_API_KEY=<YOUR_API_KEY>
make test
```
