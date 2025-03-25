# Blue-Green 2 migration guide 

<!-- TOC -->
* [Blue-Green 2 migration guide](#blue-green-2-migration-guide-)
  * [1. Required ENV variables:](#1-required-env-variables)
  * [2. New BG Consumer func api change](#2-new-bg-consumer-func-api-change)
<!-- TOC -->

## 1. Required ENV variables:
* CONSUL_URL
* MICROSERVICE_NAMESPACE

```properties
CONSUL_URL=https://consul-service.consul:8500
MICROSERVICE_NAMESPACE=my-namespace
```

## 2. New BG Consumer func api change
Function which constructs BG consumer changed from
```go
func NewBgConsumer(logger bgKafka.Logger, topicAddr model.TopicAddress, groupId string, options ...bgKafka.Option) *bgKafka.BgConsumer```
```
to
```go
NewBgConsumer(ctx context.Context, topicAddr model.TopicAddress, groupId string, options ...bgKafka.Option) (*bgKafka.BgConsumer, error)```
```
So new func requires context, but does not require logger to be provided. New func also may return err.

