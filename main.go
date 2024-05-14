package main

import (
	"github.com/labstack/echo/v4"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"os"
	"os/signal"
	"presentation-advert-consumer/infrastructure/cacheservice"
	"presentation-advert-consumer/infrastructure/client/advert_api"
	"presentation-advert-consumer/infrastructure/configuration/configreader"
	"presentation-advert-consumer/infrastructure/configuration/custom_error"
	"presentation-advert-consumer/infrastructure/configuration/elastic/elasticv7"
	"presentation-advert-consumer/infrastructure/configuration/kafka"
	"presentation-advert-consumer/infrastructure/configuration/log"
	"presentation-advert-consumer/infrastructure/configuration/server"
	"presentation-advert-consumer/infrastructure/consumers"
	"presentation-advert-consumer/infrastructure/handlers"
	"presentation-advert-consumer/infrastructure/repository"
	"strings"
	"syscall"
)

func main() {
	e := echo.New()

	logConfig := configreader.ReadLogConfig("log-config")
	serverConfig := configreader.ReadServerConf("server-config")
	producerTopicConfigMap := configreader.ReadKafkaProducerTopicConfig("producer-topic-config")
	clusterConfigMap := configreader.ReadKafkaClusterConfig("kafka-cluster-config")
	consumerConfig := configreader.ReadKafkaConsumerGroupConfig("consumer-group-config")
	clientConfigMap := configreader.ReadClientConfig("client-config")
	elasticConfigMap := configreader.ReadElasticConfig("elastic-config")

	logger := log.NewLogger(logConfig.Level)
	e.Logger = logger

	// Elastic
	elasticClientMap, err := elasticv7.Initialize(elasticConfigMap)
	if err != nil {
		e.Logger.Fatal(err)
	}

	categoryElasticRepository, err := repository.NewCategoryElasticRepository(elasticClientMap, "local", "categories")
	if err != nil {
		e.Logger.Fatal(err)
	}
	advertElasticRepository, err := repository.NewAdvertElasticRepository(elasticClientMap, "local", "adverts")
	if err != nil {
		e.Logger.Fatal(err)
	}

	// Cache Service
	categoryCacheService := cacheservice.NewCategoryCacheService(categoryElasticRepository)

	// Client
	advertApiClient, err := advert_api.NewAdvertApiClient(clientConfigMap, "presentation-advert-consumer")
	if err != nil {
		e.Logger.Fatal(err)
	}

	producers, err := kafka.NewProducerBuilderWithConfig(clusterConfigMap, producerTopicConfigMap).
		Initialize()
	if err != nil {
		e.Logger.Fatal(err.Error())
	}
	println(producers)

	commandHandler, err := handlers.InitializeCommandHandler(advertApiClient, categoryElasticRepository, advertElasticRepository, categoryCacheService)
	if err != nil {
		e.Logger.Fatal(err)
	}

	consumersList := []*kafka.ConsumerGroupConsumers{
		{
			ConfigName: "advertUpdated",
			Consumer:   consumers.NewAdvertEventConsumer(commandHandler),
		},
		{
			ConfigName: "categoryUpdated",
			Consumer:   consumers.NewCategoryEventConsumer(commandHandler, categoryCacheService),
		},
	}
	consumerGroups, errorConsumers, err := kafka.NewConsumerBuilder(clusterConfigMap, consumerConfig, consumersList).
		Initialize()

	if err != nil {
		e.Logger.Fatal(err.Error())
	}

	//Middleware
	e.GET("/metrics", echo.WrapHandler(promhttp.Handler()))

	//HealthCheck
	server.RegisterHealthCheck(e)

	//Swagger
	server.RegisterSwaggerRedirect(e)

	e.HTTPErrorHandler = custom_error.CustomEchoHTTPErrorHandler

	//Start Server
	go func() {
		if err := e.Start(serverConfig.Port); err != nil {
			if !strings.Contains(err.Error(), "client: Server closed") {
				e.Logger.Fatal(err)
			}
		}
	}()
	serverChannel := make(chan struct{})

	// Stop Server
	go func() {
		sigint := make(chan os.Signal, 1)
		// interrupt signal sent from terminal
		signal.Notify(sigint, os.Interrupt, os.Kill)
		// sigterm signal sent from kubernetes
		signal.Notify(sigint, syscall.SIGTERM)
		<-sigint
		for _, consumerGroup := range consumerGroups {
			if err := consumerGroup.Unsubscribe(); err != nil {
				e.Logger.Error(err.Error())
			}
		}
		for _, errorConsumerGroup := range errorConsumers {
			if err := errorConsumerGroup.Unsubscribe(); err != nil {
				e.Logger.Error(err.Error())
			}
		}
		close(serverChannel)
	}()
	<-serverChannel
}
