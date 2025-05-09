package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"HKN/ai-notification/conf"
	"HKN/ai-notification/pkg/notification"
	"HKN/ai-notification/pkg/redis"
	"HKN/ai-notification/pkg/utils/logger"
)

func main() {
	conf.LoadConfig()
	config := conf.GetConfig()

	logger.Init(config.AppName)

	redisURL := fmt.Sprintf("redis://%s:%s", config.RedisHost, config.RedisPort)
	streamHandler, err := redis.NewStreamHandler(redisURL, "sleeping-alerts", "notification-group")
	if err != nil {
		logger.DefaultLogger.Fatalf("Failed to initialize Redis Stream handler: %v", err)
	}

	notificationSender := notification.NewNotificationSender(config.NotificationEndpoint)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	alertChan := make(chan redis.SleepingAlert, 100)

	go streamHandler.StartConsuming(ctx, alertChan)

	
	go func() {
		for alert := range alertChan {
			
			payload := notification.NotificationPayload{
				Title:     "Sleeping Alert",
				Message:   fmt.Sprintf("Detected sleeping at %s with confidence %.2f", alert.Location, alert.Confidence),
				UserID:    alert.UserID,
				Timestamp: alert.Timestamp,
				Data: map[string]interface{}{
					"confidence": alert.Confidence,
					"location":   alert.Location,
				},
			}

			
			if err := notificationSender.SendNotification(payload); err != nil {
				logger.DefaultLogger.Errorf("Failed to send notification: %v", err)
			}
		}
	}()


	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.DefaultLogger.Info("Shutting down service...")
	cancel()
	time.Sleep(time.Second) 
}
