package main

import (
	"HKN/ai-notification/conf"
	"HKN/ai-notification/pkg/notification"
	rediss "HKN/ai-notification/pkg/redis"
	"HKN/ai-notification/pkg/utils/logger"
	"context"
	firebase "firebase.google.com/go"
	"fmt"
	"github.com/redis/go-redis/v9"
	"google.golang.org/api/option"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormLogger "gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	conf.LoadConfig()
	config := conf.GetConfig()

	logger.Init(config.AppName)

	_ = initPostgres()
	client := initRedis()
	_ = initFirebase()

	streamHandler, err := rediss.NewStreamHandler(client, "sleeping-alerts", "notification-group")
	if err != nil {
		logger.DefaultLogger.Fatalf("Failed to initialize Redis Stream handler: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	alertChan := make(chan rediss.SleepingAlert, 100)

	go streamHandler.StartConsuming(ctx, alertChan)

	alertingMap := make(map[string]chan bool)
	var alertingMapMu sync.Mutex

	go func() {
		for alert := range alertChan {
			payload := notification.NotificationPayload{
				Title:    "Sleeping Alert",
				Message:  fmt.Sprintf("Detected sleeping at %s with confidence %.2f", alert.Location, alert.Confidence),
				DriverID: alert.DriverID,
				Data: map[string]interface{}{
					"confidence": alert.Confidence,
					"location":   alert.Location,
				},
			}

			alertingMapMu.Lock()
			if alert.Confidence >= 0.8 {
				if _, exists := alertingMap[alert.DriverID]; !exists {
					stopChan := make(chan bool)
					alertingMap[alert.DriverID] = stopChan

					go func(userID string, stop <-chan bool, payload notification.NotificationPayload) {
						ticker := time.NewTicker(5 * time.Second)
						defer ticker.Stop()

						logger.DefaultLogger.Infof("Start alerting for user: %s", userID)

						for {
							select {
							case <-ticker.C:
								// TODO send noti
								log.Println("Receive alert")
							case <-stop:
								logger.DefaultLogger.Infof("Stopped alerting for user: %s", userID)
								return
							}
						}
					}(alert.DriverID, stopChan, payload)
				}
			} else {
				if stopChan, exists := alertingMap[alert.DriverID]; exists {
					stopChan <- true
					delete(alertingMap, alert.DriverID)
				}
			}
			alertingMapMu.Unlock()
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.DefaultLogger.Info("Shutting down service...")
	cancel()
	time.Sleep(time.Second)
}

func initPostgres() *gorm.DB {
	dsn := postgres.Open(fmt.Sprintf(
		"host=%s port=%s user=%s dbname=%s password=%s sslmode=disable connect_timeout=5",
		conf.GetConfig().DBHost,
		conf.GetConfig().DBPort,
		conf.GetConfig().DBUser,
		conf.GetConfig().DBName,
		conf.GetConfig().DBPass,
	))
	db, err := gorm.Open(dsn, &gorm.Config{
		NamingStrategy: &schema.NamingStrategy{
			SingularTable: true,
			//TablePrefix:   conf.GetConfig().DBSchema + ".",
		},
		Logger: gormLogger.Default.LogMode(gormLogger.Info),
	})
	if err != nil {
		log.Fatalf("error opening connection to database: %v", err)
	}

	conn, err := db.DB()
	if err != nil {
		log.Fatalf("error initializing database: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	if err = conn.PingContext(ctx); err != nil {
		log.Fatalf("error opening connection to database: %v", err)
	}
	log.Println("Postgres connected!")

	return db
}

func initRedis() *redis.Client {
	options := &redis.Options{
		Addr:     conf.GetConfig().Host + ":" + conf.GetConfig().RedisPort,
		Password: conf.GetConfig().RedisPassword,
		DB:       conf.GetConfig().RedisDB,
	}

	client := redis.NewClient(options)
	ctx := context.Background()
	_, err := client.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Could not connect to Redis: %v", err)
	}

	log.Println("Redis connected!")
	return client
}

func initFirebase() *firebase.App {
	opt := option.WithCredentialsFile("backend-hkn-firebase-adminsdk-y44p2-75ebc13d83.json")
	app, err := firebase.NewApp(context.Background(), nil, opt)
	if err != nil {
		log.Fatalf("Could not initializing Firebase: %v", err)
	}

	log.Println("Firebase connected!")
	return app
}
