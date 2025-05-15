package main

import (
	"HKN/ai-notification/conf"
	"HKN/ai-notification/pkg/utils/logger"
	"context"
	"encoding/json"
	firebase "firebase.google.com/go"
	"fmt"
	"github.com/redis/go-redis/v9"
	"google.golang.org/api/option"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormLogger "gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
	"log"
	"time"
)

type AlertData struct {
	ID         string  `json:"id"`
	DriverID   string  `json:"driver_id"`
	Confidence float64 `json:"confidence"`
	Location   string  `json:"location"`
	Status     string  `json:"status"`
}

var driverStatus = make(map[string]string)
var cancelFuncs = make(map[string]context.CancelFunc)
var lastSentTime = make(map[string]time.Time)

func main() {
	conf.LoadConfig()
	config := conf.GetConfig()

	logger.Init(config.AppName)

	_ = initPostgres()
	client := initRedis()
	_ = initFirebase()

	go consumeRedisStream(client)

	select {}
}

func consumeRedisStream(redisClient *redis.Client) {
	lastID := "$"
	ctx := context.Background()
	for {
		streams, err := redisClient.XRead(ctx, &redis.XReadArgs{
			Streams: []string{"sleeping-alerts", lastID},
			Block:   0,
		}).Result()
		if err != nil {
			log.Printf("Redis read error: %v\n", err)
			continue
		}

		for _, stream := range streams {
			for _, message := range stream.Messages {
				lastID = message.ID

				rawData := message.Values["data"].(string)
				var alert AlertData
				if err := json.Unmarshal([]byte(rawData), &alert); err != nil {
					log.Printf("JSON parse error: %v\n", err)
					continue
				}

				handleAlert(alert)
			}
		}
	}
}

func handleAlert(alert AlertData) {
	driverID := alert.DriverID
	status := alert.Status

	if driverStatus[driverID] == status {
		return
	}

	driverStatus[driverID] = status

	if status == "sleeping" {
		sendNotification(alert)

		ctx, cancel := context.WithCancel(context.Background())
		cancelFuncs[driverID] = cancel

		go sendNotificationEvery5s(ctx, alert)

	} else if status == "normal" {
		if cancel, ok := cancelFuncs[driverID]; ok {
			cancel()
			delete(cancelFuncs, driverID)
			log.Printf("Driver %s recoverd (status: normal)\n", driverID)
		}
	}
}

func sendNotificationEvery5s(ctx context.Context, alert AlertData) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sendNotification(alert)
		}
	}
}

func sendNotification(alert AlertData) {
	driverID := alert.DriverID
	now := time.Now()

	if last, ok := lastSentTime[driverID]; ok {
		if now.Sub(last) < 5*time.Second {
			log.Printf("Skip noti for %s (less than 5s since last)\n", driverID)
			return
		}
	}

	lastSentTime[driverID] = now
	// TODO send notification
	log.Printf("ALERT SLEEPING: DriverID=%s, Location=%s\n", alert.DriverID, alert.Location)
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
