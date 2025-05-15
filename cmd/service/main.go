package main

import (
	"HKN/ai-notification/conf"
	"HKN/ai-notification/pkg/utils/logger"
	"context"
	"encoding/json"
	"errors"
	firebase "firebase.google.com/go"
	"firebase.google.com/go/messaging"
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
	DriverID   int     `json:"driver_id"`
	Confidence float64 `json:"confidence"`
	Location   string  `json:"location"`
	Status     string  `json:"status"`
}

type UserDeviceFcmToken struct {
	UserID         int    `json:"user_id"`
	UserRole       string `json:"user_role"`
	DeviceFCMToken string `json:"device_fcm_token"`
}

type FCMMessage struct {
	To           string          `json:"to"`
	Notification FCMNotification `json:"notification"`
}

type FCMNotification struct {
	Title string            `json:"title"`
	Data  map[string]string `json:"data"`
}

var driverStatus = make(map[int]string)
var cancelFunc = make(map[int]context.CancelFunc)
var lastSentTime = make(map[int]time.Time)

func main() {
	conf.LoadConfig()
	config := conf.GetConfig()

	logger.Init(config.AppName)

	db := initPostgres()
	client := initRedis()
	firebaseApp := initFirebase()

	go consumeRedisStream(client, db, firebaseApp)

	select {}
}

func consumeRedisStream(redisClient *redis.Client, db *gorm.DB, fcm *firebase.App) {
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

				handleAlert(db, fcm, alert)
			}
		}
	}
}

func handleAlert(db *gorm.DB, fcm *firebase.App, alert AlertData) {
	driverID := alert.DriverID
	status := alert.Status

	if driverStatus[driverID] == status {
		return
	}

	driverStatus[driverID] = status

	if status == "sleeping" {
		sendNotification(db, alert, fcm)

		ctx, cancel := context.WithCancel(context.Background())
		cancelFunc[driverID] = cancel

		go sendNotificationEvery5s(ctx, db, fcm, alert)

	} else if status == "normal" {
		if cancel, ok := cancelFunc[driverID]; ok {
			cancel()
			delete(cancelFunc, driverID)
			log.Printf("Driver %d recoverd (status: normal)\n", driverID)
		}
	}
}

func sendNotificationEvery5s(ctx context.Context, db *gorm.DB, fcm *firebase.App, alert AlertData) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sendNotification(db, alert, fcm)
		}
	}
}

func sendNotification(db *gorm.DB, alert AlertData, fcm *firebase.App) {
	driverID := alert.DriverID
	now := time.Now()

	if last, ok := lastSentTime[driverID]; ok {
		if now.Sub(last) < 5*time.Second {
			log.Printf("Skip noti for %d (less than 5s since last)\n", driverID)
			return
		}
	}

	lastSentTime[driverID] = now
	// TODO send notification
	token, err := getDeviceToken(db, driverID)
	if err != nil {
		log.Println(err, fmt.Sprintf("Failed to get driver token firebase"))
	}

	title := fmt.Sprintf("ALERT SLEEPING: DriverID=%d, Location=%s\n", driverID, alert.Location)
	convertData := map[string]string{
		"code_message": "driver_sleeping_alert",
		"driver_id":    fmt.Sprintf("%d", driverID),
		"confidence":   fmt.Sprintf("%f", alert.Confidence),
		"status":       alert.Status,
		"location":     alert.Location,
	}

	fcMessage := FCMMessage{
		To: *token,
		Notification: FCMNotification{
			Title: title,
			Data:  convertData,
		},
	}

	err = sendFCMMessage(fcMessage, fcm)
	if err != nil {
		log.Println(err, "Error when sending fcm message")
	}

	//log.Printf("ALERT SLEEPING: DriverID=%s, Location=%s\n", alert.DriverID, alert.Location)
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
	opt := option.WithCredentialsFile("backend-hkn-firebase-y44p2-75ebc13d83.json")
	app, err := firebase.NewApp(context.Background(), nil, opt)
	if err != nil {
		log.Fatalf("Could not initializing Firebase: %v", err)
	}

	log.Println("Firebase connected!")
	return app
}

func sendFCMMessage(message FCMMessage, fcm *firebase.App) error {
	ctx := context.Background()

	// Obtain a messaging client from the Firebase app
	client, err := fcm.Messaging(ctx)
	if err != nil {
		log.Println(err, "error getting Messaging client.")
	}

	// Define the message to be sent
	messageSend := &messaging.Message{
		Notification: &messaging.Notification{
			Title: message.Notification.Title,
		},
		Token: message.To,
		Data:  message.Notification.Data,
	}

	// Send the message
	response, err := client.Send(ctx, messageSend)
	if err != nil {
		log.Println(err, "error sending message.")
		return err
	}

	// Log the response from the FCM service
	log.Printf("Successfully sent message: %s\n", response)
	return nil
}

func getDeviceToken(db *gorm.DB, userID int) (*string, error) {
	var token UserDeviceFcmToken
	err := db.Table("user_device_fcm_token").Where("user_id = ? AND user_role = ?", userID, "driver").First(&token).Error
	if err != nil {
		log.Println(err, "Device token not found.")
		if errors.Is(err, gorm.ErrRecordNotFound) {
			log.Println(err, "Device token not found.")
			return nil, err
		}
		return nil, err
	}

	return &token.DeviceFCMToken, nil
}
