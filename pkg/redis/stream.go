package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"HKN/ai-notification/pkg/utils/logger"

	"github.com/redis/go-redis/v9"
)

type SleepingAlert struct {
	ID         string  `json:"id"`
	DriverID   string  `json:"driver_id"`
	Confidence float64 `json:"confidence"`
	Location   string  `json:"location"`
}

type StreamHandler struct {
	client    *redis.Client
	streamKey string
	groupName string
}

func NewStreamHandler(client *redis.Client, streamKey, groupName string) (*StreamHandler, error) {
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %v", err)
	}

	err := client.XGroupCreateMkStream(ctx, streamKey, groupName, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return nil, fmt.Errorf("failed to create consumer group: %v", err)
	}

	return &StreamHandler{
		client:    client,
		streamKey: streamKey,
		groupName: groupName,
	}, nil
}

func (h *StreamHandler) StartConsuming(ctx context.Context, alertChan chan<- SleepingAlert) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			streams, err := h.client.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    h.groupName,
				Consumer: "notification-service",
				Streams:  []string{h.streamKey, ">"},
				Count:    1,
				Block:    0,
			}).Result()

			if err != nil {
				logger.DefaultLogger.Errorf("Error reading from stream: %v", err)
				time.Sleep(time.Second)
				continue
			}

			for _, stream := range streams {
				for _, message := range stream.Messages {
					var alert SleepingAlert
					if err := json.Unmarshal([]byte(message.Values["data"].(string)), &alert); err != nil {
						logger.DefaultLogger.Errorf("Error unmarshaling alert: %v", err)
						continue
					}

					alertChan <- alert

					if err := h.client.XAck(ctx, h.streamKey, h.groupName, message.ID).Err(); err != nil {
						logger.DefaultLogger.Errorf("Error acknowledging message: %v", err)
					}
				}
			}
		}
	}
}
