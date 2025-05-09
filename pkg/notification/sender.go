package notification

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"HKN/ai-notification/pkg/utils/logger"
)

type NotificationPayload struct {
	Title     string                 `json:"title"`
	Message   string                 `json:"message"`
	UserID    string                 `json:"user_id"`
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
}

type NotificationSender struct {
	apiEndpoint string
	httpClient  *http.Client
}

func NewNotificationSender(apiEndpoint string) *NotificationSender {
	return &NotificationSender{
		apiEndpoint: apiEndpoint,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (s *NotificationSender) SendNotification(payload NotificationPayload) error {
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal notification payload: %v", err)
	}

	req, err := http.NewRequest("POST", s.apiEndpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send notification: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("notification service returned non-200 status code: %d", resp.StatusCode)
	}

	logger.DefaultLogger.Infof("Successfully sent notification to user %s", payload.UserID)
	return nil
}
