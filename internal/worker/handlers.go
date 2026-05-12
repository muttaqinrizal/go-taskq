package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/muttaqinrizal/go-taskq/internal/domain"
)

// EmailPayload represents the data needed to send an email.
type EmailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

// HandleSendEmail is a sample handler that simulates sending an email.
func HandleSendEmail(ctx context.Context, job *domain.Job) error {
	var payload EmailPayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return fmt.Errorf("invalid payload: %w", err)
	}

	log.Printf("[Email Task] Sending email to: %s, Subject: '%s'\n", payload.To, payload.Subject)
	
	// Simulate work (e.g., calling an external API like SendGrid or AWS SES)
	time.Sleep(2 * time.Second)

	log.Printf("[Email Task] Email sent successfully to %s\n", payload.To)
	return nil
}

// ImagePayload represents the data needed to resize an image.
type ImagePayload struct {
	ImageURL string `json:"image_url"`
	Width    int    `json:"width"`
	Height   int    `json:"height"`
}

// HandleResizeImage is a sample handler that simulates image processing.
func HandleResizeImage(ctx context.Context, job *domain.Job) error {
	var payload ImagePayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return fmt.Errorf("invalid payload: %w", err)
	}

	log.Printf("[Image Task] Resizing image from %s to %dx%d\n", payload.ImageURL, payload.Width, payload.Height)
	
	// Simulate heavy CPU work
	time.Sleep(5 * time.Second)

	log.Printf("[Image Task] Image resized successfully\n")
	return nil
}
