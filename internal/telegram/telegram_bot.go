package telegram

import (
	"fmt"
	"log"

	"github.com/go-telegram-bot-api/telegram-bot-api"
)

// SendTelegramAlert sends a message to Telegram.
func SendTelegramAlert(bot *tgbotapi.BotAPI, chatID int64, message string) error {
	msg := tgbotapi.NewMessage(chatID, message)
	_, err := bot.Send(msg)
	if err != nil {
		log.Printf("Failed to send Telegram message: %v", err)
		return fmt.Errorf("failed to send telegram message: %w", err)
	}
	return nil
}
