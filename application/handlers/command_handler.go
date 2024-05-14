package handlers

import "presentation-advert-consumer/application/commands"

type CommandHandler struct {
	IndexCategory CommandHandlerDecorator[*commands.IndexCategory, error]
	IndexAdvert   CommandHandlerDecorator[*commands.IndexAdvert, error]
}
