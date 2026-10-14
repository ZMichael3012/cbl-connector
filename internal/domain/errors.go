package domain

import "errors"

// Ошибки домена коннектора; транспорт маппит их в классы платформы.
var (
	// ErrUnknownState сообщает о неизвестном провайдерском статусе.
	ErrUnknownState = errors.New("connector: unknown provider state")
	// ErrInvalidRoute сообщает о некорректном запросе маршрутизации.
	ErrInvalidRoute = errors.New("connector: invalid route request")
	// ErrUnsupportedNetwork сообщает о неподдерживаемой сети или активе.
	ErrUnsupportedNetwork = errors.New("connector: unsupported network or asset")
	// ErrProviderUnavailable сообщает о недоступности провайдера.
	ErrProviderUnavailable = errors.New("connector: provider unavailable")
	// ErrUnsignedWebhook сообщает о вебхуке без подтвержденной подписи.
	ErrUnsignedWebhook = errors.New("connector: webhook signature not verified")
	// ErrUnknownProvider сообщает о вебхуке неизвестного провайдера.
	ErrUnknownProvider = errors.New("connector: unknown provider")
	// ErrInvalidWebhook сообщает о некорректном теле вебхука.
	ErrInvalidWebhook = errors.New("connector: invalid webhook payload")
	// ErrTemporary сообщает о временном сбое провайдера: вызов можно повторить.
	ErrTemporary = errors.New("connector: temporary provider failure")
	// ErrRejected сообщает об отказе провайдера на уровне бизнес-правил: повтор бесполезен.
	ErrRejected = errors.New("connector: provider rejected the request")
	// ErrProviderOverloaded сообщает об исчерпании bulkhead провайдера.
	ErrProviderOverloaded = errors.New("connector: provider overloaded")
	// ErrInvalidGatewayRequest сообщает о некорректном запросе к шлюзу (баг вызывающего).
	ErrInvalidGatewayRequest = errors.New("connector: invalid gateway request")
)
