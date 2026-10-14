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
)
