package events

// Broadcast - глобальный канал для отправки событий из планировщика в веб-сокеты
var Broadcast = make(chan []byte, 100)
