package broker

type Message struct {
	Topic string
	Key   string
	Value []byte
}

type Topic struct {
	Name     string
	Messages chan Message
}
