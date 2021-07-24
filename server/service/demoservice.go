package service

import (
	"github.com/flurn/callisto/logger"
	"github.com/golang/protobuf/proto"
	"net/http"
)

type UpdateLogMessage struct {
	Name string `protobuf:"bytes,1,opt,name=name,json=name,proto3" json:"name,omitempty"`
}

func (m *UpdateLogMessage) Reset() { *m = UpdateLogMessage{} }

func (m *UpdateLogMessage) String() string { return proto.CompactTextString(m) }

func (*UpdateLogMessage) ProtoMessage() {}

func PushMessage(producer *Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		logger := logger.WithRequest(r)
		logger.Infof("Printing logs")
		producer.kafka.Push(r.Context(), []byte{}, "topicName")
	}
}

func WriteToDatastore() func(msg []byte) error {
	return func(msg []byte) error {
		logger := logger.GetLogger()
		logger.Infof("Writing to datastore")
		var message UpdateLogMessage
		err := proto.Unmarshal(msg, &message)
		if err != nil {
			return err
		}
		return nil
	}
}
