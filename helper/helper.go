package helper

import (
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/flurn/callisto/types"
)

func GetNextRetryTopicName(topic string, retryCounter int) string {
	baseTopicName := strings.Split(topic, types.Retry_Postfix)
	if len(baseTopicName) > 1 {
		topic = baseTopicName[0]
	}
	return topic + types.Retry_Postfix + strconv.Itoa(retryCounter)
}

// retry time in milliseconds
// for Exponential backOff
// constrain => baseTime^retryCount < 2^64 = 1024^10 = 1.0e+20
func GetBackOffTimeInMilliSeconds(retryCount int, retryType types.RetryType) int {
	baseTime := int(time.Second.Microseconds())
	maxRetryTime := (time.Minute * 5).Milliseconds()
	second := time.Second.Milliseconds()
	customRetryBackOff := map[int]int64{
		0: second,
		1: 5 * second,
		2: 30 * second,
		3: 60 * second,
		4: 5 * 60 * second,
	}

	var backOffTime int
	switch retryType {
	case types.RetryTypeExponential:
		backOffTime = baseTime << uint(retryCount)
	case types.RetryTypeCustom:
		if val, ok := customRetryBackOff[retryCount]; ok {
			backOffTime = int(val)
		} else {
			backOffTime = int(maxRetryTime)
		}
	case types.RetryTypeFixed:
		backOffTime = baseTime
	case types.RetryTypeFifo:
		backOffTime = baseTime
	default:
		backOffTime = baseTime
	}

	return int(math.Min(float64(backOffTime), float64(maxRetryTime)))
}

func GetDLQTopicName(topic string) string {
	split := strings.Split(topic, types.Retry_Postfix)
	if len(split) > 1 {
		return split[0] + types.DLQ_Postfix
	}
	return topic + types.DLQ_Postfix
}
