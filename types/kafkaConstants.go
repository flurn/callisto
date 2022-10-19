package types

const DLQ_Postfix = "_dlq"
const Retry_Postfix = "_retry_"

type OffsetType string

const (
	OffsetTypeEarliest OffsetType = "earliest"
	OffsetTypeLatest   OffsetType = "latest"
)
