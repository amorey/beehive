package beehive

import "time"

type GroupKind struct {
	Group string
	Kind  string
}

type ObjectID = int64

type Object[Spec, Status any] struct {
	ID                  ObjectID
	Group               string
	Kind                string
	Name                *string
	Spec                Spec
	Status              *Status
	Generation          int64
	ObservedGeneration  *int64
	ObservedAt          *time.Time
	ResourceVersion     int64
	DeletionRequestedAt *time.Time
	Finalizers          []string
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

type Result struct {
	RequeueAfter time.Duration
}

type ConditionStatus string

const (
	ConditionTrue    ConditionStatus = "True"
	ConditionFalse   ConditionStatus = "False"
	ConditionUnknown ConditionStatus = "Unknown"
)

type Condition struct {
	Type    string
	Status  ConditionStatus
	Reason  string
	Message string
}
