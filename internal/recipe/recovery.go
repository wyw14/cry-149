package recipe

import (
	"encoding/json"
	"fmt"
)

type Recovery struct {
	Plans map[string]Plan `json:"plans"`
}

func NewRecovery(plans map[string]Plan) Recovery {
	copyPlans := make(map[string]Plan, len(plans))
	for key, value := range plans {
		copyPlans[key] = value.Clone()
	}
	return Recovery{Plans: copyPlans}
}

func (r Recovery) Encode() ([]byte, error) {
	data, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("encode recipe recovery: %w", err)
	}
	return data, nil
}

func DecodeRecovery(data []byte) (Recovery, error) {
	var recovery Recovery
	if err := json.Unmarshal(data, &recovery); err != nil {
		return Recovery{}, fmt.Errorf("decode recipe recovery: %w", err)
	}
	for id, plan := range recovery.Plans {
		if err := plan.Validate(); err != nil {
			return Recovery{}, fmt.Errorf("recovered recipe %s: %w", id, err)
		}
		recovery.Plans[id] = plan.Clone()
	}
	return recovery, nil
}

func (r Recovery) Restore(registry *Registry) error {
	for _, plan := range r.Plans {
		if err := registry.Put(plan); err != nil {
			return err
		}
	}
	return nil
}
