// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"encoding/json"
	"fmt"
	"time"
)

// Duration wraps time.Duration to support JSON unmarshaling from string values
// like "5s", "10m", "1h30m", etc.
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case string:
		dur, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", value, err)
		}
		d.Duration = dur
		return nil
	case float64:
		d.Duration = time.Duration(int64(value))
		return nil
	default:
		return fmt.Errorf("invalid duration type %T", v)
	}
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Duration.String())
}
