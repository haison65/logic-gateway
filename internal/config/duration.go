package config

import (
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration bọc time.Duration để đọc chuỗi YAML kiểu "1000ms".
type Duration time.Duration

func (d Duration) Duration() time.Duration {
	return time.Duration(d)
}

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	if value == nil {
		return fmt.Errorf("duration is nil")
	}
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("parse duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}
