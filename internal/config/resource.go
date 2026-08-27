package config

import (
	"fmt"
	"strings"
)

// Resource là cấu hình tài nguyên ứng dụng (metadata).
// Limit OS/container thật phải cấu hình ở Docker Compose / cgroup — không enforce trong process.
type Resource struct {
	CPUCores float64 `yaml:"cpu_cores"` // vd 4, 6
	Memory   string  `yaml:"memory"`    // vd "8G", "512M"
}

// ParseMemoryBytes đổi "8G"/"8Gi"/"512M" → bytes. Chuỗi rỗng → 0, nil error.
func ParseMemoryBytes(s string) (uint64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" {
		return 0, nil
	}
	var mult uint64 = 1
	switch {
	case strings.HasSuffix(s, "GI"):
		mult = 1 << 30
		s = strings.TrimSuffix(s, "GI")
	case strings.HasSuffix(s, "G"):
		mult = 1 << 30
		s = strings.TrimSuffix(s, "G")
	case strings.HasSuffix(s, "MI"):
		mult = 1 << 20
		s = strings.TrimSuffix(s, "MI")
	case strings.HasSuffix(s, "M"):
		mult = 1 << 20
		s = strings.TrimSuffix(s, "M")
	case strings.HasSuffix(s, "KI"):
		mult = 1 << 10
		s = strings.TrimSuffix(s, "KI")
	case strings.HasSuffix(s, "K"):
		mult = 1 << 10
		s = strings.TrimSuffix(s, "K")
	}
	var n float64
	if _, err := fmt.Sscanf(s, "%f", &n); err != nil || n < 0 {
		return 0, fmt.Errorf("invalid memory %q", s)
	}
	return uint64(n * float64(mult)), nil
}

func (r Resource) Validate(prefix string) error {
	if r.CPUCores < 0 {
		return fmt.Errorf("%s.cpu_cores must be >= 0", prefix)
	}
	if _, err := ParseMemoryBytes(r.Memory); err != nil {
		return fmt.Errorf("%s.memory: %w", prefix, err)
	}
	return nil
}

func (r Resource) MemoryBytes() uint64 {
	n, _ := ParseMemoryBytes(r.Memory)
	return n
}
