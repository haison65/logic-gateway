package logger

import (
	"fmt"
	"os"
	"path/filepath"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// New tạo zap logger JSON với các khóa: time, level, module, line, Message.
func New(debug bool, module string) *zap.Logger {
	lvl := zap.InfoLevel
	if debug {
		lvl = zap.DebugLevel
	}
	enc := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "module",
		CallerKey:      "line",
		MessageKey:     "Message",
		StacktraceKey:  "",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   encodeCaller,
		EncodeName:     zapcore.FullNameEncoder,
	}
	core := zapcore.NewCore(zapcore.NewJSONEncoder(enc), zapcore.AddSync(os.Stdout), lvl)
	log := zap.New(core, zap.AddCaller())
	if module != "" {
		log = log.Named(module)
	}
	return log
}

func encodeCaller(c zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(fmt.Sprintf("%s:%d", filepath.Base(c.File), c.Line))
}

// OrNop trả về Nop nếu log nil.
func OrNop(log *zap.Logger) *zap.Logger {
	if log == nil {
		return zap.NewNop()
	}
	return log
}
