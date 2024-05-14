package log

import (
	"github.com/labstack/gommon/log"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"presentation-advert-consumer/infrastructure/configuration/custom_json"
)

var logLevels = map[string]log.Lvl{
	"INFO":  log.INFO,
	"DEBUG": log.DEBUG,
	"WARN":  log.WARN,
	"ERROR": log.ERROR,
}

func retrieveLogLevel(levelName string) log.Lvl {
	if level, exist := logLevels[levelName]; exist {
		return level
	}
	return logLevels["INFO"]
}

type Logger struct {
	*logrus.Logger
}

var logger = &Logger{logrus.New()}

func NewLogger(level string) *Logger {
	logger = &Logger{logrus.New()}
	logger.SetOutput(os.Stdout)
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetLevel(retrieveLogLevel(level))
	return logger
}

func Print(i ...interface{}) {
	logger.Print(i...)
}

func Printf(format string, i ...interface{}) {
	logger.Printf(format, i...)
}

func Debug(i ...interface{}) {
	logger.Debug(i...)
}

func Debugf(format string, args ...interface{}) {
	logger.Debugf(format, args...)
}

func Info(i ...interface{}) {
	logger.Info(i...)
}

func Infof(format string, args ...interface{}) {
	logger.Infof(format, args...)
}

func Warn(i ...interface{}) {
	logger.Warn(i...)
}

func Warnf(format string, args ...interface{}) {
	logger.Warnf(format, args...)
}

func Error(i ...interface{}) {
	logger.Error(i...)
}

func Errorf(format string, args ...interface{}) {
	logger.Errorf(format, args...)
}

func InfofWithFields(fields logrus.Fields, format string, args ...interface{}) {
	logger.WithFields(fields).Infof(format, args...)
}

func InfoWithFields(fields logrus.Fields, args ...interface{}) {
	logger.WithFields(fields).Info(args...)
}

func WarnfWithFields(fields logrus.Fields, format string, args ...interface{}) {
	logger.WithFields(fields).Warnf(format, args...)
}

func ErrorfWithFields(fields logrus.Fields, format string, args ...interface{}) {
	logger.WithFields(fields).Errorf(format, args...)
}

func ErrorWithFields(fields logrus.Fields, args ...interface{}) {
	logger.WithFields(fields).Error(args...)
}

func Fatal(i ...interface{}) {
	logger.Fatal(i...)
}

func Fatalf(format string, args ...interface{}) {
	logger.Fatalf(format, args...)
}

func Panic(i ...interface{}) {
	logger.Panic(i...)
}

func Panicf(format string, args ...interface{}) {
	logger.Panicf(format, args...)
}

func toLogrusLevel(level log.Lvl) logrus.Level {
	switch level {
	case log.DEBUG:
		return logrus.DebugLevel
	case log.INFO:
		return logrus.InfoLevel
	case log.WARN:
		return logrus.WarnLevel
	case log.ERROR:
		return logrus.ErrorLevel
	}

	return logrus.InfoLevel
}

func toEchoLevel(level logrus.Level) log.Lvl {
	switch level {
	case logrus.DebugLevel:
		return log.DEBUG
	case logrus.InfoLevel:
		return log.INFO
	case logrus.WarnLevel:
		return log.WARN
	case logrus.ErrorLevel:
		return log.ERROR
	}

	return log.OFF
}

func (l *Logger) IsDebugLevel() bool {
	return l.GetLevel() == logrus.DebugLevel
}

func (l *Logger) Output() io.Writer {
	return l.Out
}

func (l *Logger) SetOutput(w io.Writer) {
	l.Out = w
}

func (l *Logger) Level() log.Lvl {
	return toEchoLevel(l.Logger.Level)
}

func (l *Logger) Lvl() string {
	return l.Logger.Level.String()
}

func (l *Logger) SetLevel(v log.Lvl) {
	l.Logger.Level = toLogrusLevel(v)
}

func (l *Logger) SetHeader(h string) {
	// do nothing
}

func (l *Logger) Formatter() logrus.Formatter {
	return l.Logger.Formatter
}

func (l *Logger) SetFormatter(formatter logrus.Formatter) {
	l.Logger.Formatter = formatter
}

func (l *Logger) Prefix() string {
	return ""
}

func (l *Logger) SetPrefix(p string) {
	// do nothing
}

func (l *Logger) Print(i ...interface{}) {
	l.Logger.Print(i...)
}

func (l *Logger) Printf(format string, args ...interface{}) {
	l.Logger.Printf(format, args...)
}

func (l *Logger) Println(v ...interface{}) {
	l.Logger.Println(v...)
}

func (l *Logger) Printj(j log.JSON) {
	b, err := custom_json.Marshal(j)
	if err != nil {
		panic(err)
	}
	l.Logger.Println(string(b))
}

func (l *Logger) Debug(i ...interface{}) {
	l.Logger.Debug(i...)
}

func (l *Logger) Debugf(format string, args ...interface{}) {
	l.Logger.Debugf(format, args...)
}

func (l *Logger) Debugj(j log.JSON) {
	b, err := custom_json.Marshal(j)
	if err != nil {
		panic(err)
	}
	l.Logger.Debugln(string(b))
}

func (l *Logger) Info(i ...interface{}) {
	l.Logger.Info(i...)
}

func (l *Logger) Infof(format string, args ...interface{}) {
	l.Logger.Infof(format, args...)
}

func (l *Logger) Infoj(j log.JSON) {
	b, err := custom_json.Marshal(j)
	if err != nil {
		panic(err)
	}
	l.Logger.Infoln(string(b))
}

func (l *Logger) Warn(i ...interface{}) {
	l.Logger.Warn(i...)
}

func (l *Logger) Warnf(format string, args ...interface{}) {
	l.Logger.Warnf(format, args...)
}

func (l *Logger) Warnj(j log.JSON) {
	b, err := custom_json.Marshal(j)
	if err != nil {
		panic(err)
	}
	l.Logger.Warnln(string(b))
}

func (l *Logger) Error(i ...interface{}) {
	l.Logger.Error(i...)
}

func (l *Logger) Errorf(format string, args ...interface{}) {
	l.Logger.Errorf(format, args...)
}

func (l *Logger) Errorj(j log.JSON) {
	b, err := custom_json.Marshal(j)
	if err != nil {
		panic(err)
	}
	l.Logger.Errorln(string(b))
}

func (l *Logger) Fatal(i ...interface{}) {
	l.Logger.Fatal(i...)
}

func (l *Logger) Fatalf(format string, args ...interface{}) {
	l.Logger.Fatalf(format, args...)
}

func (l *Logger) Fatalj(j log.JSON) {
	b, err := custom_json.Marshal(j)
	if err != nil {
		panic(err)
	}
	l.Logger.Fatalln(string(b))
}

func (l *Logger) Panic(i ...interface{}) {
	l.Logger.Panic(i...)
}

func (l *Logger) Panicf(format string, args ...interface{}) {
	l.Logger.Panicf(format, args...)
}

func (l *Logger) Panicj(j log.JSON) {
	b, err := custom_json.Marshal(j)
	if err != nil {
		panic(err)
	}
	l.Logger.Panicln(string(b))
}
