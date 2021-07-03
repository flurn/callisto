package config

type LogConfig struct {
	logPath  string
	logLevel string
	format   string
}

func (dc LogConfig) LogPath() string {
	return dc.logPath
}

func (dc LogConfig) LogLevel() string {
	return dc.logLevel
}

func (dc LogConfig) LogFormat() string {
	return dc.format
}
