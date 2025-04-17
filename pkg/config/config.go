package config

import (
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// Config holds the application configuration
type Config struct {
	Server   ServerConfig   `mapstructure:"server"`
	Timeplus TimeplusConfig `mapstructure:"timeplus"`
}

// ServerConfig holds the HTTP server configuration
type ServerConfig struct {
	Port            string `mapstructure:"port"`
	AllowedOrigins  string `mapstructure:"allowedOrigins"`
	ShutdownTimeout int    `mapstructure:"shutdownTimeout"`
}

// TimeplusConfig holds the Timeplus connection configuration
type TimeplusConfig struct {
	Address   string `mapstructure:"address"`
	Password  string `mapstructure:"password"`
	Username  string `mapstructure:"username"`
	Workspace string `mapstructure:"workspace"`
}

// LoadConfig loads the application configuration from file or environment variables
func LoadConfig(configPath string) (*Config, error) {
	var config Config

	// Set default values
	viper.SetDefault("server.port", "8080")
	viper.SetDefault("server.allowedOrigins", "*")
	viper.SetDefault("server.shutdownTimeout", 10)

	// Allow environment variables to override config file
	viper.SetEnvPrefix("TP_ALERT")
	viper.AutomaticEnv()

	// If config file is provided, read it
	if configPath != "" {
		viper.SetConfigFile(configPath)
		if err := viper.ReadInConfig(); err != nil {
			logrus.Warnf("Error reading config file: %v", err)
		}
	}

	// Unmarshal config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, err
	}

	return &config, nil
}
