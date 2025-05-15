package conf

import (
	"log"
	"os"

	"github.com/caarlos0/env/v6"
	"github.com/joho/godotenv"
)

// AppConfig presents app conf
type AppConfig struct {
	AppName string `env:"APP_NAME" envDefault:"Go-Wedding"`
	Host    string `env:"HOST"  envDefault:"localhost"`
	Port    string `env:"PORT" envDefault:"8001"`

	// Redis Config
	RedisHost     string `env:"REDIS_HOST" envDefault:"localhost"`
	RedisPort     string `env:"REDIS_PORT" envDefault:"6379"`
	RedisPassword string `env:"REDIS_PASSWORD" envDefault:"your_password"`
	RedisDB       int    `env:"REDIS_DB" envDefault:"0"`

	// Notification Config
	NotificationEndpoint string `env:"NOTIFICATION_ENDPOINT" envDefault:"http://localhost:8000/api/notifications"`

	//DB CONFIG
	LogFormat       string `env:"LOG_FORMAT" envDefault:"127.0.0.1"`
	DBHost          string `env:"DB_HOST" envDefault:"localhost"`
	DBPort          string `env:"DB_PORT" envDefault:"5432"`
	DBUser          string `env:"DB_USER" envDefault:"postgres"`
	DBPass          string `env:"DB_PASS" envDefault:"postgres"`
	DBName          string `env:"DB_NAME" envDefault:"postgres"`
	DBSchema        string `env:"DB_SCHEMA" envDefault:"public"`
	DBReplicaDSN    string `env:"DB_REPLICA_DSN" envDefault:"host=127.0.0.1 port=5432 user=postgres dbname=todo_item password=postgres connect_timeout=10"`
	DBSSLRootCert   string `env:"DB_SSL_ROOT_CERT" envDefault:"resources/db/ca.crt"`
	DBSSLClientCert string `env:"DB_SSL_CERT" envDefault:"resources/db/client.crt"`
	DBSSLClientKey  string `env:"DB_SSL_KEY" envDefault:"resources/db/client.key"`
	DBSSLMode       string `env:"DB_SSL_MODE" envDefault:"disable"`
	EnableDB        string `env:"ENABLE_DB" envDefault:"true"`

	// ENV
	EnvName string `env:"ENV_NAME" envDefault:"dev"`

	// LOGGER
	LoggerLevel  string `env:"LOGGER_LEVEL" envDefault:"debug"`
	LoggerFormat string `env:"LOGGER_FORMAT" envDefault:"json"`

	// SWAGGER
	SwaggerHost string `env:"SWAGGER_HOST" envDefault:"localhost"`
}

var config AppConfig

func LoadConfig() {
	if _, err := os.Stat(".env"); err == nil {
		if err = godotenv.Load(); err != nil {
			log.Printf("Error loading .env file")
		}
	}

	if err := env.Parse(&config); err != nil {
		log.Fatalf("Error parsing environment variables: %v", err)
	}
}

func GetConfig() AppConfig {
	return config
}
