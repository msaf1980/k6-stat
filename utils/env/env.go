package env

import (
	"os"
	"strconv"
)

func GetEnv(key, defaultValue string) (v string) {
	v = os.Getenv(key)
	if v == "" {
		v = defaultValue
	}
	return
}

func GetEnvInt(key string, defaultValue int) (n int, err error) {
	v := os.Getenv(key)
	if v == "" {
		return defaultValue, err
	}

	if n, err = strconv.Atoi(v); err != nil {
		n = defaultValue
	}
	return
}
