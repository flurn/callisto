package config

import (
	"fmt"
	"github.com/spf13/viper"
	"strconv"
)

func mustGetString(key string) string {
	mustHave(key)
	return viper.GetString(key)
}

func mustGetInt(key string) int {
	mustHave(key)
	v, err := strconv.Atoi(viper.GetString(key))
	if err != nil {
		panic(fmt.Sprintf("key %s is not a valid Integer value", key))
	}

	return v
}

func mustGetBool(key string) bool {
	mustHave(key)
	return viper.GetBool(key)
}

func mustHave(key string) {
	if !viper.IsSet(key) {
		panic(fmt.Sprintf("key %s is not set", key))
	}
}
