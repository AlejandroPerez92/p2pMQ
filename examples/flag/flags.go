package flag

import (
	"flag"
)

type Config struct {
	ListenPort int
}

func ParseFlags() *Config {
	c := &Config{}

	flag.IntVar(&c.ListenPort, "port", 0, "node listen port (0 pick a random unused port)")

	flag.Parse()
	return c
}
