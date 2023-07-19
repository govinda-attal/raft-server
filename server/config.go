package server

import "time"

type Config struct {
	Port                   int           `yaml:"port"`
	HeartbeatInterval      time.Duration `yaml:"heartbeat_interval"`
	LeaderHeartbeatTimeout time.Duration `yaml:"leader_heartbeat_timeout"`
	Node                   string        `yaml:"node"`
	Peers                  []string      `yaml:"peers"`
}
