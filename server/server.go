package server

import (
	"context"
	"fmt"
	"sync"

	"github.com/gofiber/fiber/v2"
	"github.com/madflojo/tasks"
)

const (
	NodeTypeLeader    = "leader"
	NodeTypeFollower  = "follower"
	NodeTypeCandidate = "candidate"
)

const (
	ScheduleHeartbeat              = "heartbeat"
	ScheduleLeaderHeartbeatTimeout = "leader_heartbeat_timeout"
)

type Server struct {
	nodeType  string
	mtex      sync.Mutex
	cfg       Config
	app       *fiber.App
	scheduler *tasks.Scheduler
	leader    string
	term      int
}

func New(cfg Config) *Server {
	s := &Server{
		nodeType:  NodeTypeFollower,
		cfg:       cfg,
		app:       fiber.New(),
		scheduler: tasks.New(),
	}

	s.app.Post("/leader/heartbeat", s.LeaderHeartBeat)
	s.app.Post("/candidate/proposal", s.CandidateProposal)

	_ = s.scheduler.AddWithID(ScheduleLeaderHeartbeatTimeout, &tasks.Task{
		Interval: cfg.LeaderHeartbeatTimeout,
		TaskFunc: s.leaderHeartbeatTimeoutFunc,
		ErrFunc:  s.leaderHeartbeatTimeoutErrorFunc,
	})

	return s
}

func (s *Server) Start(ctx context.Context) error {
	return s.app.Listen(fmt.Sprintf(":%d", s.cfg.Port))
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.scheduler.Stop()
	return s.app.ShutdownWithContext(ctx)
}
