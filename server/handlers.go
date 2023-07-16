package server

import (
	"github.com/gofiber/fiber/v2"
	"github.com/madflojo/tasks"
	"github.com/valyala/fasthttp"
	"golang.org/x/exp/slog"
)

func (s *Server) LeaderHeartBeat(c *fiber.Ctx) error {
	var rq LeaderHeartbeat
	if err := c.BodyParser(&rq); err != nil {
		return err
	}
	if rq.Leader != s.leader {
		slog.Warn("heartbeat rejected from leader", "leader", rq.Leader)
		rs := LeaderHeartbeatAck{
			Leader: rq.Leader,
			Ack:    false,
			Node:   s.cfg.Node,
		}
		return c.Status(fasthttp.StatusPreconditionFailed).JSON(&rs)
	}

	s.scheduler.Del(ScheduleLeaderHeartbeatTimeout)

	_ = s.scheduler.AddWithID(ScheduleLeaderHeartbeatTimeout, &tasks.Task{
		Interval: s.cfg.LeaderHeartbeatTimeout,
		TaskFunc: s.leaderHeartbeatTimeoutFunc,
		ErrFunc:  s.leaderHeartbeatTimeoutErrorFunc,
	})

	rs := LeaderHeartbeatAck{
		Leader: rq.Leader,
		Ack:    true,
		Node:   s.cfg.Node,
	}
	slog.Info("acknowledged heartbeat from leader", "leader", rq.Leader)
	return c.JSON(&rs)
}

func (s *Server) CandidateProposal(c *fiber.Ctx) error {
	var rq CandidateProposal
	if err := c.BodyParser(&rq); err != nil {
		return err
	}
	slog.Info("received a candidate proposal", "candidate", rq.Candidate)

	s.registerNewCandidate(rq.Candidate)

	rs := CandidateProposalAck{
		Candidate: rq.Candidate,
		Ack:       true,
		Node:      s.cfg.Node,
	}
	return c.JSON(&rs)
}

func (s *Server) registerNewCandidate(candidate string) {
	s.mtex.Lock()
	defer s.mtex.Unlock()
	s.nodeType = NodeTypeFollower
	s.candidate = candidate
	s.leader = ""

	s.scheduler.Del(ScheduleLeaderHeartbeatTimeout)

	_ = s.scheduler.AddWithID(ScheduleLeaderHeartbeatTimeout, &tasks.Task{
		Interval: s.cfg.LeaderHeartbeatTimeout,
		TaskFunc: s.leaderHeartbeatTimeoutFunc,
		ErrFunc:  s.leaderHeartbeatTimeoutErrorFunc,
	})

	slog.Info("new leader candidate acknowledged", "candidate", candidate)
}

func (s *Server) LeaderCommit(c *fiber.Ctx) error {
	var rq LeaderCommit
	if err := c.BodyParser(&rq); err != nil {
		return err
	}
	slog.Info("received a leader commit transaction", "leader", rq.Leader)

	s.registerNewLeader(rq.Leader)

	rs := LeaderCommitAck{
		Leader: rq.Leader,
		Ack:    true,
		Node:   s.cfg.Node,
	}
	return c.JSON(&rs)
}

func (s *Server) registerNewLeader(leader string) {
	s.mtex.Lock()
	defer s.mtex.Unlock()
	s.nodeType = NodeTypeFollower
	s.leader = leader
	s.candidate = ""

	s.scheduler.Del(ScheduleLeaderHeartbeatTimeout)

	_ = s.scheduler.AddWithID(ScheduleLeaderHeartbeatTimeout, &tasks.Task{
		Interval: s.cfg.LeaderHeartbeatTimeout,
		TaskFunc: s.leaderHeartbeatTimeoutFunc,
		ErrFunc:  s.leaderHeartbeatTimeoutErrorFunc,
	})

	slog.Info("new leader committed", "leader", leader)
}

type LeaderHeartbeat struct {
	Leader string `json:"leader"`
}

type LeaderHeartbeatAck struct {
	Leader string `json:"leader"`
	Ack    bool   `json:"ack"`
	Node   string `json:"node"`
}

type CandidateProposal struct {
	Candidate string `json:"candidate"`
}

type CandidateProposalAck struct {
	Candidate string `json:"candidate"`
	Ack       bool   `json:"ack"`
	Node      string `json:"node"`
}

type LeaderCommit struct {
	Leader string `json:"leader"`
}

type LeaderCommitAck struct {
	Leader string `json:"leader"`
	Ack    bool   `json:"ack"`
	Node   string `json:"node"`
}
