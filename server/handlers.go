package server

import (
	"github.com/gofiber/fiber/v2"
	"github.com/madflojo/tasks"
	"github.com/valyala/fasthttp"
	"golang.org/x/exp/slog"
)

// LeaderHeartbeat handles incoming heartbeat signal from Leader node.
// This handler function will refresh leader_heartbeat_timeout scheduled task.
// Heartbeat is rejected if the term argument within incoming term is less than current term of the node.
func (s *Server) LeaderHeartbeat(c *fiber.Ctx) error {
	var rq HeartbeatRq
	if err := c.BodyParser(&rq); err != nil {
		return err
	}
	rs := HeartbeatRs{
		Ack:  s.processHeartbeat(rq.Leader, rq.Term),
		Node: s.cfg.Node,
	}

	if rs.Ack {
		slog.Info("acknowledged heartbeat from leader", "leader", rq.Leader)
		return c.JSON(&rs)
	}

	slog.Warn("heartbeat rejected", "node", rq.Leader)
	return c.Status(fasthttp.StatusPreconditionFailed).JSON(&rs)
}

func (s *Server) processHeartbeat(leader string, term int) bool {
	s.mtex.Lock()
	defer s.mtex.Unlock()
	if term < s.term {
		return false
	}
	s.leader = leader
	s.term = term

	s.scheduler.Del(ScheduleLeaderHeartbeatTimeout)

	_ = s.scheduler.AddWithID(ScheduleLeaderHeartbeatTimeout, &tasks.Task{
		Interval: s.cfg.LeaderHeartbeatTimeout,
		TaskFunc: s.leaderHeartbeatTimeoutFunc,
		ErrFunc:  s.leaderHeartbeatTimeoutErrorFunc,
	})
	return true
}

// CandidateProposal handles incoming proposal request from a candidate node.
// This handler function will acknowledge and become a follower
func (s *Server) CandidateProposal(c *fiber.Ctx) error {
	var rq CandidateProposalRq
	if err := c.BodyParser(&rq); err != nil {
		return err
	}
	slog.Info("received a candidate proposal", "candidate", rq.Candidate)

	rs := CandidateProposalRs{
		Ack:  s.processCandidateProposal(rq.Candidate, rq.Term),
		Node: s.cfg.Node,
	}

	if rs.Ack {
		slog.Info("new leader candidate accepted", "leader", rq.Candidate)
		return c.JSON(&rs)
	}

	slog.Warn("candidate proposal rejected", "node", rq.Candidate)
	return c.Status(fasthttp.StatusPreconditionFailed).JSON(&rs)
}

func (s *Server) processCandidateProposal(candidate string, term int) bool {
	s.mtex.Lock()
	defer s.mtex.Unlock()
	if term <= s.term {
		return false
	}
	s.nodeType = NodeTypeFollower
	s.leader = candidate
	s.term = term

	s.scheduler.Del(ScheduleLeaderHeartbeatTimeout)

	_ = s.scheduler.AddWithID(ScheduleLeaderHeartbeatTimeout, &tasks.Task{
		Interval: s.cfg.LeaderHeartbeatTimeout,
		TaskFunc: s.leaderHeartbeatTimeoutFunc,
		ErrFunc:  s.leaderHeartbeatTimeoutErrorFunc,
	})

	return true
}

type HeartbeatRq struct {
	Leader string `json:"leader"`
	Term   int    `json:"term"`
}

type HeartbeatRs struct {
	Ack  bool   `json:"ack"`
	Node string `json:"node"`
}

type CandidateProposalRq struct {
	Candidate string `json:"candidate"`
	Term      int    `json:"term"`
}

type CandidateProposalRs struct {
	Ack  bool   `json:"ack"`
	Node string `json:"node"`
}
