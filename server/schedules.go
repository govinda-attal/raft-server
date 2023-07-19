package server

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/dyweb/gommon/errors"
	"github.com/gofiber/fiber/v2"
	"github.com/madflojo/tasks"
	"github.com/valyala/fasthttp"
	"golang.org/x/exp/slog"
)

func (s *Server) heartbeatFunc() error {
	var (
		wg   sync.WaitGroup
		merr = errors.NewMultiErrSafe()
		term = s.currTerm()
	)
	slog.Info("running leader heartbeat scheduled task", "node", s.cfg.Node, "term", term)

	wg.Add(len(s.cfg.Peers))

	for _, peer := range s.cfg.Peers {
		go func(peer string) {
			err := s.sendHeartbeatToPeer(peer, term)
			if err != nil {
				merr.Append(err)
			}
			wg.Done()
		}(peer)
	}

	wg.Wait()
	if merr.HasError() && merr.Len() > len(s.cfg.Peers)/2 {
		slog.Error("error encountered when sending heartbeat to peers", "error", merr, "term", term)
		return merr
	}

	return nil
}

func (s *Server) sendHeartbeatToPeer(peer string, term int) (err error) {
	client := fiber.AcquireClient()
	defer fiber.ReleaseClient(client)
	agent := client.Post(fmt.Sprintf("http://%s/leader/heartbeat", peer)).JSON(&HeartbeatRq{
		Leader: s.cfg.Node,
		Term:   term,
	})
	rs := fiber.AcquireResponse()
	defer fiber.ReleaseResponse(rs)
	slog.Info("leader heartbeat sent to peer", "peer", peer, "term", term)
	if err = fasthttp.Do(agent.Request(), rs); err != nil {
		return
	}
	if rs.StatusCode() != fasthttp.StatusOK {
		err = fmt.Errorf("status code: %d", rs.StatusCode())
		return
	}
	var hbRs HeartbeatRs
	if err = json.Unmarshal(rs.Body(), &hbRs); err != nil {
		return
	}
	if !hbRs.Ack {
		err = fmt.Errorf("leader heartbeat not acknowledged by peer: %s for term %d", peer, term)
		return
	}
	return nil
}

func (s *Server) heartbeatErrorFunc(err error) {
	s.mtex.Lock()
	defer s.mtex.Unlock()
	s.nodeType = NodeTypeFollower

	slog.Error("error encountered while processing leader hearbeat scheduled task", "error", err)

	slog.Info("deleting heartbeat scheduled task")
	s.scheduler.Del(ScheduleHeartbeat)

	slog.Info("reinstating leader heartbeat timeout scheduled task")
	_ = s.scheduler.AddWithID(ScheduleLeaderHeartbeatTimeout, &tasks.Task{
		Interval: s.cfg.LeaderHeartbeatTimeout,
		TaskFunc: s.leaderHeartbeatTimeoutFunc,
		ErrFunc:  s.leaderHeartbeatTimeoutErrorFunc,
	})
}

func (s *Server) leaderHeartbeatTimeoutFunc() error {
	var (
		wg   sync.WaitGroup
		merr = errors.NewMultiErrSafe()
		term = s.incTerm()
	)
	slog.Info("triggered leader-heartbeat-timeout scheduled task; proposing self as candidate", "node", s.cfg.Node, "term", term)

	wg.Add(len(s.cfg.Peers))
	for _, peer := range s.cfg.Peers {
		go func(peer string) {
			err := s.sendCandidateProposalToPeer(peer, term)
			if err != nil {
				merr.Append(err)
			}
			wg.Done()
		}(peer)
	}

	wg.Wait()
	if merr.Len() > len(s.cfg.Peers)/2 {
		slog.Error("error encountered when sending candidate proposal to peers", "error", merr, "term", term)
		return merr
	}

	s.upgradeSelfToLeader()

	return nil
}

func (s *Server) incTerm() int {
	s.mtex.Lock()
	defer s.mtex.Unlock()
	s.term++
	return s.term
}

func (s *Server) currTerm() int {
	s.mtex.Lock()
	defer s.mtex.Unlock()
	return s.term
}

func (s *Server) sendCandidateProposalToPeer(peer string, term int) (err error) {
	client := fiber.AcquireClient()
	defer fiber.ReleaseClient(client)
	agent := client.Post(fmt.Sprintf("http://%s/candidate/proposal", peer)).JSON(&CandidateProposalRq{
		Candidate: s.cfg.Node,
		Term:      term,
	})
	rs := fiber.AcquireResponse()
	defer fiber.ReleaseResponse(rs)
	if err = fasthttp.Do(agent.Request(), rs); err != nil {
		return err
	}
	if rs.StatusCode() != fasthttp.StatusOK {
		err = fmt.Errorf("status code %d", rs.StatusCode())
		return err
	}
	var prRs CandidateProposalRs
	if err = json.Unmarshal(rs.Body(), &prRs); err != nil {
		return err
	}
	if !prRs.Ack {
		err = fmt.Errorf("failed to accept proposal from peer %s for term %d", peer, term)
		return err
	}
	return nil
}

func (s *Server) upgradeSelfToLeader() {
	s.mtex.Lock()
	defer s.mtex.Unlock()
	s.nodeType = NodeTypeLeader
	s.leader = ""

	_ = s.scheduler.AddWithID(ScheduleHeartbeat, &tasks.Task{
		Interval: s.cfg.HeartbeatInterval,
		TaskFunc: s.heartbeatFunc,
		ErrFunc:  s.heartbeatErrorFunc,
	})

	s.scheduler.Del(ScheduleLeaderHeartbeatTimeout)

	slog.Info("leader-candidate proposal successful; promoted self to leader", "node", s.cfg.Node)
}

func (s *Server) leaderHeartbeatTimeoutErrorFunc(err error) {
	s.mtex.Lock()
	defer s.mtex.Unlock()
	s.nodeType = NodeTypeFollower

	slog.Error("error encountered on task leader_heartbeat_timeout", "error", err)
}
