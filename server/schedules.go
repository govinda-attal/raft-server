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
	slog.Info("running leader heartbeat scheduled task", "node", s.cfg.Node)
	var (
		wg   sync.WaitGroup
		merr = errors.NewMultiErrSafe()
	)

	wg.Add(len(s.cfg.Peers))

	for _, peer := range s.cfg.Peers {
		go func(peer string) {
			err := s.sendHeartbeatToPeer(peer)
			if err != nil {
				merr.Append(err)
			}
			wg.Done()
		}(peer)
	}

	wg.Wait()
	if merr.HasError() && merr.Len() > len(s.cfg.Peers)/2 {
		slog.Error("error encountered when sending heartbeat to peers", "error", merr)
		return merr
	}

	return nil
}

func (s *Server) sendHeartbeatToPeer(peer string) (err error) {
	client := fiber.AcquireClient()
	defer fiber.ReleaseClient(client)
	agent := client.Post(fmt.Sprintf("http://%s/leader/heartbeat", peer)).JSON(&LeaderHeartbeat{
		Leader: s.cfg.Node,
	})
	rs := fiber.AcquireResponse()
	defer fiber.ReleaseResponse(rs)
	slog.Info("leader heartbeat sent to peer", "peer", peer)
	if err = fasthttp.Do(agent.Request(), rs); err != nil {
		return
	}
	if rs.StatusCode() != fasthttp.StatusOK {
		err = fmt.Errorf("status code: %d", rs.StatusCode())
		return
	}
	var hearbeatRs LeaderHeartbeatAck
	if err = json.Unmarshal(rs.Body(), &hearbeatRs); err != nil {
		return
	}
	if !hearbeatRs.Ack {
		err = fmt.Errorf("leader heartbeat not acknowledged by peer: %s", peer)
		return
	}
	return nil
}

func (s *Server) heartbeatErrorFunc(err error) {
	s.mtex.Lock()
	defer s.mtex.Unlock()
	s.nodeType = NodeTypeFollower

	slog.Error("error encountered while processing leader hearbeat scheduled task", "error", err)
	slog.Warn("deleting heartbeat scheduled task")
	s.scheduler.Del(ScheduleHeartbeat)
}

func (s *Server) leaderHeartbeatTimeoutFunc() error {
	slog.Info("triggered leader-heartbeat-timeout scheduled task; proposing self as candidate", "node", s.cfg.Node)
	var (
		wg   sync.WaitGroup
		merr = errors.NewMultiErrSafe()
	)
	wg.Add(len(s.cfg.Peers))
	for _, peer := range s.cfg.Peers {
		go func(peer string) {
			err := s.sendCandidateProposalToPeer(peer)
			if err != nil {
				merr.Append(err)
			}
			wg.Done()
		}(peer)
	}

	wg.Wait()
	if merr.HasError() && merr.Len() > len(s.cfg.Peers)/2 {
		slog.Error("error encountered when sending candidate proposal to peers", "error", merr)
		return merr
	}

	s.upgradeSelfToLeader()

	wg.Add(len(s.cfg.Peers))
	for _, peer := range s.cfg.Peers {
		go func(peer string) {
			err := s.sendCandidateLeaderCommitToPeer(peer)
			if err != nil {
				merr.Append(err)
			}
			wg.Done()
		}(peer)
	}

	wg.Wait()
	if merr.HasError() && merr.Len() > len(s.cfg.Peers)/2 {
		slog.Error("error encountered when sending leader-commit to peers", "error", merr)
		return merr
	}

	return nil
}

func (s *Server) sendCandidateProposalToPeer(peer string) (err error) {
	client := fiber.AcquireClient()
	defer fiber.ReleaseClient(client)
	agent := client.Post(fmt.Sprintf("http://%s/candidate/proposal", peer)).JSON(&CandidateProposal{
		Candidate: s.cfg.Node,
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
	var proposalRs CandidateProposalAck
	if err = json.Unmarshal(rs.Body(), &proposalRs); err != nil {
		return err
	}
	if !proposalRs.Ack {
		err = fmt.Errorf("failed to accept proposal from peer %s", peer)
		return err
	}
	return nil
}

func (s *Server) sendCandidateLeaderCommitToPeer(peer string) (err error) {
	client := fiber.AcquireClient()
	defer fiber.ReleaseClient(client)
	agent := client.Post(fmt.Sprintf("http://%s/candidate/leader-commit", peer)).JSON(&LeaderCommit{
		Leader: s.cfg.Node,
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
	var commitRs LeaderCommitAck
	if err = json.Unmarshal(rs.Body(), &commitRs); err != nil {
		return err
	}
	if !commitRs.Ack {
		err = fmt.Errorf("failed to receive leader-commit %s", peer)
		return err
	}
	return nil
}

func (s *Server) upgradeSelfToLeader() {
	s.mtex.Lock()
	defer s.mtex.Unlock()
	s.nodeType = NodeTypeLeader
	s.leader = ""
	s.candidate = ""

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
