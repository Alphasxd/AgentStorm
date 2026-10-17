package main

import (
	"errors"
	"net/http"
)

// errNotElectedLeader is returned by /readyz until this process is elected.
var errNotElectedLeader = errors.New("controller manager is not the elected leader")

// leaderReadyChecker fails until elected is closed.
//
// controller-runtime closes that channel when this process wins the leader
// lease. With --leader-elect left unset, it closes the channel after starting
// controllers, so /readyz does not wait for a lease. A nil or still-open
// channel means this process is not the leader yet.
//
// The select is non-blocking. A probe goroutine never waits on election, and
// leader election never waits on the probe, so the check cannot deadlock the
// manager. /healthz stays an unconditional ping: a replica that has not been
// elected remains Running and can acquire the lease after the leader stops
// renewing it. Losing the lease stops the process, so a former leader does
// not continue serving /readyz.
//
// Only the elected process becomes Ready. Manifests that pass --leader-elect
// run one replica with RollingUpdate maxUnavailable 1 and maxSurge 0, and the
// manager sets LeaderElectionReleaseOnCancel. An update therefore removes the
// current leader and releases the Lease before the replacement has to pass
// /readyz. The strategy stays RollingUpdate so a server-side apply upgrade can
// replace the default surge values. Extra electing replicas stay unready until
// they win the lease, so rollout status completes only for a single replica.
func leaderReadyChecker(elected <-chan struct{}) func(*http.Request) error {
	return func(*http.Request) error {
		select {
		case <-elected:
			return nil
		default:
			return errNotElectedLeader
		}
	}
}
