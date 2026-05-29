// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package nodestate

import (
	"github.com/netsy-dev/netsy/internal/proto"
)

// HealthFromProto converts a proto HealthState enum to the internal
// HealthState value.
func HealthFromProto(h proto.HealthState) HealthState {
	switch h {
	case proto.HealthState_HEALTH_HEALTHY:
		return HealthHealthy
	case proto.HealthState_HEALTH_DEGRADED:
		return HealthDegraded
	default:
		return HealthLoading
	}
}

// HealthToProto converts an internal HealthState to the proto enum.
func HealthToProto(h HealthState) proto.HealthState {
	switch h {
	case HealthHealthy:
		return proto.HealthState_HEALTH_HEALTHY
	case HealthDegraded:
		return proto.HealthState_HEALTH_DEGRADED
	case HealthLoading:
		return proto.HealthState_HEALTH_LOADING
	default:
		return proto.HealthState_HEALTH_UNKNOWN
	}
}

// ElectorFromProto converts a proto ElectorState enum to the internal
// ElectorState value.
func ElectorFromProto(e proto.ElectorState) ElectorState {
	switch e {
	case proto.ElectorState_ELECTOR_LEADER:
		return ElectorLeader
	default:
		return ElectorFollower
	}
}

// ElectorToProto converts an internal ElectorState to the proto enum.
func ElectorToProto(e ElectorState) proto.ElectorState {
	switch e {
	case ElectorLeader:
		return proto.ElectorState_ELECTOR_LEADER
	case ElectorFollower:
		return proto.ElectorState_ELECTOR_FOLLOWER
	default:
		return proto.ElectorState_ELECTOR_UNKNOWN
	}
}

// PrimaryFromProto converts a proto PrimaryState enum to the internal
// PrimaryState value.
func PrimaryFromProto(p proto.PrimaryState) PrimaryState {
	switch p {
	case proto.PrimaryState_PRIMARY_STARTING:
		return PrimaryStarting
	case proto.PrimaryState_PRIMARY_ACTIVE:
		return PrimaryActive
	case proto.PrimaryState_PRIMARY_DRAINING:
		return PrimaryDraining
	default:
		return PrimaryReplica
	}
}

// PrimaryToProto converts an internal PrimaryState to the proto enum.
func PrimaryToProto(p PrimaryState) proto.PrimaryState {
	switch p {
	case PrimaryStarting:
		return proto.PrimaryState_PRIMARY_STARTING
	case PrimaryActive:
		return proto.PrimaryState_PRIMARY_ACTIVE
	case PrimaryDraining:
		return proto.PrimaryState_PRIMARY_DRAINING
	case PrimaryReplica:
		return proto.PrimaryState_PRIMARY_REPLICA
	default:
		return proto.PrimaryState_PRIMARY_UNKNOWN
	}
}

// ClusterStateToProto converts an internal ClusterState to a proto
// ClusterState message. The Elector is always set; the Primary is only
// set when its NodeID is non-empty.
func ClusterStateToProto(cs ClusterState) *proto.ClusterState {
	result := &proto.ClusterState{
		Elector: &proto.NodeInfo{
			NodeId:               cs.Elector.NodeID,
			MemberId:             cs.Elector.MemberID,
			PeerAdvertiseAddress: cs.Elector.PeerAdvertiseAddr,
		},
		NodeCount: int32(cs.NodeCount),
	}
	if cs.Primary.NodeID != "" {
		result.Primary = &proto.NodeInfo{
			NodeId:               cs.Primary.NodeID,
			MemberId:             cs.Primary.MemberID,
			PeerAdvertiseAddress: cs.Primary.PeerAdvertiseAddr,
		}
	}
	return result
}

// ClusterStateFromProto converts a proto ClusterState message to the
// internal ClusterState type. The Elector is always read; the Primary
// is only read when non-nil.
func ClusterStateFromProto(cs *proto.ClusterState) ClusterState {
	result := ClusterState{
		Elector: NodeInfo{
			NodeID:            cs.GetElector().GetNodeId(),
			MemberID:          cs.GetElector().GetMemberId(),
			PeerAdvertiseAddr: cs.GetElector().GetPeerAdvertiseAddress(),
		},
		NodeCount: int(cs.GetNodeCount()),
	}
	if cs.GetPrimary() != nil {
		result.Primary = NodeInfo{
			NodeID:            cs.GetPrimary().GetNodeId(),
			MemberID:          cs.GetPrimary().GetMemberId(),
			PeerAdvertiseAddr: cs.GetPrimary().GetPeerAdvertiseAddress(),
		}
	}
	return result
}
