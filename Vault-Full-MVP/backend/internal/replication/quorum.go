package replication

import "fmt"

type QuorumStats struct {
	ReplicationFactor int `json:"replication_factor"`
	WriteQuorum       int `json:"write_quorum"`
	ReadQuorum        int `json:"read_quorum"`
}

func validateQuorumConfig(rf, wq, rq int) error {
	if rf <= 0 {
		return fmt.Errorf("replication factor must be positive")
	}
	if wq <= 0 || wq > rf {
		return fmt.Errorf("write quorum must be between 1 and replication factor (%d)", rf)
	}
	if rq <= 0 || rq > rf {
		return fmt.Errorf("read quorum must be between 1 and replication factor (%d)", rf)
	}
	return nil
}
