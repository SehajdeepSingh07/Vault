package replication

import "testing"

func TestValidateQuorumConfig(t *testing.T) {
	cases := []struct {
		name string
		rf   int
		wq   int
		rq   int
		want bool
	}{
		{"valid-majority", 3, 2, 2, true},
		{"valid-degraded-write", 5, 3, 2, true},
		{"zero-write", 3, 0, 2, false},
		{"write-too-large", 3, 4, 2, false},
		{"read-too-large", 3, 2, 4, false},
		{"zero-rf", 0, 1, 1, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateQuorumConfig(tc.rf, tc.wq, tc.rq)
			if (err == nil) != tc.want {
				t.Fatalf("validateQuorumConfig() err=%v wantValid=%v", err, tc.want)
			}
		})
	}
}

func TestQuorumStats(t *testing.T) {
	stats := QuorumStats{ReplicationFactor: 3, WriteQuorum: 2, ReadQuorum: 2}
	if stats.ReplicationFactor != 3 || stats.WriteQuorum != 2 || stats.ReadQuorum != 2 {
		t.Fatalf("unexpected quorum stats: %+v", stats)
	}
}
