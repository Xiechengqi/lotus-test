package sealing

import "os"

type SectorState string

var ExistSectorStateList = map[SectorState]struct{}{
	Empty:                       {},
	WaitDeals:                   {},
	Packing:                     {},
	AddPiece:                    {},
	AddPieceFailed:              {},
	GetTicket:                   {},
	PreCommit1:                  {},
	PreCommit2:                  {},
	PreCommitting:               {},
	PreCommitWait:               {},
	SubmitPreCommitBatch:        {},
	PreCommitBatchWait:          {},
	WaitSeed:                    {},
	Committing:                  {},
	CommitFinalize:              {},
	CommitFinalizeFailed:        {},
	SubmitCommit:                {},
	CommitWait:                  {},
	SubmitCommitAggregate:       {},
	CommitAggregateWait:         {},
	FinalizeSector:              {},
	Proving:                     {},
	Available:                   {},
	FailedUnrecoverable:         {},
	SealPreCommit1Failed:        {},
	SealPreCommit2Failed:        {},
	PreCommitFailed:             {},
	ComputeProofFailed:          {},
	CommitFailed:                {},
	PackingFailed:               {},
	FinalizeFailed:              {},
	DealsExpired:                {},
	RecoverDealIDs:              {},
	Faulty:                      {},
	FaultReported:               {},
	FaultedFinal:                {},
	Terminating:                 {},
	TerminateWait:               {},
	TerminateFinality:           {},
	TerminateFailed:             {},
	Removing:                    {},
	RemoveFailed:                {},
	Removed:                     {},
	SnapDealsWaitDeals:          {},
	SnapDealsAddPiece:           {},
	SnapDealsPacking:            {},
	UpdateReplica:               {},
	ProveReplicaUpdate:          {},
	SubmitReplicaUpdate:         {},
	ReplicaUpdateWait:           {},
	UpdateActivating:            {},
	ReleaseSectorKey:            {},
	FinalizeReplicaUpdate:       {},
	SnapDealsAddPieceFailed:     {},
	SnapDealsDealsExpired:       {},
	SnapDealsRecoverDealIDs:     {},
	ReplicaUpdateFailed:         {},
	ReleaseSectorKeyFailed:      {},
	FinalizeReplicaUpdateFailed: {},
	AbortUpgrade:                {},
	// octopus:
	ZeroPacking:   {},
	Commit1Failed: {},
	Recovery:      {},
}

// cmd/lotus-miner/info.go defines CLI colors corresponding to these states
// update files there when adding new states
const (
	UndefinedSectorState SectorState = ""

	// happy path
	Empty     SectorState = "Empty"     // deprecated
	WaitDeals SectorState = "WaitDeals" // waiting for more pieces (deals) to be added to the sector
	AddPiece  SectorState = "AddPiece"  // put deal data (and padding if required) into the sector
	Packing   SectorState = "Packing"   // sector not in sealStore, and not on chain
	// octopus
	ZeroPacking SectorState = "ZeroPacking"
	GetTicket   SectorState = "GetTicket"  // generate ticket
	PreCommit1  SectorState = "PreCommit1" // do PreCommit1
	PreCommit2  SectorState = "PreCommit2" // do PreCommit2

	PreCommitting SectorState = "PreCommitting" // on chain pre-commit
	PreCommitWait SectorState = "PreCommitWait" // waiting for precommit to land on chain

	SubmitPreCommitBatch SectorState = "SubmitPreCommitBatch"
	PreCommitBatchWait   SectorState = "PreCommitBatchWait"

	WaitSeed             SectorState = "WaitSeed"       // waiting for seed
	Committing           SectorState = "Committing"     // compute PoRep
	CommitFinalize       SectorState = "CommitFinalize" // cleanup sector metadata before submitting the proof (early finalize)
	CommitFinalizeFailed SectorState = "CommitFinalizeFailed"

	// single commit
	SubmitCommit SectorState = "SubmitCommit" // send commit message to the chain
	CommitWait   SectorState = "CommitWait"   // wait for the commit message to land on chain

	SubmitCommitAggregate SectorState = "SubmitCommitAggregate"
	CommitAggregateWait   SectorState = "CommitAggregateWait"

	FinalizeSector SectorState = "FinalizeSector"
	Proving        SectorState = "Proving"
	Available      SectorState = "Available" // proving CC available for SnapDeals

	// snap deals / cc update
	SnapDealsWaitDeals    SectorState = "SnapDealsWaitDeals"
	SnapDealsAddPiece     SectorState = "SnapDealsAddPiece"
	SnapDealsPacking      SectorState = "SnapDealsPacking"
	UpdateReplica         SectorState = "UpdateReplica"
	ProveReplicaUpdate    SectorState = "ProveReplicaUpdate"
	SubmitReplicaUpdate   SectorState = "SubmitReplicaUpdate"
	ReplicaUpdateWait     SectorState = "ReplicaUpdateWait"
	FinalizeReplicaUpdate SectorState = "FinalizeReplicaUpdate"
	UpdateActivating      SectorState = "UpdateActivating"
	ReleaseSectorKey      SectorState = "ReleaseSectorKey"

	// error modes
	Commit1Failed        SectorState = "Commit1Failed"
	FailedUnrecoverable  SectorState = "FailedUnrecoverable"
	AddPieceFailed       SectorState = "AddPieceFailed"
	SealPreCommit1Failed SectorState = "SealPreCommit1Failed"
	SealPreCommit2Failed SectorState = "SealPreCommit2Failed"
	PreCommitFailed      SectorState = "PreCommitFailed"
	ComputeProofFailed   SectorState = "ComputeProofFailed"
	CommitFailed         SectorState = "CommitFailed"
	SubmitCommitFailed   SectorState = "SubmitCommitFailed"
	PackingFailed        SectorState = "PackingFailed" // TODO: deprecated, remove
	FinalizeFailed       SectorState = "FinalizeFailed"
	DealsExpired         SectorState = "DealsExpired"
	RecoverDealIDs       SectorState = "RecoverDealIDs"

	// snap deals error modes
	SnapDealsAddPieceFailed     SectorState = "SnapDealsAddPieceFailed"
	SnapDealsDealsExpired       SectorState = "SnapDealsDealsExpired"
	SnapDealsRecoverDealIDs     SectorState = "SnapDealsRecoverDealIDs"
	AbortUpgrade                SectorState = "AbortUpgrade"
	ReplicaUpdateFailed         SectorState = "ReplicaUpdateFailed"
	ReleaseSectorKeyFailed      SectorState = "ReleaseSectorKeyFailed"
	FinalizeReplicaUpdateFailed SectorState = "FinalizeReplicaUpdateFailed"

	Faulty        SectorState = "Faulty"        // sector is corrupted or gone for some reason
	FaultReported SectorState = "FaultReported" // sector has been declared as a fault on chain
	FaultedFinal  SectorState = "FaultedFinal"  // fault declared on chain

	Terminating       SectorState = "Terminating"
	TerminateWait     SectorState = "TerminateWait"
	TerminateFinality SectorState = "TerminateFinality"
	TerminateFailed   SectorState = "TerminateFailed"

	Removing     SectorState = "Removing"
	RemoveFailed SectorState = "RemoveFailed"
	Removed      SectorState = "Removed"

	Recovery SectorState = "Recovery"
)

func toStatState(st SectorState, finEarly bool) statSectorState {
	stagedIncludeAddPieceFailed := os.Getenv("STAGED_INCLUDE_ADD_PIECE_FAILED")
	if stagedIncludeAddPieceFailed == "true" {
		switch st {
		case UndefinedSectorState, Empty, WaitDeals, AddPiece, AddPieceFailed, SnapDealsWaitDeals, SnapDealsAddPiece:
			return sstStaging
		case ZeroPacking, Packing, GetTicket, PreCommit1, PreCommit2, PreCommitting, PreCommitWait, SubmitPreCommitBatch, PreCommitBatchWait, WaitSeed, Committing, CommitFinalize, FinalizeSector, Recovery, SnapDealsPacking, UpdateReplica, ProveReplicaUpdate, FinalizeReplicaUpdate:
			return sstSealing
		case SubmitCommit, CommitWait, SubmitCommitAggregate, CommitAggregateWait, SubmitReplicaUpdate, ReplicaUpdateWait:
			if finEarly {
				// we use statSectorState for throttling storage use. With FinalizeEarly
				// we can consider sectors in states after CommitFinalize as finalized, so
				// that more sectors can enter the sealing pipeline (and later be aggregated together)
				return sstProving
			}
			return sstSealing
		case Proving, Available, UpdateActivating, ReleaseSectorKey, Removed, Removing, Terminating, TerminateWait, TerminateFinality, TerminateFailed:
			return sstProving
		}
	} else {
		switch st {
		case UndefinedSectorState, Empty, WaitDeals, AddPiece, SnapDealsWaitDeals, SnapDealsAddPiece:
			return sstStaging
		case ZeroPacking, Packing, GetTicket, PreCommit1, PreCommit2, PreCommitting, PreCommitWait, SubmitPreCommitBatch, PreCommitBatchWait, WaitSeed, Committing, CommitFinalize, FinalizeSector, Recovery, SnapDealsPacking, UpdateReplica, ProveReplicaUpdate, FinalizeReplicaUpdate:
			return sstSealing
		case SubmitCommit, CommitWait, SubmitCommitAggregate, CommitAggregateWait, SubmitReplicaUpdate, ReplicaUpdateWait:
			if finEarly {
				// we use statSectorState for throttling storage use. With FinalizeEarly
				// we can consider sectors in states after CommitFinalize as finalized, so
				// that more sectors can enter the sealing pipeline (and later be aggregated together)
				return sstProving
			}
			return sstSealing
		case Proving, Available, UpdateActivating, ReleaseSectorKey, Removed, Removing, Terminating, TerminateWait, TerminateFinality, TerminateFailed:
			return sstProving
		}
		return sstSealing
	}

	return sstFailed
}

func IsUpgradeState(st SectorState) bool {
	switch st {
	case SnapDealsWaitDeals,
		SnapDealsAddPiece,
		SnapDealsPacking,
		UpdateReplica,
		ProveReplicaUpdate,
		SubmitReplicaUpdate,

		SnapDealsAddPieceFailed,
		SnapDealsDealsExpired,
		SnapDealsRecoverDealIDs,
		AbortUpgrade,
		ReplicaUpdateFailed,
		ReleaseSectorKeyFailed,
		FinalizeReplicaUpdateFailed:
		return true
	default:
		return false
	}
}
