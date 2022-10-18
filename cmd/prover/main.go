package main

import (
	"encoding/base64"
	"flag"
	"fmt"
	"github.com/filecoin-project/filecoin-ffi/generated"
	"github.com/filecoin-project/go-address"
	commcid "github.com/filecoin-project/go-fil-commcid"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"
)

func to32ByteArray(in []byte) generated.Fil32ByteArray {
	var out generated.Fil32ByteArray
	copy(out.Inner[:], in)
	return out
}

func toProverID(minerID abi.ActorID) (generated.Fil32ByteArray, error) {
	maddr, _ := address.NewIDAddress(uint64(minerID))

	return to32ByteArray(maddr.Payload()), nil
}

func to32ByteCommR(sealedCID cid.Cid) (generated.Fil32ByteArray, error) {
	commD, err := commcid.CIDToReplicaCommitmentV1(sealedCID)
	if err != nil {
		return generated.Fil32ByteArray{}, err
	}

	return to32ByteArray(commD), nil
}

func minerToProver(m int64) (string, error) {
	a, err := toProverID(abi.ActorID(m))
	if err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(a.Inner[:]), nil
}

func cidToBase64(s string) (string, error) {
	c, err := cid.Decode(s)
	if err != nil {
		return "", err
	}
	a, err := to32ByteCommR(c)
	if err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(a.Inner[:]), nil
}

func main() {
	commR := flag.String("c", "", "comm r cid string")
	miner := flag.Int64("m", 0, "miner id")
	flag.Parse()
	if *commR != "" {
		s, _ := cidToBase64(*commR)
		fmt.Println("comm r is", s)
	}
	if *miner != 0 {
		s, _ := minerToProver(*miner)
		fmt.Println("prover is", s)
	}
}
