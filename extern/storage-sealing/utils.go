package sealing

import (
	"bytes"
	"fmt"
	"strconv"
	"context"
	"math/bits"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"

	"github.com/filecoin-project/lotus/extern/storage-sealing/sealiface"
)

var RedisSectorInfoKeyPrefix = "sectorinfo"

func fillersFromRem(in abi.UnpaddedPieceSize) ([]abi.UnpaddedPieceSize, error) {
	// Convert to in-sector bytes for easier math:
	//
	// Sector size to user bytes ratio is constant, e.g. for 1024B we have 1016B
	// of user-usable data.
	//
	// (1024/1016 = 128/127)
	//
	// Given that we can get sector size by simply adding 1/127 of the user
	// bytes
	//
	// (we convert to sector bytes as they are nice round binary numbers)

	toFill := uint64(in + (in / 127))

	// We need to fill the sector with pieces that are powers of 2. Conveniently
	// computers store numbers in binary, which means we can look at 1s to get
	// all the piece sizes we need to fill the sector. It also means that number
	// of pieces is the number of 1s in the number of remaining bytes to fill
	out := make([]abi.UnpaddedPieceSize, bits.OnesCount64(toFill))
	for i := range out {
		// Extract the next lowest non-zero bit
		next := bits.TrailingZeros64(toFill)
		psize := uint64(1) << next
		// e.g: if the number is 0b010100, psize will be 0b000100

		// set that bit to 0 by XORing it, so the next iteration looks at the
		// next bit
		toFill ^= psize

		// Add the piece size to the list of pieces we need to create
		out[i] = abi.PaddedPieceSize(psize).Unpadded()
	}
	return out, nil
}

func (m *Sealing) ListSectors() ([]SectorInfo, error) {
	var sectors []SectorInfo
	if err := m.sectors.List(&sectors); err != nil {
		return nil, err
	}
	return sectors, nil
}

func (m *Sealing) ListAllSectors() ([]SectorInfo, error) {
	//log.Infof("octopus: override ListAllSectors")
	var sectors []SectorInfo
	if err := m.sectors.List(&sectors); err != nil {
		return nil, err
	}

	mid, addressErr := address.IDFromAddress(m.maddr)
	if addressErr == nil {
		var ctx = context.Background()

		//keysKey := fmt.Sprintf("%v:sectorinfo:*", mid)
		//log.Infof("octopus: Sector Info Keys: %s", keysKey)
		//keysStr, redisErr := m.redisClient.Keys(ctx, keysKey).Result()

		sectorInfosKey := fmt.Sprintf("%v:%s", mid, RedisSectorInfoKeyPrefix)
		keysStr, redisErr := m.redisClient.SMembers(ctx, sectorInfosKey).Result()

		if redisErr == nil {
			for _, keyStr := range keysStr {
				//number, _ := strconv.ParseInt(strings.Split(keyStr, ":")[2], 10, 64)
				number, _ := strconv.ParseInt(keyStr, 10, 64)
				exists := false
				for _, sector := range sectors {
					if sector.SectorNumber == abi.SectorNumber(number) {
						exists = true
						break
					}
				}
				if !exists {
					key := fmt.Sprintf("%v:%s:%v", mid, RedisSectorInfoKeyPrefix, number)
					out, err := m.getSectorInfoFromRedis(ctx, key)
					if err == nil {
						if out != nil {
							sectors = append(sectors, *out)
						}
					} else {
						log.Errorf("octopus: get sector info from redis error: %v", err)
					}
				}
			}
			return sectors, nil
		} else {
			log.Errorf("octopus: redis GetKeys error: %v", redisErr)
		}
	} else {
		log.Errorf("octopus: IDFromAddress error: %v", addressErr)
	}
	return sectors, nil
}

func (m *Sealing) SectorsSend(id interface{}, evt interface{}) error {
	log.Debugf("octopus: override SectorsSend: %v %v", id, evt)
	exists, err := m.sectors.Has(id)
	if err == nil && !exists {
		mid, addressErr := address.IDFromAddress(m.maddr)
		if addressErr == nil {
			key := fmt.Sprintf("%v:%s:%v", mid, RedisSectorInfoKeyPrefix, id)
			out, getErr := m.getSectorInfoFromRedis(context.Background(), key)
			if getErr == nil {
				if out != nil {
					beginErr := m.sectors.Begin(id, &*out)
					if beginErr != nil {
						log.Errorf("octopus: begin error: %v", beginErr)
					}
				}
			} else {
				log.Errorf("octopus: get sector info from redis error: %v", getErr)
			}
		} else {
			log.Errorf("octopus: IDFromAddress error: %v", addressErr)
		}
	}
	log.Debugf("octopus: m.sectors.Send")
	return m.sectors.Send(id, evt)
}

func (m *Sealing) GetSectorInfo(sid abi.SectorNumber) (SectorInfo, error) {
	var out SectorInfo
	err := m.sectors.Get(uint64(sid)).Get(&out)
	log.Debugf("octopus: override GetSectorInfo: %d %v %v", uint64(sid), out.SectorNumber, out.State)

	if err != nil {
		//log.Infof("octopus: Failed to get sector %v info from datastore: %v. try from redis.", err, sid)

		mid, addressErr := address.IDFromAddress(m.maddr)
		if addressErr == nil {
			key := fmt.Sprintf("%v:%s:%v", mid, RedisSectorInfoKeyPrefix, sid)
			sectorInfo, getErr := m.getSectorInfoFromRedis(context.Background(), key)
			if getErr != nil {
				log.Errorf("get sector info from redis error: %v", err)
			} else {
				if sectorInfo != nil {
					out = *sectorInfo
					err = nil
				}
			}
		} else {
			log.Errorf("octopus: IDFromAddress error: %v", addressErr)
		}
	}

	return out, err
}

func (m *Sealing) getSectorInfoFromRedis(ctx context.Context, key string) (*SectorInfo, error) {
	exists, err := m.redisClient.Exists(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	if exists == 1 {
		value, err := m.redisClient.Get(ctx, key).Result()
		if err != nil {
			return nil, err
		}

		var sectorInfo SectorInfo
		err = sectorInfo.UnmarshalCBOR(bytes.NewBuffer([]byte(value)))
		return &sectorInfo, err

		//var out SectorInfo
		//jsonErr := json.Unmarshal([]byte(keyStr), &out)
	} else {
		return nil, nil
	}
}

func (m *Sealing) setSectorInfoToRedis(ctx context.Context, minerId uint64, info *SectorInfo) error {
	buf := new(bytes.Buffer)
	err := info.MarshalCBOR(buf)
	if err == nil {
		key := fmt.Sprintf("%d:%s:%v", minerId, RedisSectorInfoKeyPrefix, info.SectorNumber)
		_, redisErr := m.redisClient.Set(ctx, key, buf.String(), 0).Result()
		if redisErr != nil {
			log.Errorf("set sectorinfo to redis key=%s error: %v", key, redisErr)
			return redisErr
		}
		allSectorInfosKey := fmt.Sprintf("%v:%s", minerId, RedisSectorInfoKeyPrefix)
		_, redisErr = m.redisClient.SAdd(ctx, allSectorInfosKey, uint64(info.SectorNumber)).Result()
		if redisErr != nil {
			log.Errorf("sadd %v %v error: %v", allSectorInfosKey, info.SectorNumber, redisErr)
			return redisErr
		}
	} else {
		log.Errorf("marshal sector info error: %v", err)
		return err
	}
	return nil
}

func collateralSendAmount(ctx context.Context, api interface {
	StateMinerAvailableBalance(context.Context, address.Address, TipSetToken) (big.Int, error)
}, maddr address.Address, cfg sealiface.Config, collateral abi.TokenAmount) (abi.TokenAmount, error) {
	if cfg.CollateralFromMinerBalance {
		if cfg.DisableCollateralFallback {
			return big.Zero(), nil
		}

		avail, err := api.StateMinerAvailableBalance(ctx, maddr, nil)
		if err != nil {
			return big.Zero(), xerrors.Errorf("getting available miner balance: %w", err)
		}

		avail = big.Sub(avail, cfg.AvailableBalanceBuffer)
		if avail.LessThan(big.Zero()) {
			avail = big.Zero()
		}

		collateral = big.Sub(collateral, avail)
		if collateral.LessThan(big.Zero()) {
			collateral = big.Zero()
		}
	}

	return collateral, nil
}
