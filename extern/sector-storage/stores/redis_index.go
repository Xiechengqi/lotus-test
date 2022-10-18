package stores

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/extern/sector-storage/fsutil"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/go-redis/redis/v8"
	"golang.org/x/xerrors"
	"net/url"
	"os"
	gopath "path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var RedisStoresKeyPrefix = "sectorindex:stores"
var RedisSectorsKeyPrefix = "sectorindex:sectors"

type RedisIndex struct {
	*indexLocks
	lk sync.RWMutex
	//client *redis.Client
	client  *redis.ClusterClient
	minerId abi.ActorID
}

func NewRedisIndex(mi dtypes.MinerID, redisClient *redis.ClusterClient) *RedisIndex {
	//redisConn, ok := os.LookupEnv("REDIS_CONN")
	//if !ok {
	//	panic("redis-conn is not provided.")
	//}
	//redisPassword, ok := os.LookupEnv("REDIS_PASSWORD")
	//if !ok {
	//	redisPassword = ""
	//}
	//
	//log.Infof("octopus: init redis sector index: %v ******", redisConn)
	//rdb := redis.NewClusterClient(&redis.ClusterOptions{
	//	Addrs:    strings.Split(redisConn, ","),
	//	Password: redisPassword,
	//	PoolSize: 1,
	//})
	return &RedisIndex{
		indexLocks: &indexLocks{
			locks: map[abi.SectorID]*sectorLock{},
		},
		client:  redisClient,
		minerId: abi.ActorID(mi),
	}
}

func (i *RedisIndex) GetStorageEntryByKey(ctx context.Context, key string) (*storageEntry, error) {
	value, err := i.client.Get(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	//p := &storageEntry{}
	//json.Unmarshal([]byte(value), p)
	var p storageEntry
	err = p.UnmarshalCBOR(bytes.NewBuffer([]byte(value)))
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func (i *RedisIndex) SetStorageEntryByKey(ctx context.Context, key string, entry *storageEntry) error {
	buf := new(bytes.Buffer)
	err := entry.MarshalCBOR(buf)
	if err != nil {
		return err
	}
	//entryJson, _ := json.Marshal(entry)
	//_, _ = i.client.Set(ctx, key, string(entryJson), 0).Result()
	_, err = i.client.Set(ctx, key, buf.String(), 0).Result()
	if err != nil {
		return err
	}

	storesKey := fmt.Sprintf("%v:%s", i.minerId, RedisStoresKeyPrefix)
	_, err = i.client.SAdd(ctx, storesKey, string(entry.Info.ID)).Result()
	if err != nil {
		log.Errorf("octopus: redisindex: SAdd %s %s error: %v", storesKey, entry.Info.ID, err)
		return err
	}

	return nil
}

func (i *RedisIndex) decodeDeclMeta(value string) (*declMeta, error) {
	//declMetaVal := &declMeta{}
	//json.Unmarshal([]byte(value), &declMetaVal)

	var declMetaVal declMeta
	err := declMetaVal.UnmarshalCBOR(bytes.NewBuffer([]byte(value)))
	if err != nil {
		return nil, err
	}
	return &declMetaVal, nil
}

func (i *RedisIndex) encodeDeclMeta(meta *declMeta) (string, error) {
	// sidBytes, _ := json.Marshal(meta)
	// return string(sidBytes)

	buf := new(bytes.Buffer)
	err := meta.MarshalCBOR(buf)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

func (i *RedisIndex) StorageListSelected(ctx context.Context, storageKeys map[storiface.ID]string) (map[storiface.ID][]storiface.Decl, error) {
	log.Debug("octopus: redisindex StorageListSelected")

	i.lk.RLock()
	defer i.lk.RUnlock()

	byID := map[storiface.ID]map[abi.SectorID]storiface.SectorFileType{}

	for key, _ := range storageKeys {
		storageKey := fmt.Sprintf("%v:%s:%s", i.minerId, RedisStoresKeyPrefix, key)
		p, err := i.GetStorageEntryByKey(ctx, storageKey)
		if err != nil {
			log.Errorf("octopus: redisindex: GetStorageEntryByKey %s error: %v", storageKey, err)
			return nil, err
		}

		byID[p.Info.ID] = map[abi.SectorID]storiface.SectorFileType{}
	}

	allSectorsKey := fmt.Sprintf("%v:%s", i.minerId, RedisSectorsKeyPrefix)
	keys, err := i.client.SMembers(ctx, allSectorsKey).Result()
	if err != nil {
		log.Errorf("octopus: redisindex: smember %s error: %v", allSectorsKey, err)
		return nil, err
	}

	for _, key := range keys {
		keyParts := strings.Split(key, ":")
		number, _ := strconv.ParseInt(keyParts[0], 10, 64)
		sectorFileType, _ := strconv.Atoi(keyParts[1])

		sectorKey := fmt.Sprintf("%v:%s:%v:%s", i.minerId, RedisSectorsKeyPrefix, i.minerId, key)
		declMetasStr, err := i.client.LRange(ctx, sectorKey, 0, -1).Result()
		if err != nil {
			log.Errorf("octopus: redisindex: lrange %s error: %v", sectorKey, err)
			return nil, err
		}
		for _, declMetaStr := range declMetasStr {
			declMetaVal, err := i.decodeDeclMeta(declMetaStr)
			if err != nil {
				log.Errorf("decode DeclMeta error: %v", err)
				return nil, err
			}
			sectorId := abi.SectorID{Miner: i.minerId, Number: abi.SectorNumber(uint64(number))}
			if _, ok := byID[declMetaVal.Storage]; ok {
				byID[declMetaVal.Storage][sectorId] |= storiface.SectorFileType(sectorFileType)
			}
		}
	}

	out := map[storiface.ID][]storiface.Decl{}
	for id, m := range byID {
		out[id] = []storiface.Decl{}
		for sectorID, fileType := range m {
			out[id] = append(out[id], storiface.Decl{
				SectorID:       sectorID,
				SectorFileType: fileType,
			})
		}
	}

	return out, nil
}

func (i *RedisIndex) StorageList(ctx context.Context) (map[storiface.ID][]storiface.Decl, error) {
	log.Debug("octopus: redisindex StorageList")

	i.lk.RLock()
	defer i.lk.RUnlock()

	byID := map[storiface.ID]map[abi.SectorID]storiface.SectorFileType{}

	//allStoresKey := fmt.Sprintf("%v:%s:%s", i.minerId, RedisStoresKeyPrefix, "*")
	//keys, _ := i.client.Keys(ctx, allStoresKey).Result()
	allStoresKey := fmt.Sprintf("%v:%s", i.minerId, RedisStoresKeyPrefix)
	keys, err := i.client.SMembers(ctx, allStoresKey).Result()
	if err != nil {
		log.Errorf("octopus: redisindex: smembers %s error: %v", allStoresKey, err)
		return nil, err
	}
	log.Debugf("octopus: redisindex: keys len(%s)=%d", allStoresKey, len(keys))

	for _, key := range keys {
		storageKey := fmt.Sprintf("%v:%s:%s", i.minerId, RedisStoresKeyPrefix, key)
		p, err := i.GetStorageEntryByKey(ctx, storageKey)
		if err != nil {
			log.Errorf("octopus: redisindex: GetStorageEntryByKey %s error: %v", storageKey, err)
			return nil, err
		}

		byID[p.Info.ID] = map[abi.SectorID]storiface.SectorFileType{}
	}

	//allSectorsKey := fmt.Sprintf("%v:%s:%s", i.minerId, RedisSectorsKeyPrefix, "*")
	//keys, _ = i.client.Keys(ctx, allSectorsKey).Result()
	allSectorsKey := fmt.Sprintf("%v:%s", i.minerId, RedisSectorsKeyPrefix)
	keys, err = i.client.SMembers(ctx, allSectorsKey).Result()
	if err != nil {
		log.Errorf("octopus: redisindex: smember %s error: %v", allSectorsKey, err)
		return nil, err
	}

	for _, key := range keys {
		keyParts := strings.Split(key, ":")
		number, _ := strconv.ParseInt(keyParts[0], 10, 64)
		sectorFileType, _ := strconv.Atoi(keyParts[1])

		sectorKey := fmt.Sprintf("%v:%s:%v:%s", i.minerId, RedisSectorsKeyPrefix, i.minerId, key)
		declMetasStr, err := i.client.LRange(ctx, sectorKey, 0, -1).Result()
		if err != nil {
			log.Errorf("octopus: redisindex: lrange %s error: %v", sectorKey, err)
			return nil, err
		}
		for _, declMetaStr := range declMetasStr {
			declMetaVal, err := i.decodeDeclMeta(declMetaStr)
			if err != nil {
				log.Errorf("decode DeclMeta error: %v", err)
				return nil, err
			}
			sectorId := abi.SectorID{Miner: i.minerId, Number: abi.SectorNumber(uint64(number))}
			byID[declMetaVal.Storage][sectorId] |= storiface.SectorFileType(sectorFileType)
		}
	}

	out := map[storiface.ID][]storiface.Decl{}
	for id, m := range byID {
		out[id] = []storiface.Decl{}
		for sectorID, fileType := range m {
			out[id] = append(out[id], storiface.Decl{
				SectorID:       sectorID,
				SectorFileType: fileType,
			})
		}
	}

	return out, nil
}

func (i *RedisIndex) StorageAttach(ctx context.Context, si storiface.StorageInfo, st fsutil.FsStat) error {
	log.Debug("octopus: redisindex StorageAttach")

	i.lk.Lock()
	defer i.lk.Unlock()

	log.Infof("New sector storage: %s", si.ID)

	key := fmt.Sprintf("%v:%s:%v", i.minerId, RedisStoresKeyPrefix, si.ID)
	log.Debugf("octopus: redis key: %s", key)

	exists, err := i.client.Exists(ctx, key).Result()
	if err != nil {
		log.Errorf("octopus: redisindex: exists %s error: %v", key, err)
		return err
	}

	if exists == 1 {
		for _, u := range si.URLs {
			if _, err := url.Parse(u); err != nil {
				return xerrors.Errorf("failed to parse url %s: %w", si.URLs, err)
			}
		}

		entry, err := i.GetStorageEntryByKey(ctx, key)
		if err != nil {
			log.Errorf("octopus: redisindex: GetStorageEntryByKey %s error: %v", key, err)
			return err
		}
	uloop:
		for _, u := range si.URLs {
			for _, l := range entry.Info.URLs {
				if u == l {
					continue uloop
				}
			}

			entry.Info.URLs = append(entry.Info.URLs, u)
		}

		entry.Info.Weight = si.Weight
		entry.Info.MaxStorage = si.MaxStorage
		entry.Info.CanSeal = si.CanSeal
		entry.Info.CanStore = si.CanStore
		entry.Info.Groups = si.Groups
		entry.Info.AllowTo = si.AllowTo

		if err = i.SetStorageEntryByKey(ctx, key, entry); err != nil {
			log.Errorf("octopus: redisindex: SetStorageEntryByKey %s error: %v", key, err)
			return err
		}

		return nil
	}
	if err = i.SetStorageEntryByKey(ctx, key, &storageEntry{
		Info: &si,
		Fsi:  st,

		LastHeartbeat: time.Now(),
	}); err != nil {
		log.Errorf("octopus: redisindex: SetStorageEntryByKey %s error: %v", key, err)
		return err
	}
	return nil
}

func (i *RedisIndex) StorageInfo(ctx context.Context, id storiface.ID) (storiface.StorageInfo, error) {
	log.Debug("octopus: redisindex StorageInfo")

	i.lk.RLock()
	defer i.lk.RUnlock()

	key := fmt.Sprintf("%s:%s:%v", i.minerId, RedisStoresKeyPrefix, id)
	exists, err := i.client.Exists(ctx, key).Result()
	if err != nil {
		log.Errorf("octopus: redisindex: check key %s existence error: %v", key, err)
		return storiface.StorageInfo{}, err
	}
	if exists == 0 {
		return storiface.StorageInfo{}, xerrors.Errorf("sector store not found")
	}
	si, err := i.GetStorageEntryByKey(ctx, key)
	if err != nil {
		log.Errorf("octopus: redisindex: GetStorageEntryByKey %s error: %v", key, err)
		return storiface.StorageInfo{}, err
	}

	return *si.Info, nil
}

func (i *RedisIndex) StorageReportHealth(ctx context.Context, id storiface.ID, report storiface.HealthReport) error {
	log.Debug("octopus: redisindex StorageReportHealth")

	i.lk.Lock()
	defer i.lk.Unlock()

	key := fmt.Sprintf("%s:%s:%v", i.minerId, RedisStoresKeyPrefix, id)
	exists, err := i.client.Exists(ctx, key).Result()
	if err != nil {
		log.Errorf("octopus: redisindex: exists %s error: %v", key, err)
		return err
	}
	if exists == 0 {
		return xerrors.Errorf("health report for unknown storage: %s", id)
	}
	ent, err := i.GetStorageEntryByKey(ctx, key)
	if err != nil {
		log.Errorf("octopus: redisindex: GetStorageEntryByKey %s error: %v", key, err)
		return err
	}

	ent.Fsi = report.Stat
	if report.Err != "" {
		ent.HeartbeatErr = errors.New(report.Err)
	} else {
		ent.HeartbeatErr = nil
	}
	ent.LastHeartbeat = time.Now()

	if err = i.SetStorageEntryByKey(ctx, key, ent); err != nil {
		log.Errorf("octopus: redisindex: SetStorageEntryByKey %s error: %v", key, err)
		return err
	}

	return nil
}

func (i *RedisIndex) StorageDeclareSector(ctx context.Context, storageID storiface.ID, s abi.SectorID, ft storiface.SectorFileType, primary bool) error {
	log.Debug("octopus: redisindex StorageDeclareSector")

	i.lk.Lock()
	defer i.lk.Unlock()

loop:
	for _, fileType := range storiface.PathTypes {
		if fileType&ft == 0 {
			continue
		}

		key := fmt.Sprintf("%v:%s:%v:%v:%d", i.minerId, RedisSectorsKeyPrefix, s.Miner, s.Number, ft)

		sidsStrings, _ := i.client.LRange(ctx, key, 0, -1).Result()
		for index, sidString := range sidsStrings {
			sid, err := i.decodeDeclMeta(sidString)
			if err != nil {
				log.Errorf("octopus: redisindex: decode DeclMeta error: %v", err)
				return err
			}

			if sid.Storage == storageID {
				if !sid.Primary && primary {
					sid.Primary = true
					s, err := i.encodeDeclMeta(sid)
					if err != nil {
						log.Errorf("octopus: redisindex: encode DeclMeta error: %v", err)
						return err
					}
					_, err = i.client.LSet(ctx, key, int64(index), s).Result()
					if err != nil {
						log.Errorf("octopus: redisindex: lset key %s error: %v", key, err)
						return err
					}
				} else {
					log.Warnf("sector %v redeclared in %s", s, storageID)
				}
				continue loop
			}
		}
		dm, err := i.encodeDeclMeta(&declMeta{
			Storage: storageID,
			Primary: primary,
		})
		if err != nil {
			log.Errorf("octopus: redisindex: encode DeclMeta error: %v", err)
			return err
		}
		_, err = i.client.LPush(ctx, key, dm).Result()
		if err != nil {
			log.Errorf("octopus: redisindex: lpush key %s error: %v", key, err)
			return err
		}

		sectorsKey := fmt.Sprintf("%v:%s", i.minerId, RedisSectorsKeyPrefix)
		newSectorVal := fmt.Sprintf("%v:%d", s.Number, ft)
		_, err = i.client.SAdd(ctx, sectorsKey, newSectorVal).Result()
		if err != nil {
			log.Errorf("octopus: redisindex: sadd %s %s", sectorsKey, newSectorVal)
			return err
		}
	}

	return nil
}

func (i *RedisIndex) StorageDropSector(ctx context.Context, storageID storiface.ID, s abi.SectorID, ft storiface.SectorFileType) error {
	log.Debug("octopus: redisindex StorageDropSector")

	i.lk.Lock()
	defer i.lk.Unlock()

	for _, fileType := range storiface.PathTypes {
		if fileType&ft == 0 {
			continue
		}

		key := fmt.Sprintf("%v:%s:%v:%v:%d", i.minerId, RedisSectorsKeyPrefix, s.Miner, s.Number, fileType)
		len, err := i.client.LLen(ctx, key).Result()
		if err != nil {
			log.Errorf("octopus: redisindex: llen key %s error: %v", key, err)
			return err
		}

		if len == 0 {
			continue
		}

		sidStrings, _ := i.client.LRange(ctx, key, 0, -1).Result()
		for _, sidString := range sidStrings {
			sid, err := i.decodeDeclMeta(sidString)
			if err != nil {
				log.Errorf("octopus: redisindex: decode DeclMeta error: %v", err)
				return err
			}
			if sid.Storage == storageID {
				_, err = i.client.LRem(ctx, key, 0, sidString).Result()
				if err != nil {
					log.Errorf("octopus: redisindex: lrem %s error: %v", sidString, err)
					return err
				}
				len--
			}
		}
		if len == 0 {
			_, err = i.client.Del(ctx, key).Result()
			if err != nil {
				log.Errorf("octopus: redisindex: del %s error: %v", key, err)
				return err
			}
			sectorsKey := fmt.Sprintf("%v:%s", i.minerId, RedisSectorsKeyPrefix)
			sectorKeyRemoved := fmt.Sprintf("%v:%d", s.Number, fileType)
			_, err = i.client.SRem(ctx, sectorsKey, sectorKeyRemoved).Result()
			if err != nil {
				log.Errorf("octopus: redisindex: srem %s %s", sectorsKey, sectorKeyRemoved)
				return err
			}
			continue
		}
	}

	return nil
}

func (i *RedisIndex) StorageFindSector(ctx context.Context, s abi.SectorID, ft storiface.SectorFileType, ssize abi.SectorSize, allowFetch bool) ([]storiface.SectorStorageInfo, error) {
	log.Debug("octopus: redisindex StorageFindSector")

	i.lk.RLock()
	defer i.lk.RUnlock()

	storageIDs := map[storiface.ID]uint64{}
	isprimary := map[storiface.ID]bool{}

	allowTo := map[storiface.Group]struct{}{}

	for _, pathType := range storiface.PathTypes {
		if ft&pathType == 0 {
			continue
		}

		key := fmt.Sprintf("%v:%s:%v:%v:%d", i.minerId, RedisSectorsKeyPrefix, s.Miner, s.Number, pathType)
		sidStrings, _ := i.client.LRange(ctx, key, 0, -1).Result()

		for _, sidString := range sidStrings {
			sid, err := i.decodeDeclMeta(sidString)
			if err != nil {
				log.Errorf("octopus: redisindex: decode DeclMeta error: %v", err)
				return nil, err
			}
			storageIDs[sid.Storage]++
			isprimary[sid.Storage] = isprimary[sid.Storage] || sid.Primary
		}
	}

	out := make([]storiface.SectorStorageInfo, 0, len(storageIDs))

	for id, n := range storageIDs {
		key := fmt.Sprintf("%s:%s:%v", i.minerId, RedisStoresKeyPrefix, id)
		exists, _ := i.client.Exists(ctx, key).Result()
		if exists == 0 {
			log.Warnf("storage %s is not present in sector index (referenced by sector %v)", id, s)
			continue
		}

		st, err := i.GetStorageEntryByKey(ctx, key)
		if err != nil {
			log.Errorf("octopus: redisindex: GetStorageEntryByKey %s error: %v", key, err)
			return nil, err
		}

		urls := make([]string, len(st.Info.URLs))
		for k, u := range st.Info.URLs {
			rl, err := url.Parse(u)
			if err != nil {
				return nil, xerrors.Errorf("failed to parse url: %w", err)
			}

			rl.Path = gopath.Join(rl.Path, ft.String(), storiface.SectorName(s))
			urls[k] = rl.String()
		}

		out = append(out, storiface.SectorStorageInfo{
			ID:     id,
			URLs:   urls,
			Weight: st.Info.Weight * n, // storage with more sector types is better

			CanSeal:  st.Info.CanSeal,
			CanStore: st.Info.CanStore,

			Primary: isprimary[id],
		})
	}

	if allowFetch {
		//spaceReq, err := ft.SealSpaceUse(ssize)
		//if err != nil {
		//	return nil, xerrors.Errorf("estimating required space: %w", err)
		//}

		//allStoresKey := fmt.Sprintf("%v:%s:*", i.minerId, RedisStoresKeyPrefix)
		//keys, _ := i.client.Keys(ctx, allStoresKey).Result()
		allStoresKey := fmt.Sprintf("%v:%s", i.minerId, RedisStoresKeyPrefix)
		keys, err := i.client.SMembers(ctx, allStoresKey).Result()
		if err != nil {
			log.Errorf("octopus: redisindex: smember %s error: %v", allStoresKey, err)
			return nil, err
		}

		for _, key := range keys {
			storageKey := fmt.Sprintf("%v:%s:%s", i.minerId, RedisStoresKeyPrefix, key)
			st, err := i.GetStorageEntryByKey(ctx, storageKey)
			if err != nil {
				log.Errorf("octopus: redisindex: GetStorageEntryByKey %s error: %v", storageKey, err)
				return nil, err
			}

			if !st.Info.CanSeal {
				continue
			}

			// octopus: do not check space.
			//if spaceReq > uint64(st.Fsi.Available) {
			//	log.Debugf("not selecting on %s, out of space (available: %d, need: %d)", st.Info.ID, st.Fsi.Available, spaceReq)
			//	continue
			//}

			if time.Since(st.LastHeartbeat) > heartbeatThresh() {
				log.Debugf("not selecting on %s, didn't receive heartbeats for %s", st.Info.ID, time.Since(st.LastHeartbeat))
				continue
			}

			if st.HeartbeatErr != nil {
				log.Debugf("not selecting on %s, heartbeat error: %s", st.Info.ID, st.HeartbeatErr)
				continue
			}

			if _, ok := storageIDs[st.Info.ID]; ok {
				continue
			}

			if allowTo != nil {
				allow := false
				for _, group := range st.Info.Groups {
					if _, found := allowTo[group]; found {
						log.Debugf("path %s in allowed group %s", st.Info.ID, group)
						allow = true
						break
					}
				}

				if !allow {
					log.Debugf("not selecting on %s, not in allowed group, allow %+v; path has %+v", st.Info.ID, allowTo, st.Info.Groups)
					continue
				}
			}

			urls := make([]string, len(st.Info.URLs))
			for k, u := range st.Info.URLs {
				rl, err := url.Parse(u)
				if err != nil {
					return nil, xerrors.Errorf("failed to parse url: %w", err)
				}

				rl.Path = gopath.Join(rl.Path, ft.String(), storiface.SectorName(s))
				urls[k] = rl.String()
			}

			out = append(out, storiface.SectorStorageInfo{
				ID:     st.Info.ID,
				URLs:   urls,
				Weight: st.Info.Weight * 0, // TODO: something better than just '0'

				CanSeal:  st.Info.CanSeal,
				CanStore: st.Info.CanStore,

				Primary: false,
			})
		}
	}

	return out, nil
}

func heartbeatThresh() time.Duration {
	var thresh time.Duration
	threshStr := os.Getenv("STORAGE_HEARTBEAT_THRESH")
	if threshStr == "" {
		thresh = SkippedHeartbeatThresh
	} else {
		threshInt, err := strconv.Atoi(threshStr)
		if err != nil {
			thresh = SkippedHeartbeatThresh
		} else {
			thresh = time.Duration(threshInt) * time.Second
		}
	}
	return thresh
}

func (i *RedisIndex) StorageBestAlloc(ctx context.Context, allocate storiface.SectorFileType, ssize abi.SectorSize, pathType storiface.PathType) ([]storiface.StorageInfo, error) {
	log.Debug("octopus: redisindex StorageBestAlloc")

	i.lk.RLock()
	defer i.lk.RUnlock()

	var candidates []storageEntry

	//spaceReq, err := allocate.SealSpaceUse(ssize)
	//if err != nil {
	//	return nil, xerrors.Errorf("estimating required space: %w", err)
	//}

	//allStoresKey := fmt.Sprintf("%v:%s:*", i.minerId, RedisStoresKeyPrefix)
	//keys, _ := i.client.Keys(ctx, allStoresKey).Result()
	allStoresKey := fmt.Sprintf("%v:%s", i.minerId, RedisStoresKeyPrefix)
	keys, err := i.client.SMembers(ctx, allStoresKey).Result()
	if err != nil {
		log.Errorf("octopus: redisindex: smembers %s error: %v", allStoresKey, err)
		return nil, err
	}

	for _, key := range keys {
		storageKey := fmt.Sprintf("%v:%s:%s", i.minerId, RedisStoresKeyPrefix, key)
		p, err := i.GetStorageEntryByKey(ctx, storageKey)
		if err != nil {
			log.Errorf("octopus: redisindex: GetStorageEntryByKey %s error: %v", storageKey, err)
			return nil, err
		}

		if (pathType == storiface.PathSealing) && !p.Info.CanSeal {
			continue
		}
		if (pathType == storiface.PathStorage) && !p.Info.CanStore {
			continue
		}

		//if spaceReq > uint64(p.Fsi.Available) {
		//	log.Debugf("not allocating on %s, out of space (available: %d, need: %d)", p.Info.ID, p.Fsi.Available, spaceReq)
		//	continue
		//}

		if time.Since(p.LastHeartbeat) > heartbeatThresh() {
			log.Debugf("not allocating on %s, didn't receive heartbeats for %s", p.Info.ID, time.Since(p.LastHeartbeat))
			continue
		}

		if p.HeartbeatErr != nil {
			log.Debugf("not allocating on %s, heartbeat error: %s", p.Info.ID, p.HeartbeatErr)
			continue
		}

		candidates = append(candidates, *p)
	}

	if len(candidates) == 0 {
		return nil, xerrors.New("no good path found")
	}

	sort.Slice(candidates, func(i, j int) bool {
		iw := big.Mul(big.NewInt(candidates[i].Fsi.Available), big.NewInt(int64(candidates[i].Info.Weight)))
		jw := big.Mul(big.NewInt(candidates[j].Fsi.Available), big.NewInt(int64(candidates[j].Info.Weight)))

		return iw.GreaterThanEqual(jw)
	})

	out := make([]storiface.StorageInfo, len(candidates))
	for i, candidate := range candidates {
		out[i] = *candidate.Info
	}

	return out, nil
}
