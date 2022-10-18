package common

import (
	"context"
	"encoding/hex"
	"os"

	"github.com/filecoin-project/go-bitfield"

	"github.com/gbrlsnchs/jwt/v3"
	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-jsonrpc/auth"

	"github.com/filecoin-project/lotus/api"
	apitypes "github.com/filecoin-project/lotus/api/types"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/journal/alerting"
	"github.com/filecoin-project/lotus/node/modules/dtypes"
)

var session = uuid.New()

type CommonAPI struct {
	fx.In

	Alerting     *alerting.Alerting
	APISecret    *dtypes.APIAlg
	ShutdownChan dtypes.ShutdownChan
}

type jwtPayload struct {
	Allow []auth.Permission
}

func (a *CommonAPI) AuthVerify(ctx context.Context, token string) ([]auth.Permission, error) {
	var payload jwtPayload
	if _, err := jwt.Verify([]byte(token), (*jwt.HMACSHA)(a.APISecret), &payload); err != nil {
		return nil, xerrors.Errorf("JWT Verification failed: %w", err)
	}

	return payload.Allow, nil
}

func (a *CommonAPI) AuthNew(ctx context.Context, perms []auth.Permission) ([]byte, error) {
	p := jwtPayload{
		Allow: perms, // TODO: consider checking validity
	}

	return jwt.Sign(&p, (*jwt.HMACSHA)(a.APISecret))
}

func (a *CommonAPI) Discover(ctx context.Context) (apitypes.OpenRPCDocument, error) {
	return build.OpenRPCDiscoverJSON_Full(), nil
}

func (a *CommonAPI) Version(context.Context) (api.APIVersion, error) {
	v, err := api.VersionForType(api.RunningNodeType)
	if err != nil {
		return api.APIVersion{}, err
	}

	return api.APIVersion{
		Version:    build.UserVersion(),
		APIVersion: v,

		BlockDelay: build.BlockDelaySecs,
	}, nil
}

func (a *CommonAPI) LogList(context.Context) ([]string, error) {
	return logging.GetSubsystems(), nil
}

func (a *CommonAPI) LogSetLevel(ctx context.Context, subsystem, level string) error {
	return logging.SetLogLevel(subsystem, level)
}

func (a *CommonAPI) LogAlerts(ctx context.Context) ([]alerting.Alert, error) {
	return a.Alerting.GetAlerts(), nil
}

func (a *CommonAPI) Shutdown(ctx context.Context) error {
	a.ShutdownChan <- struct{}{}
	return nil
}

func (a *CommonAPI) Session(ctx context.Context) (uuid.UUID, error) {
	return session, nil
}

func (a *CommonAPI) Closing(ctx context.Context) (<-chan struct{}, error) {
	return make(chan struct{}), nil // relies on jsonrpc closing
}

func (a *CommonAPI) DecodeBitField(ctx context.Context, val string) ([]uint64, error) {
	d, err := hex.DecodeString(val)
	if err != nil {
		return []uint64{}, err
	}
	rle, err := bitfield.NewFromBytes(d)
	if err != nil {
		return []uint64{}, err
	}
	vals, err := rle.All(100000000000)
	return vals, nil
}

func (a *CommonAPI) GetEnv(ctx context.Context, v string) (string, error) {
	return os.Getenv(v), nil
}

func (a *CommonAPI) SetEnv(ctx context.Context, v string, vv string) error {
	return os.Setenv(v, vv)
}

func (a *CommonAPI) GetAllEnv(ctx context.Context) (map[string]string, error) {
	envs := []string{
		"REDIS_CONN",
		"REDIS_POOLSIZE",
		"ENABLE_WINNING_POST",
		"ENABLE_WINDOW_POST",
		"ABILITY",
		"DECLARE_SEAL_SECTORS",
		"DECLARE_STORE_SECTORS",
		"DECLARE_SECTORS",
		"CC_SECTOR_DATA_PATH",
		"STORAGE_HEARTBEAT_INTERVAL",
		"STORAGE_HEARTBEAT_THRESHOLD",
		"REMOVE_CAN_STORE",
		"STORAGE_TYPE",
		"COMMIT_BATCH_LOCK_MIN",
		"COMMIT_BATCH_MAX_INDIVIDUAL",
		"P1P2_DURATION_EPOCHS",
		"DISABLE_POST_MSG",
		"POST_BEFORE_CLOSE_EPOCHS",
		"POST_MSG_DELAY",
		"ENABLE_NEXT_DEADLINE_CHECK",
		"POST_MINER_INDEX",
		"POST_MAX_PARTITIONS",
		"POST_TOTAL_MINERS",
		"POST_RETRIES",
		"MARKET_SHARE_OFFLINE_CAR",
		"MARKET_ENABLE_OFFLINE_VERIFY",
		"MARKET_KEEP_OFFLINE_CAR",
		"SECTOR_MAX_LOG_ENTRIES",
		"SUBMIT_BLOCK_BEFOREHAND",
		"PROPAGATION_DELAY",
		"MINER_PENDING_PROVE_MSG_COUNT_LIMIT",
		"P2_CHECK_ROUNDS",
		"STAGED_INCLUDE_ADD_PIECE_FAILED",
		"ADDBALANCE_USE_WORKER",
		"DISABLE_SEALING",
		"ENABLE_FULLNODE_SWITCH",
		"FULLNODE_SWITCH_INTERVAL",
		"MAX_ESTIMATE_GAS_BLKS",
		"WDPOST_INDIVIDUAL_BATCH",
	}

	var envMap map[string]string
	envMap = make(map[string]string)
	for _, k := range envs {
		envMap[k] = os.Getenv(k)
	}
	return envMap, nil
}
