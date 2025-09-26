package service

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/pkg/errors"
	xsync "github.com/puzpuzpuz/xsync/v3"
	"github.com/shopspring/decimal"
	"github.com/sirupsen/logrus"
	fee_pool "github.com/stafiprotocol/eth-lsd-relay/bindings/FeePool"
	network_withdraw "github.com/stafiprotocol/eth-lsd-relay/bindings/NetworkWithdraw"
	node_deposit "github.com/stafiprotocol/eth-lsd-relay/bindings/NodeDeposit"
	"github.com/stafiprotocol/eth-lsd-relay/pkg/config"
	"github.com/stafiprotocol/eth-lsd-relay/pkg/connection"
	"github.com/stafiprotocol/eth-lsd-relay/pkg/connection/types"
	"github.com/stafiprotocol/eth-lsd-relay/pkg/utils"
	"github.com/stretchr/testify/assert"
)

type ReproducePriorityFeeError struct {
	connection               *connection.Connection
	nodeDepositContract      *node_deposit.CustomNodeDeposit
	feePoolContract          *fee_pool.FeePool
	feePoolAddress           common.Address
	transferFeeAddresses     []string
	platformCommissionRate   decimal.Decimal
	nodeCommissionRate       decimal.Decimal
	manager                  *ServiceManager
	eventFilterMaxSpanBlocks uint64

	validatorsByIndex map[uint64]*Validator    // validator index -> validator
	nodes             map[common.Address]*Node // nodeAddress -> node
	validators        map[string]*Validator    // pubkey(hex.encodeToString) -> validator

	// test only
	cachedBeaconBlockByExecBlockHeight *xsync.MapOf[uint64, *CachedBeaconBlock]
	log                                *logrus.Entry
	feePoolBalances                    *xsync.MapOf[uint64, *big.Int]
}

// return (user reward, node reward, platform fee) decimals 18
func (s *ReproducePriorityFeeError) getUserNodePlatformFromPriorityFee(log *logrus.Entry, latestDistributeHeight, targetEth1BlockHeight uint64) (decimal.Decimal, decimal.Decimal, decimal.Decimal, NodeNewRewardsMap, error) {
	ctx := context.Background()
	totalUserEthDeci := decimal.Zero
	totalNodeEthDeci := decimal.Zero
	totalPlatformEthDeci := decimal.Zero
	nodeNewRewardsMap := make(NodeNewRewardsMap)

	log = log.WithFields(logrus.Fields{
		"targetBlock":                        targetEth1BlockHeight,
		"getUserNodePlatformFromPriorityFee": true,
	})

	log.Debug("start filter all withdrawn events")
	// filter all withdrawn events
	withdrawals := make(map[uint64]*big.Int)
	for i := latestDistributeHeight + 1; i <= targetEth1BlockHeight; i += s.eventFilterMaxSpanBlocks {
		end := i + s.eventFilterMaxSpanBlocks - 1
		if end > targetEth1BlockHeight {
			end = targetEth1BlockHeight
		}
		withdrawIter, err := s.feePoolContract.FilterEtherWithdrawn(&bind.FilterOpts{
			Start:   i,
			End:     &end,
			Context: context.Background(),
		})
		if err != nil {
			return decimal.Zero, decimal.Zero, decimal.Zero, nil, fmt.Errorf("filter ether withdrawn failed: %w", err)
		}
		for withdrawIter.Next() {
			block := withdrawIter.Event.Raw.BlockNumber
			if _, ok := withdrawals[block]; !ok {
				withdrawals[block] = big.NewInt(0)
			}
			withdrawals[block] = new(big.Int).Add(withdrawals[block], withdrawIter.Event.Amount)
		}
	}
	log.Debug("end filter all withdrawn events")

	for i := latestDistributeHeight + 1; i <= targetEth1BlockHeight; i++ {
		// report progress in every 10 blocks
		if (i-latestDistributeHeight)%10 == 0 {
			log.WithFields(logrus.Fields{
				"block":    i,
				"progress": float64(i-latestDistributeHeight) / float64(targetEth1BlockHeight-latestDistributeHeight) * float64(100),
			}).Debug("report progress")
		}

		block, err := s.getBeaconBlock(i)
		if err != nil {
			return decimal.Zero, decimal.Zero, decimal.Zero, nil, err
		}

		// cal priority fee at this block
		curBlockNumber := big.NewInt(int64(i))
		feePoolPreBalance, err := s.getFeePoolBalance(i - 1)
		if err != nil {
			return decimal.Zero, decimal.Zero, decimal.Zero, nil, err
		}
		feePoolCurBalance, err := s.getFeePoolBalance(i)
		if err != nil {
			return decimal.Zero, decimal.Zero, decimal.Zero, nil, err
		}

		decreaseAmount, ok := withdrawals[i]
		if !ok {
			decreaseAmount = big.NewInt(0)
		}
		totalFeePoolCurBalance := new(big.Int).Add(feePoolCurBalance, decreaseAmount)
		if totalFeePoolCurBalance.Cmp(feePoolPreBalance) < 0 {
			return decimal.Zero, decimal.Zero, decimal.Zero, nil, fmt.Errorf("should not happened here when cal priority fee, block: %d", i)
		}
		feeAmountAtThisBlock := decimal.NewFromBigInt(new(big.Int).Sub(totalFeePoolCurBalance, feePoolPreBalance), 0)

		var userRewardDeci, nodeRewardDeci, platformFeeDeci decimal.Decimal
		val, _ := s.getValidatorByIndex(block.ProposerIndex)
		if val == nil {
			if feeAmountAtThisBlock.GreaterThan(decimal.Zero) {
				nodeRewardDeci = decimal.Zero
				platformFeeDeci = feeAmountAtThisBlock.Mul(s.platformCommissionRate).Floor()
				userRewardDeci = feeAmountAtThisBlock.Sub(platformFeeDeci.Add(nodeRewardDeci))
				log.WithFields(logrus.Fields{
					"block":  i,
					"amount": feeAmountAtThisBlock.DivRound(decimal.NewFromInt(1e18), 18).StringFixed(18),
				}).Debug("found transferFee")
			}
		} else {
			// get transfered fee from trace call
			trace, err := s.connection.Eth1Client().Debug_TraceBlockByNumber(ctx, curBlockNumber, connection.Tracer{Tracer: "callTracer"})
			if err != nil {
				return decimal.Zero, decimal.Zero, decimal.Zero, nil, err
			}
			transferFee := decimal.Zero
			seekFn := func(tx *connection.TxTrace) bool {
				return tx.Error == "" && strings.EqualFold(tx.To, s.feePoolAddress.String())
			}
			for _, tx := range trace {
				if tx.Result.Error != "" {
					continue // skip FAILED tx
				}

				amount := WalkTrace(seekFn, decimal.Zero, tx.Result)
				if amount.GreaterThan(decimal.Zero) {
					transferFee = transferFee.Add(amount)
					log.WithFields(logrus.Fields{
						"block":  i,
						"txHash": tx.TxHash.Hex(),
						"amount": amount.DivRound(decimal.NewFromInt(1e18), 18).StringFixed(18),
					}).Debug("found transferFee")
				}
			}
			if transferFee.GreaterThan(decimal.Zero) {
				_platformFeeDeci := transferFee.Mul(s.platformCommissionRate).Floor()
				_userRewardDeci := transferFee.Sub(_platformFeeDeci)
				userRewardDeci = userRewardDeci.Add(_userRewardDeci)
				platformFeeDeci = platformFeeDeci.Add(_platformFeeDeci)
			}

			// only distribute tip fee to node
			tipFee := feeAmountAtThisBlock.Sub(transferFee)
			if tipFee.GreaterThan(decimal.Zero) {
				// cal rewards
				_userRewardDeci, _nodeRewardDeci, _platformFeeDeci := utils.GetUserNodePlatformReward(s.nodeCommissionRate, s.platformCommissionRate, val.NodeDepositAmountDeci, tipFee)
				s.log.WithFields(logrus.Fields{
					"block":                  i,
					"amount":                 tipFee.String(),
					"userReward":             _userRewardDeci.String(),
					"nodeReward":             _nodeRewardDeci.String(),
					"platformFee":            _platformFeeDeci.String(),
					"nodeCommissionRate":     s.nodeCommissionRate.String(),
					"platformCommissionRate": s.platformCommissionRate.String(),
					"nodeDepositAmountDeci":  val.NodeDepositAmountDeci.String(),
				}).Debug("cal rewards")
				userRewardDeci = userRewardDeci.Add(_userRewardDeci)
				nodeRewardDeci = nodeRewardDeci.Add(_nodeRewardDeci)
				platformFeeDeci = platformFeeDeci.Add(_platformFeeDeci)

				// cal node reward
				nodeNewReward, exist := nodeNewRewardsMap[val.NodeAddress]
				if exist {
					nodeNewReward.TotalRewardAmount = nodeNewReward.TotalRewardAmount.Add(nodeRewardDeci)
				} else {
					n := NodeNewReward{
						Address:                val.NodeAddress.String(),
						TotalRewardAmount:      nodeRewardDeci,
						TotalExitDepositAmount: decimal.Zero,
					}
					nodeNewRewardsMap[val.NodeAddress] = &n
				}
			}
		}

		// cal total vals
		totalUserEthDeci = totalUserEthDeci.Add(userRewardDeci)
		totalNodeEthDeci = totalNodeEthDeci.Add(nodeRewardDeci)
		totalPlatformEthDeci = totalPlatformEthDeci.Add(platformFeeDeci)
	}

	{
		// hotfix: distribute blocked transfer fee
		feePoolBalance, err := s.getFeePoolBalance(targetEth1BlockHeight)
		if err != nil {
			return decimal.Zero, decimal.Zero, decimal.Zero, nil, err
		}
		feePoolBalanceDeci := decimal.NewFromBigInt(feePoolBalance, 0)
		blockedTransferFeeDeci := feePoolBalanceDeci.Sub(totalUserEthDeci.Add(totalNodeEthDeci).Add(totalPlatformEthDeci))
		if blockedTransferFeeDeci.GreaterThan(decimal.Zero) {
			maxAmountPerEra := decimal.NewFromInt(int64(s.manager.cfg.DistributeBlockedTransferFeePerEra)).Mul(utils.EtherDeci)
			currentDistributeAmount := blockedTransferFeeDeci
			if currentDistributeAmount.GreaterThan(maxAmountPerEra) {
				currentDistributeAmount = maxAmountPerEra
			}

			platformFeeDeci := currentDistributeAmount.Mul(s.platformCommissionRate).Floor()
			userRewardDeci := currentDistributeAmount.Sub(platformFeeDeci)
			log.WithFields(logrus.Fields{
				"block":  targetEth1BlockHeight,
				"amount": currentDistributeAmount.DivRound(decimal.NewFromInt(1e18), 18).StringFixed(18),
			}).Debug("distribute blocked transferee fee")

			totalUserEthDeci = totalUserEthDeci.Add(userRewardDeci)
			totalPlatformEthDeci = totalPlatformEthDeci.Add(platformFeeDeci)
		}
	}

	return totalUserEthDeci, totalNodeEthDeci, totalPlatformEthDeci, nodeNewRewardsMap, nil
}

// saveFeePoolBalancesToFile saves fee pool balances to a JSON file
func (s *ReproducePriorityFeeError) saveFeePoolBalancesToFile(filename string) error {
	balances := make(map[string]string)
	s.feePoolBalances.Range(func(key uint64, value *big.Int) bool {
		balances[strconv.FormatUint(key, 10)] = value.String()
		return true
	})

	data, err := json.MarshalIndent(balances, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal balances: %w", err)
	}

	return os.WriteFile(filename, data, 0644)
}

// loadFeePoolBalancesFromFile loads fee pool balances from a JSON file
func (s *ReproducePriorityFeeError) loadFeePoolBalancesFromFile(filename string) error {
	data, err := os.ReadFile(filename)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	var balances map[string]string
	if err := json.Unmarshal(data, &balances); err != nil {
		return fmt.Errorf("failed to unmarshal balances: %w", err)
	}

	for blockStr, balanceStr := range balances {
		block, err := strconv.ParseUint(blockStr, 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse block number: %w", err)
		}
		balance, ok := new(big.Int).SetString(balanceStr, 10)
		if !ok {
			return fmt.Errorf("failed to parse balance: %s", balanceStr)
		}
		s.feePoolBalances.Store(block, balance)
	}

	return nil
}

func (s *ReproducePriorityFeeError) cacheFeePoolBalances(filename string, fromBlock, toBlock uint64) error {
	// Load existing data from file
	if err := s.loadFeePoolBalancesFromFile(filename); err != nil {
		return err
	}

	batchQueryBalanceBlockNumbers := uint64(2500)
	s.log.Debug("start cache fee pool balances")
	defer func() {
		// Save results to file
		if err := s.saveFeePoolBalancesToFile(filename); err != nil {
			s.log.Warnf("failed to save fee pool balances to file: %v", err)
		} else {
			s.log.Debug("saved fee pool balances to file")
		}

		s.log.Debug("end cache fee pool balances")
	}()

	for i := fromBlock; i <= toBlock; i += batchQueryBalanceBlockNumbers {
		end := i + batchQueryBalanceBlockNumbers - 1
		if end > toBlock {
			end = toBlock
		}
		blocks := make([]uint64, 0, batchQueryBalanceBlockNumbers)
		for j := i; j <= end; j++ {
			if _, ok := s.feePoolBalances.Load(uint64(j)); !ok {
				blocks = append(blocks, j)
			}
		}
		if len(blocks) > 0 {
			_feePoolBalances, err := s.connection.Eth1Client().(*connection.Eth1Client).BatchBalancesAtBlocks(context.Background(), s.feePoolAddress, blocks)
			if err != nil {
				return fmt.Errorf("fail to batch query fee pool balances: %w", err)
			}
			for block, balance := range _feePoolBalances {
				s.feePoolBalances.Store(block, balance)
			}
		}
	}

	return nil
}

func (s *ReproducePriorityFeeError) saveBeaconBlockToFile(filename string) error {
	result := make(map[uint64]*CachedBeaconBlock)
	s.cachedBeaconBlockByExecBlockHeight.Range(func(key uint64, value *CachedBeaconBlock) bool {
		result[key] = value
		return true
	})
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal beacon block: %w", err)
	}

	return os.WriteFile(filename, data, 0644)
}

func (s *ReproducePriorityFeeError) loadBeaconBlockFromFile(filename string) error {
	fileContent, err := os.ReadFile(filename)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	var data map[uint64]*CachedBeaconBlock
	if err := json.Unmarshal(fileContent, &data); err != nil {
		return fmt.Errorf("failed to unmarshal beacon block: %w", err)
	}

	for block, beaconBlock := range data {
		s.cachedBeaconBlockByExecBlockHeight.Store(block, beaconBlock)
	}

	return nil
}

func (s *ReproducePriorityFeeError) cacheBeaconBlock(cacheFilename string, from, to uint64) error {
	if err := s.loadBeaconBlockFromFile(cacheFilename); err != nil {
		return err
	}
	defer func() {
		if err := s.saveBeaconBlockToFile(cacheFilename); err != nil {
			s.log.Warnf("failed to save beacon block to file: %v", err)
		}
	}()

	ch := make(chan uint64)
	go func() {
		for i := from; i <= to; i++ {
			exist := false
			s.cachedBeaconBlockByExecBlockHeight.Range(func(eth1BlockNumber uint64, value *CachedBeaconBlock) bool {
				if value.BeaconBlockId == i {
					exist = true
					return false
				}
				return true
			})
			if !exist {
				ch <- i
			}
		}
		close(ch)
	}()

	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for blockId := range ch {
				// // report progress in every 10 blocks
				// if i%10 == 0 {
				// 	s.log.WithFields(logrus.Fields{
				// 		"block":    i,
				// 		"progress": float64(blockId-from) / float64(to-from) * float64(100),
				// 	}).Debug("report cache beacon block progress")
				// }
				block, exist, err := s.connection.GetBeaconBlock(blockId)
				if err != nil {
					return
				}
				if !exist {
					s.log.WithFields(logrus.Fields{
						"block": i,
					}).Warn("beacon block not found")
				} else {
					s.cachedBeaconBlockByExecBlockHeight.Store(block.ExecutionBlockNumber, &CachedBeaconBlock{
						BeaconBlockId:        block.Slot,
						ExecutionBlockNumber: block.ExecutionBlockNumber,
						ProposerIndex:        block.ProposerIndex,
					})
				}
			}
		}()
	}
	wg.Wait()

	return nil
}

func (s *ReproducePriorityFeeError) getFeePoolBalance(blockNumber uint64) (*big.Int, error) {
	balance, exist := s.feePoolBalances.Load(blockNumber)
	if !exist {
		return nil, fmt.Errorf("getFeePoolBalance %d error: not in cache", blockNumber)
	}
	return balance, nil
}

func (s *ReproducePriorityFeeError) getBeaconBlock(eth1BlockNumber uint64) (*CachedBeaconBlock, error) {
	block, exist := s.cachedBeaconBlockByExecBlockHeight.Load(eth1BlockNumber)
	if !exist {
		return nil, fmt.Errorf("getBeaconBlockByEth1BlockNumber %d error: not in cache", eth1BlockNumber)
	}
	return block, nil
}

func (s *ReproducePriorityFeeError) getValidatorByIndex(valIndex uint64) (*Validator, bool) {
	v, exist := s.validatorsByIndex[valIndex]
	return v, exist
}

func (s *ReproducePriorityFeeError) updateValidatorsFromNetwork() error {
	// 0. fetch new Nodes
	jobResult, err := s.connection.SubmitLatestCallJob(s.nodeDepositContract.NewGetNodesLengthMultiCall())
	if err != nil {
		return err
	}
	call := jobResult.Get()
	if call.Failed {
		return fmt.Errorf("nodeDepositContract.GetNodesLength failed: %w height: %d", call.Err, call.BlockNumber)
	}
	eth1LatestBlock := call.BlockNumber
	opts := s.connection.CallOpts(big.NewInt(int64(eth1LatestBlock)))

	nodesLength := call.Outputs.(*node_deposit.GetNodesLengthMultiCallOutput).Length
	if nodesLength.Uint64() == 0 {
		return nil
	}

	if len(s.nodes) < int(nodesLength.Int64()) {
		nodesOnChain, err := s.nodeDepositContract.GetNodes(opts, big.NewInt(0), nodesLength)
		if err != nil {
			return fmt.Errorf("nodeDepositContract.GetNodes failed: %w", err)
		}
		newNodes := nodesOnChain[len(s.nodes):]
		for i, nodeAddress := range newNodes {
			s.log.WithFields(logrus.Fields{
				"nodeAddress": nodeAddress,
				"total":       len(newNodes),
				"current":     i + 1,
			}).Info("fetching new node info")
			nodeInfo, err := s.nodeDepositContract.NodeInfoOf(opts, nodeAddress)
			if err != nil {
				return err
			}
			pubkeys, err := s.nodeDepositContract.GetPubkeysOfNode(opts, nodeAddress)
			if err != nil {
				return err
			}
			newVals, err := s.fetchNewVals(opts, pubkeys)
			if err != nil {
				return errors.Wrapf(err, "new node fetchNewVals")
			}

			// cache validators
			for key, val := range newVals {
				s.validators[key] = val
			}
			// cache node
			s.nodes[nodeAddress] = &Node{
				NodeAddress:  nodeAddress,
				NodeType:     nodeInfo.NodeType,
				PubkeyNumber: uint64(len(newVals)),
			}
			s.log.WithFields(logrus.Fields{
				"nodeAddress": nodeAddress,
				"total":       len(newNodes),
				"pubkeys":     len(newVals),
				"current":     i + 1,
			}).Info("added new node to validators list")
		}
	}

	// 1 fetch node's new pubkey
	for addr, node := range s.nodes {
		pubkeys, err := s.nodeDepositContract.GetPubkeysOfNode(opts, addr)
		if err != nil {
			return errors.Wrap(err, "get pubkeys of node: "+addr.String())
		}

		s.log.WithFields(logrus.Fields{
			"node":              node.NodeAddress,
			"pubkeysLenOnChain": len(pubkeys),
		}).Debug("updateValidatorsFromNetwork")

		if len(pubkeys) > int(node.PubkeyNumber) {
			newPubkeys := pubkeys[int(node.PubkeyNumber):]
			newVals, err := s.fetchNewVals(opts, newPubkeys)
			if err != nil {
				return errors.Wrapf(err, "new pubkey fetchNewVals")
			}

			// cache validators
			for key, val := range newVals {
				s.validators[key] = val
			}
			// cache node
			node.PubkeyNumber += uint64(len(newVals))
		}
	}

	// 2. update validator status on network
	validValidatorPubkeys := make([][]byte, 0, len(s.validators))
	for _, val := range s.validators {
		if val.Status > utils.ValidatorStatusWithdrawUnmatch {
			continue
		}

		if val.Status == utils.ValidatorStatusStaked {
			continue
		}

		validValidatorPubkeys = append(validValidatorPubkeys, val.Pubkey)
	}
	pubkeyInfo, err := s.nodeDepositContract.GetPubkeyInfoList(opts, validValidatorPubkeys)
	if err != nil {
		return errors.Wrapf(err, "get pubkey info list, len: %d", len(validValidatorPubkeys))
	}
	for pubkeyStr, info := range pubkeyInfo {
		s.validators[pubkeyStr].Status = info.Status
	}

	return nil
}

func (s *ReproducePriorityFeeError) fetchNewVals(call *bind.CallOpts, pubkeys [][]byte) (map[string]*Validator, error) {
	newVals := make(map[string]*Validator)

	pubkeyInfoList, err := s.nodeDepositContract.GetPubkeyInfoList(call, pubkeys)
	if err != nil {
		return nil, err
	}

	for pubkeyStr, pubkeyInfo := range pubkeyInfoList {
		nodeLocal, exist := s.nodes[pubkeyInfo.Owner]
		if !exist {
			nodeInfo, err := s.nodeDepositContract.NodeInfoOf(call, pubkeyInfo.Owner)
			if err != nil {
				return nil, err
			}

			node := Node{
				NodeAddress: pubkeyInfo.Owner,
				NodeType:    nodeInfo.NodeType,
			}

			s.nodes[node.NodeAddress] = &node

			nodeLocal = &node
		}

		pubkey, err := hex.DecodeString(pubkeyStr)
		if err != nil {
			return nil, err
		}
		val := Validator{
			Pubkey:      pubkey,
			NodeAddress: pubkeyInfo.Owner,
			// DepositSignature:      depositSig,
			NodeDepositAmountDeci: decimal.NewFromBigInt(pubkeyInfo.NodeDepositAmount, 0),
			NodeDepositAmount:     new(big.Int).Div(pubkeyInfo.NodeDepositAmount, big.NewInt(1e9)).Uint64(), // convert wei to Gwei
			DepositBlock:          pubkeyInfo.DepositBlock.Uint64(),
			ActiveEpoch:           0,
			EligibleEpoch:         0,
			ExitEpoch:             0,
			WithdrawableEpoch:     0,
			Balance:               0,
			EffectiveBalance:      0,
			NodeType:              nodeLocal.NodeType,
			Status:                pubkeyInfo.Status,
			ValidatorIndex:        0,
		}
		newVals[pubkeyStr] = &val
	}

	if len(pubkeys) != len(newVals) {
		return nil, fmt.Errorf("fetchNewVals, pubkeys length: %d not match newVals length: %d", len(pubkeys), len(newVals))
	}

	return newVals, nil

}

func (s *ReproducePriorityFeeError) updateValidatorsFromBeacon() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*120)
	defer cancel()

	pubkeys := make([]types.ValidatorPubkey, 0)
	for _, val := range s.validators {
		if val.Status == 3 || val.Status > 4 {
			pubkeys = append(pubkeys, types.ValidatorPubkey(val.Pubkey))
		}
	}
	if len(pubkeys) == 0 {
		s.log.Warn("no validators to update from beacon")
		return nil
	}

	validatorStatusMap, err := s.connection.GetValidatorStatuses(ctx, pubkeys, nil)
	if err != nil {
		return errors.Wrap(err, "syncValidatorLatestInfo GetValidatorStatuses failed")
	}

	s.log.WithFields(logrus.Fields{
		"validatorStatuses len": len(validatorStatusMap),
	}).Debug("validator statuses")

	for pubkey, status := range validatorStatusMap {
		pubkeyStr := pubkey.String()
		if status.Exists {
			// must exist here
			validator, exist := s.validators[pubkeyStr]
			if !exist {
				return fmt.Errorf("validator %s not exist", pubkeyStr)
			}

			updateBaseInfo := func() {
				// validator's info may be inited at any status
				validator.ActiveEpoch = status.ActivationEpoch
				validator.EligibleEpoch = status.ActivationEligibilityEpoch
				validator.ValidatorIndex = status.Index

				exitEpoch := status.ExitEpoch
				if exitEpoch == math.MaxUint64 {
					exitEpoch = 0
				}
				validator.ExitEpoch = exitEpoch

				withdrawableEpoch := status.WithdrawableEpoch
				if withdrawableEpoch == math.MaxUint64 {
					withdrawableEpoch = 0
				}
				validator.WithdrawableEpoch = withdrawableEpoch
			}

			updateBalance := func() {
				validator.Balance = status.Balance
				validator.EffectiveBalance = status.EffectiveBalance
			}
			validator.Status, err = mapValidatorStatus(&status)
			if err != nil {
				return fmt.Errorf("unsupported validator status %d", status.Status)
			}
			switch validator.Status {
			case utils.ValidatorStatusWaiting:
				validator.ValidatorIndex = status.Index
			case utils.ValidatorStatusActive, utils.ValidatorStatusActiveSlash,
				utils.ValidatorStatusExited, utils.ValidatorStatusExitedSlash,
				utils.ValidatorStatusWithdrawable, utils.ValidatorStatusWithdrawableSlash,
				utils.ValidatorStatusWithdrawDone, utils.ValidatorStatusWithdrawDoneSlash:
				updateBaseInfo()
				updateBalance()
			}
		}
	}

	// cache validators by index
	for _, validator := range s.validators {
		if validator.ValidatorIndex > 0 {
			s.validatorsByIndex[validator.ValidatorIndex] = validator
		}
	}

	return nil
}

func Test_ReproducePriorityFeeError(t *testing.T) {
	utils.MaxPartialWithdrawalAmount = 8000000 * 1e9                                                                       // unit Gwei
	utils.MaxPartialWithdrawalAmountDeci = decimal.NewFromInt(int64(utils.MaxPartialWithdrawalAmount)).Mul(utils.GweiDeci) // unit wei

	utils.StandardEffectiveBalance = 32000000 * 1e9                                                                    // unit Gwei
	utils.StandardEffectiveBalanceDeci = decimal.NewFromInt(int64(utils.StandardEffectiveBalance)).Mul(utils.GweiDeci) // unit wei

	logger := logrus.New()
	logger.SetLevel(logrus.TraceLevel)
	log := logrus.NewEntry(logger)

	endpoints := []config.Endpoint{
		{Eth1: os.Getenv("ETH1_ENDPOINT"), Eth2: os.Getenv("ETH2_ENDPOINT")},
	}
	c, err := connection.NewConnection(endpoints, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	latestDistributeHeight := uint64(24445107)
	targetEth1BlockHeight := uint64(24447478)

	feePoolAddress := common.HexToAddress("0x5eAd01d58067a68D0D700374500580eC5C961D0d")
	feePoolContract, err := fee_pool.NewFeePool(feePoolAddress, c.Eth1Client())
	if err != nil {
		t.Fatal(err)
	}

	reproducePriorityFeeError := &ReproducePriorityFeeError{
		connection:      c,
		feePoolContract: feePoolContract,
		feePoolAddress:  feePoolAddress,
		// transferFeeAddresses:   []string{"0x54da21340773FeCAF9A5bad0883a7fc594945d0A"},
		// platformCommissionRate: decimal.NewFromFloat(0.1),
		// nodeCommissionRate:     decimal.NewFromFloat(0.1),
		manager: &ServiceManager{
			cfg: &config.Config{
				DistributeBlockedTransferFeePerEra: 0,
			},
		},
		log:                      log,
		eventFilterMaxSpanBlocks: 2500,

		nodes:             make(map[common.Address]*Node),
		validators:        make(map[string]*Validator),
		validatorsByIndex: make(map[uint64]*Validator),

		cachedBeaconBlockByExecBlockHeight: xsync.NewMapOf[uint64, *CachedBeaconBlock](),
		feePoolBalances:                    xsync.NewMapOf[uint64, *big.Int](),
	}

	networkWithdrawContract, err := network_withdraw.NewNetworkWithdraw(common.HexToAddress("0x1F082785Ca889388Ce523BF3de6781E40b99B060"), c.Eth1Client())
	if err != nil {
		t.Fatal(err)
	}

	// init commission
	nodeCommissionRate, err := networkWithdrawContract.NodeCommissionRate(nil)
	if err != nil {
		t.Fatal(err)
	}
	reproducePriorityFeeError.nodeCommissionRate = decimal.NewFromBigInt(nodeCommissionRate, 0).Div(decimal.NewFromInt(1e18))
	platformCommissionRate, err := networkWithdrawContract.PlatformCommissionRate(nil)
	if err != nil {
		t.Fatal(err)
	}
	reproducePriorityFeeError.platformCommissionRate = decimal.NewFromBigInt(platformCommissionRate, 0).Div(decimal.NewFromInt(1e18))

	reproducePriorityFeeError.nodeDepositContract, err = node_deposit.NewCustomNodeDeposit(
		common.HexToAddress("0x3f82615aE0C027d587FD0d04d9EaCc8f0BaCFf94"),
		c.Eth1Client(),
		c.MultiCaller())
	if err != nil {
		t.Fatal(err)
	}

	if err := reproducePriorityFeeError.updateValidatorsFromNetwork(); err != nil {
		t.Fatal(err)
	}
	if err := reproducePriorityFeeError.updateValidatorsFromBeacon(); err != nil {
		t.Fatal(err)
	}

	{
		// cache beacon blocks
		from := uint64(7345000) // eth1: 24,443,720
		to := uint64(7351250)   // eth1: 24,449,898
		cacheFilename := fmt.Sprintf("/Users/duchengbin/Downloads/cache/beacon_block_cache.json")
		if err := reproducePriorityFeeError.cacheBeaconBlock(cacheFilename, from, to); err != nil {
			t.Fatal(err)
		}
	}

	{
		cacheFilename := fmt.Sprintf("/Users/duchengbin/Downloads/cache/fee_pool_balances_cache.json")
		if err := reproducePriorityFeeError.cacheFeePoolBalances(cacheFilename, latestDistributeHeight, targetEth1BlockHeight); err != nil {
			t.Fatal(err)
		}
	}

	userRewardDeci, nodeRewardDeci, platformFeeDeci, _, err := reproducePriorityFeeError.getUserNodePlatformFromPriorityFee(log, latestDistributeHeight, targetEth1BlockHeight)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("userRewardDeci: ", userRewardDeci.StringFixed(18))
	fmt.Println("nodeRewardDeci: ", nodeRewardDeci.StringFixed(18))
	fmt.Println("platformFeeDeci: ", platformFeeDeci.StringFixed(18))
}

func TestGetUserNodePlatformReward(t *testing.T) {
	utils.MaxPartialWithdrawalAmount = 8000000 * 1e9                                                                       // unit Gwei
	utils.MaxPartialWithdrawalAmountDeci = decimal.NewFromInt(int64(utils.MaxPartialWithdrawalAmount)).Mul(utils.GweiDeci) // unit wei

	utils.StandardEffectiveBalance = 32000000 * 1e9                                                                    // unit Gwei
	utils.StandardEffectiveBalanceDeci = decimal.NewFromInt(int64(utils.StandardEffectiveBalance)).Mul(utils.GweiDeci) // unit wei

	tipFee, err := decimal.NewFromString("1758039296323799286219")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(tipFee.Div(decimal.NewFromInt(1e18)).StringFixed(18))

	nodeCommissionRate, err := decimal.NewFromString("0.05")
	if err != nil {
		t.Fatal(err)
	}

	platformCommissionRate, err := decimal.NewFromString("0.05")
	if err != nil {
		t.Fatal(err)
	}

	nodeDepositAmountDeci, err := decimal.NewFromString("12000000000000000000000000")
	if err != nil {
		t.Fatal(err)
	}

	userReward, nodeReward, platformFee := utils.GetUserNodePlatformReward(nodeCommissionRate, platformCommissionRate, nodeDepositAmountDeci, tipFee)

	fmt.Println(tipFee.String())
	fmt.Println(userReward.String())
	fmt.Println(nodeReward.String())
	fmt.Println(platformFee.String())
	fmt.Println(userReward.Add(nodeReward).Add(platformFee).String())
	assert.Equal(t, tipFee.String(), userReward.Add(nodeReward).Add(platformFee).String())

	{
		/*
			dealedHeight 24447478
			 userAmountDeci 5257798.659626005335752430
			 nodeAmountDeci 1895.771269371582846659
			 platformAmountDeci 276826.022678704048347313
			 eraEndBalanceDeci 4123361.568838488347564763 left -1413158.884735592619381639
		*/
		user, _ := decimal.NewFromString("3915297719127192347339872")
		node, _ := decimal.NewFromString("1895771269371582846659")
		platform, _ := decimal.NewFromString("206168078441924417378232")
		user = user.Div(decimal.NewFromInt(1e18))
		node = node.Div(decimal.NewFromInt(1e18))
		platform = platform.Div(decimal.NewFromInt(1e18))
		eraEndBalance, _ := decimal.NewFromString("4123361.568838488347564763")
		fmt.Println(user.Add(node).Add(platform).String())
		fmt.Println(eraEndBalance.Sub(user.Add(node).Add(platform)).String())
	}
}
