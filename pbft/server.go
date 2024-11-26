package main

import (
	"context"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"log"
	"net"
	pbft "pbft/pbft"
	"pbft/queue"
	"pbft/utils"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"
)

const (
	MaxMsgNO                = 100
	maxfoutlyMaxFaultyNodes = 2
	MaxFaultyNode           = 2
)
const StateTimerOut = 5 * time.Second
const MaxStateMsgNO = 100
const CheckPointInterval = 1 << 5 //32
const CheckPointK = 2 * CheckPointInterval

type Stage int

const (
	Idle Stage = iota
	PrePrepared
	Prepared
	Committed
	Reply
	Byzantine
)

func (s Stage) String() string {
	switch s {
	case Idle:
		return "Idle"
	case PrePrepared:
		return "PrePrepared"
	case Prepared:
		return "Prepared"
	case Committed:
		return "Committed"
	case Reply:
		return "Reply"
	case Byzantine:
		return "Byzantine"

	}
	return "Unknown"
}

// func (t Stage) String() string {
// 	switch t {
// 	case Idle:
// 		return "Idle"
// 	case PrePrepared:
// 		return "PrePrepared"
// 	case Prepared:
// 		return "Prepared"
// 	case Committed:
// 		return "Committed"
// 	case Reply:
// 		return "Reply"
// 	}
// }

type server struct {
	pbft.UnimplementedPbftServer
	id                   int
	isPrimary            bool
	Datastore            map[int64]string
	Log                  []pbft.TransactionRequest
	peers                []*server
	collector            int
	signal               chan int
	currentviewid        int64
	sequenceid           int64
	MaxSeq               int64
	MiniSeq              int64
	mu                   sync.Mutex
	Viewid               int64
	activePeers          []*server
	preprepareResponses  map[int64][]*pbft.PrePrepareResponse
	prepareResponses     map[int64][]*pbft.Prepared
	commitResponses      map[int64][]*pbft.TransactionRequest
	balance              int64
	Timer                *time.Timer
	checkpoint           *CheckPoint
	lastCP               *CheckPoint
	lastCPMsg            map[int64]*CheckPoint
	pMsg                 map[int64]*PTuple
	primaryid            int64
	msgLogs              map[int64]*NormalLog
	sCache               *VCCache
	cliRecord            map[string]*ClientRecord
	LasExeSeq            int64 // Add this line
	checks               map[int64]*CheckPoint
	currentSequnece      int64
	directReplyChan      chan pbft.Reply
	host                 string
	port                 int
	address              string
	logs                 map[int64]*Request
	clients              map[string]*Client
	sequnecenumber       int64
	isByzantine          bool
	acceptedCount        int
	clientQueues         map[string]chan *pbft.TransactionRequest
	privateKey           *rsa.PrivateKey
	publicKey            *rsa.PublicKey // Add this line
	isActive             bool
	viewChangeInProgress bool
	viewChangeCond       *sync.Cond
	transactionQueue     chan *pbft.TransactionRequest
	requestTimeout       time.Duration
	timers               map[int64]*time.Timer
	requestCompleted     map[int64]bool
	transqueue           *queue.Queue
	transactionMap       map[int64]bool
	status               map[int64]string
}
type Client struct {
	id      string
	Balance int
}

func (s *server) getDirectReplyChan() chan pbft.Reply {
	return s.directReplyChan
}

type Request struct {
	Sequencenumber int64
	TimeStamp      int64
	ClientID       string
	Operation      string
}

type ClientRecord struct {
	LastReplyTime int64
	Request       map[int64]pbft.TransactionRequest
	Reply         map[int64]pbft.Reply
}
type PrePrepare struct {
	ViewID         int64
	Sequencenumber int64
	Digest         string
	Serverid       int64
	Transaction    []*pbft.Transaction
}

type PrepareMsg map[int64]*Prepare
type Prepare struct {
	ViewID         int64
	Sequencenumber int64
	Digest         string
	serverid       int64
}
type PTuple struct {
	PPMsg PrePrepare
	PMsg  PrepareMsg
}
type Commit struct {
	viewid         int64
	Sequencenumber int64
	Digest         string
	serverid       int64
}
type NormalLog struct {
	clientID   string
	PrePrepare *pbft.PrePrepareRequest
	Prepare    PrepareMsg
	Commit     map[int64]Commit
	Stage      Stage
}

func NewNormalLog() *NormalLog {
	nl := &NormalLog{
		Stage:      Idle,
		PrePrepare: nil,
		Prepare:    make(PrepareMsg),
		Commit:     make(map[int64]Commit),
	}
	return nl
}

type CheckPoint struct {
	Sequencenumber int64
	Digest         string
	isStable       bool
	ViewID         int64
	NodeID         int64
	CPMsg          map[int64]*CheckPoint
}
type ViewChange struct {
	NewViewID int64
	LastCPSeq int64
	NodeID    int64
	CMsg      map[int64]*CheckPoint
	PMsg      map[int64]*PTuple
}
type VMessage map[int64]*ViewChange
type NewView struct {
	NewViewID int64
	VMsg      VMessage
	OMsg      OMessage
	NMsg      OMessage
}
type OMessage map[int64]*PrePrepare

type VCCache struct {
	vcMsg VMessage
	nvMsg map[int64]NewView
}

func NewCheckPoint(seq int64, vid int64) *CheckPoint {
	cp := &CheckPoint{
		Sequencenumber: seq,
		isStable:       false,
		Digest:         "",
		ViewID:         vid,
		CPMsg:          make(map[int64]*CheckPoint),
	}
	return cp
}

func NewVCCache() *VCCache {
	return &VCCache{
		vcMsg: make(VMessage),
		nvMsg: make(map[int64]NewView),
	}
}

func (vcc *VCCache) pushVC(vc ViewChange) {
	vcc.vcMsg[vc.NodeID] = &vc
}

func (vcc *VCCache) hasNewViewYet(vid int64) bool {
	if _, ok := vcc.nvMsg[vid]; ok {
		return true
	}
	return false
}

func (vcc *VCCache) addNewView(nv NewView) {
	vcc.nvMsg[nv.NewViewID] = nv
}

// getOrCreateLog retrieves the log entry for the given sequence number or creates a new one if it doesn't exist.
func (s *server) getOrCreateLog(seqNum int64) *NormalLog {
	log, ok := s.msgLogs[seqNum]
	if !ok {
		log = NewNormalLog()
		s.msgLogs[seqNum] = log
	}
	return log
}

func NewServer(id int, isPrimary bool, host string, port int) *server {
	byzantineServers := map[int]bool{
		1: false,
		2: false,
		3: false,
		4: false,
		5: false,
		6: false,
		7: false,
	}
	ActiveServers := map[int]bool{
		1: true,
		2: true,
		3: true,
		4: true,
		5: true,
		6: true,
		7: true,
	}
	privateKey, publicKey, err := utils.GenerateKeys()
	if err != nil {
		log.Fatalf("Failed to generate private key: %v", err)
	}

	s := &server{
		id:               id,
		Datastore:        make(map[int64]string),
		Log:              make([]pbft.TransactionRequest, 0),
		peers:            make([]*server, 0),
		address:          fmt.Sprintf("%s:%d", host, port),
		collector:        0,
		signal:           make(chan int),
		Timer:            time.NewTimer(time.Second * 10),
		currentviewid:    1,
		MiniSeq:          0,
		MaxSeq:           CheckPointK + 0,
		balance:          10,
		logs:             make(map[int64]*Request),
		msgLogs:          make(map[int64]*NormalLog),
		clients:          make(map[string]*Client),
		checks:           make(map[int64]*CheckPoint),
		sCache:           NewVCCache(),
		sequenceid:       1,
		sequnecenumber:   1,
		isByzantine:      byzantineServers[id],
		clientQueues:     make(map[string]chan *pbft.TransactionRequest),
		privateKey:       privateKey,
		publicKey:        publicKey,
		isActive:         ActiveServers[id],
		transactionQueue: make(chan *pbft.TransactionRequest, 100),
		requestTimeout:   20 * time.Second, // Adjust as needed
		timers:           make(map[int64]*time.Timer),
		requestCompleted: make(map[int64]bool),
		transqueue:       queue.NewQueue(),
		directReplyChan:  make(chan pbft.Reply),
		transactionMap:   make(map[int64]bool),
		status:           make(map[int64]string),
	}
	for i := 0; i < 10; i++ {
		clientid := string('A' + i)
		s.clients[clientid] = &Client{id: clientid, Balance: 10}
	}
	s.viewChangeCond = sync.NewCond(&s.mu)

	// privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	// if err != nil {
	// 	log.Fatalf("Failed to generate private key: %v", err)
	// }
	// s.privateKey = privateKey

	s.primaryid = s.currentviewid % 7
	s.isPrimary = (s.id == int(s.primaryid))
	return s
}
func InitializeServers(n int, leaderID int, basePort int) []*server {
	host := "localhost"
	servers := make([]*server, n)
	for i := 0; i < n; i++ {
		id := i + 1
		isPrimary := i+1 == leaderID
		port := basePort + i // Assign a unique port to each server
		servers[i] = NewServer(id, isPrimary, host, port)
	}
	for _, server := range servers {
		server.peers = append(server.peers, servers...)
	}
	return servers
}
func (s *server) GetBalanceRequest(ctx context.Context, req *pbft.BalanceRequest) (*pbft.BalanceResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Collect and sort client IDs from 'a' to 'j'
	clientIDs := make([]string, 0, len(s.clients))
	for id := range s.clients {
		clientIDs = append(clientIDs, id)
	}
	sort.Strings(clientIDs)

	balances64 := make(map[string]int64)
	for _, id := range clientIDs {
		balances64[id] = int64(s.clients[id].Balance)

	}
	return &pbft.BalanceResponse{Balances: balances64}, nil

}

func (s *server) GetStatus(ctx context.Context, req *pbft.Statusrequest) (*pbft.StatusReponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return &pbft.StatusReponse{
		Status:         s.status[req.Sequencenumber],
		Serverid:       int64(s.id),
		Sequencenumber: req.Sequencenumber,
	}, nil

}

func (s *server) ProcessTransaction(ctx context.Context, t *pbft.TransactionRequest) (*pbft.Reply, error) {
	if t == nil {
		return nil, fmt.Errorf("transaction request is nil")
	}
	if len(t.Transaction) == 0 {
		log.Printf("Transaction request is empty")
		return nil, fmt.Errorf("transaction request is empty")
	}
	start := time.Now()
	s.transqueue.Enqueue(t)
	go func() {
		for {
			tx := s.transqueue.Dequeue()
			if tx == nil {
				break
			}
			s.processTransactionFromQueue(ctx, tx)
		}
		stop := time.Now()
		taken := stop.Sub(start)
		fmt.Printf("Processing Time : %v\n", taken)
	}()
	s.status[t.Sequencenumber] = "NO Status"
	if s.transactionMap[t.Sequencenumber] {
		return &pbft.Reply{
			Sequencenumber: t.Sequencenumber,
			Clientid:       t.Transaction[0].Sender,
			Viewid:         s.currentviewid,
			Response:       "Success",
			Serverid:       int64(s.id),
			Transaction:    t.Transaction[0],
		}, nil
	} else {
		return &pbft.Reply{
			Sequencenumber: t.Sequencenumber,
			Clientid:       t.Transaction[0].Sender,
			Viewid:         s.currentviewid,
			Response:       "Failure",
			Serverid:       int64(s.id),
			Transaction:    t.Transaction[0],
		}, nil
	}

	// if transaction execution is successful, return a success response success else return a failure response

}

func (s *server) processTransactionFromQueue(ctx context.Context, t *pbft.TransactionRequest) {
	message, err := json.Marshal(t.Transaction[0])
	publicKey := &s.privateKey.PublicKey
	s.timers[t.Sequencenumber] = time.NewTimer(s.requestTimeout)
	// fmt.Printf("message: %s\n", message)
	// fmt.Printf("prive key: %v\n", s.privateKey)
	if err != nil {
		log.Fatalf("Failed to serialize received transaction: %v", err)
	}
	// publicKey := &s.privateKey.PublicKey
	// fmt.Printf("public key: %v\n", publicKey)
	isValid := utils.VerifySignature(publicKey, message, t.Sign)
	if isValid == nil {
		log.Printf("Invalid signature: verification failed")
	}

	if s.isPrimary {
		fmt.Printf("Leader Server %d: Broadcasting pre-prepare message.\n", s.id)
		fmt.Printf("Transaction: %v\n", t.Transaction)

		for _, peer := range s.peers {
			if peer.id != s.id {
				go peer.PrePrepare(context.Background(), &pbft.PrePrepareRequest{
					Viewid:         s.currentviewid,
					Sequencenumber: s.sequenceid,
					Digest:         t.Digest,
					Serverid:       int64(s.id),
					Serversign:     string(t.Sign),
					Transaction:    t.Transaction,
				})
			}

		}
		log := s.getOrCreateLog(s.sequenceid)
		log.PrePrepare = &pbft.PrePrepareRequest{
			Viewid:         s.currentviewid,
			Sequencenumber: t.Sequencenumber,
			Digest:         t.Digest,
			Serverid:       int64(s.id),
			Transaction:    t.Transaction,
		}
		s.status[t.Sequencenumber] = "PrePrepared"

		log.Stage = PrePrepared
		s.sequenceid++
		return

	}

}

// func (s *server) consensus(ctx context.Context, req *pbft.TransactionRequest, res *pbft.TransactionResponse) {
// 	s.mu.Lock()
// 	defer s.mu.Unlock()
// 	go s.PrePrepare(ctx, req)(res, err)
// 	if log.Stage = PrePrepared {

// }
func (s *server) PrePrepare(ctx context.Context, req *pbft.PrePrepareRequest) (*pbft.PrePrepareResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isPrimary {
		return nil, fmt.Errorf("server is primary, cannot process PrePrepare")
	}

	fmt.Printf("Server %d: Received PrePrepare for transaction %d.\n", s.id, req.Sequencenumber)

	response := &pbft.PrePrepareResponse{
		Sequencenumber: req.Sequencenumber,
		Serverid:       int64(s.id),
		Viewid:         req.Viewid,
	}

	if req.Viewid != s.currentviewid {
		response.Accepted = false
		response.Reason = fmt.Sprintf("Invalid view ID. Expected %d, got %d", s.currentviewid, req.Viewid)
		fmt.Printf("Server %d: %s\n", s.id, response.Reason)
		return response, nil
	}

	if req.Sequencenumber < s.MiniSeq || req.Sequencenumber > s.MaxSeq {
		response.Accepted = false
		response.Reason = fmt.Sprintf("Sequence number %d out of bounds (%d-%d)", req.Sequencenumber, s.MiniSeq, s.MaxSeq)
		fmt.Printf("Server %d: %s\n", s.id, response.Reason)
		return response, nil
	}

	log := s.getOrCreateLog(req.Sequencenumber)

	if log.Stage != Idle {
		fmt.Errorf("invalid stage[current %s] when to prePrepared", log.Stage)
		return response, nil
	}

	if log.PrePrepare != nil {
		if log.PrePrepare.Digest != req.Digest {
			fmt.Errorf("pre-Prepare message in same v-n but not same digest")
			return response, nil
		} else {
			fmt.Println("======>[idle2PrePrepare] duplicate pre-Prepare message")
			return response, nil
		}
	}

	log.PrePrepare = req

	if log.Prepare == nil {
		log.Prepare = make(PrepareMsg)
	}
	log.Prepare[log.PrePrepare.Serverid] = &Prepare{
		ViewID:         log.PrePrepare.Viewid,
		Sequencenumber: log.PrePrepare.Sequencenumber,
		Digest:         log.PrePrepare.Digest,
		serverid:       log.PrePrepare.Serverid,
	}

	log.Stage = PrePrepared
	s.status[req.Sequencenumber] = "PrePrepared"

	response.Accepted = true
	fmt.Printf("Server %d: PrePrepare for transaction %d accepted.\n", s.id, req.Sequencenumber)

	var primaryPeer *server
	for _, peer := range s.peers {
		if peer.isPrimary {
			primaryPeer = peer
			break
		}
	}
	go primaryPeer.collectPrePrepareResponse(ctx, *response)

	return response, nil
}

func (s *server) collectPrePrepareResponse(ctx context.Context, resp pbft.PrePrepareResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.acceptedCount = 1

	if s.isPrimary {
		if s.preprepareResponses == nil {
			s.preprepareResponses = make(map[int64][]*pbft.PrePrepareResponse)
		}
		if resp.Accepted {
			s.preprepareResponses[resp.Serverid] = append(s.preprepareResponses[resp.Serverid], &resp)
		}
		uniqueResponders := make(map[int64]bool)
		for serverID, responses := range s.preprepareResponses {
			for _, r := range responses {
				if r.Sequencenumber == resp.Sequencenumber && r.Accepted {
					uniqueResponders[serverID] = true
					break
				}
			}
		}

		s.acceptedCount = +len(uniqueResponders)
		f := (len(s.peers) - 1) / 3
		threshold := 2*f + 1
		if s.acceptedCount == threshold {
			fmt.Printf("Acceted preprepare values are %d\n", s.acceptedCount)
			fmt.Printf("Leader Server %d: Collected 2f+1 PrePrepare responses for transaction %d.\n", s.id, resp.Sequencenumber)
			prepareRequest := pbft.PrepareRequest{
				Viewid:         s.currentviewid,
				Sequencenumber: resp.Sequencenumber,
				Digest:         "",
				Serverid:       int64(s.id),
			}

			go s.InitiatePrepare(ctx, prepareRequest)

		} else {
			fmt.Printf("Transaction does not have enough PrePrepare responses yet.\n")
		}
		s.acceptedCount = 1

	}
}
func (s *server) InitiatePrepare(ctx context.Context, t pbft.PrepareRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.isPrimary {

		fmt.Printf("Leader Server %d: Broadcasting Prepare for transaction %d.\n", s.id, t.Sequencenumber)
		pr := &pbft.PrepareRequest{
			Viewid:         t.Viewid,
			Sequencenumber: t.Sequencenumber,
			Digest:         t.Digest,
			Serverid:       t.Serverid,
		}

		for _, peer := range s.peers {
			go peer.Prepare(ctx, pr)
		}
		prepareresponse := pbft.Prepared{
			Sequencenumber: pr.Sequencenumber,
			Serverid:       int64(s.id),
			Viewid:         pr.Viewid,
		}

		prepareresponse.Accepted = true
		log := s.getOrCreateLog(pr.Sequencenumber)
		log.Prepare[pr.Serverid] = &Prepare{
			ViewID:         pr.Viewid,
			Sequencenumber: pr.Sequencenumber,
			Digest:         pr.Digest,
			serverid:       pr.Serverid,
		}
		log.Stage = Prepared
		s.status[t.Sequencenumber] = "Prepared"
		if s.isByzantine {
			for _, peer := range s.peers {
				if peer.id != s.id {
					go peer.ViewChange()

				}
			}
			return
		}
	}
}
func (s *server) Prepare(ctx context.Context, req *pbft.PrepareRequest) (*pbft.Prepared, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.isPrimary {
		return nil, fmt.Errorf("server is primary, cannot process Prepare")
	}
	if s.isByzantine {
		return nil, nil
	}

	if req.Sequencenumber%CheckPointInterval == 0 {
		go s.CheckingPoint(&CheckPoint{
			Sequencenumber: req.Sequencenumber,
			Digest:         req.Digest,
			ViewID:         req.Viewid,
			NodeID:         int64(s.id),
			isStable:       false,
		})

	}
	if !s.isActive {
		return nil, nil
	}

	fmt.Printf("Server %d: Received Prepare for transaction %d.\n", s.id, req.Sequencenumber)
	prepareresponse := pbft.Prepared{
		Sequencenumber: req.Sequencenumber,
		Serverid:       int64(s.id),
		Viewid:         req.Viewid,
	}

	// Validate View ID and Sequence Number
	if req.Viewid != s.currentviewid {
		prepareresponse.Accepted = false
		prepareresponse.Reason = fmt.Sprintf("Invalid view ID. Expected %d, got %d", s.currentviewid, req.Viewid)
		fmt.Printf("Server %d: %s\n", s.id, prepareresponse.Reason)
		return &prepareresponse, nil
	}

	if req.Sequencenumber < s.MiniSeq || req.Sequencenumber > s.MaxSeq {
		prepareresponse.Accepted = false
		prepareresponse.Reason = fmt.Sprintf("Sequence number %d out of bounds (%d-%d)", req.Sequencenumber, s.MiniSeq, s.MaxSeq)
		fmt.Printf("Server %d: %s\n", s.id, prepareresponse.Reason)
		return &prepareresponse, nil
	}

	prepareresponse.Accepted = true

	log := s.getOrCreateLog(req.Sequencenumber)
	log.Prepare[req.Serverid] = &Prepare{
		ViewID:         req.Viewid,
		Sequencenumber: req.Sequencenumber,
		Digest:         req.Digest,
		serverid:       req.Serverid,
	}
	log.Stage = Prepared
	s.status[req.Sequencenumber] = "Prepared"
	fmt.Printf("Server %d: Prepare log %v\n", s.id, log.Stage)
	if s.isByzantine {
		return nil, nil
	}
	var primaryPeer *server
	for _, peer := range s.peers {
		if peer.isPrimary {
			primaryPeer = peer
			break
		}
	}
	go primaryPeer.CollectPrepareResponse(ctx, &prepareresponse)

	return &prepareresponse, nil
}
func (s *server) CollectPrepareResponse(ctx context.Context, resp *pbft.Prepared) {

	s.acceptedCount = 1
	if s.isPrimary {
		if s.isByzantine {
			return
		}
		if s.prepareResponses == nil {
			s.prepareResponses = make(map[int64][]*pbft.Prepared)
		}

		if resp.Accepted {
			s.prepareResponses[resp.Serverid] = append(s.prepareResponses[resp.Serverid], resp)
		}
		uniqueResponders := make(map[int64]bool)
		for serverID, responses := range s.prepareResponses {
			for _, r := range responses {
				if r.Sequencenumber == resp.Sequencenumber && r.Accepted {
					uniqueResponders[serverID] = true
					break
				}
			}
		}
		s.acceptedCount += len(uniqueResponders)
		f := (len(s.peers) - 1) / 3
		threshold := 2*f + 1
		fmt.Printf("Accepted values are %d\n", s.acceptedCount)
		if s.acceptedCount == threshold {
			fmt.Printf("Leader Server %d: Collected 2f+1 Prepare responses for transaction %d.\n", s.id, resp.Sequencenumber)

			go s.Commit(ctx, pbft.CommitRequest{
				Viewid:         resp.Viewid,
				Sequencenumber: resp.Sequencenumber,
				Digest:         resp.Digest,
				Serverid:       resp.Serverid,
			})
		}
		s.acceptedCount = 1
	}
}

func (s *server) Commit(ctx context.Context, c pbft.CommitRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.isActive {
		return
	}

	if s.isPrimary {
		fmt.Printf("Leader Server %d: Broadcasting Commit for transaction %d.\n", s.id, c.Sequencenumber)
		for _, peer := range s.peers {
			if peer.id != s.id {
				go peer.Commit(ctx, c)
			}
		}
		commit := pbft.CommitRequest{
			Viewid:         c.Viewid,
			Sequencenumber: c.Sequencenumber,
			Digest:         c.Digest,
			Serverid:       int64(s.id),
			Transaction:    c.Transaction,
		}

		go s.processcommit(ctx, commit)
	} else {
		fmt.Printf("Server %d: Received Commit for transaction %d.\n", s.id, c.Sequencenumber)

		commit := pbft.CommitRequest{
			Viewid:         c.Viewid,
			Sequencenumber: c.Sequencenumber,
			Digest:         c.Digest,
			Serverid:       int64(s.id),
			Transaction:    c.Transaction,
		}
		go s.processcommit(ctx, commit)
	}
}

func (s *server) processcommit(ctx context.Context, c pbft.CommitRequest) {
	if !s.isActive {
		return
	}
	if s.isByzantine {
		return
	}

	if s.currentviewid != c.Viewid {
		fmt.Printf("Server %d: Invalid view ID. Expected %d, got %d\n", s.id, s.currentviewid, c.Viewid)
		return
	}

	if c.Sequencenumber < s.MiniSeq || c.Sequencenumber > s.MaxSeq {
		fmt.Printf("Server %d: Sequence number %d out of bounds (%d-%d)\n", s.id, c.Sequencenumber, s.MiniSeq, s.MaxSeq)
		return
	}

	log, ok := s.msgLogs[c.Sequencenumber]

	if !ok {
		fmt.Printf("======>[prePrepare2Prepare]:=>havn't got log for message(%d) yet", c.Sequencenumber)
		return
	}

	if log.Stage != Prepared {
		fmt.Printf("======>[prePrepare2Prepare] current[seq=%d] state isn't PrePrepared:[%s]\n", c.Sequencenumber, log.Stage)
		return
	}

	ppMsg := log.PrePrepare
	// fmt.Println(ppMsg)

	//fmt.Printf("print this message %v\n", ppMsg)
	if ppMsg == nil {
		fmt.Println("======>[prePrepare stage havn't got pre-Prepare message(%d) yet", c.Sequencenumber)
		return
	}

	if ppMsg.Viewid != c.Viewid ||
		ppMsg.Sequencenumber != c.Sequencenumber ||
		ppMsg.Digest != c.Digest {
		fmt.Errorf("[Prepare]:=>not same with pre-Prepare message")
		return
	}

	commit := Commit{
		viewid:         c.Viewid,
		Sequencenumber: c.Sequencenumber,
		Digest:         c.Digest,
		serverid:       int64(s.id),
	}
	log.Commit[commit.serverid] = commit
	log.Stage = Committed
	s.status[c.Sequencenumber] = "Committed"
	s.executeTransaction(ctx, c)

}

func (s *server) executeTransaction(ctx context.Context, c pbft.CommitRequest) {
	if !s.isActive {
		return
	}
	if s.isByzantine {
		return
	}

	tx := s.msgLogs[c.Sequencenumber].PrePrepare.Transaction

	fmt.Printf("Server %d: Executing transaction %v.\n", s.id, tx)
	sequenceNumber := c.Sequencenumber
	sender := tx[0].Sender
	receiver := tx[0].Receiver
	amount := tx[0].Amount
	if senderClient, ok := s.clients[sender]; ok {
		if senderClient.Balance < int(amount) {
			fmt.Printf("Server %d: Insufficient balance for transaction %d. Skipping execution.\n", s.id, sequenceNumber)
			s.LasExeSeq = sequenceNumber
			s.transactionMap[sequenceNumber] = false
			return
		}
		senderClient.Balance -= int(amount)
		s.transactionMap[sequenceNumber] = true
	} else {
		fmt.Printf("Server %d: Sender %s does not exist. Skipping execution.\n", s.id, sender)
		s.LasExeSeq = sequenceNumber
		return
	}
	if receiverClient, ok := s.clients[receiver]; ok {

		receiverClient.Balance += int(amount)
	} else {
		s.LasExeSeq = sequenceNumber
		fmt.Printf("Server %d: Receiver %s does not exist. Skipping execution.\n", s.id, receiver)
		return

	}
	// Update the datastore with the transaction details.
	s.Datastore[sequenceNumber] = fmt.Sprintf("Transaction %d: %s -> %s, Amount: %d", sequenceNumber, sender, receiver, amount)

	fmt.Printf("Server %d: Transaction %d executed successfully.\n", s.id, sequenceNumber)
	s.status[sequenceNumber] = "Executed"

	fmt.Printf("Server %d: status of the transactions is with sequence number %d is  %s\n", s.id, sequenceNumber, s.status[sequenceNumber])

	s.LasExeSeq = sequenceNumber
	return
}
func (s *server) aggregateViewChanges() *ViewChange {
	var maxNewViewID int64 = 0
	var selectedVC *ViewChange

	for _, vc := range s.sCache.vcMsg {
		if vc.NewViewID > maxNewViewID {
			maxNewViewID = vc.NewViewID
			selectedVC = vc
		}
	}

	if selectedVC == nil {
		return nil
	}

	// Optionally, merge other fields if necessary
	return selectedVC

}

func (s *server) TransactionStream(stream pbft.Pbft_TransactionStreamServer) error {
	for {
		reply := &pbft.Reply{
			Sequencenumber: 1,
		}
		if err := stream.Send(reply); err != nil {
			return err
		}
	}
}

// server.go

func (s *server) sendReplys(ctx context.Context, reply *pbft.Reply) error {
	grpcConn, err := grpc.Dial("client_address:50051", grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer grpcConn.Close()
	conn := pbft.NewPbftClient(grpcConn)

	_, err = conn.SendReply(context.Background(), reply)
	if err != nil {
		return err
	}
	return nil
}

func (s *server) printDB(ctx context.Context, response *pbft.Balresponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.printDBs()
	return nil
}
func (s *server) printDBs() {
	clientIDs := []string{}
	for cientID := range s.clients {
		clientIDs = append(clientIDs, cientID)
	}
	sort.Strings(clientIDs)
	if s.id == 1 {
		fmt.Println("Server")
		for _, clientID := range clientIDs {
			fmt.Printf(" %s", clientID)
		}
		fmt.Println()
	}
	fmt.Printf("S%d", s.id)
	for _, clientID := range clientIDs {
		balance := s.clients[clientID].Balance
		fmt.Printf(" %d", balance)

	}

}

// view change
func (s *server) ViewChange() {
	fmt.Printf("view change has started\n")
	if s.viewChangeInProgress {
		fmt.Println("View change already in progress. Skipping.")
		return
	}
	s.viewChangeInProgress = true

	//s.Timer.Reset(time.Second * 10)
	if s.lastCP == nil {
		s.lastCP = &CheckPoint{
			Sequencenumber: 1,
			CPMsg:          nil,
		}
	}
	pMsg := s.computemsg()

	fmt.Printf("currentviewid: %d and checkpoint sequence number: %d\n", s.currentviewid, s.lastCP.Sequencenumber)
	vc := ViewChange{
		NewViewID: s.currentviewid + 1,
		LastCPSeq: s.lastCP.Sequencenumber + 1,
		NodeID:    int64(s.id),
		CMsg:      s.lastCP.CPMsg,
		PMsg:      pMsg,
	}
	nextPrimaryID := vc.NewViewID % int64(len(s.peers))
	if int64(s.id) == nextPrimaryID {
		s.sCache.pushVC(vc)
	}
	go s.createCheckPoint(s.currentSequnece)

	s.broadcastViewChange(&vc)
	s.currentviewid = vc.NewViewID
	s.msgLogs = make(map[int64]*NormalLog)
	s.viewChangeInProgress = false

}

func (s *server) broadcastViewChange(vc *ViewChange) {
	for _, peer := range s.peers {
		if peer.id != s.id { // Avoid sending to self
			fmt.Printf("Server %d: Broadcasting ViewChange to Server %d with view change  %+v.\n", s.id, peer.id, vc)
			go peer.ReceiveViewChange(context.Background(), vc)
		}
	}
}

type Set map[interface{}]bool

func (s Set) put(key interface{}) {
	if s == nil {
		s = make(map[interface{}]bool)
	}
	s[key] = true
}
func (s *server) ReceiveViewChange(ctx context.Context, vc *ViewChange) (v *ViewChange) {

	s.mu.Lock()
	defer s.mu.Unlock()
	fmt.Printf("Server %d: Received ViewChange from Server %d.\n", s.id, vc.NodeID)
	if s.currentviewid > vc.NewViewID {
		fmt.Errorf("it's[%d] not for me[%d] view change", vc.NewViewID, s.currentviewid)
		return
	}

	MaxFaultyNode := 2
	fmt.Printf("vc.CMsg: %v\n", vc.CMsg)
	// if len(vc.CMsg) <= MaxFaultyNode && len(vc.CMsg) != 0 {
	// 	fmt.Errorf("view message checking C message failed")
	// 	return
	// }
	go s.procViewChange(*vc)

	var counter = make(map[int64]Set)
	for id, cp := range vc.CMsg {
		fmt.Print("id and cp", id, cp)
		if cp.ViewID >= vc.NewViewID {
			continue
		}

		if cp.Sequencenumber != vc.LastCPSeq {
			fmt.Errorf("view change message C msg's n[]%d is different from vc's"+
				" h[%d]", cp.Sequencenumber, vc.LastCPSeq)
			return
		}

		if counter[cp.ViewID] == nil {
			counter[cp.ViewID] = make(Set)
		}

		counter[cp.ViewID].put(id)
	}

	CMsgIsOK := false
	for vid, set := range counter {
		if len(set) > MaxFaultyNode {
			fmt.Printf("view change check C message success[%d]:\n", vid)
			CMsgIsOK = true
			break
		}
	}
	fmt.Printf("CMsgIsOK: %v\n", CMsgIsOK)
	// if !CMsgIsOK {
	// 	fmt.Println("no valid C message in view change msg")
	// }

	// counter = make(map[int64]Set)
	// for seq, pt := range vc.PMsg {

	// 	ppView := pt.PPMsg.ViewID

	// 	if seq <= vc.LastCPSeq || seq > vc.LastCPSeq+CheckPointK {
	// 		fmt.Errorf("view change message checking P message faild pre-prepare n=%d,"+
	// 			" checkpoint h=%d", seq, vc.LastCPSeq)
	// 		return
	// 	}
	// 	if ppView >= vc.NewViewID {
	// 		fmt.Errorf("view change message checking P message faild pre-prepare view=%d,"+
	// 			" new view id=%d", pt.PPMsg.ViewID, vc.NewViewID)
	// 		return
	// 	}

	// 	for nid, prepare := range pt.PMsg {
	// 		if ppView != prepare.ViewID {
	// 			fmt.Errorf("view change message checking view id[%d] in pre-prepare is not "+
	// 				"same as prepare's[%d]", ppView, prepare.ViewID)
	// 			return
	// 		}
	// 		if seq != prepare.Sequencenumber {
	// 			fmt.Errorf("view change message checking seq id[%d] in pre-prepare"+
	// 				"is different from prepare's[%d]", seq, prepare.Sequencenumber)
	// 			return
	// 		}
	// 		counter[ppView].put(nid)
	// 	}

	// PMsgIsOk := false
	// for vid, set := range counter {
	// 	if len(set) >= 2*MaxFaultyNode {
	// 		fmt.Printf("view change check P message success[%d]:\n", vid)
	// 		PMsgIsOk = true
	// 		break
	// 	}
	// }

	// if !PMsgIsOk {
	// 	fmt.Println("view change check p message failed")
	// }

	// if !CMsgIsOK || !PMsgIsOk {
	// 	fmt.Println("view change check failed")
	s.sCache.pushVC(*vc)
	return vc

}

func (s *server) computemsg() map[int64]*PTuple {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := make(map[int64]*PTuple)
	for seq := s.MiniSeq; seq <= s.MaxSeq; seq++ {
		log, ok := s.msgLogs[seq]
		if !ok {

			continue
		}
		fmt.Printf("log is %v\n", log.PrePrepare)
		tupel := PTuple{
			PPMsg: PrePrepare{
				ViewID:         log.PrePrepare.Viewid,
				Sequencenumber: log.PrePrepare.Sequencenumber,
				Digest:         log.PrePrepare.Digest,
				Serverid:       log.PrePrepare.Serverid,
				Transaction:    log.PrePrepare.Transaction,
			},
			PMsg: log.Prepare,
		}
		p[seq] = &tupel
	}
	fmt.Printf("p is %v\n", p)
	return p

}

func (s *server) changeleader() {
	// reomve last primary
	fmt.Print("change leader ")
	for _, peer := range s.peers {
		if peer.isPrimary {
			peer.isPrimary = false
		}
	}
}

func (s *server) procViewChange(vc ViewChange) error {
	nextPrimaryID := vc.NewViewID % int64(len(s.peers))
	if int64(s.id) != nextPrimaryID {
		fmt.Printf("im[ %d ] not the new[ %d ] primary node\n", s.id, nextPrimaryID)
		return nil
	}
	s.changeleader()
	if int64(s.id) == nextPrimaryID {
		fmt.Printf("im[ %d] the new[ %d ] primary node\n", s.id, nextPrimaryID)
		s.isPrimary = true
		go s.createNewViewMsg(vc.NewViewID)

	}

	// if err := s.Checkviewchange(vc); err != nil {
	// 	fmt.Errorf("view change failed: %v", err)
	// 	return err
	// }

	s.sCache.pushVC(vc)
	// if len(s.sCache.vcMsg) < MaxFaultyNode*2 {
	// 	return nil
	// }
	// if s.sCache.hasNewViewYet(vc.NewViewID) {
	// 	fmt.Printf("view change[ %d] is in processing......\n", vc.NewViewID)
	// 	return nil
	// }

	return nil

}

func (s *server) GetON(newVID int64) (int64, int64, OMessage, OMessage, ViewChange) {
	mergeP := make(map[int64]PTuple)
	var maxNinV int64 = 0
	var maxNinO int64 = 0

	var cpVC ViewChange
	for _, vc := range s.sCache.vcMsg {
		if vc.LastCPSeq > maxNinV {
			maxNinV = vc.LastCPSeq
			cpVC = *vc
		}
		for seq, pMsg := range vc.PMsg {
			if _, ok := mergeP[seq]; ok {
				continue
			}
			mergeP[seq] = *pMsg
			if seq > maxNinO {
				maxNinO = seq
			}
		}
	}

	O := make(OMessage)
	N := make(OMessage)
	for i := maxNinV + 1; i <= maxNinO; i++ {
		pt, ok := mergeP[i]
		if ok {
			O[i] = &pt.PPMsg
			O[i].ViewID = newVID
		} else {
			N[i] = &PrePrepare{
				ViewID:         newVID,
				Sequencenumber: i,
				Digest:         "",
			}
		}
	}

	return maxNinV, maxNinO, O, N, cpVC
}

func (s *server) createNewViewMsg(vid int64) error {
	s.currentviewid = vid
	newcp, newseq, o, n, cpvc := s.GetON(vid)

	nv := NewView{
		NewViewID: vid,
		VMsg:      s.sCache.vcMsg,
		OMsg:      o,
		NMsg:      n,
	}
	fmt.Printf("Server %d: Creating NewView message for view %d.\n", s.id, vid)
	s.sCache.addNewView(nv)
	s.currentSequnece = newseq
	s.broadcastViewChange(&cpvc)
	s.updateStateNV(newcp, cpvc)
	s.cleanRequest()

	return nil
}

func (s *server) cleanRequest() {
	for cid, client := range s.cliRecord {
		for seq, req := range client.Request {
			if req.Timestamp < client.LastReplyTime {
				delete(client.Request, seq)
				fmt.Printf("cleaning request[%d] when view changed for client[%s]\n", seq, cid)
			}
		}
	}
	return
}

func (s *server) updateStateNV(maxNV int64, vc ViewChange) {

	if maxNV > s.lastCP.Sequencenumber {
		cp := NewCheckPoint(maxNV, s.currentviewid)
		cp.CPMsg = vc.CMsg
		s.checks[maxNV] = cp
		s.SendCheckPoint(context.Background(), cp.Sequencenumber)

		s.createCheckPoint(maxNV)
	}

	if maxNV > s.LasExeSeq {

		s.LasExeSeq = maxNV
	}

	return
}

func (m OMessage) EQ(msg OMessage) bool {
	return true
}

func (s *server) didChangeView(nv NewView) error {

	newVID := nv.NewViewID
	s.sCache.vcMsg = nv.VMsg
	newCP, newSeq, O, N, cpVC := s.GetON(newVID)
	if !O.EQ(nv.OMsg) {
		return fmt.Errorf("new view checking O message faliled")
	}
	if !N.EQ(nv.NMsg) {
		return fmt.Errorf("new view checking N message faliled")
	}
	O = s.sortOMessage(O)

	for _, ppMsg := range O {
		fmt.Printf("printing the failed prepare message %v\n", ppMsg)

		s.PrePrepare(context.Background(), &pbft.PrePrepareRequest{
			Viewid:         ppMsg.ViewID,
			Sequencenumber: ppMsg.Sequencenumber,
			Digest:         ppMsg.Digest,
		})
		return nil
	}

	N = s.sortN(N)
	for _, ppMsg := range N {
		s.PrePrepare(context.Background(), &pbft.PrePrepareRequest{
			Viewid:         ppMsg.ViewID,
			Sequencenumber: ppMsg.Sequencenumber,
			Digest:         ppMsg.Digest,
		})
		return nil
	}

	s.sCache.addNewView(nv)
	s.currentSequnece = newSeq
	s.updateStateNV(newCP, cpVC)
	s.cleanRequest()
	return nil
}
func (s *server) sortOMessage(O OMessage) OMessage {
	var keys []int
	for k := range O {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)

	newO := make(OMessage)
	for _, k := range keys {
		newO[int64(k)] = O[int64(k)]
	}
	return newO
}
func (s *server) sortN(N OMessage) OMessage {
	var keys []int
	for k := range N {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)

	newN := make(OMessage)
	for _, k := range keys {
		newN[int64(k)] = N[int64(k)]
	}
	return newN
}
func (s *server) createCheckPoint(seq int64) {
	// Ensure the checkpoint for the sequence exists
	if _, exists := s.checks[seq]; !exists {
		cp := NewCheckPoint(seq, s.currentviewid)
		s.checks[seq] = cp
	}

	msg := &CheckPoint{
		Sequencenumber: seq,
		Digest:         "", // Define or calculate a digest for this checkpoint as needed
		ViewID:         s.currentviewid,
		NodeID:         int64(s.id),
		isStable:       false,
	}

	// Add the checkpoint message to CPMsg
	cp := s.checks[seq]
	if cp.CPMsg == nil {
		cp.CPMsg = make(map[int64]*CheckPoint)
	}
	cp.CPMsg[int64(s.id)] = msg

	// Print debug information
	fmt.Printf("Printing the checks: %v\n", s.checks)
	fmt.Printf("Printing the CP message: %+v\n", cp)
	fmt.Printf("Printing the msg: %+v\n", msg)

	// Broadcast the checkpoint message to other peers
	for _, peer := range s.peers {
		if peer.id != s.id {
			go peer.CheckingPoint(msg)
		}
	}
}

func (s *server) CheckingPoint(msg *CheckPoint) {
	cp, exists := s.checks[msg.Sequencenumber]
	if !exists {
		cp = NewCheckPoint(msg.Sequencenumber, s.currentviewid)
		s.checks[msg.Sequencenumber] = cp
	}
	cp.CPMsg[msg.NodeID] = msg
	s.SendCheckPoint(context.Background(), msg.Sequencenumber)
}

func (s *server) SendCheckPoint(ctx context.Context, seq int64) {
	cp, ok := s.checks[seq]

	if !ok {
		fmt.Printf("Server %d: Checkpoint %d not found\n", s.id, seq)
		return
	}

	// Check if we have enough checkpoint messages for stability
	if len(cp.CPMsg) <= MaxFaultyNode && cp.CPMsg == nil {
		fmt.Printf("Server %d: Checkpoint %d has insufficient messages\n", s.id, seq)
		return
	}

	// If the checkpoint is already stable, we don’t need to process further
	if cp.isStable {
		fmt.Printf("Server %d: Checkpoint %d is already stable\n", s.id, seq)
		return
	}

	// Mark the checkpoint as stable
	cp.isStable = true

	// Clean up older logs and checkpoints
	for id, log := range s.msgLogs {
		if id > cp.Sequencenumber {
			continue
		}
		log.PrePrepare = nil
		log.Commit = nil
		delete(s.msgLogs, id)
	}

	for id, cps := range s.checks {
		if id >= cp.Sequencenumber {
			continue
		}
		cps.CPMsg = nil
		delete(s.checks, id)
	}

	// Update sequence limits and last checkpoint
	s.MiniSeq = cp.Sequencenumber
	s.MaxSeq = s.MiniSeq + CheckPointK
	s.lastCP = cp
}

func main() {
	// Define server details
	basePort := 5000
	numServers := 7

	servers := InitializeServers(numServers, 1, basePort)
	var wg sync.WaitGroup
	for _, srv := range servers {
		wg.Add(1)
		go func(s *server, port int) {
			defer wg.Done()
			startServer(s, port)

		}(srv, basePort+srv.id)

	}
	wg.Wait()
}

type Peer struct {
	id      int
	address string
	client  pbft.PbftClient
}

// startServer initializes and starts the gRPC server on a specific port
func startServer(s *server, port int) {
	grpcServer := grpc.NewServer()
	pbft.RegisterPbftServer(grpcServer, s)

	// Listen on the specified port
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("Server %d failed to listen on port %d: %v", s.id, port, err)
	}

	log.Printf("Server %d started and listening on port %d (Primary: %v)", s.id, port, s.isPrimary)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Server %d failed to serve: %v", s.id, err)
	}

}
