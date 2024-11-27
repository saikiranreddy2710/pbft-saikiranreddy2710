package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"pbft/utils"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	pbft "pbft/pbft" // Update with the actual path to your pbft proto package

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ServerClient struct {
	pbft.UnimplementedPbftServer
	client pbft.PbftClient
	conn   *grpc.ClientConn
	reply  *pbft.Reply
}

type accounts struct {
	id      string
	balance int
}
type Reply struct {
	Response    string
	Transaction pbft.Transaction
	SequenceNum int64
	viewid      int64
}

type Sets struct {
	SetNumber        int
	SetList          []string
	Transactions     []pbft.Transaction
	ByzantineServers []string
}

// create 10 accounts  now
var account = []accounts{
	{id: "A", balance: 10},
	{id: "B", balance: 10},
	{id: "C", balance: 10},
	{id: "D", balance: 10},
	{id: "E", balance: 10},
	{id: "F", balance: 10},
	{id: "G", balance: 10},
	{id: "H", balance: 10},
	{id: "I", balance: 10},
	{id: "J", balance: 10},
}

func printBalances(server *ServerClient, ctx context.Context) error {
	response, err := server.client.GetBalanceRequest(ctx, &pbft.BalanceRequest{})
	if err != nil {
		return err
	}

	// Print balances in sorted order by client ID for readability
	clientIDs := make([]string, 0, len(response.Balances))
	for id := range response.Balances {
		clientIDs = append(clientIDs, id)
	}
	sort.Strings(clientIDs)

	for _, id := range clientIDs {
		fmt.Printf("%s: %d ", id, response.Balances[id])

	}
	fmt.Println()
	return nil
}

func printstatus(server *ServerClient, ctx context.Context, seq int64) error {
	status, err := server.client.GetStatus(ctx, &pbft.Statusrequest{Sequencenumber: seq})
	if err != nil {
		return err
	}
	fmt.Println(status)
	return nil
}
func receivereply(ctx context.Context, reply *pbft.Reply) {
	fmt.Println("Reply received from server: ", reply)
	threshold := 5
	count := 0

	if reply.Response == "Success" {
		count++

	}
	if count >= threshold {
		for i := 0; i < len(account); i++ {
			if account[i].id == reply.Transaction.Sender {
				account[i].balance = account[i].balance - int(reply.Transaction.Amount)
			}
			if account[i].id == reply.Transaction.Receiver {
				account[i].balance = account[i].balance + int(reply.Transaction.Amount)
			}
		}
		fmt.Println("Transaction committed successfully")
	} else {
		fmt.Println("Transaction not received reply by 2f+1 servers")

	}
}

// connectToServer connects to a PBFT server at a given address.
func connectToServer(address string) (*ServerClient, error) {
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	client := pbft.NewPbftClient(conn)
	return &ServerClient{client: client, conn: conn}, nil
}

// func generateKeys() (*ecdsa.PrivateKey, *ecdsa.PublicKey, error) {
// 	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
// 	if err != nil {
// 		return nil, nil, err
// 	}
// 	return privKey, &privKey.PublicKey, nil
// }
// func signMessage(privKey *ecdsa.PrivateKey, message []byte) ([]byte, []byte, error) {
// 	// Hash the message with SHA-256
// 	hash := sha256.Sum256(message)

// 	// Sign the hash with the private key
// 	r, s, err := ecdsa.Sign(rand.Reader, privKey, hash[:])
// 	if err != nil {
// 		return nil, nil, err
// 	}

// 	return r.Bytes(), s.Bytes(), nil
// }

// func verifySignature(pubKey *ecdsa.PublicKey, message []byte, r, s []byte) bool {
// 	// Hash the message with SHA-256
// 	hash := sha256.Sum256(message)

//		// Verify the signature
//		valid := ecdsa.Verify(pubKey, hash[:], r, s)
//		return valid
//	}
var privateKey, publicKey, _ = utils.GenerateKeys()

// asyncSendTransaction sends a transaction to a specific server asynchronously.
func asyncSendTransaction(wg *sync.WaitGroup, client pbft.PbftClient, transaction *pbft.Transaction, sequenceNumber int64, allClients []pbft.PbftClient) {
	defer wg.Done()

	// Define a retry flag
	retry := false

	// Function to send transaction to a specific server
	sendTransaction := func(client pbft.PbftClient) error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		message, err := json.Marshal(transaction)
		if err != nil {
			return fmt.Errorf("failed to marshal transaction: %v", err)
		}

		signature, err := utils.SignMessage(privateKey, message)
		if err != nil {
			return fmt.Errorf("failed to sign message: %v", err)
		}

		// Create a TransactionRequest
		sendingTransaction := &pbft.TransactionRequest{
			Type:           "",
			Viewid:         1,
			Sequencenumber: sequenceNumber,
			Digest:         "", // Set Digest if needed
			Sign:           signature,
			Transaction:    []*pbft.Transaction{transaction},
		}
		res, err := client.ProcessTransaction(ctx, sendingTransaction)
		if err != nil {
			return fmt.Errorf("failed to send transaction: %v", err)
		}
		log.Printf("Reply message Received: %v", res)

		return nil
	}

	// Initial attempt to send transaction to primary server
	if err := sendTransaction(client); err != nil {
		retry = true
	}

	// If primary server fails, send to all other servers
	if retry {
		for _, secondaryClient := range allClients {
			// Skip sending to the primary again
			if secondaryClient == client {
				continue
			}
			wg.Add(1)
			go func(sc pbft.PbftClient) {
				defer wg.Done()
				if err := sendTransaction(sc); err != nil {

				} else {
					log.Printf("Transaction %d successfully sent\n", sequenceNumber)
				}
			}(secondaryClient)
		}

	}
}
func digest(sender, receiver, amount string) string {
	return fmt.Sprintf("%s-%s-%s", sender, receiver, amount)
}

func ParseCSV(filename string) ([]Sets, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("error reading csv file: %v", err)
	}

	var sets []Sets
	var currentSet *Sets
	numPattern := regexp.MustCompile(`^-?\d+$`)

	for _, row := range records {
		if len(row) < 3 {
			// Ensure the row has at least 3 columns
			fmt.Println("Skipping row with insufficient columns:", row)
			continue
		}

		line := row[0]
		transactionStr := row[1]
		transactionStr = strings.Trim(transactionStr, "()")
		transactionParts := strings.Split(transactionStr, ",")
		if len(transactionParts) != 3 {
			// Ensure transaction has exactly 3 parts
			fmt.Println("Invalid transaction format:", row)
			continue
		}

		source := strings.TrimSpace(transactionParts[0])
		destination := strings.TrimSpace(transactionParts[1])
		amount, err := strconv.Atoi(strings.TrimSpace(transactionParts[2]))
		if err != nil {
			return nil, fmt.Errorf("error parsing amount: %v", err)
		}

		transaction := pbft.Transaction{
			Sender:   source,
			Receiver: destination,
			Amount:   int64(amount),
			Digest:   digest(transactionParts[0], transactionParts[1], transactionParts[2]),
		}

		if numPattern.MatchString(line) {
			// If the line contains a set number
			if currentSet != nil {
				sets = append(sets, *currentSet)
			}

			setNumber, err := strconv.Atoi(line)
			if err != nil {
				return nil, fmt.Errorf("error parsing set number: %v", err)
			}

			setListStr := strings.Trim(row[2], "[]")
			setList := strings.Split(setListStr, ",")
			for i := range setList {
				setList[i] = strings.TrimSpace(setList[i])
			}

			currentSet = &Sets{
				SetNumber:    setNumber,
				SetList:      setList,
				Transactions: []pbft.Transaction{transaction}, // Start with the first transaction
			}
		} else {
			// If it's not a new set, append the transaction to the current set
			if currentSet != nil {
				currentSet.Transactions = append(currentSet.Transactions, transaction)
			} else {
				fmt.Println("Skipping transaction as no set is defined yet:", transaction)
			}
		}
	}

	// Append the last set if it's non-nil
	if currentSet != nil {
		sets = append(sets, *currentSet)
	}

	return sets, nil

}

// func ParseCSV(filename string) ([]Sets, error) {
// 	file, err := os.Open(filename)
// 	if err != nil {
// 		return nil, fmt.Errorf("error opening file: %v", err)
// 	}
// 	defer file.Close()

// 	reader := csv.NewReader(file)
// 	reader.FieldsPerRecord = -1 // Allows variable fields
// 	reader.LazyQuotes = true

// 	records, err := reader.ReadAll()
// 	if err != nil {
// 		return nil, fmt.Errorf("error reading csv file: %v", err)
// 	}

// 	var sets []Sets
// 	var currentSet *Sets
// 	numPattern := regexp.MustCompile(`^-?\d+$`)

// 	for i, row := range records {
// 		// Skip empty rows
// 		if len(strings.TrimSpace(strings.Join(row, ""))) == 0 {
// 			continue
// 		}

// 		// Make sure we have at least 4 fields in the row
// 		for len(row) < 4 {
// 			row = append(row, "")
// 		}

// 		line := row[0]
// 		transactionStr := row[1]
// 		transactionStr = strings.Trim(transactionStr, "() ")
// 		transactionParts := strings.Split(transactionStr, ",")
// 		if len(transactionParts) != 3 {
// 			fmt.Println("Invalid transaction format:", row)
// 			continue
// 		}

// 		source := strings.TrimSpace(transactionParts[0])
// 		destination := strings.TrimSpace(transactionParts[1])
// 		amount, err := strconv.Atoi(strings.TrimSpace(transactionParts[2]))
// 		if err != nil {
// 			return nil, fmt.Errorf("error parsing amount on line %d: %v", i+1, err)
// 		}

// 		transaction := pbft.Transaction{
// 			Sender:   source,
// 			Receiver: destination,
// 			Amount:   int64(amount),
// 		}

// 		if numPattern.MatchString(line) {
// 			// New set
// 			if currentSet != nil {
// 				sets = append(sets, *currentSet)
// 			}

// 			setNumber, err := strconv.Atoi(line)
// 			if err != nil {
// 				return nil, fmt.Errorf("error parsing set number on line %d: %v", i+1, err)
// 			}

// 			setListStr := strings.Trim(row[2], "[] ")
// 			setList := strings.Split(setListStr, ",")
// 			for i := range setList {
// 				setList[i] = strings.TrimSpace(setList[i])
// 			}

// 			var byzantineServers []string
// 			if row[3] != "" {
// 				byzantineServersStr := strings.Trim(row[3], "[] ")
// 				byzantineServers = strings.Split(byzantineServersStr, ",")
// 				for i := range byzantineServers {
// 					byzantineServers[i] = strings.TrimSpace(byzantineServers[i])
// 				}
// 			}

// 			currentSet = &Sets{
// 				SetNumber:        setNumber,
// 				SetList:          setList,
// 				Transactions:     []pbft.Transaction{transaction},
// 				ByzantineServers: byzantineServers,
// 			}
// 		} else {
// 			// If no new set, add transaction to current set
// 			if currentSet != nil {
// 				currentSet.Transactions = append(currentSet.Transactions, transaction)
// 			} else {
// 				fmt.Println("Skipping transaction as no set is defined yet:", transaction)
// 			}
// 		}
// 	}

// 	// Append the final set if it's non-nil
// 	if currentSet != nil {
// 		sets = append(sets, *currentSet)
// 	}

//		return sets, nil
//	}
func main() {
	// define client addresses

	serverAddresses := []string{
		"localhost:5001",
		"localhost:5002",
		"localhost:5003",
		"localhost:5004",
		"localhost:5005",
		"localhost:5006",
		"localhost:5007",
	}
	sets, err := ParseCSV("pbft.csv")
	if err != nil {
		log.Fatalf("Error parsing CSV: %v", err)
	}

	// Establish connections to each server
	var servers []*ServerClient
	for _, addr := range serverAddresses {
		server, err := connectToServer(addr)
		if err != nil {
			log.Fatalf("Failed to connect to server at %s: %v", addr, err)
		}
		servers = append(servers, server)
		defer server.conn.Close()
	}

	var wg sync.WaitGroup
	sequenceNumber := int64(1)
	for _, set := range sets {
		start := time.Now()
		for _, transaction := range set.Transactions {
			fmt.Printf("Sending transaction from clinet %v", transaction.Sender, "to %v", transaction.Receiver, "of amount %d", transaction.Amount)
			for _, server := range servers {
				wg.Add(1)
				asyncSendTransaction(&wg, server.client, &transaction, sequenceNumber, []pbft.PbftClient{})

			}
			sequenceNumber++
		}
		wg.Wait()
		stop := time.Now()

		// Example transactions
		// transactions := []*pbft.Transaction{
		// 	{Sender: "A", Receiver: "I", Amount: 2, Timestamp: time.Now().Unix()},
		// 	{Sender: "B", Receiver: "E", Amount: 6, Timestamp: time.Now().Unix()},
		// 	{Sender: "H", Receiver: "D", Amount: 3, Timestamp: time.Now().Unix()},
		// 	{Sender: "A", Receiver: "F", Amount: 1, Timestamp: time.Now().Unix()},
		// 	{Sender: "A", Receiver: "G", Amount: 1, Timestamp: time.Now().Unix()},
		// 	{Sender: "I", Receiver: "G", Amount: 2, Timestamp: time.Now().Unix()},
		// }

		// for _, txn := range set.Transactions {
		// 	serverAddr, ok := serverMap[txn.Sender]
		// 	if !ok {
		// 		log.Fatalf("No server found for sender %s", txn.Sender)
		// 	}

		// 	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		// 	if err != nil {
		// 		log.Fatalf("Could not connect to server %s: %v", serverAddr, err)
		// 	}

		// 	client := pbft.NewPbftClient(conn)

		// 	wg.Add(1) // Increment the wait group counter
		// 	go func(client pbft.PbftClient, txn Transaction) {
		// 		defer wg.Done() // Decrement the counter when the goroutine completes
		// 		fmt.Printf("Sending transaction from %s to %s of amount %d to server %s\n", txn.Sender, txn.Receiver, txn.Amount, serverAddr)
		// 		sendTransaction(client, txn.Sender, txn.Receiver, int32(txn.Amount), int32(txn.sequencenumber), serverAddr)
		// 	}(client, txn)

		// 	defer conn.Close() // Ensure connection is closed after sending the transaction
		// }

		// wg.Wait()

		// Send transactions asynchronously to all servers
		// var wg sync.WaitGroup
		// sequenceNumber := int64(1)
		// for _, tx := range transactions {
		// 	for _, server := range servers {
		// 		wg.Add(1)
		// 		asyncSendTransaction(&wg, server.client, tx, sequenceNumber, []pbft.PbftClient{})
		// 	}
		// 	sequenceNumber++
		// }

		// Wait for all transactions to complete

		//after clicking enter clinet will proceed to get the balances
		fmt.Println("Press Enter to continue...")
		fmt.Scanln()

		// Retrieve and print balances from each server
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
		defer cancel()
		for i, server := range servers {
			fmt.Printf("Balances of clients on server %d is:\n", i+1)
			if err := printBalances(server, ctx); err != nil {
				log.Printf("Failed to get balances from server %d: %v", i, err)
			}
		}

		fmt.Println("Enter the sequence number of the transaction you want to verify: ")
		fmt.Println("enter esc to exit")

		var seq int64
		// take multiple inputs

		fmt.Scanln(&seq)

		ctx, cancel = context.WithTimeout(context.Background(), time.Second*20)
		defer cancel()
		for i, server := range servers {
			fmt.Printf("Status of Transaction on server %d is:\n", i+1)
			if err := printstatus(server, ctx, seq); err != nil {
				log.Printf("Failed to get status from server %d: %v", i, err)
			}
		}

		fmt.Printf("Time taken to send all transactions: %v\n", stop.Sub(start))

		fmt.Println("All transactions have been sent and balances retrieved.")

		// go func() {
		// 	for {
		// 		for _, server := range servers {
		// 			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		// 			defer cancel()
		// 				if server.reply == nil {
		// 					continue
		// 				}
		// 				status := &pbft.StatusRequest{
		// 					Status: "Committed",
		// 				}
		// 				message := *server.reply
		// 				reply, err := server.client.SendReply(ctx, status, message)
		// 				if err != nil {
		// 					log.Printf("Failed to receive reply: %v", err)
		// 					break
		// 				}
		// 				receivereply(ctx, reply)
		// 			}
		// 		}
		// }()

		fmt.Println("All transactions have been sent.")
	}
}
