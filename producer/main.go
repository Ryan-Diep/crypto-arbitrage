package main

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	pb "github.com/Ryan-Diep/cloud-arbitrage/producer/pkg/model"
)

const (
	KafkaTopic      = "crypto-prices"
	KafkaBroker     = "localhost:9092"
	RefreshInterval = 6 * time.Hour

	CoinbaseWS  = "wss://advanced-trade-ws.coinbase.com"
	CoinbaseAPI = "https://api.exchange.coinbase.com/products"

	BinanceBase = "wss://stream.binance.com:9443/stream?streams="
	BinanceAPI  = "https://api.binance.com/api/v3/exchangeInfo"

	BybitWS  = "wss://stream.bybit.com/v5/public/spot"
	BybitAPI = "https://api.bybit.com/v5/market/instruments-info?category=spot"

	OkxWS  = "wss://ws.okx.com:8443/ws/v5/public"
	OkxAPI = "https://www.okx.com/api/v5/public/instruments?instType=SPOT"
)

var stablecoins = map[string]bool{
	"USD":  true,
	"USDT": true,
	"USDC": true,
	"DAI":  true,
}

var globalMessageCount atomic.Int64

type SymbolManager struct {
	mu              sync.RWMutex
	coinbaseSymbols []string
	binanceSymbols  []string
	bybitSymbols    []string
	okxSymbols      []string
}

func NewSymbolManager() *SymbolManager {
	return &SymbolManager{}
}

func (sm *SymbolManager) GetSymbols(exchange string) []string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	switch exchange {
	case "COINBASE":
		return sm.coinbaseSymbols
	case "BINANCE":
		return sm.binanceSymbols
	case "BYBIT":
		return sm.bybitSymbols
	case "OKX":
		return sm.okxSymbols
	default:
		return nil
	}
}

func (sm *SymbolManager) UpdateSymbols() error {
	var wg sync.WaitGroup
	var cbSyms, bnSyms, bySyms, okSyms []string
	var errCb, errBn, errBy, errOk error

	wg.Add(4)
	go func() { defer wg.Done(); cbSyms, errCb = fetchCoinbaseSymbols() }()
	go func() { defer wg.Done(); bnSyms, errBn = fetchBinanceSymbols() }()
	go func() { defer wg.Done(); bySyms, errBy = fetchBybitSymbols() }()
	go func() { defer wg.Done(); okSyms, errOk = fetchOkxSymbols() }()

	wg.Wait()

	if errCb != nil {
		log.Printf("Error fetching Coinbase: %v", errCb)
	}
	if errBn != nil {
		log.Printf("Error fetching Binance: %v", errBn)
	}
	if errBy != nil {
		log.Printf("Error fetching Bybit: %v", errBy)
	}
	if errOk != nil {
		log.Printf("Error fetching Okx: %v", errOk)
	}

	baseAssetMap := make(map[string]map[string]bool)

	tally := func(exchangeName string, symbols []string) {
		for _, s := range symbols {
			parts := strings.Split(s, "-")
			if len(parts) != 2 {
				continue
			}

			base := parts[0]
			if baseAssetMap[base] == nil {
				baseAssetMap[base] = make(map[string]bool)
			}
			baseAssetMap[base][exchangeName] = true
		}
	}

	tally("COINBASE", cbSyms)
	tally("BINANCE", bnSyms)
	tally("BYBIT", bySyms)
	tally("OKX", okSyms)

	validBases := make(map[string]bool)
	for base, exchanges := range baseAssetMap {
		if len(exchanges) >= 2 {
			validBases[base] = true
		}
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.coinbaseSymbols = filterByBase(cbSyms, validBases)
	sm.binanceSymbols = filterByBase(bnSyms, validBases)
	sm.bybitSymbols = filterByBase(bySyms, validBases)
	sm.okxSymbols = filterByBase(okSyms, validBases)

	log.Printf("Smart Filter Complete (Match >= 2 exchanges). Tracking: CB: %d | BN: %d | BY: %d | OK: %d",
		len(sm.coinbaseSymbols), len(sm.binanceSymbols), len(sm.bybitSymbols), len(sm.okxSymbols))

	return nil
}

func filterByBase(source []string, validBases map[string]bool) []string {
	var result []string
	for _, s := range source {
		parts := strings.Split(s, "-")
		if len(parts) == 2 && validBases[parts[0]] {
			result = append(result, s)
		}
	}
	return result
}

func fetchCoinbaseSymbols() ([]string, error) {
	resp, err := http.Get(CoinbaseAPI)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var products []struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&products); err != nil {
		return nil, err
	}

	var symbols []string
	for _, p := range products {
		if p.Status == "online" {
			symbols = append(symbols, p.ID)
		}
	}
	return symbols, nil
}

func fetchBinanceSymbols() ([]string, error) {
	resp, err := http.Get(BinanceAPI)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var exchangeInfo struct {
		Symbols []struct {
			Symbol     string `json:"symbol"`
			Status     string `json:"status"`
			BaseAsset  string `json:"baseAsset"`
			QuoteAsset string `json:"quoteAsset"`
		} `json:"symbols"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&exchangeInfo); err != nil {
		return nil, err
	}

	var symbols []string
	for _, s := range exchangeInfo.Symbols {
		if s.Status == "TRADING" && stablecoins[s.QuoteAsset] {
			symbols = append(symbols, s.BaseAsset+"-"+s.QuoteAsset)
		}
	}
	return symbols, nil
}

func fetchBybitSymbols() ([]string, error) {
	resp, err := http.Get(BybitAPI)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		Result struct {
			List []struct {
				Symbol    string `json:"symbol"`
				BaseCoin  string `json:"baseCoin"`
				QuoteCoin string `json:"quoteCoin"`
				Status    string `json:"status"`
			} `json:"list"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	var symbols []string
	for _, s := range result.Result.List {
		if s.Status == "Trading" && stablecoins[s.QuoteCoin] {
			symbols = append(symbols, s.BaseCoin+"-"+s.QuoteCoin)
		}
	}
	return symbols, nil
}

func fetchOkxSymbols() ([]string, error) {
	resp, err := http.Get(OkxAPI)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		Data []struct {
			InstID   string `json:"instId"`
			BaseCcy  string `json:"baseCcy"`
			QuoteCcy string `json:"quoteCcy"`
			State    string `json:"state"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	var symbols []string
	for _, s := range result.Data {
		if s.State == "live" && stablecoins[s.QuoteCcy] {
			symbols = append(symbols, s.InstID)
		}
	}
	return symbols, nil
}

func main() {
	if err := godotenv.Load("../.env"); err != nil {
		godotenv.Load()
	}

	kafkaWriter := &kafka.Writer{
		Addr:         kafka.TCP(KafkaBroker),
		Topic:        KafkaTopic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    300,
		BatchTimeout: 10 * time.Millisecond,
		Async:        true,
		RequiredAcks: kafka.RequireOne,
		Compression:  kafka.Snappy,
	}
	defer kafkaWriter.Close()

	symbolMgr := NewSymbolManager()

	log.Println("Discovering symbols across 4 exchanges...")
	if err := symbolMgr.UpdateSymbols(); err != nil {
		log.Printf("Initial symbol discovery warning: %v", err)
	}

	go func() {
		ticker := time.NewTicker(RefreshInterval)
		defer ticker.Stop()
		for range ticker.C {
			log.Println("Refreshing symbol list...")
			symbolMgr.UpdateSymbols()
		}
	}()

	var wg sync.WaitGroup
	wg.Add(4)

	go func() { defer wg.Done(); startCoinbase(kafkaWriter, symbolMgr) }()
	go func() { defer wg.Done(); startBinance(kafkaWriter, symbolMgr) }()
	go func() { defer wg.Done(); startBybit(kafkaWriter, symbolMgr) }()
	go func() { defer wg.Done(); startOkx(kafkaWriter, symbolMgr) }()

	log.Printf("Engine Started. Multi-Exchange Mode (Match >= 2).")

	startTime := time.Now()

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		var lastCount int64

		for range ticker.C {
			currentCount := globalMessageCount.Load()

			instantRate := float64(currentCount-lastCount) / 5.0
			lastCount = currentCount

			elapsed := time.Since(startTime).Seconds()
			avgRate := 0.0
			if elapsed > 0 {
				avgRate = float64(currentCount) / elapsed
			}

			kStats := kafkaWriter.Stats()

			log.Printf("[STATS] Current: %.0f msg/sec | Avg: %.0f msg/sec | Total: %d | Errors: %d | Queue: %d",
				instantRate, avgRate, currentCount, kStats.Errors, kStats.QueueLength)
		}
	}()

	wg.Wait()
}

func startBybit(kw *kafka.Writer, sm *SymbolManager) {
	for {
		symbols := sm.GetSymbols("BYBIT")
		if len(symbols) == 0 {
			time.Sleep(5 * time.Second)
			continue
		}

		var topics []string
		for _, s := range symbols {
			raw := strings.ReplaceAll(s, "-", "")
			topics = append(topics, "tickers."+raw)
		}

		chunks := chunkSymbols(topics, 10)

		log.Printf("Bybit: Connecting for %d symbols...", len(symbols))
		conn, _, err := websocket.DefaultDialer.Dial(BybitWS, nil)
		if err != nil {
			log.Printf("Bybit Connect Error: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		for _, chunk := range chunks {
			req := map[string]interface{}{"op": "subscribe", "args": chunk}
			if err := conn.WriteJSON(req); err != nil {
				conn.Close()
				break
			}
			time.Sleep(50 * time.Millisecond)
		}

		func() {
			defer conn.Close()
			for {
				_, message, err := conn.ReadMessage()
				if err != nil {
					return
				}

				var msg BybitMessage
				if err := json.Unmarshal(message, &msg); err != nil {
					continue
				}

				if msg.Data.LastPrice == "" {
					continue
				}

				rawSym := msg.Data.Symbol
				var stdSym string
				if strings.HasSuffix(rawSym, "USDT") {
					stdSym = strings.TrimSuffix(rawSym, "USDT") + "-USDT"
				} else if strings.HasSuffix(rawSym, "USDC") {
					stdSym = strings.TrimSuffix(rawSym, "USDC") + "-USDC"
				} else if strings.HasSuffix(rawSym, "USD") {
					stdSym = strings.TrimSuffix(rawSym, "USD") + "-USD"
				} else if strings.HasSuffix(rawSym, "DAI") {
					stdSym = strings.TrimSuffix(rawSym, "DAI") + "-DAI"
				} else {
					continue
				}

				price, _ := strconv.ParseFloat(msg.Data.LastPrice, 64)

				sendToKafka(kw, &pb.Ticker{
					Source:    "BYBIT",
					Symbol:    stdSym,
					Price:     price,
					Timestamp: time.Now().UnixMilli(),
				})
			}
		}()
		time.Sleep(2 * time.Second)
	}
}

func startOkx(kw *kafka.Writer, sm *SymbolManager) {
	for {
		symbols := sm.GetSymbols("OKX")
		if len(symbols) == 0 {
			time.Sleep(5 * time.Second)
			continue
		}

		log.Printf("OKX: Connecting for %d symbols...", len(symbols))
		conn, _, err := websocket.DefaultDialer.Dial(OkxWS, nil)
		if err != nil {
			log.Printf("OKX Connect Error: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		var args []map[string]string
		for _, s := range symbols {
			args = append(args, map[string]string{
				"channel": "tickers",
				"instId":  s,
			})
		}

		chunkSize := 50
		for i := 0; i < len(args); i += chunkSize {
			end := i + chunkSize
			if end > len(args) {
				end = len(args)
			}

			req := map[string]interface{}{"op": "subscribe", "args": args[i:end]}
			if err := conn.WriteJSON(req); err != nil {
				conn.Close()
				break
			}
			time.Sleep(50 * time.Millisecond)
		}

		func() {
			defer conn.Close()
			for {
				_, message, err := conn.ReadMessage()
				if err != nil {
					return
				}

				var msg OkxMessage
				if err := json.Unmarshal(message, &msg); err != nil {
					continue
				}

				if len(msg.Data) > 0 && msg.Data[0].Last != "" {
					price, _ := strconv.ParseFloat(msg.Data[0].Last, 64)
					sendToKafka(kw, &pb.Ticker{
						Source:    "OKX",
						Symbol:    msg.Data[0].InstID,
						Price:     price,
						Timestamp: time.Now().UnixMilli(),
					})
				}
			}
		}()
		time.Sleep(2 * time.Second)
	}
}

func startBinance(kw *kafka.Writer, sm *SymbolManager) {
	for {
		symbols := sm.GetSymbols("BINANCE")
		if len(symbols) == 0 {
			time.Sleep(5 * time.Second)
			continue
		}

		var streams []string
		for _, s := range symbols {
			parts := strings.Split(s, "-")
			if len(parts) == 2 {
				streams = append(streams, strings.ToLower(parts[0]+parts[1])+"@ticker")
			}
		}

		chunks := chunkStreams(streams, 200)
		var wg sync.WaitGroup

		for i, chunk := range chunks {
			wg.Add(1)
			go func(idx int, ch []string) {
				defer wg.Done()
				connectBinanceStream(kw, ch, idx)
			}(i, chunk)
		}
		wg.Wait()
		time.Sleep(5 * time.Second)
	}
}

func connectBinanceStream(kw *kafka.Writer, streams []string, connIdx int) {
	url := BinanceBase + strings.Join(streams, "/")

	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		log.Printf("Binance[%d] Connect Error: %v", connIdx, err)
		return
	}

	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	pingTicker := time.NewTicker(20 * time.Second)
	stopPing := make(chan struct{})

	go func() {
		for {
			select {
			case <-pingTicker.C:
				if err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(10*time.Second)); err != nil {
					return
				}
			case <-stopPing:
				return
			}
		}
	}()

	func() {
		defer conn.Close()
		defer pingTicker.Stop()
		defer close(stopPing)

		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				return
			}

			var streamMsg BinanceStreamMessage
			if err := json.Unmarshal(message, &streamMsg); err != nil {
				continue
			}

			data := streamMsg.Data
			if data.LastPrice == "" {
				continue
			}

			priceStr := string(data.LastPrice)
			price, _ := strconv.ParseFloat(priceStr, 64)

			rawSymbol := strings.ToUpper(data.Symbol)
			var stdSym string
			if strings.HasSuffix(rawSymbol, "USDT") {
				stdSym = strings.TrimSuffix(rawSymbol, "USDT") + "-USDT"
			} else if strings.HasSuffix(rawSymbol, "USDC") {
				stdSym = strings.TrimSuffix(rawSymbol, "USDC") + "-USDC"
			} else if strings.HasSuffix(rawSymbol, "USD") {
				stdSym = strings.TrimSuffix(rawSymbol, "USD") + "-USD"
			} else if strings.HasSuffix(rawSymbol, "DAI") {
				stdSym = strings.TrimSuffix(rawSymbol, "DAI") + "-DAI"
			} else {
				continue
			}

			sendToKafka(kw, &pb.Ticker{
				Source:    "BINANCE",
				Symbol:    stdSym,
				Price:     price,
				Timestamp: time.Now().UnixMilli(),
			})
		}
	}()
}

func startCoinbase(kw *kafka.Writer, sm *SymbolManager) {
	apiKeyName := os.Getenv("COINBASE_API_KEY")
	apiSecret := os.Getenv("COINBASE_SECRET_KEY")

	for {
		symbols := sm.GetSymbols("COINBASE")
		if len(symbols) == 0 {
			time.Sleep(5 * time.Second)
			continue
		}

		tokenString, err := buildJWT(apiKeyName, apiSecret)
		if err != nil {
			log.Printf("Coinbase JWT Error: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		conn, _, err := websocket.DefaultDialer.Dial(CoinbaseWS, nil)
		if err != nil {
			log.Printf("Coinbase Connect Error: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		log.Printf("Coinbase: Subscribing to %d symbols...", len(symbols))
		chunks := chunkSymbols(symbols, 200)
		for _, chunk := range chunks {
			sub := map[string]interface{}{
				"type":        "subscribe",
				"channel":     "ticker",
				"product_ids": chunk,
				"jwt":         tokenString,
			}
			if err := conn.WriteJSON(sub); err != nil {
				conn.Close()
				break
			}
			time.Sleep(100 * time.Millisecond)
		}

		func() {
			defer conn.Close()
			for {
				_, message, err := conn.ReadMessage()
				if err != nil {
					return
				}

				var response CoinbaseResponse
				if err := json.Unmarshal(message, &response); err != nil {
					continue
				}

				for _, event := range response.Events {
					for _, tick := range event.Tickers {
						price, _ := strconv.ParseFloat(tick.Price, 64)
						sendToKafka(kw, &pb.Ticker{
							Source:    "COINBASE",
							Symbol:    tick.ProductID,
							Price:     price,
							Timestamp: time.Now().UnixMilli(),
						})
					}
				}
			}
		}()
		time.Sleep(2 * time.Second)
	}
}

func sendToKafka(kw *kafka.Writer, t *pb.Ticker) {
	data, err := proto.Marshal(t)
	if err != nil {
		log.Printf("Marshal error: %v", err)
		return
	}

	globalMessageCount.Add(1)

	err = kw.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(t.Symbol),
		Value: data,
	})
	if err != nil {
		log.Printf("Kafka write error: %v", err)
	}
}

func buildJWT(keyName, secretKey string) (string, error) {
	block, _ := pem.Decode([]byte(secretKey))
	if block == nil {
		return "", fmt.Errorf("bad pem")
	}
	key, err := x509.ParseECPrivateKey(block.Bytes)
	if err != nil {
		return "", err
	}
	claims := jwt.MapClaims{
		"iss": "cdp",
		"nbf": time.Now().Unix(),
		"exp": time.Now().Add(2 * time.Minute).Unix(),
		"sub": keyName,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	token.Header["kid"] = keyName
	token.Header["nonce"] = fmt.Sprintf("%d", time.Now().UnixNano())
	return token.SignedString(key)
}

func chunkStreams(streams []string, chunkSize int) [][]string {
	var chunks [][]string
	for i := 0; i < len(streams); i += chunkSize {
		end := i + chunkSize
		if end > len(streams) {
			end = len(streams)
		}
		chunks = append(chunks, streams[i:end])
	}
	return chunks
}

func chunkSymbols(symbols []string, chunkSize int) [][]string {
	var chunks [][]string
	for i := 0; i < len(symbols); i += chunkSize {
		end := i + chunkSize
		if end > len(symbols) {
			end = len(symbols)
		}
		chunks = append(chunks, symbols[i:end])
	}
	return chunks
}

type BinanceStreamMessage struct {
	Stream string        `json:"stream"`
	Data   BinanceTicker `json:"data"`
}

type BinanceTicker struct {
	EventType          string      `json:"e"`
	EventTime          int64       `json:"E"`
	Symbol             string      `json:"s"`
	PriceChange        json.Number `json:"p"`
	PriceChangePercent json.Number `json:"P"`
	WeightedAvgPrice   json.Number `json:"w"`
	LastPrice          json.Number `json:"c"`
	LastQty            json.Number `json:"Q"`
	OpenPrice          json.Number `json:"o"`
	HighPrice          json.Number `json:"h"`
	LowPrice           json.Number `json:"l"`
	BaseVolume         json.Number `json:"v"`
	QuoteVolume        json.Number `json:"q"`
	OpenTime           int64       `json:"O"`
	CloseTime          int64       `json:"C"`
	FirstTradeID       int64       `json:"F"`
	LastTradeID        int64       `json:"L"`
	TotalTrades        int64       `json:"n"`
}

type CoinbaseResponse struct {
	Events []CoinbaseEvent `json:"events"`
}

type CoinbaseEvent struct {
	Tickers []CoinbaseTicker `json:"tickers"`
}

type CoinbaseTicker struct {
	ProductID string `json:"product_id"`
	Price     string `json:"price"`
}

type BybitMessage struct {
	Topic string    `json:"topic"`
	Data  BybitData `json:"data"`
}

type BybitData struct {
	Symbol    string `json:"symbol"`
	LastPrice string `json:"lastPrice"`
}

type OkxMessage struct {
	Data []OkxTicker `json:"data"`
}

type OkxTicker struct {
	InstID string `json:"instId"`
	Last   string `json:"last"`
}
