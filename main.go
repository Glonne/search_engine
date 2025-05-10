package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	pb "search_engine/proto"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alibaba/sentinel-golang/api"
	"github.com/alibaba/sentinel-golang/core/config"
	"github.com/alibaba/sentinel-golang/core/flow"
	"github.com/dgraph-io/badger/v4"
	"github.com/gin-gonic/gin"
	"github.com/huandu/skiplist"
	farmhash "github.com/leemcloughlin/gofarmhash"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
)

var (
	mode         = flag.Int("mode", 1, "启动哪类服务。1-standalone web server, 2-http index server, 3-distributed web server")
	rebuildIndex = flag.Bool("index", false, "server启动时是否需要重建索引")
	port         = flag.Int("port", 5678, "server的工作端口")
	dbPath       = flag.String("dbPath", "data/local_db/video_bolt", "正排索引数据的存放路径")
	totalWorkers = flag.Int("totalWorkers", 0, "分布式环境中一共有几台index worker")
	workerIndex  = flag.Int("workerIndex", 0, "本机是第几台index worker(从0开始编号)")
	csvFile      = flag.String("csvFile", "data/bili_video.csv", "CSV 文件路径")
)

var (
	etcdServers = []string{"127.0.0.1:2379"} // etcd 集群地址
)

type indexServer struct {
	pb.UnimplementedIndexServiceServer
	worker *IndexWorker
}

// Keyword 定义关键词结构
type Keyword struct {
	Field string `json:"field"`
	Word  string `json:"word"`
}

func (k *Keyword) ToString() string {
	return k.Field + "\001" + k.Word
}
func initSentinel() {
	// 初始化 Sentinel 配置
	conf := config.NewDefaultConfig()
	// 可选：设置日志路径
	conf.Sentinel.Log.Dir = "./sentinel_logs"
	if err := api.InitWithConfig(conf); err != nil {
		panic(fmt.Sprintf("Failed to initialize Sentinel: %v", err))
	}

	// 定义流量控制规则
	_, err := flow.LoadRules([]*flow.Rule{
		{
			Resource:               "search_service", // 资源名，表示搜索服务
			Threshold:              50,               // QPS 阈值，限制为 50
			TokenCalculateStrategy: flow.Direct,
			ControlBehavior:        flow.Reject, // 超出时直接拒绝
		},
	})
	if err != nil {
		panic(fmt.Sprintf("Failed to load Sentinel rules: %v", err))
	}
	fmt.Println("Sentinel initialized with QPS limit: 50")
}

// Document 定义文档结构
type Document struct {
	Id          string     `json:"id"`           // 字符串形式的文档 ID，如 "BV1YK411e7gn"
	IntId       uint64     `json:"int_id"`       // 整数形式的唯一 ID，由 ForwardServer 分配
	Keywords    []*Keyword `json:"keywords"`     // 关键词列表
	Bytes       []byte     `json:"bytes"`        // 序列化的视频数据
	BitsFeature []byte     `json:"bits_feature"` // BitsFeature 占位符，您可定义具体内容
}

// SkipListValue 定义跳表的值
type SkipListValue struct {
	Id          string // 文档的字符串 ID
	BitsFeature []byte // BitsFeature 占位符
}

// Video 定义视频结构
type Video struct {
	Id       string   `json:"id"`
	Title    string   `json:"title"`
	PostTime int64    `json:"postTime"`
	Author   string   `json:"author"`
	View     int32    `json:"view"`
	Like     int32    `json:"like"`
	Coin     int32    `json:"coin"`
	Favorite int32    `json:"favorite"`
	Share    int32    `json:"share"`
	Keywords []string `json:"keywords"`
}

// ForwardServer 正排服务器
type ForwardServer struct {
	db       *badger.DB
	maxIntId uint64
	mu       sync.Mutex
}

func (s *indexServer) Search(ctx context.Context, req *pb.SearchRequest) (*pb.SearchResponse, error) {
	results := s.worker.Search(req.Classes, req.Author, req.Keywords, req.ViewFrom, req.ViewTo)
	videos := make([]*pb.Video, len(results))
	for i, v := range results {
		videos[i] = &pb.Video{
			Id:       v.Id,
			Title:    v.Title,
			PostTime: v.PostTime,
			Author:   v.Author,
			View:     v.View,
			Like:     v.Like,
			Coin:     v.Coin,
			Favorite: v.Favorite,
			Share:    v.Share,
			Keywords: v.Keywords,
		}
	}
	return &pb.SearchResponse{Videos: videos}, nil
}

// AddDoc 实现 gRPC AddDoc 方法
func (s *indexServer) AddDoc(ctx context.Context, req *pb.AddDocRequest) (*pb.AddDocResponse, error) {
	doc := Document{
		Id:          req.Id,
		Keywords:    make([]*Keyword, len(req.Keywords)),
		Bytes:       req.Bytes,
		BitsFeature: req.BitsFeature,
	}
	for i, k := range req.Keywords {
		doc.Keywords[i] = &Keyword{Field: k.Field, Word: k.Word}
	}
	updatedDoc, err := s.worker.forward.AddDoc(doc)
	if err != nil {
		return &pb.AddDocResponse{Status: "error"}, err
	}
	s.worker.reverse.AddDoc(updatedDoc)
	return &pb.AddDocResponse{Status: "OK"}, nil
}

// Search 方法（为 gRPC 提供支持）
func (w *IndexWorker) Search(classes []string, author string, keywords []string, viewFrom, viewTo int32) []Video {
	if len(keywords) == 0 && author == "" && len(classes) == 0 {
		return nil
	}

	var keywordIds, authorIds, classIds []string
	for _, kw := range keywords {
		fullKeyword := "content\001" + kw
		docIds := w.reverse.Search(fullKeyword)
		keywordIds = append(keywordIds, docIds...)
	}
	if author != "" {
		fullAuthor := "author\001" + author
		authorIds = w.reverse.Search(fullAuthor)
	}
	for _, class := range classes {
		fullClass := "content\001" + class
		docIds := w.reverse.Search(fullClass)
		classIds = append(classIds, docIds...)
	}

	idMap := make(map[string]int)
	conditionCount := 0
	if len(keywordIds) > 0 {
		conditionCount++
		for _, id := range keywordIds {
			idMap[id]++
		}
	}
	if len(authorIds) > 0 {
		conditionCount++
		for _, id := range authorIds {
			idMap[id]++
		}
	}
	if len(classIds) > 0 {
		conditionCount++
		for _, id := range classIds {
			idMap[id]++
		}
	}

	var matchedIds []string
	for id, count := range idMap {
		if count == conditionCount {
			matchedIds = append(matchedIds, id)
		}
	}

	var results []Video
	for _, id := range matchedIds {
		doc, exists, err := w.forward.Get(id)
		if err != nil || !exists {
			fmt.Printf("Failed to get doc %s: %v\n", id, err)
			continue
		}
		var video Video
		if err := gob.NewDecoder(bytes.NewReader(doc.Bytes)).Decode(&video); err != nil {
			fmt.Printf("Decode video %s failed: %v\n", id, err)
			continue
		}
		if video.View >= viewFrom && video.View <= viewTo {
			results = append(results, video)
		}
	}
	return results
}
func NewForwardServer(dbPath string) (*ForwardServer, error) {
	db, err := badger.Open(badger.DefaultOptions(dbPath))
	if err != nil {
		return nil, err
	}
	return &ForwardServer{db: db}, nil
}

func (s *ForwardServer) AddDoc(doc Document) (Document, error) {
	s.mu.Lock()
	s.maxIntId++
	doc.IntId = s.maxIntId // 分配全局唯一的 IntId
	s.mu.Unlock()

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(doc); err != nil {
		return Document{}, err
	}
	err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(doc.Id), buf.Bytes())
	})
	if err != nil {
		return Document{}, err
	}
	return doc, nil // 返回更新后的 doc
}

func (s *ForwardServer) Get(id string) (Document, bool, error) {
	var doc Document
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(id))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return gob.NewDecoder(bytes.NewReader(val)).Decode(&doc)
		})
	})
	if err != nil || doc.Id == "" {
		return Document{}, false, err
	}
	return doc, true, nil
}

func (s *ForwardServer) IterDB(fn func(doc Document) error) error {
	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			var doc Document
			err := item.Value(func(val []byte) error {
				return gob.NewDecoder(bytes.NewReader(val)).Decode(&doc)
			})
			if err != nil {
				return err
			}
			if err := fn(doc); err != nil {
				return err
			}
		}
		return nil
	})
}

// ReverseServer 倒排服务器
type ReverseServer struct {
	segments []*Segment
	segCount int
}

type Segment struct {
	table *sync.Map // 键是 keyword，值是 *skiplist.SkipList
}

func NewReverseServer(segCount int) *ReverseServer {
	segments := make([]*Segment, segCount)
	for i := 0; i < segCount; i++ {
		segments[i] = &Segment{table: &sync.Map{}}
	}
	return &ReverseServer{segments: segments, segCount: segCount}
}

func (s *ReverseServer) hash(key string) int {
	h := 0
	for _, c := range key {
		h = h*31 + int(c)
	}
	segIdx := h % s.segCount
	if segIdx < 0 {
		segIdx += s.segCount // 调整负数
	}
	return segIdx
}

func (s *ReverseServer) AddDoc(doc Document) {
	for _, kw := range doc.Keywords {
		key := kw.ToString()
		segIdx := s.hash(key)
		seg := s.segments[segIdx]
		sklValue := SkipListValue{
			Id:          doc.Id,
			BitsFeature: doc.BitsFeature, // 存储 BitsFeature
		}
		if val, exists := seg.table.Load(key); exists {
			list := val.(*skiplist.SkipList)
			list.Set(doc.IntId, sklValue) // 以 IntId 作为键
		} else {
			list := skiplist.New(skiplist.Uint64)
			list.Set(doc.IntId, sklValue) // 以 IntId 作为键
			seg.table.Store(key, list)
		}
	}
}

func (s *ReverseServer) Search(keyword string) []string {
	segIdx := s.hash(keyword)
	seg := s.segments[segIdx]

	if val, exists := seg.table.Load(keyword); exists {
		list := val.(*skiplist.SkipList)
		result := make([]string, 0, list.Len())
		node := list.Front()
		for node != nil {
			skv := node.Value.(SkipListValue)
			result = append(result, skv.Id) // 返回字符串 ID
			node = node.Next()
		}
		return result
	}
	return nil
}

// IndexWorker HTTP 索引服务
type IndexWorker struct {
	forward *ForwardServer
	reverse *ReverseServer
}

func NewIndexWorker(dbPath string, segCount int) (*IndexWorker, error) {
	fwd, err := NewForwardServer(dbPath)
	if err != nil {
		return nil, err
	}
	return &IndexWorker{
		forward: fwd,
		reverse: NewReverseServer(segCount),
	}, nil
}

func (w *IndexWorker) AddDocHandler(c *gin.Context) {
	var doc Document
	if err := c.BindJSON(&doc); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	updatedDoc, err := w.forward.AddDoc(doc)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	w.reverse.AddDoc(updatedDoc)
	c.JSON(http.StatusOK, gin.H{"status": "OK"})
}

func (w *IndexWorker) SearchHandler(c *gin.Context) {
	// 搜索请求：json类别，作者，关键词，播放量
	type SearchRequest struct {
		Classes  []string `json:"Classes"`
		Author   string   `json:"Author"`
		Keywords []string `json:"Keywords"`
		ViewFrom int      `json:"ViewFrom"`
		ViewTo   int      `json:"ViewTo"`
	}
	// req解析为JSON
	var req SearchRequest
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
		return
	}
	fmt.Printf("Received search request: %+v\n", req)
	// 关键词，作者，类别都为空
	if len(req.Keywords) == 0 && req.Author == "" && len(req.Classes) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "at least one search condition is required"})
		return
	}

	// 分别获取每个条件的匹配 ID
	var keywordIds, authorIds, classIds []string

	// 搜索关键词（content 字段）
	for _, kw := range req.Keywords {
		fullKeyword := "content\001" + kw
		docIds := w.reverse.Search(fullKeyword)
		keywordIds = append(keywordIds, docIds...)
	}

	// 搜索作者（author 字段）
	if req.Author != "" {
		fullAuthor := "author\001" + req.Author
		authorIds = w.reverse.Search(fullAuthor)
	}

	// 搜索类别（content 字段）
	for _, class := range req.Classes {
		fullClass := "content\001" + class
		docIds := w.reverse.Search(fullClass)
		classIds = append(classIds, docIds...)
	}

	// 计算交集
	idMap := make(map[string]int) // 使用计数器统计每个 ID 出现的次数
	conditionCount := 0           // 非空条件的数量

	if len(keywordIds) > 0 {
		conditionCount++
		for _, id := range keywordIds {
			idMap[id]++
		}
	}
	if len(authorIds) > 0 {
		conditionCount++
		for _, id := range authorIds {
			idMap[id]++
		}
	}
	if len(classIds) > 0 {
		conditionCount++
		for _, id := range classIds {
			idMap[id]++
		}
	}

	// 筛选同时满足所有条件的 ID
	var matchedIds []string
	for id, count := range idMap {
		if count == conditionCount { // ID 必须出现在所有非空条件中
			matchedIds = append(matchedIds, id)
		}
	}

	// 获取视频详情并过滤播放量
	var results []Video
	for _, id := range matchedIds {
		doc, exists, err := w.forward.Get(id)
		if err != nil || !exists {
			fmt.Printf("Failed to get doc %s: %v\n", id, err)
			continue
		}
		var video Video
		if err := gob.NewDecoder(bytes.NewReader(doc.Bytes)).Decode(&video); err != nil {
			fmt.Printf("Decode video %s failed: %v\n", id, err)
			continue
		}
		if video.View >= int32(req.ViewFrom) && video.View <= int32(req.ViewTo) {
			results = append(results, video)
		}
	}

	fmt.Printf("[SearchHandler] 搜索结果: %+v\n", results)
	c.JSON(http.StatusOK, results)
}

func (w *IndexWorker) LoadIndex() error {
	return w.forward.IterDB(func(doc Document) error {
		w.reverse.AddDoc(doc)
		return nil
	})
}

// loadCSVToIndex 从 CSV 文件加载数据并构建索引
func loadCSVToIndex(worker *IndexWorker, csvFile string, totalWorkers, workerIndex int) error {
	file, err := os.Open(csvFile)
	if err != nil {
		return fmt.Errorf("open CSV file failed: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("read CSV file failed: %v", err)
	}

	loc, _ := time.LoadLocation("Asia/Shanghai")
	const layout = "2006/1/2 15:04"

	for _, record := range records {
		if len(record) < 10 {
			fmt.Printf("Skipping invalid record: %v\n", record)
			continue
		}

		docID := strings.TrimPrefix(record[0], "https://www.bilibili.com/video/")
		if totalWorkers > 0 {
			hash := farmhash.Hash32WithSeed([]byte(docID), 0)
			if int(hash)%totalWorkers != workerIndex {
				continue
			}
		}

		title := record[1]
		postTimeStr := record[2]
		author := record[3]
		view, _ := strconv.Atoi(record[4])
		like, _ := strconv.Atoi(record[5])
		coin, _ := strconv.Atoi(record[6])
		favorite, _ := strconv.Atoi(record[7])
		share, _ := strconv.Atoi(record[8])
		keywordsStr := record[9]

		// 解析发布时间
		postTime, err := time.ParseInLocation(layout, postTimeStr, loc)
		if err != nil {
			fmt.Printf("Parse time %s failed: %v\n", postTimeStr, err)
			postTime = time.Now()
		}

		// 解析关键词
		keywordList := strings.Split(strings.Trim(keywordsStr, "\""), ",")
		keywords := make([]*Keyword, 0, len(keywordList)+1)
		keywordStrs := make([]string, 0, len(keywordList))
		for _, kw := range keywordList {
			kw = strings.TrimSpace(kw)
			if kw != "" {
				keywords = append(keywords, &Keyword{Field: "content", Word: kw})
				keywordStrs = append(keywordStrs, kw)
			}
		}
		if author != "" {
			keywords = append(keywords, &Keyword{Field: "author", Word: author})
		}

		video := Video{
			Id:       docID,
			Title:    title,
			PostTime: postTime.Unix(),
			Author:   author,
			View:     int32(view),
			Like:     int32(like),
			Coin:     int32(coin),
			Favorite: int32(favorite),
			Share:    int32(share),
			Keywords: keywordStrs,
		}
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(video); err != nil {
			return fmt.Errorf("encode video failed: %v", err)
		}

		doc := Document{
			Id:          docID,
			Keywords:    keywords,
			Bytes:       buf.Bytes(), // video详细信息
			BitsFeature: []byte{},    // 占位符，您可替换为实际内容
		}

		updatedDoc, err := worker.forward.AddDoc(doc) // 给doc加入一个intid
		if err != nil {
			return fmt.Errorf("add document to forward index failed: %v", err)
		}
		worker.reverse.AddDoc(updatedDoc)
	}

	return nil
}

// Proxy 分布式模式下的请求分发
type Proxy struct {
	etcdClient  *clientv3.Client
	workerAddrs []string
	limiters    map[string]*rate.Limiter
	mu          sync.RWMutex
}

func NewProxy(etcdEndpoints []string) (*Proxy, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	p := &Proxy{
		etcdClient: client,
		limiters:   make(map[string]*rate.Limiter),
	}
	go p.watchWorkers()
	return p, nil
}
func (p *Proxy) watchWorkers() {
	watcher := clientv3.NewWatcher(p.etcdClient)
	watchChan := watcher.Watch(context.Background(), "/radic/index/grpc", clientv3.WithPrefix())
	fmt.Println("[Proxy] Watching for gRPC worker updates...")

	// 初始加载 gRPC 地址
	addrs, err := getService(p.etcdClient, "index")
	if err == nil {
		p.mu.Lock()
		p.workerAddrs = addrs
		for _, addr := range addrs {
			p.limiters[addr] = rate.NewLimiter(rate.Every(time.Second/50), 50)
		}
		p.mu.Unlock()
		fmt.Printf("[Proxy] Initial gRPC workers: %v\n", addrs)
	}

	// 监听变化
	for resp := range watchChan {
		p.mu.Lock()
		for _, ev := range resp.Events {
			switch ev.Type {
			case clientv3.EventTypePut:
				addr := string(ev.Kv.Value) // gRPC 地址，如 127.0.0.1:6678
				if !contains(p.workerAddrs, addr) {
					p.workerAddrs = append(p.workerAddrs, addr)
					p.limiters[addr] = rate.NewLimiter(rate.Every(time.Second/50), 50)
					fmt.Printf("[Proxy] gRPC Worker added: %s\n", addr)
				}
			case clientv3.EventTypeDelete:
				addr := string(ev.Kv.Value)
				p.workerAddrs = remove(p.workerAddrs, addr)
				delete(p.limiters, addr)
				fmt.Printf("[Proxy] gRPC Worker removed: %s\n", addr)
			}
		}
		p.mu.Unlock()
		fmt.Printf("[Proxy] Current gRPC workers: %v\n", p.workerAddrs)
	}
}
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func remove(slice []string, item string) []string {
	var result []string
	for _, s := range slice {
		if s != item {
			result = append(result, s)
		}
	}
	return result
}
func (p *Proxy) SearchHandler(c *gin.Context) {
	entry, err := api.Entry("search_service")
	if err != nil {
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "rate limit exceeded by Sentinel"})
		return
	}
	defer entry.Exit()
	var err1 error
	body, err1 := c.GetRawData()
	if err1 != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read request body"})
		return
	}

	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.workerAddrs) == 0 {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "no workers available"})
		return
	}

	var req struct {
		Classes  []string `json:"Classes"`
		Author   string   `json:"Author"`
		Keywords []string `json:"Keywords"`
		ViewFrom int      `json:"ViewFrom"`
		ViewTo   int      `json:"ViewTo"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON"})
		return
	}

	var results []Video
	var wg sync.WaitGroup
	resultChan := make(chan []Video, len(p.workerAddrs))

	for _, grpcAddr := range p.workerAddrs {
		if !p.limiters[grpcAddr].Allow() {
			fmt.Printf("[Proxy] Rate limit exceeded for %s\n", grpcAddr)
			continue
		}

		wg.Add(1)
		go func(grpcAddr string) {
			defer wg.Done()
			conn, err := grpc.Dial(grpcAddr, grpc.WithInsecure())
			if err != nil {
				fmt.Printf("Failed to connect to %s: %v\n", grpcAddr, err)
				return
			}
			defer conn.Close()

			client := pb.NewIndexServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := client.Search(ctx, &pb.SearchRequest{
				Classes:  req.Classes,
				Author:   req.Author,
				Keywords: req.Keywords,
				ViewFrom: int32(req.ViewFrom),
				ViewTo:   int32(req.ViewTo),
			})
			if err != nil {
				fmt.Printf("Failed to query %s: %v\n", grpcAddr, err)
				return
			}

			workerResults := make([]Video, len(resp.Videos))
			for i, v := range resp.Videos {
				workerResults[i] = Video{
					Id:       v.Id,
					Title:    v.Title,
					PostTime: v.PostTime,
					Author:   v.Author,
					View:     v.View,
					Like:     v.Like,
					Coin:     v.Coin,
					Favorite: v.Favorite,
					Share:    v.Share,
					Keywords: v.Keywords,
				}
			}
			resultChan <- workerResults
		}(grpcAddr)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	for workerResults := range resultChan {
		results = append(results, workerResults...)
	}

	c.JSON(http.StatusOK, results)
}

func (p *Proxy) AddDocHandler(c *gin.Context) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.workerAddrs) == 0 {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "no workers available"})
		return
	}

	var doc Document
	if err := c.BindJSON(&doc); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	keywords := make([]*pb.Keyword, len(doc.Keywords))
	for i, k := range doc.Keywords {
		keywords[i] = &pb.Keyword{Field: k.Field, Word: k.Word}
	}

	for _, grpcAddr := range p.workerAddrs {
		conn, err := grpc.Dial(grpcAddr, grpc.WithInsecure())
		if err != nil {
			fmt.Printf("Failed to connect to %s: %v\n", grpcAddr, err)
			continue
		}
		defer conn.Close()

		client := pb.NewIndexServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, err := client.AddDoc(ctx, &pb.AddDocRequest{
			Id:          doc.Id,
			Keywords:    keywords,
			Bytes:       doc.Bytes,
			BitsFeature: doc.BitsFeature,
		})
		if err != nil || resp.Status != "OK" {
			fmt.Printf("Failed to add to %s: %v\n", grpcAddr, err)
			continue
		}
	}
	c.JSON(http.StatusOK, gin.H{"status": "OK"})
}

// 服务注册
func registerService(client *clientv3.Client, serviceName, addr string) {
	lease := clientv3.NewLease(client)
	leaseResp, err := lease.Grant(context.Background(), 10)
	if err != nil {
		panic(err)
	}
	// 注册 HTTP 地址
	httpKey := fmt.Sprintf("/radic/%s/http/%s", serviceName, addr)
	_, err = client.Put(context.Background(), httpKey, addr, clientv3.WithLease(leaseResp.ID))
	if err != nil {
		panic(err)
	}
	// 计算并注册 gRPC 地址
	parts := strings.Split(strings.Replace(addr, "http://", "", 1), ":")
	if len(parts) != 2 {
		panic(fmt.Sprintf("Invalid HTTP address: %s", addr))
	}
	httpPort, err := strconv.Atoi(parts[1])
	if err != nil {
		panic(fmt.Sprintf("Invalid port in %s: %v", addr, err))
	}
	grpcPort := httpPort + 1000
	grpcAddr := fmt.Sprintf("%s:%d", parts[0], grpcPort)
	grpcKey := fmt.Sprintf("/radic/%s/grpc/%s", serviceName, grpcAddr)
	_, err = client.Put(context.Background(), grpcKey, grpcAddr, clientv3.WithLease(leaseResp.ID))
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			_, err := lease.KeepAliveOnce(context.Background(), leaseResp.ID)
			if err != nil {
				fmt.Println("Keep alive failed:", err)
				return
			}
			time.Sleep(5 * time.Second)
		}
	}()
}

func getService(client *clientv3.Client, serviceName string) ([]string, error) {
	resp, err := client.Get(context.Background(), "/radic/"+serviceName+"/grpc", clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	var addrs []string
	for _, kv := range resp.Kvs {
		addrs = append(addrs, string(kv.Value))
	}
	return addrs, nil
}

// WebServerMain 单机或分布式 Web 服务器
func WebServerMain(mode int) {
	engine := gin.Default()
	gin.SetMode(gin.ReleaseMode)
	engine.Static("js", "example/views/js")
	engine.Static("css", "example/views/css")
	engine.Static("img", "example/views/img")
	engine.StaticFile("/favicon.ico", "img/dqq.png")
	engine.LoadHTMLFiles("example/views/search.html", "example/views/up_search.html")

	classes := [...]string{"资讯", "社会", "热点", "生活", "知识", "环球", "游戏", "综合", "日常", "影视", "科技", "编程"}
	engine.GET("/", func(c *gin.Context) {
		c.HTML(http.StatusOK, "search.html", classes)
	})

	engine.GET("/up", func(c *gin.Context) {
		c.HTML(http.StatusOK, "up_search.html", classes)
	})

	if mode == 1 {
		worker, err := NewIndexWorker(*dbPath, 16)
		if err != nil {
			panic(err)
		}
		if *rebuildIndex {
			if err := loadCSVToIndex(worker, *csvFile, *totalWorkers, *workerIndex); err != nil {
				panic(fmt.Sprintf("load CSV to index failed: %v", err))
			}
			fmt.Println("Successfully loaded data from CSV and built index")
		} else {
			if err := worker.LoadIndex(); err != nil {
				panic(fmt.Sprintf("load index from DB failed: %v", err))
			}
			fmt.Println("Successfully loaded index from existing database")
		}
		engine.POST("/search", worker.SearchHandler)
		engine.POST("/add", worker.AddDocHandler)
	} else if mode == 3 {
		proxy, err := NewProxy(etcdServers)
		if err != nil {
			panic(err)
		}
		engine.POST("/search", proxy.SearchHandler)
		engine.POST("/add", proxy.AddDocHandler)
	}

	go engine.Run("127.0.0.1:" + strconv.Itoa(*port))
	// addr := "127.0.0.1:" + strconv.Itoa(*port)
	// fmt.Printf("Starting server on %s...\n", addr)
	// if err := engine.Run(addr); err != nil {
	// 	fmt.Printf("Failed to start server: %v\n", err)
	// }
}

// HttpIndexerMain HTTP 索引服务
func HttpIndexerMain() {
	// 动态生成 dbPath
	workerDbPath := fmt.Sprintf("%s_%d", *dbPath, *workerIndex)
	worker, err := NewIndexWorker(workerDbPath, 16)
	if err != nil {
		panic(err)
	}
	if *rebuildIndex {
		if err := loadCSVToIndex(worker, *csvFile, *totalWorkers, *workerIndex); err != nil {
			panic(fmt.Sprintf("load CSV to index failed: %v", err))
		}
		fmt.Println("Successfully loaded data from CSV and built index")
	} else {
		if err := worker.LoadIndex(); err != nil {
			panic(fmt.Sprintf("load index from DB failed: %v", err))
		}
		fmt.Println("Successfully loaded index from existing database")
	}

	engine := gin.Default()
	engine.POST("/add", worker.AddDocHandler)
	engine.POST("/search", worker.SearchHandler)
	grpcPort := *port + 1000 // 例如 6678, 6679
	lis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", grpcPort))
	if err != nil {
		panic(err)
	}
	s := grpc.NewServer()
	pb.RegisterIndexServiceServer(s, &indexServer{worker: worker})
	go func() {
		fmt.Printf("Index worker %d gRPC listening on :%d\n", *workerIndex, grpcPort)
		if err := s.Serve(lis); err != nil {
			panic(err)
		}
	}()
	client, err := clientv3.New(clientv3.Config{Endpoints: etcdServers, DialTimeout: 5 * time.Second})
	if err != nil {
		panic(err)
	}
	addr := fmt.Sprintf("http://127.0.0.1:%d", *port)
	registerService(client, "index", addr)

	fmt.Printf("Index worker %d listening on :%d\n", *workerIndex, *port)
	if err := engine.Run("127.0.0.1:" + strconv.Itoa(*port)); err != nil {
		panic(err)
	}
}

func main() {
	flag.Parse()
	initSentinel()
	switch *mode {
	case 1, 3:
		WebServerMain(*mode)
		select {}
	case 2:
		HttpIndexerMain()
	}
}
