package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hirochachacha/go-smb2"
)

type Config struct {
	Routines   int      `json:"routines"`
	SrcPath    []string `json:"src_path"`
	Host       string   `json:"host"`
	Port       int      `json:"port"`
	Username   string   `json:"username"`
	Password   string   `json:"password"`
	Share      string   `json:"share"`
	DestPath   string   `json:"dest_path"`
	BufferSize int      `json:"buffer_size"`
	RetryTimes int      `json:"retry_times"`
	PoolSize   int      `json:"pool_size"`
	Verbose    bool     `json:"verbose"` // 详细日志
}

type FileTask struct {
	SourcePath string
	RelPath    string
	Size       int64
	BaseDir    string // 新增：源路径的基础目录名
}

type Stats struct {
	TotalFiles     int64
	TotalBytes     int64
	ProcessedFiles int64
	ProcessedBytes int64
	FailedFiles    int64
	StartTime      time.Time
}

type SMBConnection struct {
	session *smb2.Session
	share   *smb2.Share
	id      int
}

type SMBPool struct {
	connChan  chan *SMBConnection
	config    *Config
	closeOnce sync.Once
	wg        sync.WaitGroup
	mu        sync.Mutex
	closed    bool
	connCount int32 // 跟踪当前连接数
}

// 全局目录创建锁，避免并发创建同一目录
type DirCreator struct {
	mu      sync.Mutex
	created map[string]bool
}

func NewDirCreator() *DirCreator {
	return &DirCreator{
		created: make(map[string]bool),
	}
}

func (dc *DirCreator) EnsureDir(share *smb2.Share, path string) error {
	if path == "" || path == "." {
		return nil
	}

	dc.mu.Lock()
	// 检查是否已创建
	if dc.created[path] {
		dc.mu.Unlock()
		return nil
	}
	dc.mu.Unlock()

	// 递归确保父目录存在
	parentDir := ""
	if idx := strings.LastIndex(path, "/"); idx != -1 {
		parentDir = path[:idx]
	}

	if parentDir != "" {
		if err := dc.EnsureDir(share, parentDir); err != nil {
			return err
		}
	}

	// 尝试创建目录
	dc.mu.Lock()
	defer dc.mu.Unlock()

	// 再次检查（可能在等待锁期间已被创建）
	if dc.created[path] {
		return nil
	}

	err := share.Mkdir(path, 0755)
	if err == nil {
		dc.created[path] = true
		return nil
	}

	// 检查是否已存在
	if os.IsExist(err) ||
		strings.Contains(err.Error(), "file exists") ||
		strings.Contains(err.Error(), "Cannot create a file when that file already exists") {
		dc.created[path] = true
		return nil
	}

	return err
}

// isTCPConnectionError 判断是否为 TCP 网络连接错误（需要无限重试）
func isTCPConnectionError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := strings.ToLower(err.Error())

	tcpIndicators := []string{
		"read tcp",
		"write tcp",
		"connection refused",
		"connection reset",
		"connection abort",
		"software caused connection abort",
		"broken pipe",
		"i/o timeout",
		"network is unreachable",
		"no route to host",
		"connection timed out",
		"transport endpoint is not connected",
		"use of closed network connection",
	}

	for _, indicator := range tcpIndicators {
		if strings.Contains(errMsg, indicator) {
			return true
		}
	}

	if strings.Contains(errMsg, "eof") {
		if strings.Contains(errMsg, "connection") {
			return true
		}
		if errMsg == "eof" || strings.HasSuffix(errMsg, "eof") {
			return true
		}
	}

	return false
}

// isSMBSessionError 判断是否为 SMB 会话层错误
func isSMBSessionError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := strings.ToLower(err.Error())

	if isTCPConnectionError(err) {
		return false
	}

	smbSessionIndicators := []string{
		"user session deleted",
		"network name deleted",
		"logon failure",
		"authentication failed",
		"access denied",
		"invalid session",
		"session expired",
		"status_network_session_expired",
		"status_user_session_deleted",
	}

	for _, indicator := range smbSessionIndicators {
		if strings.Contains(errMsg, indicator) {
			return true
		}
	}

	return false
}

// isFileSystemError 判断是否为文件系统操作错误
func isFileSystemError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := strings.ToLower(err.Error())

	if isTCPConnectionError(err) || isSMBSessionError(err) {
		return false
	}

	fsErrorIndicators := []string{
		"not a directory",
		"is not a directory",
		"a requested opened file is not a directory",
		"file exists",
		"cannot create a file when that file already exists",
		"permission denied",
		"no such file or directory",
		"directory not empty",
		"invalid argument",
		"file name too long",
		"disk quota exceeded",
		"no space left",
		"read-only file system",
		"status_not_a_directory",
		"status_object_name_collision",
		"status_object_name_invalid",
		"status_object_path_invalid",
	}

	for _, indicator := range fsErrorIndicators {
		if strings.Contains(errMsg, indicator) {
			return true
		}
	}

	return false
}

func NewSMBPool(config *Config, size int) (*SMBPool, error) {
	if size > 16 {
		log.Printf("Warning: Pool size %d may exceed SMB connection limit, reducing to 16", size)
		size = 16
	}

	pool := &SMBPool{
		connChan:  make(chan *SMBConnection, size),
		config:    config,
		closed:    false,
		connCount: 0,
	}

	// 预创建所有连接
	for i := 0; i < size; i++ {
		share, session, err := pool.createConnection(i)
		if err != nil {
			pool.Close()
			return nil, fmt.Errorf("failed to create connection %d: %v", i, err)
		}

		conn := &SMBConnection{
			session: session,
			share:   share,
			id:      i,
		}
		pool.connChan <- conn
		atomic.AddInt32(&pool.connCount, 1)
	}

	log.Printf("SMB connection pool initialized with %d connections", size)
	return pool, nil
}

func (p *SMBPool) createConnection(id int) (*smb2.Share, *smb2.Session, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", p.config.Host, p.config.Port))
	if err != nil {
		return nil, nil, err
	}

	d := &smb2.Dialer{
		Initiator: &smb2.NTLMInitiator{
			User:     p.config.Username,
			Password: p.config.Password,
		},
	}

	session, err := d.Dial(conn)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}

	share, err := session.Mount(p.config.Share)
	if err != nil {
		session.Logoff()
		return nil, nil, err
	}

	return share, session, nil
}

// GetOrCreate 获取连接，如果超时则尝试创建新连接
func (p *SMBPool) GetOrCreate(timeout time.Duration) (*SMBConnection, error) {
	select {
	case conn := <-p.connChan:
		return conn, nil
	case <-time.After(timeout):
		// 超时后尝试创建临时连接
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, fmt.Errorf("pool is closed")
		}

		currentCount := atomic.LoadInt32(&p.connCount)
		log.Printf("[Pool] Timeout waiting for connection, current pool has %d connections, attempting to create new one", currentCount)
		p.mu.Unlock()

		// 尝试创建新连接
		share, session, err := p.createConnection(-1) // -1 表示临时连接
		if err != nil {
			return nil, fmt.Errorf("failed to create emergency connection: %v", err)
		}

		newConn := &SMBConnection{
			session: session,
			share:   share,
			id:      -1, // 临时连接ID
		}

		log.Printf("[Pool] Successfully created emergency connection")
		return newConn, nil
	}
}

func (p *SMBPool) Get(timeout time.Duration) (*SMBConnection, error) {
	select {
	case conn := <-p.connChan:
		return conn, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("timeout waiting for SMB connection")
	}
}

// RecreateConnection 重建损坏的连接，确保成功后放回池子
func (p *SMBPool) RecreateConnection(oldConn *SMBConnection) (*SMBConnection, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, fmt.Errorf("pool is closed")
	}

	// 关闭旧连接
	if oldConn != nil {
		p.closeConnection(oldConn)
		if oldConn.id >= 0 {
			atomic.AddInt32(&p.connCount, -1)
		}
	}

	// 创建新连接，使用重试机制
	id := 0
	if oldConn != nil && oldConn.id >= 0 {
		id = oldConn.id
	}

	var share *smb2.Share
	var session *smb2.Session
	var err error

	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		share, session, err = p.createConnection(id)
		if err == nil {
			break
		}
		log.Printf("[Pool] Failed to recreate connection (attempt %d/%d): %v", i+1, maxRetries, err)
		if i < maxRetries-1 {
			time.Sleep(time.Second * time.Duration(i+1))
		}
	}

	if err != nil {
		return nil, fmt.Errorf("failed to recreate connection after %d attempts: %v", maxRetries, err)
	}

	newConn := &SMBConnection{
		session: session,
		share:   share,
		id:      id,
	}

	atomic.AddInt32(&p.connCount, 1)
	log.Printf("[Pool] Successfully recreated connection %d, current pool count: %d", id, atomic.LoadInt32(&p.connCount))

	return newConn, nil
}

func (p *SMBPool) Put(conn *SMBConnection) {
	if conn == nil {
		return
	}

	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()

	if closed {
		p.closeConnection(conn)
		return
	}

	select {
	case p.connChan <- conn:
		// 成功放回池子
	default:
		// 池子满了，关闭连接
		log.Printf("Warning: connection pool full, closing connection %d", conn.id)
		p.closeConnection(conn)
		if conn.id >= 0 {
			atomic.AddInt32(&p.connCount, -1)
		}
	}
}

func (p *SMBPool) closeConnection(conn *SMBConnection) {
	if conn.share != nil {
		conn.share.Umount()
	}
	if conn.session != nil {
		conn.session.Logoff()
	}
}

func (p *SMBPool) Close() {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		p.mu.Unlock()

		close(p.connChan)
		count := 0
		for conn := range p.connChan {
			p.closeConnection(conn)
			count++
		}
		log.Printf("Closed %d SMB connections", count)
	})
}

// normalizeSMBPath 标准化 SMB 路径
func normalizeSMBPath(path string) string {
	path = strings.Trim(path, "/\\")
	path = strings.ReplaceAll(path, "\\", "/")
	return path
}

// joinSMBPath 拼接 SMB 路径
func joinSMBPath(parts ...string) string {
	var cleaned []string
	for _, part := range parts {
		normalized := normalizeSMBPath(part)
		if normalized != "" {
			cleaned = append(cleaned, normalized)
		}
	}
	result := strings.Join(cleaned, "/")
	return result
}

func loadConfig(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	config := &Config{
		Port:       445,
		BufferSize: 1024 * 1024 * 2, // 2MB 默认缓冲
		RetryTimes: 3,
		PoolSize:   12,
		Verbose:    false,
	}

	err = json.Unmarshal(data, config)
	if err != nil {
		return nil, err
	}

	// 标准化目标路径
	config.DestPath = normalizeSMBPath(config.DestPath)

	// 连接池大小限制
	if config.PoolSize > 16 {
		log.Printf("Warning: pool_size %d too large, limiting to 16 for Windows compatibility", config.PoolSize)
		config.PoolSize = 16
	}
	if config.PoolSize < 1 {
		config.PoolSize = 1
	}

	return config, nil
}

func scanFiles(paths []string, taskChan chan<- FileTask, stats *Stats) {
	defer close(taskChan)

	for _, rootPath := range paths {
		baseDir := filepath.Base(rootPath)
		log.Printf("Scanning %s -> base directory: %s", rootPath, baseDir)

		err := filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				log.Printf("Error accessing path %s: %v", path, err)
				return nil
			}

			if !info.IsDir() {
				relPath, err := filepath.Rel(rootPath, path)
				if err != nil {
					log.Printf("Error getting relative path: %v", err)
					return nil
				}

				task := FileTask{
					SourcePath: path,
					RelPath:    filepath.ToSlash(relPath),
					Size:       info.Size(),
					BaseDir:    baseDir,
				}

				atomic.AddInt64(&stats.TotalFiles, 1)
				atomic.AddInt64(&stats.TotalBytes, info.Size())

				taskChan <- task
			}
			return nil
		})

		if err != nil {
			log.Printf("Error walking path %s: %v", rootPath, err)
		}
	}

	log.Printf("Scan complete: %d files, %.2f GB",
		atomic.LoadInt64(&stats.TotalFiles),
		float64(atomic.LoadInt64(&stats.TotalBytes))/1024/1024/1024)
}

func uploadFile(pool *SMBPool, task FileTask, config *Config, stats *Stats, dirCreator *DirCreator) error {
	var conn *SMBConnection
	var lastErr error
	fileRetryCount := 0
	tcpRetryCount := 0
	consecutiveTCPErrors := 0

	for {
		// 如果没有连接，尝试获取或创建
		if conn == nil {
			timeout := 10 * time.Second
			var err error

			// 使用 GetOrCreate，超时后会尝试创建新连接
			conn, err = pool.GetOrCreate(timeout)

			if err != nil {
				tcpRetryCount++
				log.Printf("[Connection Error] Failed to get/create connection (attempt %d): %v", tcpRetryCount, err)

				// 指数退避，但最多等待30秒
				waitTime := time.Second * time.Duration(min(tcpRetryCount, 30))
				log.Printf("[Connection Error] Waiting %v before retry...", waitTime)
				time.Sleep(waitTime)
				continue // 无限重试获取连接
			}

			// 成功获取连接，重置计数
			if tcpRetryCount > 0 {
				log.Printf("[Connection Restored] Successfully obtained connection after %d attempts", tcpRetryCount)
				tcpRetryCount = 0
			}
		}

		// 执行上传
		err := uploadFileOnce(conn.share, task, config, dirCreator)

		if err == nil {
			// 上传成功
			pool.Put(conn)
			atomic.AddInt64(&stats.ProcessedFiles, 1)
			atomic.AddInt64(&stats.ProcessedBytes, task.Size)
			if config.Verbose {
				log.Printf("[OK] %s", task.SourcePath)
			}
			return nil
		}

		// 判断错误类型
		if isTCPConnectionError(err) {
			// TCP 连接错误 - 无限重试
			consecutiveTCPErrors++
			tcpRetryCount++

			log.Printf("[TCP Connection Error] %s (TCP retry #%d): %v - Recreating connection...",
				task.SourcePath, consecutiveTCPErrors, err)

			// 尝试重建连接
			newConn, recreateErr := pool.RecreateConnection(conn)
			if recreateErr != nil {
				log.Printf("[TCP Connection Error] Failed to recreate connection: %v - Will get new connection", recreateErr)
				conn = nil
				waitTime := time.Duration(min(consecutiveTCPErrors, 10)) * time.Second
				time.Sleep(waitTime)
			} else {
				log.Printf("[TCP Connection Restored] Connection recreated successfully")
				// 立即放回池子，让其他等待的worker也能使用
				pool.Put(newConn)
				conn = nil // 下次循环重新获取
				time.Sleep(time.Millisecond * 500)
			}
			continue

		} else if isSMBSessionError(err) {
			// SMB 会话错误
			consecutiveTCPErrors = 0
			fileRetryCount++
			lastErr = err

			if fileRetryCount <= config.RetryTimes {
				log.Printf("[SMB Session Error - Retry %d/%d] %s: %v - Recreating connection...",
					fileRetryCount, config.RetryTimes, task.SourcePath, err)

				newConn, recreateErr := pool.RecreateConnection(conn)
				if recreateErr != nil {
					log.Printf("[SMB Session Error] Failed to recreate connection: %v", recreateErr)
					conn = nil
					time.Sleep(time.Millisecond * time.Duration(200*fileRetryCount))
				} else {
					log.Printf("[SMB Session Restored] Connection recreated")
					// 放回池子
					pool.Put(newConn)
					conn = nil
					time.Sleep(time.Millisecond * time.Duration(200*fileRetryCount))
				}
				continue
			} else {
				if conn != nil {
					pool.Put(conn)
				}
				atomic.AddInt64(&stats.FailedFiles, 1)
				log.Printf("[FAILED] %s - SMB Session Error: %v (after %d retries)",
					task.SourcePath, lastErr, config.RetryTimes)
				return lastErr
			}

		} else if isFileSystemError(err) {
			// 文件系统错误
			consecutiveTCPErrors = 0
			fileRetryCount++
			lastErr = err

			if fileRetryCount <= config.RetryTimes {
				log.Printf("[File System Error - Retry %d/%d] %s: %v",
					fileRetryCount, config.RetryTimes, task.SourcePath, err)

				pool.Put(conn)
				conn = nil
				time.Sleep(time.Millisecond * time.Duration(100*fileRetryCount))
				continue
			} else {
				if conn != nil {
					pool.Put(conn)
				}
				atomic.AddInt64(&stats.FailedFiles, 1)
				log.Printf("[FAILED] %s - File System Error: %v (after %d retries)",
					task.SourcePath, lastErr, config.RetryTimes)
				return lastErr
			}

		} else {
			// 未分类错误
			consecutiveTCPErrors = 0
			fileRetryCount++
			lastErr = err

			if fileRetryCount <= config.RetryTimes {
				log.Printf("[Unknown Error - Retry %d/%d] %s: %v",
					fileRetryCount, config.RetryTimes, task.SourcePath, err)

				pool.Put(conn)
				conn = nil
				time.Sleep(time.Millisecond * time.Duration(100*fileRetryCount))
				continue
			} else {
				if conn != nil {
					pool.Put(conn)
				}
				atomic.AddInt64(&stats.FailedFiles, 1)
				log.Printf("[FAILED] %s - Unknown Error: %v (after %d retries)",
					task.SourcePath, lastErr, config.RetryTimes)
				return lastErr
			}
		}
	}
}

func uploadFileOnce(share *smb2.Share, task FileTask, config *Config, dirCreator *DirCreator) error {
	srcFile, err := os.Open(task.SourcePath)
	if err != nil {
		return fmt.Errorf("open source: %v", err)
	}
	defer srcFile.Close()

	destPath := joinSMBPath(config.DestPath, task.BaseDir, task.RelPath)

	if config.Verbose {
		log.Printf("[PATH] src=%s, base=%s, rel=%s, dst=%s",
			task.SourcePath, task.BaseDir, task.RelPath, destPath)
	}

	destDir := ""
	if idx := strings.LastIndex(destPath, "/"); idx != -1 {
		destDir = destPath[:idx]
	}

	if destDir != "" {
		if config.Verbose {
			log.Printf("[MKDIR] %s", destDir)
		}
		err = dirCreator.EnsureDir(share, destDir)
		if err != nil {
			return fmt.Errorf("create dir '%s': %v", destDir, err)
		}
	}

	if config.Verbose {
		log.Printf("[CREATE] %s", destPath)
	}
	dstFile, err := share.Create(destPath)
	if err != nil {
		return fmt.Errorf("create dest '%s': %v", destPath, err)
	}
	defer dstFile.Close()

	buffer := make([]byte, config.BufferSize)
	written, err := io.CopyBuffer(dstFile, srcFile, buffer)
	if err != nil {
		return fmt.Errorf("copy data: %v", err)
	}

	if written != task.Size {
		return fmt.Errorf("size mismatch: expected %d, wrote %d", task.Size, written)
	}

	return nil
}

func worker(id int, pool *SMBPool, taskChan <-chan FileTask, config *Config, stats *Stats, dirCreator *DirCreator, wg *sync.WaitGroup) {
	defer wg.Done()

	for task := range taskChan {
		err := uploadFile(pool, task, config, stats, dirCreator)
		if err != nil {
			// 错误已在 uploadFile 中记录
			_ = err
		}
	}
}

func statsReporter(stats *Stats, done <-chan struct{}) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	lastBytes := int64(0)
	lastTime := stats.StartTime

	for {
		select {
		case <-ticker.C:
			elapsed := time.Since(stats.StartTime).Seconds()
			processed := atomic.LoadInt64(&stats.ProcessedFiles)
			total := atomic.LoadInt64(&stats.TotalFiles)
			processedBytes := atomic.LoadInt64(&stats.ProcessedBytes)
			failed := atomic.LoadInt64(&stats.FailedFiles)

			now := time.Now()
			intervalSeconds := now.Sub(lastTime).Seconds()
			intervalBytes := processedBytes - lastBytes
			instantSpeed := float64(intervalBytes) / intervalSeconds / 1024 / 1024

			avgSpeed := float64(processedBytes) / elapsed / 1024 / 1024
			progress := float64(processed) / float64(total) * 100

			if total > 0 && processed > 0 {
				eta := time.Duration(float64(elapsed)/float64(processed)*float64(total-processed)) * time.Second
				log.Printf("Progress: %d/%d (%.1f%%) | Speed: %.2f MB/s (avg: %.2f MB/s) | Failed: %d | ETA: %v",
					processed, total, progress, instantSpeed, avgSpeed, failed, eta.Round(time.Second))
			}

			lastBytes = processedBytes
			lastTime = now

		case <-done:
			return
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func main() {
	var configPath string

	if len(os.Args) < 2 {
		exePath, err := os.Executable()
		if err != nil {
			log.Fatalf("Failed to get executable path: %v", err)
		}
		exeDir := filepath.Dir(exePath)
		defaultConfig := filepath.Join(exeDir, "config.json")

		if _, err := os.Stat(defaultConfig); err == nil {
			configPath = defaultConfig
			log.Printf("Using default config file: %s", configPath)
		} else {
			log.Println("Error: No config file specified and config.json not found in program directory")
			log.Fatal("Usage: smb-backup [config.json]")
		}
	} else {
		configPath = os.Args[1]
	}

	config, err := loadConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Configuration loaded:")
	log.Printf("  Routines: %d", config.Routines)
	log.Printf("  Pool Size: %d", config.PoolSize)
	log.Printf("  Retry Times: %d (for SMB session and file system errors)", config.RetryTimes)
	log.Printf("  Buffer Size: %d bytes", config.BufferSize)
	log.Printf("  Verbose: %v", config.Verbose)
	log.Printf("  Source paths: %v", config.SrcPath)
	log.Printf("  Destination: //%s/%s/%s", config.Host, config.Share, config.DestPath)
	log.Printf("")
	log.Printf("Error Handling Strategy:")
	log.Printf("  - TCP Connection Errors: Retry indefinitely with exponential backoff")
	log.Printf("  - SMB Session Errors: Retry up to %d times with connection recreation", config.RetryTimes)
	log.Printf("  - File System Errors: Retry up to %d times", config.RetryTimes)
	log.Printf("  - Connection Pool Timeout: Automatically create emergency connections")
	log.Printf("")

	stats := &Stats{
		StartTime: time.Now(),
	}

	pool, err := NewSMBPool(config, config.PoolSize)
	if err != nil {
		log.Fatalf("Failed to create SMB pool: %v", err)
	}
	defer pool.Close()

	dirCreator := NewDirCreator()

	log.Println("Testing destination path...")
	testConn, err := pool.Get(10 * time.Second)
	if err != nil {
		log.Fatalf("Failed to get test connection: %v", err)
	}

	testPath := joinSMBPath(config.DestPath, ".test")
	log.Printf("  Test path: %s", testPath)

	err = dirCreator.EnsureDir(testConn.share, config.DestPath)
	if err != nil {
		log.Printf("  Warning: Failed to create base dir: %v", err)
	} else {
		log.Printf("  Base directory OK")
	}

	testFile, err := testConn.share.Create(testPath)
	if err != nil {
		log.Printf("  Warning: Failed to create test file: %v", err)
	} else {
		testFile.Write([]byte("test"))
		testFile.Close()
		testConn.share.Remove(testPath)
		log.Printf("  Test file creation OK")
	}

	pool.Put(testConn)

	taskChan := make(chan FileTask, config.Routines*2)

	doneChan := make(chan struct{})
	go statsReporter(stats, doneChan)

	var wg sync.WaitGroup
	for i := 0; i < config.Routines; i++ {
		wg.Add(1)
		go worker(i, pool, taskChan, config, stats, dirCreator, &wg)
	}

	log.Println("Scanning files...")
	scanFiles(config.SrcPath, taskChan, stats)

	wg.Wait()
	close(doneChan)

	elapsed := time.Since(stats.StartTime)
	log.Println("========================================")
	log.Printf("Backup completed in %v", elapsed.Round(time.Second))
	log.Printf("Total files: %d", stats.TotalFiles)
	log.Printf("Processed files: %d", stats.ProcessedFiles)
	log.Printf("Failed files: %d", stats.FailedFiles)
	log.Printf("Total size: %.2f GB", float64(stats.TotalBytes)/1024/1024/1024)
	log.Printf("Average speed: %.2f MB/s", float64(stats.ProcessedBytes)/elapsed.Seconds()/1024/1024)
	log.Println("========================================")

	if stats.FailedFiles > 0 {
		os.Exit(1)
	}
}
