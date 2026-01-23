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

func NewSMBPool(config *Config, size int) (*SMBPool, error) {
	// 限制最大连接数
	if size > 16 {
		log.Printf("Warning: Pool size %d may exceed SMB connection limit, reducing to 16", size)
		size = 16
	}

	pool := &SMBPool{
		connChan: make(chan *SMBConnection, size),
		config:   config,
	}

	// 预创建所有连接
	for i := 0; i < size; i++ {
		share, session, err := pool.createConnection(i)
		if err != nil {
			// 清理已创建的连接
			pool.Close()
			return nil, fmt.Errorf("failed to create connection %d: %v", i, err)
		}

		conn := &SMBConnection{
			session: session,
			share:   share,
			id:      i,
		}
		pool.connChan <- conn
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

func (p *SMBPool) Get(timeout time.Duration) (*SMBConnection, error) {
	select {
	case conn := <-p.connChan:
		return conn, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("timeout waiting for SMB connection")
	}
}

func (p *SMBPool) Put(conn *SMBConnection) {
	if conn != nil {
		select {
		case p.connChan <- conn:
		default:
			log.Printf("Warning: connection pool full, closing connection %d", conn.id)
			p.closeConnection(conn)
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
		// 获取源路径的基础目录名（最后一层目录）
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
					BaseDir:    baseDir, // 保存基础目录名
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
	var lastErr error

	for retry := 0; retry <= config.RetryTimes; retry++ {
		if retry > 0 {
			log.Printf("[Retry %d/%d] %s", retry, config.RetryTimes, task.SourcePath)
			// 重试前等待，避免立即重试
			time.Sleep(time.Millisecond * time.Duration(100*retry))
		}

		// 获取连接，设置超时时间
		timeout := 5 * time.Second
		if retry > 0 {
			timeout = 10 * time.Second
		}

		conn, err := pool.Get(timeout)
		if err != nil {
			lastErr = fmt.Errorf("failed to get SMB connection: %v", err)
			continue
		}

		// 执行上传
		err = uploadFileOnce(conn.share, task, config, dirCreator)

		// 立即归还连接
		pool.Put(conn)

		if err == nil {
			atomic.AddInt64(&stats.ProcessedFiles, 1)
			atomic.AddInt64(&stats.ProcessedBytes, task.Size)
			if config.Verbose {
				log.Printf("[OK] %s", task.SourcePath)
			}
			return nil
		}

		lastErr = err
	}

	atomic.AddInt64(&stats.FailedFiles, 1)
	log.Printf("[FAILED] %s - %v", task.SourcePath, lastErr)
	return lastErr
}

func uploadFileOnce(share *smb2.Share, task FileTask, config *Config, dirCreator *DirCreator) error {
	// 打开源文件
	srcFile, err := os.Open(task.SourcePath)
	if err != nil {
		return fmt.Errorf("open source: %v", err)
	}
	defer srcFile.Close()

	// 构建目标路径：dest_path/BaseDir/RelPath
	destPath := joinSMBPath(config.DestPath, task.BaseDir, task.RelPath)

	if config.Verbose {
		log.Printf("[PATH] src=%s, base=%s, rel=%s, dst=%s",
			task.SourcePath, task.BaseDir, task.RelPath, destPath)
	}

	// 获取目录路径
	destDir := ""
	if idx := strings.LastIndex(destPath, "/"); idx != -1 {
		destDir = destPath[:idx]
	}

	// 确保目录存在（使用全局锁避免并发冲突）
	if destDir != "" {
		if config.Verbose {
			log.Printf("[MKDIR] %s", destDir)
		}
		err = dirCreator.EnsureDir(share, destDir)
		if err != nil {
			return fmt.Errorf("create dir '%s': %v", destDir, err)
		}
	}

	// 创建目标文件
	if config.Verbose {
		log.Printf("[CREATE] %s", destPath)
	}
	dstFile, err := share.Create(destPath)
	if err != nil {
		return fmt.Errorf("create dest '%s': %v", destPath, err)
	}
	defer dstFile.Close()

	// 使用缓冲传输
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

			// 计算瞬时速度
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

func main() {
	// 确定配置文件路径
	var configPath string

	if len(os.Args) < 2 {
		// 获取程序所在目录
		exePath, err := os.Executable()
		if err != nil {
			log.Fatalf("Failed to get executable path: %v", err)
		}
		exeDir := filepath.Dir(exePath)
		defaultConfig := filepath.Join(exeDir, "config.json")

		// 检查默认配置文件是否存在
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

	// 加载配置
	config, err := loadConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Configuration loaded:")
	log.Printf("  Routines: %d", config.Routines)
	log.Printf("  Pool Size: %d", config.PoolSize)
	log.Printf("  Retry Times: %d", config.RetryTimes)
	log.Printf("  Buffer Size: %d bytes", config.BufferSize)
	log.Printf("  Verbose: %v", config.Verbose)
	log.Printf("  Source paths: %v", config.SrcPath)
	log.Printf("  Destination: //%s/%s/%s", config.Host, config.Share, config.DestPath)

	// 初始化统计
	stats := &Stats{
		StartTime: time.Now(),
	}

	// 创建 SMB 连接池
	pool, err := NewSMBPool(config, config.PoolSize)
	if err != nil {
		log.Fatalf("Failed to create SMB pool: %v", err)
	}
	defer pool.Close()

	// 创建目录管理器
	dirCreator := NewDirCreator()

	// 测试目标路径
	log.Println("Testing destination path...")
	testConn, err := pool.Get(10 * time.Second)
	if err != nil {
		log.Fatalf("Failed to get test connection: %v", err)
	}

	testPath := joinSMBPath(config.DestPath, ".test")
	log.Printf("  Test path: %s", testPath)

	// 创建测试目录
	err = dirCreator.EnsureDir(testConn.share, config.DestPath)
	if err != nil {
		log.Printf("  Warning: Failed to create base dir: %v", err)
	} else {
		log.Printf("  Base directory OK")
	}

	// 创建测试文件
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

	// 创建任务通道（缓冲大小根据情况调整）
	taskChan := make(chan FileTask, config.Routines*2)

	// 启动统计报告
	doneChan := make(chan struct{})
	go statsReporter(stats, doneChan)

	// 启动工作协程
	var wg sync.WaitGroup
	for i := 0; i < config.Routines; i++ {
		wg.Add(1)
		go worker(i, pool, taskChan, config, stats, dirCreator, &wg)
	}

	// 扫描文件
	log.Println("Scanning files...")
	scanFiles(config.SrcPath, taskChan, stats)

	// 等待所有任务完成
	wg.Wait()
	close(doneChan)

	// 最终统计
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
