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
// 这类错误表示底层网络连接出现问题，需要重新建立连接并无限重试
func isTCPConnectionError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := strings.ToLower(err.Error())

	// TCP 层面的连接错误特征
	tcpIndicators := []string{
		"read tcp",                            // TCP 读取错误
		"write tcp",                           // TCP 写入错误
		"connection refused",                  // 连接被拒绝
		"connection reset",                    // 连接被重置
		"connection abort",                    // 连接中止
		"software caused connection abort",    // 软件导致连接中止
		"broken pipe",                         // 管道破裂
		"i/o timeout",                         // I/O 超时
		"network is unreachable",              // 网络不可达
		"no route to host",                    // 无路由到主机
		"connection timed out",                // 连接超时
		"transport endpoint is not connected", // 传输端点未连接
		"use of closed network connection",    // 使用已关闭的网络连接
	}

	for _, indicator := range tcpIndicators {
		if strings.Contains(errMsg, indicator) {
			return true
		}
	}

	// 特殊处理 EOF：如果是纯 EOF 或包含 connection 相关的 EOF，认为是连接错误
	if strings.Contains(errMsg, "eof") {
		// 排除 "unexpected eof" 在文件传输中的情况（可能是文件损坏）
		// 但如果同时包含 connection 相关字样，仍然认为是连接错误
		if strings.Contains(errMsg, "connection") {
			return true
		}
		// 纯 EOF 也可能是连接断开
		if errMsg == "eof" || strings.HasSuffix(errMsg, "eof") {
			return true
		}
	}

	return false
}

// isSMBSessionError 判断是否为 SMB 会话层错误（需要重新连接但按 retry_times 重试）
// 这类错误表示 SMB 会话失效，需要重新认证，但可能是暂时的或配置问题
func isSMBSessionError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := strings.ToLower(err.Error())

	// SMB 会话相关错误，但排除 TCP 层面的错误
	// 必须先确认不是 TCP 连接错误
	if isTCPConnectionError(err) {
		return false
	}

	// SMB 会话特定的错误
	smbSessionIndicators := []string{
		"user session deleted",           // 用户会话被删除
		"network name deleted",           // 网络名称被删除
		"logon failure",                  // 登录失败
		"authentication failed",          // 认证失败
		"access denied",                  // 访问被拒绝（可能是会话过期）
		"invalid session",                // 无效会话
		"session expired",                // 会话过期
		"status_network_session_expired", // SMB 状态码
		"status_user_session_deleted",    // SMB 状态码
	}

	for _, indicator := range smbSessionIndicators {
		if strings.Contains(errMsg, indicator) {
			return true
		}
	}

	return false
}

// isFileSystemError 判断是否为文件系统操作错误（按 retry_times 重试）
// 这类错误表示文件系统层面的问题，可能是路径、权限、文件状态等问题
func isFileSystemError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := strings.ToLower(err.Error())

	// 确保不是 TCP 或 SMB 会话错误
	if isTCPConnectionError(err) || isSMBSessionError(err) {
		return false
	}

	// 文件系统特定的错误
	fsErrorIndicators := []string{
		"not a directory",                                    // 不是目录
		"is not a directory",                                 // 不是目录
		"a requested opened file is not a directory",         // Windows SMB 错误
		"file exists",                                        // 文件已存在
		"cannot create a file when that file already exists", // 文件已存在
		"permission denied",                                  // 权限被拒绝
		"no such file or directory",                          // 文件或目录不存在
		"directory not empty",                                // 目录非空
		"invalid argument",                                   // 无效参数
		"file name too long",                                 // 文件名太长
		"disk quota exceeded",                                // 磁盘配额超出
		"no space left",                                      // 磁盘空间不足
		"read-only file system",                              // 只读文件系统
		"status_not_a_directory",                             // SMB 状态码
		"status_object_name_collision",                       // SMB 对象名冲突
		"status_object_name_invalid",                         // SMB 对象名无效
		"status_object_path_invalid",                         // SMB 路径无效
	}

	for _, indicator := range fsErrorIndicators {
		if strings.Contains(errMsg, indicator) {
			return true
		}
	}

	return false
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
		closed:   false,
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

// RecreateConnection 重建损坏的连接
func (p *SMBPool) RecreateConnection(oldConn *SMBConnection) (*SMBConnection, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, fmt.Errorf("pool is closed")
	}

	// 关闭旧连接
	if oldConn != nil {
		p.closeConnection(oldConn)
	}

	// 创建新连接
	id := 0
	if oldConn != nil {
		id = oldConn.id
	}

	share, session, err := p.createConnection(id)
	if err != nil {
		return nil, fmt.Errorf("failed to recreate connection: %v", err)
	}

	newConn := &SMBConnection{
		session: session,
		share:   share,
		id:      id,
	}

	return newConn, nil
}

func (p *SMBPool) Put(conn *SMBConnection) {
	if conn != nil {
		p.mu.Lock()
		closed := p.closed
		p.mu.Unlock()

		if closed {
			p.closeConnection(conn)
			return
		}

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
	var conn *SMBConnection
	var lastErr error
	fileRetryCount := 0       // 用于文件传输和 SMB 会话错误的重试计数（受 retry_times 限制）
	tcpRetryCount := 0        // 用于 TCP 连接错误的重试计数（无限重试，仅用于延迟策略）
	consecutiveTCPErrors := 0 // 连续 TCP 错误计数

	for {
		// 如果没有连接，尝试获取
		if conn == nil {
			timeout := 10 * time.Second
			var err error
			conn, err = pool.Get(timeout)

			if err != nil {
				tcpRetryCount++
				log.Printf("[Connection Pool Error] Failed to get connection (attempt %d): %v", tcpRetryCount, err)
				time.Sleep(time.Second * time.Duration(min(tcpRetryCount, 30)))
				continue // 无限重试获取连接
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

		// 判断错误类型（按优先级判断）
		if isTCPConnectionError(err) {
			// 类型 1: TCP 连接错误 - 无限重试
			consecutiveTCPErrors++
			tcpRetryCount++

			log.Printf("[TCP Connection Error] %s (TCP retry #%d): %v - Will retry indefinitely...",
				task.SourcePath, consecutiveTCPErrors, err)

			// 尝试重建连接
			newConn, recreateErr := pool.RecreateConnection(conn)
			if recreateErr != nil {
				log.Printf("[TCP Connection Error] Failed to recreate connection: %v - Waiting before retry...", recreateErr)
				conn = nil
				// 等待后继续重试，使用指数退避
				waitTime := time.Duration(min(consecutiveTCPErrors, 30)) * time.Second
				time.Sleep(waitTime)
			} else {
				log.Printf("[TCP Connection Restored] Connection recreated successfully, retrying upload...")
				conn = newConn
				// 短暂等待后重试
				time.Sleep(time.Millisecond * 500)
			}
			// 继续循环重试，不增加 fileRetryCount
			continue

		} else if isSMBSessionError(err) {
			// 类型 2: SMB 会话错误 - 需要重新连接，但按 retry_times 计数
			consecutiveTCPErrors = 0 // 重置 TCP 错误计数
			fileRetryCount++
			lastErr = err

			if fileRetryCount <= config.RetryTimes {
				log.Printf("[SMB Session Error - Retry %d/%d] %s: %v - Recreating connection...",
					fileRetryCount, config.RetryTimes, task.SourcePath, err)

				// 尝试重建连接
				newConn, recreateErr := pool.RecreateConnection(conn)
				if recreateErr != nil {
					log.Printf("[SMB Session Error] Failed to recreate connection: %v", recreateErr)
					conn = nil
					time.Sleep(time.Millisecond * time.Duration(200*fileRetryCount))
				} else {
					log.Printf("[SMB Session Restored] Connection recreated, retrying upload...")
					conn = newConn
					time.Sleep(time.Millisecond * time.Duration(200*fileRetryCount))
				}
				continue
			} else {
				// 超过重试次数
				if conn != nil {
					pool.Put(conn)
				}
				atomic.AddInt64(&stats.FailedFiles, 1)
				log.Printf("[FAILED] %s - SMB Session Error: %v (after %d retries)",
					task.SourcePath, lastErr, config.RetryTimes)
				return lastErr
			}

		} else if isFileSystemError(err) {
			// 类型 3: 文件系统错误 - 按 retry_times 重试
			consecutiveTCPErrors = 0 // 重置 TCP 错误计数
			fileRetryCount++
			lastErr = err

			if fileRetryCount <= config.RetryTimes {
				log.Printf("[File System Error - Retry %d/%d] %s: %v",
					fileRetryCount, config.RetryTimes, task.SourcePath, err)

				// 归还当前连接（连接可能仍然可用）
				pool.Put(conn)
				conn = nil

				// 等待后重试
				time.Sleep(time.Millisecond * time.Duration(100*fileRetryCount))
				continue
			} else {
				// 超过重试次数，标记失败
				if conn != nil {
					pool.Put(conn)
				}
				atomic.AddInt64(&stats.FailedFiles, 1)
				log.Printf("[FAILED] %s - File System Error: %v (after %d retries)",
					task.SourcePath, lastErr, config.RetryTimes)
				return lastErr
			}

		} else {
			// 类型 4: 未分类的错误 - 按文件传输错误处理（按 retry_times 重试）
			consecutiveTCPErrors = 0 // 重置 TCP 错误计数
			fileRetryCount++
			lastErr = err

			if fileRetryCount <= config.RetryTimes {
				log.Printf("[Unknown Error - Retry %d/%d] %s: %v",
					fileRetryCount, config.RetryTimes, task.SourcePath, err)

				// 归还当前连接
				pool.Put(conn)
				conn = nil

				// 等待后重试
				time.Sleep(time.Millisecond * time.Duration(100*fileRetryCount))
				continue
			} else {
				// 超过重试次数，标记失败
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
	log.Printf("")

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
