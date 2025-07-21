package main

import (
	"archive/zip"
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/schollz/progressbar/v3"
)

// PauseController 用于控制暂停/继续
type PauseController struct {
	paused int32 // 使用 atomic 操作
	mu     sync.Mutex
	cond   *sync.Cond
}

func NewPauseController() *PauseController {
	pc := &PauseController{}
	pc.cond = sync.NewCond(&pc.mu)
	return pc
}

func (pc *PauseController) Toggle() {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if atomic.LoadInt32(&pc.paused) == 0 {
		atomic.StoreInt32(&pc.paused, 1)
		fmt.Fprintln(os.Stderr, "\n[暂停] 按回车键继续...")
	} else {
		atomic.StoreInt32(&pc.paused, 0)
		fmt.Fprintln(os.Stderr, "[继续] 按回车键暂停...")
		pc.cond.Broadcast()
	}
}

func (pc *PauseController) WaitIfPaused() {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	for atomic.LoadInt32(&pc.paused) == 1 {
		pc.cond.Wait()
	}
}

func (pc *PauseController) IsPaused() bool {
	return atomic.LoadInt32(&pc.paused) == 1
}

// SpeedTracker 用于跟踪传输速度
type SpeedTracker struct {
	mu           sync.Mutex
	totalBytes   int64
	lastBytes    int64
	lastTime     time.Time
	currentSpeed float64 // bytes per second
}

func NewSpeedTracker() *SpeedTracker {
	return &SpeedTracker{
		lastTime: time.Now(),
	}
}

func (st *SpeedTracker) Update(bytes int64) {
	st.mu.Lock()
	defer st.mu.Unlock()

	st.totalBytes += bytes
	now := time.Now()

	// 每500ms更新一次速度计算
	if now.Sub(st.lastTime) >= 500*time.Millisecond {
		elapsed := now.Sub(st.lastTime).Seconds()
		if elapsed > 0 {
			st.currentSpeed = float64(st.totalBytes-st.lastBytes) / elapsed
		}
		st.lastBytes = st.totalBytes
		st.lastTime = now
	}
}

func (st *SpeedTracker) GetSpeed() float64 {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.currentSpeed
}

func (st *SpeedTracker) GetSpeedString() string {
	speed := st.GetSpeed()
	if speed < 1024 {
		return fmt.Sprintf("%.0f B/s", speed)
	} else if speed < 1024*1024 {
		return fmt.Sprintf("%.1f KB/s", speed/1024)
	} else if speed < 1024*1024*1024 {
		return fmt.Sprintf("%.1f MB/s", speed/1024/1024)
	} else {
		return fmt.Sprintf("%.1f GB/s", speed/1024/1024/1024)
	}
}

// ProgressReader 包装 io.Reader 以跟踪读取的字节数
type ProgressReader struct {
	reader          io.Reader
	bar             *progressbar.ProgressBar
	speedTracker    *SpeedTracker
	pauseController *PauseController
}

func NewProgressReader(reader io.Reader, bar *progressbar.ProgressBar, speedTracker *SpeedTracker, pauseController *PauseController) *ProgressReader {
	return &ProgressReader{
		reader:          reader,
		bar:             bar,
		speedTracker:    speedTracker,
		pauseController: pauseController,
	}
}

func (pr *ProgressReader) Read(p []byte) (n int, err error) {
	// 检查是否暂停
	if pr.pauseController != nil {
		pr.pauseController.WaitIfPaused()
	}

	n, err = pr.reader.Read(p)
	if n > 0 {
		if pr.bar != nil {
			pr.bar.Add(n)
		}
		if pr.speedTracker != nil {
			pr.speedTracker.Update(int64(n))
		}
	}
	return n, err
}

// BufferedWriter 提供带缓冲区的写入器
type BufferedWriter struct {
	writer io.Writer
	buffer []byte
	offset int
}

func NewBufferedWriter(writer io.Writer, bufSize int) *BufferedWriter {
	return &BufferedWriter{
		writer: writer,
		buffer: make([]byte, bufSize),
		offset: 0,
	}
}

func (bw *BufferedWriter) Write(p []byte) (n int, err error) {
	n = len(p)
	remaining := len(p)
	srcOffset := 0

	for remaining > 0 {
		available := len(bw.buffer) - bw.offset
		if available == 0 {
			// 缓冲区已满，刷新
			if err = bw.Flush(); err != nil {
				return n - remaining, err
			}
			available = len(bw.buffer)
		}

		copySize := remaining
		if copySize > available {
			copySize = available
		}

		copy(bw.buffer[bw.offset:], p[srcOffset:srcOffset+copySize])
		bw.offset += copySize
		srcOffset += copySize
		remaining -= copySize
	}

	return n, nil
}

func (bw *BufferedWriter) Flush() error {
	if bw.offset == 0 {
		return nil
	}

	_, err := bw.writer.Write(bw.buffer[:bw.offset])
	bw.offset = 0
	return err
}

// readLines 从指定文件中读取所有行，并去除每行首尾的引号和空白
func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		line = strings.Trim(line, "\"") // 去除可能存在的引号
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines, scanner.Err()
}

func main() {
	// 配置日志记录器
	log.SetFlags(log.LstdFlags) // 设置日志格式为 YYYY/MM/DD HH:MM:SS

	// 1. 从 src.txt 和 dst.txt 读取配置
	sources, err := readLines("src.txt")
	if err != nil {
		log.Fatalf("错误: 无法读取源文件列表 src.txt: %v", err)
	}
	if len(sources) == 0 {
		log.Fatalln("错误: src.txt 为空或不存在。")
	}

	destLines, err := readLines("dst.txt")
	if err != nil {
		log.Fatalf("错误: 无法读取目标文件配置 dst.txt: %v", err)
	}
	if len(destLines) == 0 {
		log.Fatalln("错误: dst.txt 为空或不存在。")
	}
	destFile := destLines[0]

	// 确保不会将输出文件打包到自身
	absDest, err := filepath.Abs(destFile)
	if err != nil {
		log.Fatalf("错误: 无法获取目标绝对路径: %v", err)
	}
	for _, source := range sources {
		absSource, err := filepath.Abs(source)
		if err != nil {
			log.Fatalf("错误: 无法获取源 '%s' 的绝对路径: %v", source, err)
		}
		if strings.HasPrefix(absDest, absSource) {
			log.Fatalf("错误: 目标zip文件 '%s' 不能位于源目录 '%s' 中。", destFile, source)
		}
	}

	// --- 阶段 1: 扫描文件以统计总数和大小 ---
	log.Println("阶段 1/2: 正在扫描文件...")
	var totalFiles int64
	var totalSize int64
	for _, source := range sources {
		err := filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				totalFiles++
				totalSize += info.Size()
			}
			return nil
		})
		if err != nil {
			log.Fatalf("错误: 扫描文件 '%s' 时出错: %v", source, err)
		}
	}
	log.Printf("扫描完成。共找到 %d 个文件, 总大小 %.2f MB\n", totalFiles, float64(totalSize)/1024/1024)

	// --- 阶段 2: 执行压缩并显示进度条 ---
	log.Println("阶段 2/2: 开始压缩文件...")
	log.Println("提示: 按回车键可以暂停/继续压缩过程")

	file, err := os.Create(destFile)
	if err != nil {
		log.Fatalf("错误: 无法创建目标文件 %s: %v", destFile, err)
	}
	defer file.Close()

	// 初始化暂停控制器
	pauseController := NewPauseController()

	// 初始化速度跟踪器
	speedTracker := NewSpeedTracker()

	// 初始化进度条
	bar := progressbar.NewOptions64(
		totalSize,
		progressbar.OptionSetDescription(fmt.Sprintf("正在压缩到 %s", filepath.Base(destFile))),
		progressbar.OptionSetWriter(os.Stderr),
		progressbar.OptionShowBytes(true),
		progressbar.OptionSetWidth(15),
		progressbar.OptionThrottle(65*time.Millisecond),
		progressbar.OptionShowCount(),
		progressbar.OptionOnCompletion(func() {
			fmt.Fprint(os.Stderr, "\n")
		}),
		progressbar.OptionSpinnerType(14),
		progressbar.OptionFullWidth(),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "=",
			SaucerHead:    ">",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
		progressbar.OptionSetRenderBlankState(true),
	)

	// 启动协程监听键盘输入
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			pauseController.Toggle()
		}
	}()

	// 启动一个协程定期更新进度条描述以显示速度和暂停状态
	done := make(chan bool)
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		baseName := filepath.Base(destFile)
		for {
			select {
			case <-ticker.C:
				speedStr := speedTracker.GetSpeedString()
				var statusStr string
				if pauseController.IsPaused() {
					statusStr = "[已暂停]"
				} else {
					statusStr = fmt.Sprintf("[%s]", speedStr)
				}
				newDesc := fmt.Sprintf("正在压缩到 %s %s", baseName, statusStr)
				bar.Describe(newDesc)
			case <-done:
				return
			}
		}
	}()

	// 创建带缓冲的文件写入器
	bufferedFile := NewBufferedWriter(file, 10*1024*1024)

	// 创建 Zip Writer
	zipWriter := zip.NewWriter(bufferedFile)
	defer func() {
		zipWriter.Close()
		bufferedFile.Flush()
	}()

	// 遍历所有源，将它们添加到zip中
	for _, source := range sources {
		if err := addFiles(zipWriter, source, bar, speedTracker, pauseController); err != nil {
			done <- true
			log.Fatalf("错误: 压缩 '%s' 过程中发生错误: %v", source, err)
		}
	}

	done <- true

	finalSpeed := speedTracker.GetSpeedString()
	log.Printf("压缩完成。平均速度: %s", finalSpeed)
}

// addFiles 遍历路径并将其中的文件和目录添加到zip.Writer中
func addFiles(w *zip.Writer, basePath string, bar *progressbar.ProgressBar, speedTracker *SpeedTracker, pauseController *PauseController) error {
	info, err := os.Stat(basePath)
	if err != nil {
		return err
	}

	var baseDir string
	if info.IsDir() {
		baseDir = basePath
	} else {
		// 如果 basePath 是一个文件，则其父目录是 baseDir
		baseDir = filepath.Dir(basePath)
	}

	copyBuffer := make([]byte, 1024*1024) // 1MB缓冲区

	return filepath.Walk(basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if pauseController != nil {
			pauseController.WaitIfPaused()
		}

		if bar != nil {
			log.Printf("处理中: %s", path)
		}

		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}

		// 创建正确的相对路径
		relPath, err := filepath.Rel(baseDir, path)
		if err != nil {
			return err
		}
		// 如果源本身是文件，我们希望它在zip的根目录
		if !info.IsDir() && baseDir == filepath.Dir(basePath) && basePath == path {
			relPath = filepath.Base(path)
		}

		header.Name = filepath.ToSlash(relPath)
		header.Method = zip.Store // 不压缩

		if info.IsDir() {
			header.Name += "/"
		}

		writer, err := w.CreateHeader(header)
		if err != nil {
			return err
		}

		if !info.IsDir() {
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			defer file.Close()

			for {
				if pauseController != nil {
					pauseController.WaitIfPaused()
				}

				n, err := file.Read(copyBuffer)
				if n > 0 {
					if _, writeErr := writer.Write(copyBuffer[:n]); writeErr != nil {
						return writeErr
					}

					if bar != nil {
						bar.Add(n)
					}
					if speedTracker != nil {
						speedTracker.Update(int64(n))
					}
				}
				if err != nil {
					if err == io.EOF {
						break
					}
					return err
				}
			}
		}
		return nil
	})
}
