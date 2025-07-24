package main

import (
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	bufferSize     = 32 * 1024 * 1024       // 32MB buffer for reading files
	updateInterval = 500 * time.Millisecond // Update progress every 500ms
)

type ProgressTracker struct {
	totalBytes         int64
	processedBytes     int64
	startTime          time.Time
	lastUpdateTime     time.Time
	lastProcessedBytes int64
	filename           string
	currentSpeed       float64
	avgSpeed           float64
}

func (p *ProgressTracker) reset(totalBytes int64, filename string) {
	p.totalBytes = totalBytes
	p.processedBytes = 0
	p.startTime = time.Now()
	p.lastUpdateTime = time.Now()
	p.lastProcessedBytes = 0
	p.filename = filename
	p.currentSpeed = 0
	p.avgSpeed = 0
}

func (p *ProgressTracker) update(bytesRead int64) {
	p.processedBytes += bytesRead
	now := time.Now()

	// Calculate current speed (bytes per second)
	if now.Sub(p.lastUpdateTime) >= updateInterval {
		bytesInInterval := p.processedBytes - p.lastProcessedBytes
		timeInterval := now.Sub(p.lastUpdateTime).Seconds()

		if timeInterval > 0 {
			p.currentSpeed = float64(bytesInInterval) / timeInterval
		}

		// Calculate average speed since start
		totalElapsed := now.Sub(p.startTime).Seconds()
		if totalElapsed > 0 {
			p.avgSpeed = float64(p.processedBytes) / totalElapsed
		}

		p.lastUpdateTime = now
		p.lastProcessedBytes = p.processedBytes

		p.displayProgress()
	}
}

func (p *ProgressTracker) displayProgress() {
	elapsed := time.Since(p.startTime)
	percentage := float64(p.processedBytes) / float64(p.totalBytes) * 100

	// Clear the line and display progress
	fmt.Print("\r\033[K")

	if p.processedBytes > 0 && p.avgSpeed > 0 {
		estimatedTotal := time.Duration(float64(p.totalBytes) / p.avgSpeed * float64(time.Second))
		remaining := estimatedTotal - elapsed

		if remaining < 0 {
			remaining = 0
		}

		fmt.Printf("进度: %.2f%% | %s/%s | 当前速度: %s/s | 平均速度: %s/s | 已用时: %s | 剩余: %s",
			percentage,
			formatBytes(p.processedBytes),
			formatBytes(p.totalBytes),
			formatBytes(int64(p.currentSpeed)),
			formatBytes(int64(p.avgSpeed)),
			formatDuration(elapsed),
			formatDuration(remaining))
	} else {
		fmt.Printf("进度: %.2f%% | %s/%s | 已用时: %s",
			percentage,
			formatBytes(p.processedBytes),
			formatBytes(p.totalBytes),
			formatDuration(elapsed))
	}
}

func (p *ProgressTracker) finish() {
	fmt.Println() // New line after progress
	elapsed := time.Since(p.startTime)
	finalSpeed := float64(p.totalBytes) / elapsed.Seconds()

	log.Printf("处理完成统计:")
	log.Printf("- 总字节数: %s", formatBytes(p.totalBytes))
	log.Printf("- 总用时: %s", formatDuration(elapsed))
	log.Printf("- 平均速度: %s/s", formatBytes(int64(finalSpeed)))
	log.Printf("- 最高速度: %s/s", formatBytes(int64(p.currentSpeed)))
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func formatDuration(d time.Duration) string {
	if d < 0 {
		return "计算中..."
	}

	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60

	if hours > 0 {
		return fmt.Sprintf("%dh%dm%ds", hours, minutes, seconds)
	} else if minutes > 0 {
		return fmt.Sprintf("%dm%ds", minutes, seconds)
	} else {
		return fmt.Sprintf("%ds", seconds)
	}
}

// generateSHA256 计算文件的SHA256哈希值
func generateSHA256(filename string, tracker *ProgressTracker) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := sha256.New()
	buffer := make([]byte, bufferSize)

	log.Printf("开始计算SHA256哈希值...")
	log.Printf("缓冲区大小: %s", formatBytes(bufferSize))
	log.Println()

	for {
		n, err := file.Read(buffer)
		if n > 0 {
			hash.Write(buffer[:n])
			tracker.update(int64(n))
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
	}

	tracker.finish()
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

// generateSHA256File 为指定文件生成SHA256文件
func generateSHA256File(filename string) error {
	log.Println("=== SHA256 生成模式 ===")

	// 检查文件是否存在
	info, err := os.Stat(filename)
	if err != nil {
		return fmt.Errorf("文件不存在: %s", filename)
	}

	if info.IsDir() {
		return fmt.Errorf("不能处理目录: %s", filename)
	}

	tracker := &ProgressTracker{}
	tracker.reset(info.Size(), filename)

	log.Printf("目标文件: %s", filename)
	log.Printf("文件大小: %s", formatBytes(info.Size()))
	log.Printf("修改时间: %s", info.ModTime().Format("2006-01-02 15:04:05"))
	log.Println()

	hash, err := generateSHA256(filename, tracker)
	if err != nil {
		return fmt.Errorf("无法计算SHA256: %v", err)
	}

	// 保存SHA256到文件
	sha256Filename := filename + ".sha256"
	sha256Content := fmt.Sprintf("%s  %s\n", hash, filepath.Base(filename))

	err = os.WriteFile(sha256Filename, []byte(sha256Content), 0644)
	if err != nil {
		return fmt.Errorf("无法写入SHA256文件 %s: %v", sha256Filename, err)
	}

	log.Println()
	log.Printf("=== 生成完成 ===")
	log.Printf("✓ SHA256值: %s", hash)
	log.Printf("✓ 输出文件: %s", sha256Filename)

	return nil
}

// verifySHA256File 验证文件的SHA256
func verifySHA256File(filename string) error {
	log.Println("=== SHA256 验证模式 ===")

	sha256Filename := filename + ".sha256"

	// 检查原文件是否存在
	info, err := os.Stat(filename)
	if err != nil {
		return fmt.Errorf("文件不存在: %s", filename)
	}

	if info.IsDir() {
		return fmt.Errorf("不能处理目录: %s", filename)
	}

	// 检查SHA256文件是否存在
	sha256Info, err := os.Stat(sha256Filename)
	if err != nil {
		return fmt.Errorf("SHA256文件不存在: %s", sha256Filename)
	}

	// 读取SHA256文件
	sha256Content, err := os.ReadFile(sha256Filename)
	if err != nil {
		return fmt.Errorf("无法读取SHA256文件 %s: %v", sha256Filename, err)
	}

	// 解析SHA256内容
	line := strings.TrimSpace(string(sha256Content))
	parts := strings.Fields(line)
	if len(parts) < 1 {
		return fmt.Errorf("SHA256文件 %s 格式错误", sha256Filename)
	}

	expectedHash := parts[0]

	log.Printf("目标文件: %s", filename)
	log.Printf("文件大小: %s", formatBytes(info.Size()))
	log.Printf("修改时间: %s", info.ModTime().Format("2006-01-02 15:04:05"))
	log.Printf("校验文件: %s", sha256Filename)
	log.Printf("校验文件创建时间: %s", sha256Info.ModTime().Format("2006-01-02 15:04:05"))
	log.Printf("期望SHA256: %s", expectedHash)
	log.Println()

	tracker := &ProgressTracker{}
	tracker.reset(info.Size(), filename)

	actualHash, err := generateSHA256(filename, tracker)
	if err != nil {
		return fmt.Errorf("无法计算SHA256: %v", err)
	}

	log.Println()
	log.Printf("=== 验证结果 ===")
	log.Printf("期望SHA256: %s", expectedHash)
	log.Printf("实际SHA256: %s", actualHash)

	if actualHash == expectedHash {
		log.Printf("✓ 文件完整性验证通过!")
		log.Printf("✓ 文件 %s 未被篡改", filename)
		return nil
	} else {
		return fmt.Errorf("✗ 文件完整性验证失败! 文件可能已被篡改或损坏")
	}
}

func printUsage() {
	log.Println("SHA256 文件校验工具 v2.0")
	log.Println("支持实时进度显示和详细速度统计")
	log.Println()
	log.Println("用法:")
	log.Println("  生成模式: go run sha256_tool.go generate <file>")
	log.Println("  验证模式: go run sha256_tool.go verify <file>")
	log.Println()
	log.Println("简写模式:")
	log.Println("  生成: go run sha256_tool.go gen <file>")
	log.Println("  生成: go run sha256_tool.go g <file>")
	log.Println("  验证: go run sha256_tool.go check <file>")
	log.Println("  验证: go run sha256_tool.go v <file>")
	log.Println()
	log.Println("说明:")
	log.Println("  generate - 为指定文件生成 .sha256 校验文件")
	log.Println("  verify   - 验证文件与对应的 .sha256 校验文件")
	log.Println()
	log.Println("功能特性:")
	log.Println("  • 实时进度显示")
	log.Println("  • 当前速度和平均速度统计")
	log.Println("  • 剩余时间估算")
	log.Println("  • 详细的文件信息显示")
	log.Println("  • 大文件优化处理 (10MB缓冲)")
	log.Println()
	log.Println("示例:")
	log.Println("  go run sha256_tool.go generate large_file.zip")
	log.Println("  go run sha256_tool.go verify large_file.zip")
}

func main() {
	if len(os.Args) != 3 {
		printUsage()
		os.Exit(1)
	}

	mode := os.Args[1]
	filename := os.Args[2]

	// 记录开始时间
	startTime := time.Now()

	var err error
	switch mode {
	case "generate", "gen", "g":
		err = generateSHA256File(filename)
	case "verify", "check", "v":
		err = verifySHA256File(filename)
	default:
		log.Printf("错误: 未知模式 '%s'\n\n", mode)
		printUsage()
		os.Exit(1)
	}

	if err != nil {
		log.Printf("错误: %v\n", err)
		os.Exit(1)
	}

	// 显示总体完成信息
	totalTime := time.Since(startTime)
	log.Printf("程序总用时: %s", formatDuration(totalTime))
}
