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
	bufferSize = 10 * 1024 * 1024 // 10MB buffer for reading files
)

type ProgressTracker struct {
	totalBytes     int64
	processedBytes int64
	startTime      time.Time
	filename       string
}

func (p *ProgressTracker) reset(totalBytes int64, filename string) {
	p.totalBytes = totalBytes
	p.processedBytes = 0
	p.startTime = time.Now()
	p.filename = filename
}

func (p *ProgressTracker) update(bytesRead int64) {
	p.processedBytes += bytesRead

	elapsed := time.Since(p.startTime)
	percentage := float64(p.processedBytes) / float64(p.totalBytes) * 100

	if p.processedBytes > 0 {
		estimatedTotal := time.Duration(float64(elapsed) / float64(p.processedBytes) * float64(p.totalBytes))
		remaining := estimatedTotal - elapsed

		fmt.Printf("\r进度: %.2f%% (%s/%s) | 文件: %s | 剩余时间: %s",
			percentage,
			formatBytes(p.processedBytes),
			formatBytes(p.totalBytes),
			filepath.Base(p.filename),
			formatDuration(remaining))
	}
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

	log.Printf("处理文件: %s (大小: %s)\n", filename, formatBytes(info.Size()))
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

	log.Printf("\n\n=== 生成完成 ===\n")
	log.Printf("✓ 生成: %s\n", sha256Filename)
	log.Printf("用时: %s\n", formatDuration(time.Since(tracker.startTime)))

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

	tracker := &ProgressTracker{}
	tracker.reset(info.Size(), filename)

	log.Printf("验证文件: %s (大小: %s)\n", filename, formatBytes(info.Size()))
	log.Printf("期望SHA256: %s\n", expectedHash)
	log.Println()

	actualHash, err := generateSHA256(filename, tracker)
	if err != nil {
		return fmt.Errorf("无法计算SHA256: %v", err)
	}

	log.Printf("\n\n=== 验证完成 ===\n")
	log.Printf("期望: %s\n", expectedHash)
	log.Printf("实际: %s\n", actualHash)
	log.Printf("用时: %s\n", formatDuration(time.Since(tracker.startTime)))

	if actualHash == expectedHash {
		log.Printf("✓ 验证通过: %s\n", filename)
		return nil
	} else {
		return fmt.Errorf("✗ 验证失败: %s", filename)
	}
}

func printUsage() {
	log.Println("SHA256 文件校验工具")
	log.Println()
	log.Println("用法:")
	log.Println("  生成模式: go run sha256_tool.go generate <file>")
	log.Println("  验证模式: go run sha256_tool.go verify <file>")
	log.Println()
	log.Println("说明:")
	log.Println("  generate - 为指定文件生成 .sha256 校验文件")
	log.Println("  verify   - 验证文件与对应的 .sha256 校验文件")
	log.Println()
	log.Println("示例:")
	log.Println("  go run sha256_tool.go generate file1.txt")
	log.Println("  go run sha256_tool.go verify file1.txt")
}

func main() {
	if len(os.Args) != 3 {
		printUsage()
		os.Exit(1)
	}

	mode := os.Args[1]
	filename := os.Args[2]

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
}
