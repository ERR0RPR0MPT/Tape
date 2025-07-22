# LTFS ZIP Archiver

使用自实现的 [LTFS ZIP Archiver](https://github.com/ERR0RPR0MPT/Tape/blob/main/ltfs-zip-archiver) 创建 zip 档案。

LTFS ZIP Archiver 实现了按顺序写入 zip 档案压缩数据，适用于写入磁带，防止倒带降低速度。

经测试，此方案即使在处理大量小文件时，也能保持满速写入（140+ MB/s, LTO 5）

## 特性

- 通过 LTFS ZIP Archiver 直接写入磁带，避免 LTFS 处理小文件速度过慢的问题
- LTFS ZIP Archiver 为顺序写入磁带进行优化，存取速度快，效率较高
- 当处理的数据大于单带容量时，可以手动拆分数据，并存储到多个磁带
- 适合个人手动归档数据
- 在开源方案中，传输速度可达到 LTO 标准上限

## 使用

在 `ltfs-zip-archiver.exe` 的同目录下创建 `src.txt` 和 `dst.txt`。

在 Windows 资源管理器中选择你需要备份的文件/目录（可选择多项）。

右键，选择“复制文件地址”，并粘贴到 `src.txt`。

在 `dst.txt` 第一行输入生成 zip 文件的路径。

使用终端打开 `ltfs-zip-archiver.exe` 即可，此时程序会读取数据并按照顺序写入磁带，
终端会显示传输进度。
