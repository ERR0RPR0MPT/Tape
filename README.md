# LTO Tape Data Index

该项目用于整理 LTO 磁带的备份数据，详细记录索引和内容说明。

整理好的源数据请见 [index.xlsx](https://github.com/ERR0RPR0MPT/Tape/blob/main/index.xlsx)

## 数据存取方案：LTFS + LTFS ZIP Archiver

经过多个方案的尝试，我选择 LTFS + LTFS ZIP Archiver 直接写入数据到磁带 (LTO 5+)。

加密使用 [LTOEnc](https://github.com/ERR0RPR0MPT/Tape/blob/main/LTOEnc) 开启磁带机自带的硬件加密。

压缩使用 [LTFS](https://github.com/LinearTapeFileSystem/ltfs) 在格式化磁带时选择开启的压缩功能。

使用自实现的 [LTFS ZIP Archiver](https://github.com/ERR0RPR0MPT/ltfs-zip-archiver) 创建 zip 档案。

LTFS ZIP Archiver 实现了按顺序写入 zip 档案压缩数据，适用于写入磁带，防止倒带降低速度。

经测试，此方案即使在处理大量小文件时，也能保持满速写入（140+ MB/s, LTO 5）

### 优点

- 使用的软件全部开源且为通用标准
- 直接使用磁带机硬件加密，不损失性能
- 通过 LTFS ZIP Archiver 直接写入磁带，避免 LTFS 处理小文件速度过慢的问题
- LTFS ZIP Archiver 为顺序写入磁带进行优化，存取速度快，效率较高
- LTFS ZIP Archiver 写入时自动计算 SHA256 hash 并写入到 .sha256 文件，并且不损失写入速度
- 无需预先在硬盘中打包生成 hash，方便后续的数据校验
- 当处理的数据大于单带容量时，可以手动拆分数据，并存储到多个磁带
- 适合个人手动归档数据
- 在开源方案中，传输速度可达到 LTO 标准上限

### 读取时的注意事项

- 从硬盘中读取 zip 归档数据时，由于文件头存储在磁带尾部，
  需等待磁带先倒带到最后，再倒带到恢复数据的最开始，才能开始读取数据
  （这个几乎不影响读取性能，专业软件也需要读文件头，只不过它们把文件头放到了磁带头部；
  tar 格式需要读取完整个磁带的数据才能显示文件列表，使用 zip 归档在等量的数据下比 tar 要快得多）

## 尝试过的其他方案

### Backup Exec

#### 优点

- 专业化备份软件，功能一应俱全

#### 缺点

- 跑不起来，用起来麻烦
- 需要昂贵的许可证

### Iperius Backup

#### 优点

- 专为磁带设计的存储格式，文件头存储在磁带头部，存取速度较快

#### 缺点

- 不开源，没有软件就读不出数据
- 单带无法追加数据，如果备份数据没有存满磁带，剩余空间无法追加存储其他数据，浪费多余的磁带空间
- 多磁带的拆分数据备份非常麻烦，需要一个一个添加目录，不能直接全选目录添加

### LTFS + FastCopy 直接存文件

#### 优点

- 操作方便，只需打开资源管理器就能查看文件

#### 缺点

- 当写入大量小文件时，速度会非常缓慢（~10 files/s）

### LTFS + 7-Zip (zip)

#### 缺点

- `7-Zip` zip 实现包含随机读写，写入磁带会发生倒带，降低写入效率。

### LTFS + 7-Zip (7z)

#### 缺点

- 比 `LTFS + 7-Zip (zip)` 更慢
