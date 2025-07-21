# LTO Tape Data Index

该项目用于整理 LTO 磁带的备份数据，详细记录索引和内容说明。

整理好的源数据请见 [index.xlsx](https://github.com/ERR0RPR0MPT/Tape/blob/main/index.xlsx)

CSV 表格格式请见 [index.csv](https://github.com/ERR0RPR0MPT/Tape/blob/main/index.csv)

AI 生成的索引整理说明请见 [INDEX.md](https://github.com/ERR0RPR0MPT/Tape/blob/main/INDEX.md)

## 数据存取方案：LTFS + Zip Archive

经过多个方案的尝试，我选择 LTFS + Zip Archive 直接写入数据到磁带 (LTO 5+)。

加密使用 [LTOEnc](https://github.com/ERR0RPR0MPT/Tape/blob/main/LTOEnc) 开启磁带机自带的硬件加密。

压缩使用 [LTFS](https://github.com/LinearTapeFileSystem/ltfs) 在格式化磁带时选择开启的压缩功能。

使用 [ltfs-zip-archiver](https://github.com/ERR0RPR0MPT/Tape/blob/main/ltfs-zip-archiver) 创建 zip 档案，并将数据直接顺序写入磁带。

### 优点
- 使用的软件全部开源且为通用标准
- 直接使用磁带机硬件加密，不损失性能
- 通过自实现的 ltfs-zip-archiver 创建 zip 档案，并直接写入磁带，避免 LTFS 处理小文件速度过慢的问题
- ltfs-zip-archiver 为写入磁带进行优化，存取速度快，效率较高
- 当处理的数据大于单带容量时，可以手动拆分数据，并存储到多个磁带
- 适合个人手动归档数据
- 在开源方案中，传输速度贴近闭源备份方案

### 缺点
- 某些软件的 zip 实现并非使用顺序写入，而是随机写入，可能会出现较多倒带的情况（较影响性能）
- 从硬盘中读取归档数据时，由于文件头存储在磁带尾部，需等待几分钟才能导出数据（需优化）

### TODO

- 7-Zip 使用单线程观察是否倒带，以及速度是否有影响
- 开发严格按照顺序写入的 go-zip 实现
- 调整 LTFS Block Size 观察小文件写入效率是否提升
- 使用更快速的 Go zip 实现

## 尝试过的其他方案

### Backup Exec

#### 优点
- 专业化备份软件，功能一应俱全

#### 缺点
- 跑不起来
- 需要昂贵的许可证

### Iperius Backup

#### 优点
- 专为磁带设计的存储格式，文件头存储在磁带头部，存取速度最快

#### 缺点
- 不开源，没有软件就读不出数据
- 单带无法追加数据，如果备份数据没有存满磁带，剩余空间无法追加存储其他数据，浪费多余的磁带空间
- 多磁带的拆分数据备份非常麻烦，需要一个一个添加目录，不能直接全选目录添加

### LTFS + FastCopy 直接存文件

#### 优点
- 操作方便，只需打开资源管理器就能查看文件

#### 缺点
- 当写入大量小文件时，速度会非常缓慢（~10 files/s）

### LTFS + 7-Zip (7z)

#### 缺点
- 比 zip 慢
