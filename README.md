# compresslab

数据压缩运行平台基础：按块选择压缩策略、可随机读取的压缩流、可扩展的编码器注册表。

## 结构

- codec.go — Codec 接口与全局注册表；内置 store（原样保留）、flate、rle。
- format.go — 流格式：头部、块数据、索引、尾部（含索引 CRC 校验）。
- compress.go — 并行压缩管线：按块试所有候选编码器选最小，压缩后更大则保留原块；发布严格按输入顺序，在途块数量有界（慢块产生背压，不会无限堆积）；CompressFile 先写临时文件、成功后原子改名，取消或出错不留看似完整的成品。
- reader.go — Reader 随机访问读取，只解码覆盖请求区间的块并逐块校验 CRC；DecodeBlock 是独立的逐块解码入口，供校验工具使用。

## 流格式

    header: magic(8) | blockSize u32
    blocks: 各块编码后的字节依次拼接
    index:  每块 codecID u8 | rawLen u32 | compLen u32 | crc32 u32
    footer: indexOffset u64 | blockCount u32 | indexCRC u32 | magic(8)

索引记录每块实际采用的编码方式；尾部固定长度，读取端从文件尾定位索引，并校验索引 CRC、块布局与流大小的一致性。

## 增加新编码器

实现 Codec 接口并在 init 中 Register，使用从未发布过的稳定 ID。旧数据按索引里的 ID 查找解码器，新增编码器不影响已有数据的读取。

## 测试

    go test -race ./...

覆盖：独立解码过程逐块对照原文、随机读段、并行保序、取消不留成品、索引损坏与中途截断的报错、新增编码器后旧数据仍可读、未知编码 ID 明确报错。
