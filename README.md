# 数据压缩运行平台基础

核心包提供按固定大小切分的数据块、可插拔编码策略、独立数据帧、尾部索引和逐块随机读取。

## 使用

```go
var out bytes.Buffer
err := datacompression.Compress(ctx, &out, input, datacompression.Options{
    BlockSize:   64 * 1024,
    Concurrency: 4,
})

reader, err := datacompression.NewReaderFromBytes(out.Bytes(), extraCodecs)
block, err := reader.ReadBlock(seq)
```

文件场景使用 `CompressFile` 和 `OpenReader`。压缩输出先写入同目录临时文件，完整索引和页脚落盘并同步后才重命名到目标路径；取消或写入失败会移除临时文件。

## 扩展编码器

实现 `Codec` 并通过 `Options.Codecs` 传入：

- `ID()` 返回稳定且唯一的 `uint16`，已发布数据不能复用或修改该编号。
- `Encode` 可以尝试新编码；当输出大于原块时，压缩器自动改回 `store`。
- 读取旧归档时把新编码器传给 `NewReader`、`NewReaderFromBytes` 或 `OpenReader`；旧编码仍按索引中的编号解码。
- 归档包含当前读取器不认识的编号时，索引仍可读取，访问对应块才返回 `ErrUnknownCodec`。

## 归档布局

```text
header
data frame N...
index header + index entries + index CRC32
footer
```

每个数据帧都包含编码编号、原始长度、压缩长度和原始数据 CRC32，后接该块独立的编码载荷。读取某一块只需通过索引定位对应帧，不依赖其他数据块。页脚记录索引位置、长度、块数和索引 CRC，索引本身记录每块实际采用的编码方式。

## 并行与一致性

压缩 worker 数量由 `Options.Concurrency` 控制；从读取、编码到等待按序发布的整个在途窗口都以并发度为上限。结果可乱序完成，但收集器严格按原始块序号发布，块发布后才允许读取后续输入。上下文取消、读取失败、编码失败或写入失败都会停止管线，不写索引和页脚；文件输出也不会重命名，并会移除未发布完成的临时文件。
