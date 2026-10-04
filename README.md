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

## 断点续传

`CompressWithCheckpoint` 在每个块确认发布后回调一次 `Checkpoint`；`CompressFileResumable` 把进度落盘到 `目标.partial` 与 `目标.checkpoint`，中断时两者都保留作为现场证据。`Resume` / `ResumeFile` 校验检查点（格式版本、块大小、编码器集合的指纹）并重读已确认输入前缀逐块核对 CRC，输入内容或编码设置变化会在写入任何输出前以 `ErrIncompatibleState` 拒绝，原输出与检查点保持不动。确认后的续传只追加新帧，不重发已发布前缀；恢复时可以使用不同的 `Concurrency`，最终内容与一次完整编码解码一致。

## 部分输出读取

`NewPartialReader` 不需要页脚：从头部顺序扫描数据帧，逐帧解码并校验 CRC 后才计入已确认块。`Entries` 只包含完整可用的块，`ConfirmedOffset` 给出已确认的原始字节数，`TailError` 说明尾部为何不可用（`ErrIncomplete` 表示截断，`ErrBadChecksum`/`ErrBadArchive` 表示损坏）；截断或损坏的尾部永远不会被当成完成。存在合法索引和页脚且与逐帧扫描结果一致时 `Complete` 为真。

## 范围修复

归档局部损坏后不必整体重压：`PlanRepair` 把请求范围内的每个块分类为可直接复用（归档中校验通过）、必须重新生成（归档已损坏但源输入覆盖）和暂时缺少原始输入（损坏且输入未覆盖）；输入与已校验帧内容不一致的块会标记为冲突。`RepairFile` 把修复候选写入 `路径.repair`，进度落盘到 `路径.repairstate`，原始文件始终不被修改。

- 复用帧从损坏归档逐字节拷贝，损坏块由源输入重新编码；候选在每个块确认后保存状态，取消或重开时从已确认前缀继续。
- 多个修复任务可用 `MergeRepairPlans` 合并相邻或重叠范围，重叠块只保留一份经过核对的结果；同一重叠块若由不同输入版本生成则返回 `ErrRepairConflict`，绝不拼进同一文件。续修时输入版本变化会丢弃旧的再生成块并用新输入重做，无法重做的块同样以 `ErrRepairConflict` 拒绝。
- 只有 `RepairReport.ReplaceOriginal` 为真（候选完整解压且请求范围之外的帧与原文件逐字节一致）时才可正式替换原文件；存在缺口时只能交付带缺口的候选，原文件与状态文件都保留，待输入补齐后重开即可续修完成。
