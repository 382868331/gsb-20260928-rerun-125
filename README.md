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

归档局部损坏后不必整体重压。`RepairTask` 把修复限定在请求的块序号范围内，`PlanRepair` 在写入任何内容之前先给出分类计划：

- `reuse`：请求范围内仍然完好的块，逐字节复用原帧，不重新编码。
- `regenerate`：已损坏且原始输入覆盖到的块，用输入重新编码。
- `missing_input`：已损坏但输入未覆盖的块，暂时无法修复，记为缺口。
- `input_conflict`：输入 CRC 与索引记录不符，属于另一输入版本，拒绝拼接。

`Repair` / `RepairFile` 只写修复候选（`目标.repair` 与 `目标.repair.json` 清单），原文件保持不动。每确认一帧就保存一次清单；取消或中断后 `ResumeRepairFile` 校验输入指纹与已确认帧，从第一个未确认的块继续。输入版本在确认前缀之后发生变化的，以 `ErrIncompatibleState` 拒绝，不会把不同版本的内容拼进同一候选。

多个修复任务可以覆盖相邻或重叠的范围。`MergeRepairBundles` 先逐帧核对候选内容再合并清单：重叠块只采用一份经过核对的结果，同一块 CRC 不一致（即来自不同输入版本）时以 `ErrRepairConflict` 拒绝合并。

`CommitRepairFile` 是唯一会替换原文件的操作，且仅当每个块都有一帧经过核对的帧时才成功：它把请求范围内的修复帧与范围外的原始帧组装成完整归档，整体解码校验后原子替换原文件，并返回 `RepairProof`——其中 `OutsideRange` 逐字节列出从原文件原样拷贝的区间，证明请求范围之外没有被改写。只要清单仍带缺口或候选间存在冲突，提交就返回 `ErrRepairIncomplete` / `ErrRepairConflict`，原文件与候选都保持不动，此时只能交付带缺口的候选文件。
