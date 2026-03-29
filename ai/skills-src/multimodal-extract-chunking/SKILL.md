---
name: multimodal-extract-chunking
description: Use when designing or changing OCR, layout analysis, Step Functions extraction flow, per-format extraction, chunk types, or multimodal slicing rules.
---

# 多模态提取与切片

当任务影响 `Step Functions Standard` 提取链路、`Extract Worker` 行为、chunk 边界、OCR 使用方式、页面渲染策略，或者影响视觉型文档召回质量时，使用这个 skill。

需要看规则细节时，再读：

- [chunking-rules.md](./references/chunking-rules.md)
- [ocr-api-return-shape-reference.md](./references/ocr-api-return-shape-reference.md)
- [ocr-api-return-reference-1.json](./references/ocr-api-return-reference-1.json)
- [ocr-api-return-reference-2.json](./references/ocr-api-return-reference-2.json)
- [ocr-api-return-reference-3.json](./references/ocr-api-return-reference-3.json)
- [paddleocr-async-api-example.md](./references/paddleocr-async-api-example.md)
- [paddleocr-sync-api-example.md](./references/paddleocr-sync-api-example.md)

## 当前默认链路

```text
S3 Event Notification
-> SQS ingest queue
-> Ingest Lambda
-> Step Functions Standard
   -> submit async OCR job
   -> Wait
   -> query
   -> fetch resultUrl/jsonUrl
   -> persist manifest
```

不要再把旧的 `Lambda Durable Function` 写成当前默认提取链路。

## 核心立场

本项目不把“提取”理解成“把所有内容都转成纯文本”。

提取目标是一组可检索资产：

- 文本资产
- 图片资产
- 版面资产
- 结构化元数据
- 邻接关系

## 必守规则

- `PaddleOCR` 通过 API 使用，不以本地 OCR 运行时为前提
- `PaddleOCR` 当前公开异步接口默认采用轮询，不采用 callback token 或 webhook
- `PaddleOCR` 的 `resultUrl/jsonUrl` 是结果下载域，不是提交作业主站；结果域可能与 `JOB_URL` 不同，部署白名单要按实际返回值单独配置
- 异步结果应先下载 `jsonUrl`，再按页级结果单元消费内容；当返回项同时包含 `prunedResult`、`markdown`、`outputImages` 时，把这三者视为同一页的不同视图，不要只保留 `markdown.text`
- 异步结果应优先从每个页面结果项的 `markdown.text` 合成拆分后的 `.md` 派生文件；不要假定 PaddleOCR 直接提供 `.md` 下载链接
- `prunedResult.parsing_res_list` 是内容主结构，`layout_det_res.boxes` 是布局检测元数据，二者都要保留到 manifest 或派生资产里，不能只留纯文本
- `block_label` 需要按语义区别处理，至少要识别 `text`、`paragraph_title`、`doc_title`、`figure_title`、`table`、`image`、`seal`、`number`
- `table` 块可能直接携带 HTML 表格内容，`image` 和 `figure_title` 块可能在 `markdown.text` 中只呈现为 `<img>` 或居中的标题，不要把它们一律降级成纯文本
- `markdown.images` 里的相对路径是 Markdown 派生资产路径，必须和对应的预签名 URL 一起落盘，方便回放和重建
- `number` 块通常只是页码或序号，应保留为布局线索，但默认不要并入正文 chunk
- `seal` 是第一类结构对象，遇到印章、签章、盖章语义时不能直接丢弃
- `PPT/PPTX` 默认必须同时产出 `slide_text_chunk` 和 `slide_image_chunk`
- `PDF` 优先单页
- `window_pdf_chunk` 只用于强连续内容
- 不要预生成所有页窗口组合
- 提取阶段负责产出 manifest、chunk 和派生资产

## 设计要求

- 先识别源格式
- 先判断哪些信息必须在提取后保留
- 对视觉型文档并行产出 text chunk 和 image chunk
- 切片以规则引擎为主，不为切片额外引入第二个 LLM
- `Step Functions` 只负责编排，不要把 chunk 规则写死在状态机定义里

