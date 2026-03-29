# 切片规则参考

只有在需要精确 chunk 枚举或按格式默认策略时再读本文件。

## Chunk 类型

```text
page_text_chunk
page_image_chunk
section_text_chunk
window_pdf_chunk
slide_text_chunk
slide_image_chunk
image_text_chunk
image_chunk
table_text_chunk
```

## 按格式默认规则

### PDF

- 有原生文本层时优先提取原生文本。
- `PaddleOCR` 用于补充或兜底。
- 每页都保留页面级元数据。
- 视觉页保留 `page_image_chunk`。

### PPT / PPTX

- 必须生成 `slide_text_chunk`。
- 必须生成 `slide_image_chunk`。
- `slide_text_chunk` 至少包含标题、bullet、notes、OCR 文本。

### Word / DOCX

- 主要按标题和 section 切片。
- 表格文本单独抽取。
- 只有当版面本身影响召回时，才补页面图像 chunk。

### 图片

- 生成 `image_chunk`。
- 有 OCR 文本时生成 `image_text_chunk`。

### TXT / Markdown / HTML

- 按标题和段落切片。
- 保留 section 路径。

## 何时生成 window_pdf_chunk

仅在满足以下至少一项时生成：

- 下一页没有新标题且内容明显延续
- 跨页表格
- 跨页图示
- OCR 文本很少但视觉结构连续
- 同一主题横跨多页才构成完整语义单元

## 反模式

- 所有格式一律转纯文本
- 不看结构只按字符数切片
- 丢掉 slide 图片
- 预生成所有 `1-6` 页窗口组合
