# PaddleOCR 返回结构参考

本文件用于补充真实 OCR 返回形态，不替代官方 API 文档。

这 3 份样例覆盖了以下几类页面：

- 大表格页
- 图文混排页
- 标题、正文、印章、页码混排页

## 参考案例

- `ocr-api-return-reference-1.json`
  - 以大表格页为主
  - 能看到 `table` 块、`number` 块、`markdown.text` 中的 HTML table 输出
  - 适合验证表格页不要被拍平成纯文本
- `ocr-api-return-reference-2.json`
  - 以多页图文混排为主
  - 能看到 `image`、`figure_title`、`paragraph_title`、`text` 混合输出
  - 适合验证图片资产、图题和正文的共同保留
- `ocr-api-return-reference-3.json`
  - 以标题、正文、印章、页码混排为主
  - 能看到 `doc_title`、`paragraph_title`、`image`、`seal`、`number`
  - 适合验证印章、标题层级和页码不要被误删

## 统一返回单元

在本仓库的提取链路里，OCR 返回应按“页级结果单元”消费。一个页面结果通常同时包含：

- `prunedResult`
- `markdown`
- `outputImages`
- `inputImage`

不要把 `markdown.text` 当成唯一真相源。它只是页面视图的一种输出，真正的结构信息还在 `prunedResult` 里。

## 关键字段

### `prunedResult.model_settings`

记录本次 OCR 的模型开关和布局策略，例如：

- `use_layout_detection`
- `use_seal_recognition`
- `merge_layout_blocks`
- `format_block_content`
- `return_layout_polygon_points`

这部分适合落入 manifest 的执行上下文，便于回放和排障。

### `prunedResult.parsing_res_list`

这是内容主结构，通常是最终切片和派生资产的依据。每个元素至少关注：

- `block_label`
- `block_content`
- `block_bbox`
- `block_polygon_points`
- `block_id`
- `group_id`
- `block_order`

常见 `block_label`：

- `text`
- `paragraph_title`
- `doc_title`
- `figure_title`
- `table`
- `image`
- `seal`
- `number`

### `prunedResult.layout_det_res.boxes`

这是布局检测结果，偏元数据层，适合做：

- 页面几何回放
- 图像块定位
- 内容区域调试
- 误检排查

如果页面上出现多个相邻块，优先参考 `block_order` 和块之间的语义关系，不要只按 bbox 面积排序。

### `markdown.text`

这是最接近可直接派生的页面文本视图，但它不是纯文本：

- 表格可能保留为 HTML table
- 图片块可能保留为 `<img>` 标签
- 标题块可能保留为 Markdown heading
- 图文混排时，正文和图片会一起出现在同一段页面 Markdown 里

### `markdown.images`

这是 Markdown 派生资产映射表。

- key 是相对路径，例如 `imgs/img_in_image_box_...jpg`
- value 是预签名 URL

如果页面里有可回放图片，必须把这部分资产一起保存到 manifest 或派生目录。

### `outputImages`

这是调试和回放图。

常见键：

- `layout_det_res`

它用于查看布局检测效果，不应被当成最终正文内容。

## 处理建议

- 先按页保存完整原始结果，再从中派生 chunk
- 表格页优先保留表格结构，不要先拍平成一段普通正文
- 图片页优先保留图片块和图片资产，不要只抽文字
- 印章、页码、页眉页脚要按规则过滤或降权，但不要在最初入库阶段直接删除原始信息
- 生成 chunk 时要保留 `page_no`、`block_id`、`block_label`、`bbox`、`polygon_points`
- 当 `markdown.text`、`parsing_res_list`、`layout_det_res` 出现不一致时，优先保留结构更完整的那个页面结果，并在 manifest 里记录差异

## 这 3 份样例的共性

- 都包含 `model_settings`
- 都包含 `parsing_res_list`
- 都包含 `layout_det_res`
- 大多数页面都同时输出 `markdown.text`
- 图像类页面会额外输出 `markdown.images`
- 带标题的页面常见 `doc_title` 或 `paragraph_title`
- 带页码的页面常见 `number`

## 设计落点

在提取链路中，推荐把这类返回映射成以下内部对象：

- 页面级 OCR 结果
- 块级结构结果
- 页面级 Markdown 派生物
- 页面级图片资产
- 页面级布局调试图

这样可以同时满足：

- chunk 切分
- manifest 回放
- 图片重建
- 版面调试
- 后续多模态检索
