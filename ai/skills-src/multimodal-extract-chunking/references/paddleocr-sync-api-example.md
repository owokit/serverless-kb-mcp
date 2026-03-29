# PaddleOCR 官方同步 API 参考代码

本文件只保存官方同步参考代码，供设计 `Extract Worker`、抽取适配层、PoC 脚本时对照使用。

不要把这里的代码片段重写成“仓库规范版本”后再当成官方事实引用。这里的目的就是保留官方接口原貌。

## 关键事实

- 地址：`https://a3u500dcqcy9hei1.aistudio-app.com/layout-parsing`
- 鉴权头：`Authorization: token {TOKEN}`
- 模式：本地文件转 `base64`
- `fileType`：
  - `0` 表示 PDF
  - `1` 表示图片
- 一次请求直接返回 `result.layoutParsingResults`
- 返回结果会被解析并落地 `markdown.text`、`markdown.images`、`outputImages`

```python
# Please make sure the requests library is installed
# pip install requests
import base64
import os
import requests

API_URL = "https://a3u500dcqcy9hei1.aistudio-app.com/layout-parsing"
TOKEN = "3e8d187ac09429834e6af61c24e67b30aabca830"

file_path = "<local file path>"

with open(file_path, "rb") as file:
    file_bytes = file.read()
    file_data = base64.b64encode(file_bytes).decode("ascii")

headers = {
    "Authorization": f"token {TOKEN}",
    "Content-Type": "application/json"
}

required_payload = {
    "file": file_data,
    "fileType": <file type>,  # For PDF documents, set `fileType` to 0; for images, set `fileType` to 1
}

optional_payload = {
    "useDocOrientationClassify": False,
    "useDocUnwarping": False,
    "useChartRecognition": False,
}

payload = {**required_payload, **optional_payload}

response = requests.post(API_URL, json=payload, headers=headers)
print(response.status_code)
assert response.status_code == 200
result = response.json()["result"]

output_dir = "output"
os.makedirs(output_dir, exist_ok=True)

for i, res in enumerate(result["layoutParsingResults"]):
    md_filename = os.path.join(output_dir, f"doc_{i}.md")
    with open(md_filename, "w") as md_file:
        md_file.write(res["markdown"]["text"])
    print(f"Markdown document saved at {md_filename}")
    for img_path, img in res["markdown"]["images"].items():
        full_img_path = os.path.join(output_dir, img_path)
        os.makedirs(os.path.dirname(full_img_path), exist_ok=True)
        img_bytes = requests.get(img).content
        with open(full_img_path, "wb") as img_file:
            img_file.write(img_bytes)
        print(f"Image saved to: {full_img_path}")
    for img_name, img in res["outputImages"].items():
        img_response = requests.get(img)
        if img_response.status_code == 200:
            # Save image to local
            filename = os.path.join(output_dir, f"{img_name}_{i}.jpg")
            with open(filename, "wb") as f:
                f.write(img_response.content)
            print(f"Image saved to: {filename}")
        else:
            print(f"Failed to download image, status code: {img_response.status_code}")
```
