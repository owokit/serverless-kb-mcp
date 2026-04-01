---
name: organization-bilingual-programming
description: ????????????????????UTF-8 ??????????
---

# 双语编程技能

## 目的

为多种编程语言提供标准化的中英文双语代码注释规范。每个语言专属参考涵盖惯用模式、最佳实践和常见陷阱。

## 核心原则

1. **交叉 EN/CN 注释**：先英文行后中文行，绝不混合
2. **禁止单行混合**：`/** EN: xxx / CN: xxx */` 禁止使用
3. **UTF-8 编码**：所有中文文本文件 UTF-8 无 BOM
4. **完整的 @params**：每个参数都有文档
5. **语言惯用性**：遵循每种语言的原生风格
6. **性能意识**：避免每种语言的反模式

## 目录结构

```
bilingual-programming/
├── SKILL.md
├── references/
│   ├── 01-cpp.md              # C++ 核心模式
│   ├── 01a-unreal-engine.md   # UE C++ 扩展（独立参考）
│   ├── 02-nodejs.md           # Node.js / TypeScript
│   ├── 03-python.md           # Python
│   ├── 04-go.md               # Go
│   ├── 05-java.md             # Java
│   └── 06-rust.md             # Rust
└── scripts/
    └── validate-encoding.py   # UTF-8 BOM 检查器
```

## 质量检查清单

- [ ] EN/CN 交叉分行
- [ ] 所有参数使用 `@param` 文档化
- [ ] 返回值使用 `@return` 文档化
- [ ] 异常情况使用 `@throws` 文档化
- [ ] 提供使用示例
- [ ] 标注性能影响
- [ ] 验证 UTF-8 编码
- [ ] 遵循语言惯用模式
