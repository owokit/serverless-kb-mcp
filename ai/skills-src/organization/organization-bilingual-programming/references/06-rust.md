# Rust 双语指南

## EN: Overview

Rust uses rustdoc style. `///` for doc comments, `//` for inline.

## CN: 概述

Rust 使用 rustdoc 风格。`///` 用于文档注释，`//` 用于行内。

---

## 函数

```rust
/// Returns the maximum of two values.
///
/// EN: Returns a if a > b, otherwise returns b.
/// CN: 如果 a > b 返回 a，否则返回 b。
///
/// # Example
///
/// ```
/// let m = max(1, 2);
/// assert_eq!(m, 2);
/// ```
///
/// # Panics
///
/// EN: Never panics.
/// CN: 永不 panic。
#[must_use]
pub fn max(a: i32, b: i32) -> i32 {
    if a > b { a } else { b }
}
```

---

## 结构体

```rust
/// Generic configuration holder.
///
/// EN: Stores key-value configuration pairs.
/// CN: 存储键值配置对。
///
/// # Thread Safety
///
/// EN: Read-only after initialization.
/// CN: 初始化后只读。
pub struct Config {
    /// EN: Configuration values.
    /// CN: 配置值。
    values: HashMap<String, String>,
}
```

---

## Trait

```rust
/// Defines input validation behavior.
///
/// EN: Implement for custom validation logic.
/// CN: 为自定义验证逻辑实现。
pub trait Validator {
    /// EN: Validates the input.
    /// CN: 验证输入。
    ///
    /// # Errors
    ///
    /// EN: Returns error if invalid.
    /// CN: 无效则返回错误。
    fn validate(&self, input: &str) -> Result<(), ValidationError>;
}
```

---

## 行内

```rust
// EN: Guard against invalid index.
// CN: 防御无效索引。
if index >= len { return None; }

// EN: Use unwrap_or for default.
// CN: 用 unwrap_or 提供默认值。
let count = data.get("count").unwrap_or(&0);
```

---

## 命名

| 类型 | 风格 | 示例 |
|------|------|------|
| 函数 | snake_case | `max` |
| 结构体/枚举 | PascalCase | `Config` |
| Trait | PascalCase | `Validator` |
| 变量 | snake_case | `max_value` |
| 常量 | SCREAMING_SNAKE | `MAX_RETRIES` |
