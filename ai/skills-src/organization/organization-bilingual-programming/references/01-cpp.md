# C++ 双语指南

## EN: Overview

C++ uses Doxygen-style comments. Block comments `/** */` for declarations, `//` for inline.

## CN: 概述

C++ 使用 Doxygen 风格注释。块注释 `/** */` 用于声明，行注释 `//` 用于行内。

---

## 函数

```cpp
/**
 * EN: Returns the maximum of two values.
 * CN: 返回两个值中的最大值。
 *
 * @param a
 *        EN: First value.
 *        CN: 第一个值。
 * @param b
 *        EN: Second value.
 *        CN: 第二个值。
 * @return
 *        EN: Maximum value.
 *        CN: 最大值。
 */
template <typename T>
[[nodiscard]] constexpr const T& max(const T& a, const T& b);
```

---

## 类

```cpp
/**
 * EN: Generic configuration holder.
 * CN: 通用配置持有者。
 *
 * @important
 *        EN: Immutable after construction.
 *        CN: 构造后不可变。
 */
template <typename T>
class Config {
    /**
     * EN: Current configuration values.
     * CN: 当前配置值。
     */
    std::unordered_map<std::string, T> values;
};
```

---

## 枚举

```cpp
/**
 * EN: Status codes for operations.
 * CN: 操作的状态码。
 */
enum class Status {
    /**
     * EN: Success.
     * CN: 成功。
     */
    OK,
    /**
     * EN: Resource not found.
     * CN: 资源未找到。
     */
    NotFound,
    /**
     * EN: Invalid input parameters.
     * CN: 输入参数无效。
     */
    InvalidInput,
};
```

---

## 行内

```cpp
// EN: Check if pointer is valid.
// CN: 检查指针是否有效。
if (ptr == nullptr) return;

// EN: Reserve capacity to avoid reallocation.
// CN: 预分配容量避免重分配。
vec.reserve(100);
```

---

## 命名

| 类型 | 风格 | 示例 |
|------|------|------|
| 类 | PascalCase | `ConfigManager` |
| 函数 | snake_case | `max` |
| 变量 | snake_case | `max_value` |
| 常量 | kPascalCase | `kMaxRetries` |
