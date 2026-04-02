# Java 双语指南

## EN: Overview

Java uses Javadoc style. Block comments `/** */` above declarations.

## CN: 概述

Java 使用 Javadoc 风格。块注释 `/** */` 在声明上方。

---

## 方法

```java
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
public static <T extends Comparable<T>> T max(T a, T b) {
    return a.compareTo(b) > 0 ? a : b;
}
```

---

## 类

```java
/**
 * EN: Generic configuration holder.
 * CN: 通用配置持有者。
 *
 * @author
 *        EN: Development Team.
 *        CN: 开发团队。
 *
 * @threadSafety
 *        EN: Immutable after construction.
 *        CN: 构造后不可变。
 */
public final class Config<T> {
    /**
     * EN: Configuration values.
     * CN: 配置值。
     */
    private final Map<String, T> values;
}
```

---

## 接口

```java
/**
 * EN: Defines input validation contract.
 * CN: 定义输入验证契约。
 *
 * @important
 *        EN: Implementations must be thread-safe.
 *        CN: 实现必须线程安全。
 */
@FunctionalInterface
public interface Validator<T> {
    /**
     * EN: Validates the input.
     * CN: 验证输入。
     *
     * @param input
     *        EN: Input to validate.
     *        CN: 要验证的输入。
     * @return
     *        EN: true if valid.
     *        CN: 有效则返回 true。
     */
    boolean validate(T input);
}
```

---

## 行内

```java
// EN: Guard against null.
// CN: 防御空值。
if (obj == null) return;

// EN: Use Optional for nullability.
// CN: 用 Optional 处理可空值。
String result = Optional.ofNullable(value).orElse("default");
```

---

## 命名

| 类型 | 风格 | 示例 |
|------|------|------|
| 类 | PascalCase | `ConfigManager` |
| 方法 | camelCase | `max` |
| 变量 | camelCase | `maxValue` |
| 常量 | SCREAMING_SNAKE | `MAX_RETRIES` |
| 接口 | PascalCase | `Validator` |
