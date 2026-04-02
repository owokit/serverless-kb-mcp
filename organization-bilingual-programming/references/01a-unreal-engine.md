# Unreal Engine C++ 扩展

## EN: Overview

Unreal Engine 5 specific patterns, macros, and best practices for C++ development within the UE ecosystem.

## CN: 概述

UE5 特定的模式、宏和 C++ 开发最佳实践。

---

## UE 特定注解

```cpp
/**
 * EN: Applies damage to target actor with game-specific rules.
 * CN: 使用游戏特定规则对目标 Actor 施加伤害。
 *
 * @param BaseDamage
 *        EN: Base damage value before modifiers.
 *        CN: 修饰符前的基准伤害值。
 * @param TargetActor
 *        EN: The actor to apply damage to.
 *        CN: 要施加伤害的目标 Actor。
 * @param DamageTypeClass
 *        EN: Type of damage (fire, physical, etc.).
 *        CN: 伤害类型（火焰、物理等）。
 *
 * @return
 *        EN: Actual damage dealt after all calculations.
 *        CN: 所有计算后实际造成的伤害。
 *
 * @performance
 *        EN: O(1), can be called frequently.
 *        CN: O(1)，可频繁调用。
 *
 * @note
 *        EN: "Automatically validates target."
 *        CN: "自动验证目标有效性。"
 */
UFUNCTION(BlueprintCallable, Category = "MyPlugin|Damage")
float ApplyDamage(
    float BaseDamage,
    AActor* TargetActor,
    TSubclassOf<UDamageType> DamageTypeClass
);
```

---

## 组件缓存

```cpp
/**
 * EN: Cached movement component for performance.
 * CN: 缓存的运动组件以提升性能。
 *
 * @performance
 *        EN: "Avoids FindComponentByClass in Tick."
 *        CN: "避免在 Tick 中调用 FindComponentByClass。"
 */
UPROPERTY()
TObjectPtr<UCharacterMovementComponent> CachedMovementComponent;

void AMyCharacter::BeginPlay()
{
    Super::BeginPlay();

    // EN: Cache once in BeginPlay
    // CN: 在 BeginPlay 中缓存一次
    CachedMovementComponent = FindComponentByClass<UCharacterMovementComponent>();
}
```

---

## 定时器模式（蓝图中无 DELAY）

```cpp
private:
    /**
     * EN: Timer handle for delayed execution.
     * CN: 延迟执行的定时器句柄。
     */
    FTimerHandle DelayedActionHandle;

/**
 * EN: Execute action after delay without Blueprint DELAY.
 * CN: 无需蓝图 DELAY 延迟执行操作。
 *
 * @param Delay
 *        EN: Seconds to wait.
 *        CN: 等待秒数。
 */
void UMyComponent::ExecuteDelayed(float Delay)
{
    GetWorld()->GetTimerManager().SetTimer(
        DelayedActionHandle,
        this,
        &UMyComponent::OnDelayedExecute,
        Delay,
        false
    );
}

void UMyComponent::EndPlay(const EEndPlayReason::Type Reason)
{
    // EN: Always clear timers
    // CN: 始终清除定时器
    GetWorld()->GetTimerManager().ClearTimer(DelayedActionHandle);
    Super::EndPlay(Reason);
}
```

---

## 带句柄的委托

```cpp
private:
    /**
     * EN: Handle for damage binding.
     * CN: 伤害绑定的句柄。
     */
    FDelegateHandle DamageEventHandle;

/**
 * EN: Bind to damage event with proper handle storage.
 * CN: 正确存储句柄绑定到伤害事件。
 *
 * @commonMistake
 *        EN: "Not storing handle causes memory leak."
 *        CN: "不存储句柄会导致内存泄漏。"
 */
void UMyComponent::BindDamageEvent()
{
    if (DamageEventHandle.IsValid())
    {
        DamageSystem->OnDamage.Remove(DamageEventHandle);
    }

    DamageEventHandle = DamageSystem->OnDamage.AddUObject(
        this,
        &UMyComponent::HandleDamage
    );
}

void UMyComponent::EndPlay(const EEndPlayReason::Type Reason)
{
    if (DamageEventHandle.IsValid())
    {
        DamageSystem->OnDamage.Remove(DamageEventHandle);
    }
    Super::EndPlay(Reason);
}
```

---

## 日志类别

```cpp
// In PrivatePCH.h
DECLARE_LOG_CATEGORY_EXTERN(LogMyPlugin, Log, All);
DECLARE_LOG_CATEGORY_EXTERN(LogMyPluginDamage, Warning, All);
```

## 蓝图接口（无 CAST TO）

```cpp
// EN: Define interface instead of casting
// CN: 定义接口代替类型转换
UINTERFACE(Blueprintable)
class UIDamageable : public UInterface
{
    GENERATED_BODY()
};

class IDamageable
{
    GENERATED_BODY()
public:
    /**
     * EN: Receive damage from any source.
     * CN: 从任何来源接收伤害。
     */
    UFUNCTION(BlueprintImplementableEvent, Category = "Combat")
    void ReceiveDamage(float Amount, AActor* Source);
};
```
