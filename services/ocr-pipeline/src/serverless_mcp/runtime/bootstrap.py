"""
EN: Runtime composition root for the serverless_mcp service package.
CN: serverless_mcp 服务包的运行时组合根。
"""
from __future__ import annotations

from dataclasses import dataclass

from serverless_mcp.runtime.aws_clients import AwsClientBundle, get_aws_clients
from serverless_mcp.runtime.config import Settings, load_settings


@dataclass(frozen=True, slots=True)
class RuntimeContext:
    """
    EN: Resolved runtime settings and cached AWS clients for service builders.
    CN: 供服务构建器使用的已解析运行时配置和缓存 AWS 客户端。
    """

    settings: Settings
    clients: AwsClientBundle


def build_runtime_context(*, settings: Settings | None = None, clients: AwsClientBundle | None = None) -> RuntimeContext:
    """
    EN: Resolve settings and AWS clients into a single composition-root context.
    CN: 将 settings 和 AWS clients 解析为单一的组合根上下文。
    """
    active_settings = settings or load_settings()
    active_clients = clients or get_aws_clients()
    return RuntimeContext(settings=active_settings, clients=active_clients)


__all__ = ["RuntimeContext", "build_runtime_context"]
