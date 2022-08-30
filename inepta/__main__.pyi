from typing import Any, Self, NoReturn

class ColumnType: ...  # how?

class _DefaultIni:
    @staticmethod
    def loads(s: str | None) -> dict: ...
    @staticmethod
    def dumps(d: dict | None) -> str: ...

class Node:
    url: str
    parameters: dict[str, Any] | None
    proxy: dict | None
    name: str
    bulk_size: int

    parser: ...  # ?
    _attrs: tuple
    _cfg: dict
    _buffer: list[dict]
    _quota: int

    def __init__(
        self,
        description: str | None = ...,
        columns: dict[str, ColumnType] | None = ...,
        parameters: str | dict[str, str | int] | None = ...,
        reset_url_semantic: bool = ...,
    ): ...
    def add(
        self,
        url: str,
        title: str | None,
        content: bytes | str | None,
        **columns: bytes | str | None,
    ) -> None: ...
    def __enter__(self) -> Self | NoReturn: ...
    def _flush(self) -> None: ...

def _handle_cli(description, columns, parameters, reset_url_semantic) -> str | NoReturn: ...
def _setup_logging(log_folder: str, is_debug: bool, name: str) -> None: ...
