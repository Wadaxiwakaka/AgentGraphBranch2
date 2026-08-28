"""知识库摄取 CLI：本地文档 → 切块 → embeddings → 本地 JSON 索引。

设计依据 docs/plans/03-knowledge-base-design.md 第 5、6 节。支持
``.md`` / ``.txt`` / ``.docx`` / ``.pdf``；未知扩展名与零文本文件点名失败；
混合型 pdf 的零文本页号记入成功摘要的 warnings。

硬性保证：本模块没有 mock / 离线 / dry-run 模式，embeddings 调用不可绕过；
模拟端点只存在于测试代码（注入 ``embeddings_client`` 的构造由测试完成）。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import tempfile
import xml.etree.ElementTree as ElementTree
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from openai import AsyncOpenAI
from pypdf import PdfReader

SUPPORTED_SUFFIXES = frozenset({".md", ".txt", ".docx", ".pdf"})
EMBEDDING_BATCH_SIZE = 32
_DOCX_PARAGRAPH = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}p"
_DOCX_TEXT = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}t"
_PLACEHOLDER_KEY = "LOCAL_NO_AUTH"


class IngestError(Exception):
    """摄取失败；str(error) 即为面向运维者的固定脱敏消息。"""


@dataclass(frozen=True, slots=True)
class SourceFile:
    """一个待摄取文件的相对显示名与绝对路径。"""

    display: str
    path: Path


def collect_source_files(sources: Path) -> list[SourceFile]:
    """递归枚举源目录下全部文件并按相对路径确定性排序。

    参数:
        sources: 源目录路径。

    返回值:
        按 POSIX 相对路径升序排列的 ``SourceFile`` 列表。

    异常:
        IngestError: 目录不存在、目录为空或存在不支持的扩展名时抛出；
        不支持的文件会在消息中逐个点名，绝不静默跳过。

    状态变化:
        只读取目录元数据，不修改文件系统。
    """

    if not sources.is_dir():
        raise IngestError("源目录不存在或不是目录")
    entries = sorted(
        (entry for entry in sources.rglob("*") if entry.is_file()),
        key=lambda entry: entry.relative_to(sources).as_posix(),
    )
    if not entries:
        raise IngestError("未发现待摄取文件")
    unsupported = [
        entry.relative_to(sources).as_posix()
        for entry in entries
        if entry.suffix.lower() not in SUPPORTED_SUFFIXES
    ]
    if unsupported:
        raise IngestError("存在不支持的文件类型: " + ", ".join(unsupported))
    return [
        SourceFile(entry.relative_to(sources).as_posix(), entry)
        for entry in entries
    ]


def _extract_docx_text(path: Path) -> str:
    try:
        with zipfile.ZipFile(path) as archive:
            document = archive.read("word/document.xml")
        root = ElementTree.fromstring(document)
    except (OSError, zipfile.BadZipFile, KeyError, ElementTree.ParseError):
        return ""
    paragraphs = []
    for paragraph in root.iter(_DOCX_PARAGRAPH):
        paragraphs.append(
            "".join(
                node.text or "" for node in paragraph.iter(_DOCX_TEXT)
            )
        )
    return "\n\n".join(paragraphs)


def _extract_pdf_text(path: Path) -> tuple[str, list[int]]:
    try:
        reader = PdfReader(path)
        page_texts = [
            page.extract_text() or "" for page in reader.pages
        ]
    except Exception:
        return "", []
    pages_without_text = [
        number
        for number, text in enumerate(page_texts, start=1)
        if not text.strip()
    ]
    return "\n\n".join(page_texts), pages_without_text


def extract_document_text(path: Path, display: str) -> tuple[str, list[int]]:
    """按扩展名提取单个文档的纯文本与 pdf 零文本页号。

    参数:
        path: 文件绝对路径。
        display: 面向错误消息的相对显示名。

    返回值:
        ``(text, pages_without_text)``；非 pdf 文档的页号列表恒为空。

    异常:
        IngestError: 文件读取或解码失败时抛出，消息点名 ``display``。

    状态变化:
        只读取文件内容，不修改文件系统。

    已知简化（故意为之）:
        - pdf 内图片一律跳过，不做 OCR 与视觉模型描述；
        - docx 只取段落文本流（含表格内文字），忽略样式与结构。
    """

    suffix = path.suffix.lower()
    if suffix in {".md", ".txt"}:
        try:
            return path.read_text(encoding="utf-8"), []
        except (OSError, UnicodeDecodeError):
            raise IngestError(f"文件读取或解码失败: {display}") from None
    if suffix == ".docx":
        text = _extract_docx_text(path)
        if not text and path.stat().st_size > 0:
            # 空字符串既可能是合法零文本，也可能是损坏文件；两者都按
            # 零文本文件由调用方点名失败，不做静默区分。
            return text, []
        return text, []
    if suffix == ".pdf":
        text, pages_without_text = _extract_pdf_text(path)
        return text, pages_without_text
    raise IngestError(f"存在不支持的文件类型: {display}")


def chunk_text(text: str, limit: int) -> list[str]:
    """把文本按空行切段落并累积成不超过 ``limit`` 字符的块。

    参数:
        text: 已提取的文档纯文本。
        limit: 单块字符上限，必须为正整数。

    返回值:
        块文本列表；块内段落以空行连接，空段落被丢弃。

    异常:
        本函数不主动抛出异常。

    状态变化:
        纯函数，不修改输入与文件系统。
    """

    chunks: list[str] = []
    current = ""
    for paragraph in re.split(r"\n\s*\n", text):
        stripped = paragraph.strip()
        if not stripped:
            continue
        pieces = [
            stripped[start : start + limit]
            for start in range(0, len(stripped), limit)
        ]
        for piece in pieces:
            if current and len(current) + 2 + len(piece) > limit:
                chunks.append(current)
                current = piece
            else:
                current = f"{current}\n\n{piece}" if current else piece
    if current:
        chunks.append(current)
    return chunks


async def _embed_all(
    client: AsyncOpenAI,
    model: str,
    texts: list[str],
) -> tuple[list[list[float]], int]:
    """分批向量化全部块文本并校验维度一致。

    参数:
        client: 已构造的 embeddings 客户端。
        model: embedding 模型名。
        texts: 块文本列表，元素数量必须大于零。

    返回值:
        ``(vectors, dimension)``；向量顺序与 ``texts`` 一一对应。

    异常:
        IngestError: 端点调用失败、返回数量不符、向量为空或维度不一致时
        抛出固定消息，不透传上游异常细节。

    状态变化:
        只产生网络请求，不修改本地文件。
    """

    vectors: list[list[float]] = []
    dimension: int | None = None
    for start in range(0, len(texts), EMBEDDING_BATCH_SIZE):
        batch = texts[start : start + EMBEDDING_BATCH_SIZE]
        try:
            response = await client.embeddings.create(
                model=model,
                input=batch,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            raise IngestError("embeddings 端点调用失败") from None
        items = sorted(response.data, key=lambda item: item.index)
        if len(items) != len(batch):
            raise IngestError("embeddings 端点调用失败")
        for item in items:
            vector = [float(value) for value in item.embedding]
            if not vector:
                raise IngestError("embeddings 向量为空")
            if dimension is None:
                dimension = len(vector)
            elif len(vector) != dimension:
                raise IngestError("embeddings 维度不一致")
            vectors.append(vector)
    assert dimension is not None
    return vectors, dimension


def _write_index_atomic(index_path: Path, payload: dict[str, Any]) -> None:
    """以临时文件 + fsync + os.replace 原子写出索引 JSON。

    参数:
        index_path: 目标索引路径。
        payload: 完整索引字典。

    返回值:
        ``None``。

    异常:
        OSError: 目录创建或写盘失败时抛出；临时文件会被尽力清理。

    状态变化:
        成功时目标文件被原子替换；失败时旧文件保持不变。
    """

    index_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="\n",
            dir=index_path.parent,
            prefix=f".{index_path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary_file:
            temporary_path = Path(temporary_file.name)
            json.dump(
                payload,
                temporary_file,
                ensure_ascii=False,
                separators=(",", ":"),
            )
            temporary_file.write("\n")
            temporary_file.flush()
            os.fsync(temporary_file.fileno())
        os.replace(temporary_path, index_path)
    except BaseException:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise


def _positive_int(value: str) -> int:
    try:
        number = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("必须为正整数") from None
    if number <= 0:
        raise argparse.ArgumentTypeError("必须为正整数")
    return number


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="kb_ingest.py",
        description="知识库摄取：本地文档 → 切块 → embeddings → JSON 索引。",
    )
    parser.add_argument(
        "--sources",
        default="knowledge/sources",
        help="源文档目录（默认 knowledge/sources）",
    )
    parser.add_argument(
        "--index",
        default="knowledge/index.json",
        help="输出索引路径（默认 knowledge/index.json）",
    )
    parser.add_argument(
        "--base-url",
        required=True,
        help="embeddings 端点基础 URL（必填，无默认值）",
    )
    parser.add_argument(
        "--embedding-model",
        required=True,
        help="embedding 模型名（必填，无默认值）",
    )
    parser.add_argument(
        "--chunk-chars",
        type=_positive_int,
        default=800,
        help="单块字符上限（默认 800）",
    )
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    embeddings_client: AsyncOpenAI | None = None,
) -> int:
    """摄取入口：提取、切块、向量化并原子写索引。

    参数:
        argv: 命令行参数；``None`` 时读取 ``sys.argv``。
        embeddings_client: 可选注入的 embeddings 客户端（测试接缝）；
            ``None`` 时按 ``--base-url`` 与环境变量 ``OPENAI_API_KEY``
            构造自有客户端，环境变量未设置时使用固定占位符。

    返回值:
        ``0`` 成功；``5`` 摄取失败（固定脱敏消息写入 stderr）。
        参数无效时 argparse 直接 ``SystemExit(2)``。

    异常:
        本函数不向调用方抛出 IngestError；写盘 OSError 同样转为码 ``5``。

    状态变化:
        成功时写出索引文件并向 stdout 打印一行摘要 JSON（含 base_url、
        embedding_model、文件数、块数、维度与 warnings）。
    """

    args = _build_parser().parse_args(argv)
    owned_client = embeddings_client is None
    client = embeddings_client
    try:
        files = collect_source_files(Path(args.sources))
        chunk_records: list[dict[str, Any]] = []
        warnings: list[dict[str, Any]] = []
        for source in files:
            text, pages_without_text = extract_document_text(
                source.path, source.display
            )
            if not text.strip():
                raise IngestError(f"文件未提取到文本: {source.display}")
            if pages_without_text:
                warnings.append(
                    {
                        "file": source.display,
                        "pages_without_text": pages_without_text,
                    }
                )
            for position, chunk in enumerate(
                chunk_text(text, args.chunk_chars)
            ):
                chunk_records.append(
                    {
                        "source": source.display,
                        "chunk_index": position,
                        "text": chunk,
                    }
                )

        if client is None:
            client = AsyncOpenAI(
                base_url=args.base_url,
                api_key=os.environ.get("OPENAI_API_KEY") or _PLACEHOLDER_KEY,
            )

        async def _embed_and_close() -> tuple[list[list[float]], int]:
            assert client is not None
            try:
                return await _embed_all(
                    client,
                    args.embedding_model,
                    [record["text"] for record in chunk_records],
                )
            finally:
                if owned_client:
                    await client.close()

        vectors, dimension = asyncio.run(_embed_and_close())
        payload = {
            "schema_version": 1,
            "embedding_model": args.embedding_model,
            "dimension": dimension,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "chunks": [
                {
                    "source": record["source"],
                    "chunk_index": record["chunk_index"],
                    "text": record["text"],
                    "embedding": vector,
                }
                for record, vector in zip(chunk_records, vectors)
            ],
        }
        try:
            _write_index_atomic(Path(args.index), payload)
        except OSError:
            raise IngestError("索引写入失败") from None
    except IngestError as error:
        print(str(error), file=sys.stderr)
        return 5

    summary = {
        "base_url": args.base_url,
        "embedding_model": args.embedding_model,
        "files": len(files),
        "chunks": len(chunk_records),
        "dimension": dimension,
        "warnings": warnings,
    }
    print(json.dumps(summary, ensure_ascii=False))
    return 0


# ponytail: JSON 向量索引 + 全量重建；索引超过约 5k 块时换 sqlite-vec/numpy。
if __name__ == "__main__":
    raise SystemExit(main())
