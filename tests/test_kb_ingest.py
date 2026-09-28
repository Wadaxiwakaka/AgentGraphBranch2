"""知识库摄取 CLI（kb_ingest.py）的契约测试。

覆盖 docs/plans/03-knowledge-base-design.md 第 6 节与第 10 节 3a 部分：
文档提取（md/txt/docx/pdf）、切块、零文本页警告、未知扩展名与零文本文件
点名失败、mock embeddings 端点端到端、退出码与脱敏错误消息、重跑确定性。

所有 embeddings 调用均使用 mocked endpoint（httpx.MockTransport 注入 SDK
http_client），不存在任何真实网络依赖；mock handler 记录并断言批次数，
实现若绕过 embeddings 调用则测试失败。
"""

from __future__ import annotations

import json
import shutil
import zipfile
from pathlib import Path
from xml.sax.saxutils import escape

import httpx
import pytest
from openai import AsyncOpenAI

import kb_ingest

FIXTURES = Path(__file__).parent / "fixtures"


def _write_text(path: Path, content: str) -> Path:
    path.write_text(content, encoding="utf-8")
    return path


def _write_docx(path: Path, paragraphs: list[str]) -> Path:
    body = "".join(
        f"<w:p><w:r><w:t>{escape(paragraph)}</w:t></w:r></w:p>"
        for paragraph in paragraphs
    )
    document = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<w:document xmlns:w="'
        'http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        f"<w:body>{body}</w:body></w:document>"
    )
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("word/document.xml", document)
    return path


def _deterministic_embedding(text: str) -> list[float]:
    return [float(len(text)), 1.0, 0.5]


def _mock_embeddings_client(calls: list[int]) -> AsyncOpenAI:
    """构造使用 mocked endpoint 的 embeddings 客户端，并记录每批输入数。"""

    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content.decode("utf-8"))
        batch = payload["input"]
        calls.append(len(batch))
        data = [
            {
                "object": "embedding",
                "index": position,
                "embedding": _deterministic_embedding(text),
            }
            for position, text in enumerate(batch)
        ]
        return httpx.Response(
            200,
            json={
                "object": "list",
                "model": payload.get("model", ""),
                "data": data,
                "usage": {"prompt_tokens": 0, "total_tokens": 0},
            },
        )

    return AsyncOpenAI(
        base_url="http://kb-mock.test/v1",
        api_key="mock-key",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )


def _failing_embeddings_client() -> AsyncOpenAI:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="upstream exploded")

    return AsyncOpenAI(
        base_url="http://kb-mock.test/v1",
        api_key="mock-key",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )


# ---------------------------------------------------------------------------
# 1. 切块与文件收集
# ---------------------------------------------------------------------------


def test_chunk_text_accumulates_paragraphs_under_limit() -> None:
    """相邻短段落累积进同一块，块内以空行连接且不超过上限。"""

    text = "ab\ncd\n\nef"
    chunks = kb_ingest.chunk_text(text, 20)
    assert chunks == ["ab\ncd\n\nef"]


def test_chunk_text_closes_chunk_when_limit_exceeded() -> None:
    """加入下一段会超限时封块开新块，两块都不超限。"""

    text = "0123456789\n\n0123456789"
    chunks = kb_ingest.chunk_text(text, 15)
    assert chunks == ["0123456789", "0123456789"]


def test_chunk_text_hard_splits_oversized_paragraph() -> None:
    """单段落超过上限时按字符硬切，不产生超限块。"""

    text = "x" * 25
    chunks = kb_ingest.chunk_text(text, 10)
    assert [len(chunk) for chunk in chunks] == [10, 10, 5]


def test_chunk_text_drops_empty_paragraphs() -> None:
    """空段落被丢弃，不产生空块。"""

    chunks = kb_ingest.chunk_text("a\n\n \n\n\nb", 100)
    assert chunks == ["a\n\nb"]


def test_collect_source_files_sorted_and_rejects_unknown(tmp_path: Path) -> None:
    """文件按相对路径确定性排序；未知扩展名点名拒绝。"""

    _write_text(tmp_path / "b.txt", "x")
    _write_text(tmp_path / "a.md", "y")
    subdirectory = tmp_path / "sub"
    subdirectory.mkdir()
    _write_text(subdirectory / "c.md", "z")
    files = kb_ingest.collect_source_files(tmp_path)
    assert [file.display for file in files] == ["a.md", "b.txt", "sub/c.md"]

    (tmp_path / "legacy.doc").write_bytes(b"x")
    with pytest.raises(kb_ingest.IngestError, match="legacy.doc"):
        kb_ingest.collect_source_files(tmp_path)


# ---------------------------------------------------------------------------
# 1a. docx 提取
# ---------------------------------------------------------------------------


def test_extract_docx_paragraphs(tmp_path: Path) -> None:
    """最小 docx 逐段提取，段落间以空行连接，页警告为空。"""

    path = _write_docx(tmp_path / "note.docx", ["第一段。", "第二段。"])
    text, pages_without_text = kb_ingest.extract_document_text(path, "note.docx")
    assert text == "第一段。\n\n第二段。"
    assert pages_without_text == []


def test_ingest_docx_zero_text_fails_named(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """零文本 docx 以退出码 5 点名失败，不写入空块。"""

    sources = tmp_path / "sources"
    sources.mkdir()
    _write_docx(sources / "empty.docx", [""])
    return_code = kb_ingest.main(
        [
            "--sources",
            str(sources),
            "--index",
            str(tmp_path / "index.json"),
            "--base-url",
            "http://kb-mock.test/v1",
            "--embedding-model",
            "mock-embedding-model",
        ],
        embeddings_client=_mock_embeddings_client([]),
    )
    assert return_code == 5
    stderr = capsys.readouterr().err
    assert "empty.docx" in stderr
    assert "未提取到文本" in stderr
    assert not (tmp_path / "index.json").exists()


# ---------------------------------------------------------------------------
# 1b. pdf 提取
# ---------------------------------------------------------------------------


def test_extract_pdf_fixture() -> None:
    """minimal.pdf 由 pypdf 提取出两行文本，无零文本页。"""

    text, pages_without_text = kb_ingest.extract_document_text(
        FIXTURES / "minimal.pdf", "minimal.pdf"
    )
    assert "KB minimal fixture line one." in text
    assert "KB minimal fixture line two." in text
    assert pages_without_text == []


def test_ingest_mixed_pdf_reports_warning_pages(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """混合型 pdf（文本页+空白页）摄取成功，warnings 以 1 基页号列出零文本页。"""

    sources = tmp_path / "sources"
    sources.mkdir()
    shutil.copy(FIXTURES / "mixed.pdf", sources / "mixed.pdf")
    calls: list[int] = []
    return_code = kb_ingest.main(
        [
            "--sources",
            str(sources),
            "--index",
            str(tmp_path / "index.json"),
            "--base-url",
            "http://kb-mock.test/v1",
            "--embedding-model",
            "mock-embedding-model",
        ],
        embeddings_client=_mock_embeddings_client(calls),
    )
    assert return_code == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["warnings"] == [
        {"file": "mixed.pdf", "pages_without_text": [2]}
    ]
    assert calls and sum(calls) >= 1


# ---------------------------------------------------------------------------
# 2. 端到端（mocked embeddings endpoint）
# ---------------------------------------------------------------------------


def test_end_to_end_writes_index(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """md+txt 摄取写出完整索引：字段齐、排序稳定、批量 32、无临时文件残留。"""

    sources = tmp_path / "sources"
    sources.mkdir()
    paragraphs = "\n\n".join(f"p{i:04d}" for i in range(40))
    _write_text(sources / "notes.md", paragraphs)
    _write_text(sources / "extra.txt", "补充段落")
    calls: list[int] = []
    return_code = kb_ingest.main(
        [
            "--sources",
            str(sources),
            "--index",
            str(tmp_path / "index.json"),
            "--base-url",
            "http://kb-mock.test/v1",
            "--embedding-model",
            "mock-embedding-model",
            "--chunk-chars",
            "7",
        ],
        embeddings_client=_mock_embeddings_client(calls),
    )
    assert return_code == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["base_url"] == "http://kb-mock.test/v1"
    assert summary["embedding_model"] == "mock-embedding-model"
    assert summary["files"] == 2
    assert summary["chunks"] == 41
    assert summary["dimension"] == 3
    assert summary["warnings"] == []
    # 40 个单段块 + 1 个 extra.txt 块共 41 块，扁平分批为 [32, 9]。
    assert calls == [32, 9]

    payload = json.loads((tmp_path / "index.json").read_text(encoding="utf-8"))
    assert payload["schema_version"] == 1
    assert payload["embedding_model"] == "mock-embedding-model"
    assert payload["dimension"] == 3
    chunks = payload["chunks"]
    assert len(chunks) == 41
    keys = [(chunk["source"], chunk["chunk_index"]) for chunk in chunks]
    assert keys == sorted(keys)
    first_md = next(
        chunk for chunk in chunks if chunk["source"] == "notes.md"
    )
    assert first_md["chunk_index"] == 0
    assert first_md["embedding"] == _deterministic_embedding(first_md["text"])
    assert not list(tmp_path.glob(".*tmp*"))


def test_rerun_produces_identical_index(tmp_path: Path) -> None:
    """同一输入重跑摄取，除 generated_at 外索引逐字节一致。"""

    sources = tmp_path / "sources"
    sources.mkdir()
    _write_text(sources / "doc.md", "同一段落。")
    args = [
        "--sources",
        str(sources),
        "--base-url",
        "http://kb-mock.test/v1",
        "--embedding-model",
        "mock-embedding-model",
    ]
    first_index = tmp_path / "first.json"
    second_index = tmp_path / "second.json"
    assert (
        kb_ingest.main(
            args + ["--index", str(first_index)],
            embeddings_client=_mock_embeddings_client([]),
        )
        == 0
    )
    assert (
        kb_ingest.main(
            args + ["--index", str(second_index)],
            embeddings_client=_mock_embeddings_client([]),
        )
        == 0
    )
    first = json.loads(first_index.read_text(encoding="utf-8"))
    second = json.loads(second_index.read_text(encoding="utf-8"))
    first.pop("generated_at")
    second.pop("generated_at")
    assert first == second


# ---------------------------------------------------------------------------
# 3. 退出码
# ---------------------------------------------------------------------------


def test_missing_required_args_exit_2() -> None:
    """缺少必填参数 --embedding-model 时 argparse 以码 2 退出。"""

    with pytest.raises(SystemExit) as excinfo:
        kb_ingest.main(["--base-url", "http://kb-mock.test/v1"])
    assert excinfo.value.code == 2


def test_invalid_chunk_chars_exit_2() -> None:
    """--chunk-chars 非正整数时以码 2 退出。"""

    with pytest.raises(SystemExit) as excinfo:
        kb_ingest.main(
            [
                "--sources",
                "s",
                "--base-url",
                "http://kb-mock.test/v1",
                "--embedding-model",
                "m",
                "--chunk-chars",
                "0",
            ]
        )
    assert excinfo.value.code == 2


def test_missing_sources_dir_exit_5(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """源目录不存在时以码 5 失败，消息固定。"""

    return_code = kb_ingest.main(
        [
            "--sources",
            str(tmp_path / "nowhere"),
            "--base-url",
            "http://kb-mock.test/v1",
            "--embedding-model",
            "m",
        ],
        embeddings_client=_mock_embeddings_client([]),
    )
    assert return_code == 5
    assert "源目录不存在" in capsys.readouterr().err


def test_empty_sources_dir_exit_5(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """空源目录以码 5 失败。"""

    sources = tmp_path / "sources"
    sources.mkdir()
    return_code = kb_ingest.main(
        [
            "--sources",
            str(sources),
            "--base-url",
            "http://kb-mock.test/v1",
            "--embedding-model",
            "m",
        ],
        embeddings_client=_mock_embeddings_client([]),
    )
    assert return_code == 5
    assert "未发现待摄取文件" in capsys.readouterr().err


def test_undecodable_txt_exit_5_named(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """非 UTF-8 文本文件以码 5 点名失败。"""

    sources = tmp_path / "sources"
    sources.mkdir()
    (sources / "bad.txt").write_bytes(b"\xff\xfe\x00\xff")
    return_code = kb_ingest.main(
        [
            "--sources",
            str(sources),
            "--base-url",
            "http://kb-mock.test/v1",
            "--embedding-model",
            "m",
        ],
        embeddings_client=_mock_embeddings_client([]),
    )
    assert return_code == 5
    stderr = capsys.readouterr().err
    assert "bad.txt" in stderr
    assert "读取或解码失败" in stderr


def test_unknown_extension_exit_5_named(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """未知扩展名文件以码 5 点名失败，不静默跳过。"""

    sources = tmp_path / "sources"
    sources.mkdir()
    _write_text(sources / "ok.md", "正常内容")
    (sources / "keep.doc").write_bytes(b"legacy")
    return_code = kb_ingest.main(
        [
            "--sources",
            str(sources),
            "--base-url",
            "http://kb-mock.test/v1",
            "--embedding-model",
            "m",
        ],
        embeddings_client=_mock_embeddings_client([]),
    )
    assert return_code == 5
    stderr = capsys.readouterr().err
    assert "keep.doc" in stderr
    assert "不支持的文件类型" in stderr


def test_endpoint_failure_exit_5_fixed_message(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """embeddings 端点失败以码 5 报固定消息，不回显 URL 或 key。"""

    sources = tmp_path / "sources"
    sources.mkdir()
    _write_text(sources / "doc.md", "内容")
    return_code = kb_ingest.main(
        [
            "--sources",
            str(sources),
            "--index",
            str(tmp_path / "index.json"),
            "--base-url",
            "http://kb-mock.test/v1",
            "--embedding-model",
            "m",
        ],
        embeddings_client=_failing_embeddings_client(),
    )
    assert return_code == 5
    stderr = capsys.readouterr().err
    assert "embeddings 端点调用失败" in stderr
    assert "kb-mock.test" not in stderr
    assert "mock-key" not in stderr
    assert not (tmp_path / "index.json").exists()
