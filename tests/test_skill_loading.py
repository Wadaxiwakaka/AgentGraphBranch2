"""Skill 目录加载与文件解析的契约测试。

覆盖 docs/plans/02-skill-system-design.md 第 3、4 节：文件格式、名字一致性、
字数上限、非法文件名、空目录与缺失目录语义。
"""

from __future__ import annotations

from pathlib import Path

import pytest

import core


LEGAL_SKILL_DOCUMENT = """name: research
description: 系统性多源调研方法

## 目标
接到调研类任务时，先明确范围与验收标准。

key: not_parsed

后续内容仍属于正文。
"""


def _write_skill(directory: Path, filename: str, content: str) -> Path:
    path = directory / filename
    path.write_text(content, encoding="utf-8")
    return path


def test_parse_legal_skill_document(tmp_path: Path) -> None:
    """合法文件解析出 name、description 和完整正文；空行后的 key: 行不解析。"""

    path = _write_skill(tmp_path, "research.md", LEGAL_SKILL_DOCUMENT)
    skill = core.parse_skill_file(path)
    assert skill.name == "research"
    assert skill.description == "系统性多源调研方法"
    assert skill.body.startswith("## 目标")
    assert "key: not_parsed" in skill.body
    assert "后续内容仍属于正文。" in skill.body


@pytest.mark.parametrize(
    "filename, content, reason_fragment",
    [
        (
            "research.md",
            "description: 缺少 name\n\n正文。\n",
            "name",
        ),
        (
            "research.md",
            "name: research\n\n正文。\n",
            "description",
        ),
        (
            "research.md",
            "name: wrong_name\ndescription: 名字与文件名不一致\n\n正文。\n",
            "一致",
        ),
        (
            "research.md",
            "name: research\ndescription: 正常\nversion: 2\n\n正文。\n",
            "未知",
        ),
        (
            "research.md",
            "not_a_header_line\ndescription: 头部前出现非 key: value 行\n\n正文。\n",
            "key",
        ),
        (
            "research.md",
            "name: research\ndescription: 正文超限\n\n" + ("长" * 2001) + "\n",
            "2000",
        ),
    ],
)
def test_parse_rejects_invalid_documents(
    tmp_path: Path,
    filename: str,
    content: str,
    reason_fragment: str,
) -> None:
    """缺失必填键、名字不一致、未知键、非法头部行、正文超限都拒绝。"""

    path = _write_skill(tmp_path, filename, content)
    with pytest.raises(core.ConfigError) as excinfo:
        core.parse_skill_file(path)
    assert "research.md" in str(excinfo.value)
    assert reason_fragment in str(excinfo.value)


def test_parse_rejects_invalid_filename(tmp_path: Path) -> None:
    """文件名不满足 ^[A-Za-z][A-Za-z0-9_-]*$ 时拒绝。"""

    path = _write_skill(
        tmp_path,
        "1bad.md",
        "name: 1bad\ndescription: 文件名非法\n\n正文。\n",
    )
    with pytest.raises(core.ConfigError) as excinfo:
        core.parse_skill_file(path)
    assert "1bad.md" in str(excinfo.value)


def test_load_skills_directory(tmp_path: Path) -> None:
    """目录加载返回名字到 SkillSpec 的映射；非 .md 文件被忽略。"""

    skills_directory = tmp_path / "skills"
    skills_directory.mkdir()
    _write_skill(
        skills_directory,
        "alpha.md",
        "name: alpha\ndescription: 第一个技能\n\n正文 A。\n",
    )
    _write_skill(
        skills_directory,
        "beta.md",
        "name: beta\ndescription: 第二个技能\n\n正文 B。\n",
    )
    (skills_directory / "notes.txt").write_text("忽略我", encoding="utf-8")

    skills = core.load_skills(skills_directory)
    assert set(skills) == {"alpha", "beta"}
    assert skills["beta"].body == "正文 B。\n"


def test_load_skills_empty_directory(tmp_path: Path) -> None:
    """空目录加载为空映射，不报错。"""

    skills_directory = tmp_path / "skills"
    skills_directory.mkdir()
    assert core.load_skills(skills_directory) == {}


def test_load_skills_missing_directory(tmp_path: Path) -> None:
    """缺失目录视为空映射：未配置 skill 的既有部署零影响。"""

    assert core.load_skills(tmp_path / "does_not_exist") == {}
