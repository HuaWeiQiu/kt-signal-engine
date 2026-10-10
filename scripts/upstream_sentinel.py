#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""上游哨兵：Signal 服务器 capability 强制 / 引擎依赖上游异动的感知脚本。

背景（ADR：connector 0003 signal-cli-fallback-policy + 桌面 matrix 权威注
2026-10-10）：Signal 服务器按客户端上报 capability（SPQR 等）做强制过滤，
且 release note 不预告——2026-03 那轮批量注销的最快信号是 signal-cli#1993
类 issue，不是官方公告。本脚本定期核对三个信号源，异常命中时 exit 1，
便于节律系统（kt-tick）接 Telegram 告警。

检查面：
  1. signalapp/Signal-Server 最近 commits —— capability/deprecation 关键词
  2. AsamK/signal-cli 最近 issues —— 注销/拒连/能力关键词（最快信号源）
  3. whisperfish/presage + whisperfish/libsignal-service-rs 上游 HEAD rev
     —— 对照引擎 Cargo.toml 的 pinned rev，漂移即报（升级窗口提示，不阻塞）

用法：
  python3 scripts/upstream_sentinel.py [--days 7] [--json]
环境：
  GITHUB_TOKEN  可选；匿名 API 限额 60 次/小时/IP，配 token 提升。
退出码：
  0 = 三源平静；1 = 命中告警关键词或上游 rev 漂移；2 = 网络/API 错误。
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys
import urllib.request

# —— 告警关键词（小写子串匹配；来源：SPQR 事件复盘，signal-cli#1993/#2005）——
SERVER_COMMIT_KEYWORDS = [
    "capability", "deprecat", "spqr", "sparse_post_quantum", "enforce",
    "unregister", "remove.*device", "not registered",
]
CLI_ISSUE_KEYWORDS = [
    "unregistered", "deprecat", "capability", "403", "authorization failed",
    "removed.*device", "not registered", "version too old",
]
WINDOW_DAYS_DEFAULT = 7

ENGINE_REPO = "https://github.com/whisperfish/presage"
SERVICE_REPO = "https://github.com/whisperfish/libsignal-service-rs"
API = "https://api.github.com"


def gh_get(path: str) -> object:
    url = f"{API}{path}"
    req = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json"})
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def pinned_rev_from_cargo(repo_root: str, crate: str) -> str | None:
    cargo = os.path.join(repo_root, "Cargo.toml")
    try:
        text = open(cargo, encoding="utf-8").read()
    except OSError:
        return None
    m = re.search(rf'{crate}\s*=\s*\{{[^}}]*rev\s*=\s*"([0-9a-f]+)"', text)
    return m.group(1) if m else None


def keyword_hits(entries: list[tuple[str, str]], patterns: list[str]) -> list[str]:
    hits = []
    for title, body in entries:
        hay = f"{title} {body}".lower()
        for pat in patterns:
            if re.search(pat, hay):
                hits.append(f"{title}  [matched: {pat}]")
                break
    return hits


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=WINDOW_DAYS_DEFAULT)
    parser.add_argument("--json", action="store_true", help="结构化输出（供节律系统消费）")
    parser.add_argument("--repo-root", default=os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    args = parser.parse_args()
    since = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=args.days)).strftime("%Y-%m-%dT%H:%M:%SZ")

    report: dict = {"window_days": args.days, "alerts": [], "info": [], "errors": []}
    exit_code = 0

    # 1) Signal-Server commits
    try:
        commits = gh_get(f"/repos/signalapp/Signal-Server/commits?since={since}&per_page=100")
        entries = [(c["commit"]["message"].split("\n")[0], c["commit"]["message"]) for c in commits]
        hits = keyword_hits(entries, SERVER_COMMIT_KEYWORDS)
        report["info"].append(f"Signal-Server: {len(entries)} commits in window")
        for h in hits:
            report["alerts"].append(f"[server-commit] {h}")
    except Exception as exc:  # noqa: BLE001 —— 哨兵脚本逐源容错
        report["errors"].append(f"Signal-Server commits: {exc}")

    # 2) signal-cli issues（最快信号源）
    try:
        issues = gh_get(f"/repos/AsamK/signal-cli/issues?since={since}&state=all&per_page=100")
        entries = [(i["title"], i.get("body") or "") for i in issues if "pull_request" not in i]
        hits = keyword_hits(entries, CLI_ISSUE_KEYWORDS)
        report["info"].append(f"signal-cli: {len(entries)} issues in window")
        for h in hits:
            report["alerts"].append(f"[signal-cli-issue] {h}")
    except Exception as exc:  # noqa: BLE001
        report["errors"].append(f"signal-cli issues: {exc}")

    # 3) 引擎依赖上游 rev 漂移（提示级，不进 alerts）
    for crate, repo in (("presage", ENGINE_REPO), ("libsignal-service", SERVICE_REPO)):
        try:
            pinned = pinned_rev_from_cargo(args.repo_root, crate)
            head = gh_get(f"/repos/{repo.split('github.com/')[1]}/commits/HEAD")["sha"]
            label = f"{crate}: pinned={pinned or '未找到'} upstream={head[:12]}"
            if pinned and not head.startswith(pinned):
                report["info"].append(f"[rev-drift] {label} —— 有上游新提交，升级窗口时评估")
            else:
                report["info"].append(label)
        except Exception as exc:  # noqa: BLE001
            report["errors"].append(f"{crate} rev: {exc}")

    if report["alerts"]:
        exit_code = 1
    if report["errors"]:
        exit_code = max(exit_code, 2)

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        for section, mark in (("alerts", "!!"), ("errors", "??"), ("info", "--")):
            for line in report[section]:
                print(f"{mark} {line}")
        print(f"exit={exit_code} (0 平静 / 1 告警 / 2 错误)")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
