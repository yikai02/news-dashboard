#!/usr/bin/env bash
set -euo pipefail

TITLE="${1:-Antigravity 任務完成}"
MESSAGE="${2:-新聞儀表板已更新並部署完畢，點擊直接查看！}"
BODY="${TITLE}：${MESSAGE}"

curl -4 -fsS --retry 2 --max-time 10 \
  -H "Title: Dashboard Updated" \
  -H "Priority: default" \
  -H "Tags: white_check_mark,newspaper" \
  -H "Click: https://yikai02.github.io/news-dashboard/" \
  -d "${BODY}" \
  "https://ntfy.sh/kyle-anti-0910"
