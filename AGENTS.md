# 新聞儀表板 (news-dashboard) — Antigravity 專案工作流程規範

本文件定義 AI Agent 操作 `/Users/jiangyikai/Documents/Antigravity/新聞儀表板` (`https://github.com/yikai02/news-dashboard`) 時必須遵守的工作流程規則。

## 專案概覽

- **部署網址**：`https://yikai02.github.io/news-dashboard/`
- **資料抓取管線**：`scripts/fetch_news.py`（產出 `public/daily_news.json`、`public/daily_news_morning.json`、`public/daily_news_evening.json` 與 `public/index.html`）
- **任務完成通知腳本**：`scripts/notify.sh`（發送推播至 ntfy 頻道 `https://ntfy.sh/kyle-anti-0910`）

## 工作流程規則：任務完成自動推播通知

以後凡是在本專案執行以下任一任務時，請在**任務的最後一步**自動執行通知腳本：

1. 執行大型修改（跨檔案修改、結構重構或較大規模功能／樣式調整）
2. 完成 `git push` 部署
3. 執行完 `fetch_news`（`python scripts/fetch_news.py`）

執行指令：

```bash
scripts/notify.sh "Antigravity 任務完成" "新聞儀表板已更新並部署完畢，點擊直接查看！"
```

若目前終端機工作目錄不在專案根目錄，請切換至專案根目錄或使用完整路徑 `"/Users/jiangyikai/Documents/Antigravity/新聞儀表板/scripts/notify.sh"` 執行。
