# 現代商業與深度時事新聞儀表板 (Modern Business & Current Affairs Dashboard)

靜態資料驅動、高質感 Editorial 排版之台灣正體中文財經、科技與深度公共議題新聞儀表板。

每日台灣時間 **07:30 晨報版** 與 **16:00 晚報版** 由 GitHub Actions 雲端自動抓取、跨媒體事件分群、四維度評分並發布至 GitHub Pages。

---

## 核心特色

1. **白名單來源（嚴格排除政論八卦與瑣碎社會案件）**：
   - **商業與總經**：Reuters、Bloomberg、WSJ、經濟日報、工商時報（限產業、總經、科技版位）。
   - **科技與創新**：TechNews 科技新報、iThome、數位時代。
   - **深度公共議題**：報導者 (The Reporter)、公視新聞網 (PNN 議題中心)。
2. **四維度重要性漏斗（只保留 80 分以上高價值報導）**：
   - 跨媒體共鳴度 (35%)
   - 實質結構影響 (35%，扣除主觀口水與單純公關稿)
   - 數據事實密度 (20%)
   - 時效持久度 (10%)
3. **事實查核與多方視角對比**：
   - `core_facts`：跨媒體共同確認之客觀數據事實。
   - `media_perspectives`：2～3 家媒體報導切入角度差異對照。
   - `unverified_notice`：單一匿名消息源查核警示。
4. **原生首圖提取與數據帳本備援**：
   - 提取原始網頁 `<meta property="og:image">`，絕不使用假圖或圖庫照片。
   - 若原始網頁無首圖，自動呈現結構化「關鍵數據查核帳本」。
5. **總經關鍵指標看板**：
   - 美國 10 年期公債殖利率、美元指數 (DXY)、美元/台幣匯率、台灣景氣對策信號。
6. **Hallmark Editorial 設計系統**：
   - 通過 `sloplint` 嚴格審計（無 AI 廉價感膠囊 Eyebrow、無彩字標題、100% 採用 Token 變數、零橫向溢出）。

---

## 本地開發與預覽

```bash
# 安裝依賴（主要採用 Python 標準庫，安裝 tzdata 確保時區精確）
pip install -r requirements.txt

# 執行抓取與同步渲染
python scripts/fetch_news.py

# 啟動本地靜態預覽
python -m http.server 8080 --directory public
```

---

## GitHub Actions 自動化與 GitHub Pages 設定

1. 在儲存庫頁面點擊 **Settings** -> **Pages**。
2. 在 **Build and deployment** 下方的 **Source** 選擇 **GitHub Actions**。
3. 每天台灣時間 07:30 與 16:00 會自動執行排程更新，也可在 **Actions** 頁面手動點選 **Run workflow** 一鍵更新。
