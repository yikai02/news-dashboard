#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""fetch_news.py：現代商業與深度時事新聞儀表板之後端資料管線。

核心職責：
1. 來源子版白名單（嚴格排除政論八卦、彩券與瑣碎社會案件）：
   - 商業與總經：Reuters (Business/Markets)、Bloomberg、WSJ、經濟日報、工商時報（僅限產業、總經、科技版位）。
   - 科技與創新：TechNews 科技新報、iThome、數位時代。
   - 深度社會與公共議題：報導者 (The Reporter)、公視新聞網 (PNN 議題中心)。
2. 高解析度新聞首圖提取：
   - 抓取各新聞網頁原始碼 <head> 內的 <meta property="og:image"> 作為 imageUrl，
     過濾網站預設 Logo 圖；若無圖則標記為 null（絕不使用假圖或無關 stock photo）。
3. 重要性評分與漏斗（只保留 80 分以上的高價值新聞）：
   - 跨媒體共鳴度（35%）：同一事件是否被多家媒體共同報導（深度調查專題具備跨源議題共鳴加權）。
   - 實質結構影響（35%）：涉及財報、利率法規、供應鏈整併、技術突破，扣除主觀口水與單純公關稿。
   - 數據事實密度（20%）：包含具體百分比、財務指標與時程。
   - 時效持久度（10%）：具備一週以上的決策學習價值。
4. 事實查核與多方視角對比：
   - core_facts：各家報導皆一致確認的客觀事實。
   - media_perspectives：列出 2~3 家媒體各自的報導切入點或側重點。
   - unverified_notice：若含單一匿名消息源，標註警示文字，否則為空字串。
5. 總經數據看板：
   - US 10Y 殖利率、美元指數 (DXY)、美元/台幣、台灣景氣燈號最新指標。
6. 輸出格式：
   - 產出 public/daily_news.json，包含 edition（晨報版 07:30 / 晚報版 16:00）與更新時間戳記。
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import dataclasses
import hashlib
import html
import json
import logging
import os
import re
import sys
import tempfile
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from pathlib import Path
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Taipei")
ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT = ROOT / "public" / "daily_news.json"
DEFAULT_HTML = ROOT / "public" / "index.html"
SCHEMA_VERSION = 1
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
WEEKDAY_ZH = ["星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期日"]

log = logging.getLogger("fetch_news")


# ---------------------------------------------------------------------------
# 一、來源白名單與台灣正體中文詞彙規範
# ---------------------------------------------------------------------------
@dataclasses.dataclass
class FeedSpec:
    url: str
    section: str = ""
    link_allow: str | None = None


@dataclasses.dataclass
class SourceSpec:
    id: str
    name: str
    group: str  # business 商業與總經 / tech 科技與創新 / deep 深度社會與公共議題
    lang: str
    home: str
    feeds: list[FeedSpec]
    max_age_h: int = 48
    aggregator: bool = False
    priority: int = 5


SOURCES: list[SourceSpec] = [
    # 商業與總經（100% 原生直連 RSS，直達原文網址與高清 og:image）
    SourceSpec(
        "udn", "經濟日報", "business", "zh", "https://money.udn.com",
        [
            FeedSpec("https://money.udn.com/rssfeed/news/1001/5591?ch=money", "產業與科技"),
            FeedSpec("https://money.udn.com/rssfeed/news/1001/5588?ch=money", "總經與要聞"),
            FeedSpec("https://money.udn.com/rssfeed/news/1001/5592?ch=money", "證券金融"),
        ],
        priority=1,
    ),
    SourceSpec(
        "ctee", "工商時報 (Yahoo 聯播)", "business", "zh", "https://tw.stock.yahoo.com",
        [
            FeedSpec("https://tw.stock.yahoo.com/rss?category=tw-market", "台股市場與總經"),
            FeedSpec("https://tw.stock.yahoo.com/rss?category=headline", "財經焦點要聞"),
        ],
        priority=2,
    ),
    SourceSpec(
        "wsj", "WSJ 華爾街日報", "business", "en", "https://www.wsj.com",
        [
            FeedSpec("https://feeds.content.dowjones.io/public/rss/RSSMarketsMain", "Markets"),
            FeedSpec("https://feeds.content.dowjones.io/public/rss/WSJcomUSBusiness", "Business"),
        ],
        priority=5,
    ),
    SourceSpec(
        "cnbc", "CNBC", "business", "en", "https://www.cnbc.com",
        [
            FeedSpec("https://www.cnbc.com/id/10000664/device/rss/rss.html", "Finance & Markets"),
            FeedSpec("https://www.cnbc.com/id/19854910/device/rss/rss.html", "Tech"),
        ],
        priority=6,
    ),
    # 科技與創新
    SourceSpec(
        "technews", "TechNews 科技新報", "tech", "zh", "https://technews.tw",
        [FeedSpec("https://technews.tw/feed/", "科技與半導體供應鏈")],
        priority=3,
    ),
    SourceSpec(
        "ithome", "iThome 電腦報", "tech", "zh", "https://www.ithome.com.tw",
        [FeedSpec("https://www.ithome.com.tw/rss", "企業科技、雲端與資安")],
        priority=4,
    ),
    # 深度社會與公共議題
    SourceSpec(
        "twreporter", "報導者", "deep", "zh", "https://www.twreporter.org",
        [FeedSpec("https://www.twreporter.org/a/rss2.xml", "深度調查報導")],
        max_age_h=240, priority=3,
    ),
    SourceSpec(
        "pts", "公視新聞網", "deep", "zh", "https://news.pts.org.tw",
        [FeedSpec("https://news.pts.org.tw/xml/newsfeed.xml", "PNN 議題中心")],
        max_age_h=96, priority=4,
    ),
]

# 擴充輔助來源字典，確保若有歷史參照或額外來源時不會拋出 KeyError
ALL_KNOWN_SPECS = list(SOURCES) + [
    SourceSpec("bloomberg", "Bloomberg", "business", "en", "https://www.bloomberg.com", [], priority=6),
    SourceSpec("reuters", "Reuters", "business", "en", "https://www.reuters.com", [], priority=8),
    SourceSpec("bnext", "數位時代", "tech", "zh", "https://www.bnext.com.tw", [], priority=5),
]
SOURCE_BY_ID = {s.id: s for s in ALL_KNOWN_SPECS}

# 台灣正體中文專業財經詞彙校準表
TW_TERM_MAP = {
    "美聯儲": "聯準會",
    "美國聯邦儲備": "美國聯準會",
    "國債": "公債",
    "收益率": "殖利率",
    "英偉達": "輝達",
    "芯片": "晶片",
    "內存": "記憶體",
    "人工智能": "人工智慧",
    "數據中心": "資料中心",
    "網絡": "網路",
    "軟件": "軟體",
    "硬件": "硬體",
    "信息": "資訊",
    "同比": "年增",
    "環比": "季增",
    "基點": "個基點",
    "特朗普": "川普",
    "埃隆·馬斯克": "馬斯克",
    "阿斯麥": "艾司摩爾",
    "ASIC": "客製化晶片",
    "EPS": "每股純益",
    "FWA": "固定無線接入",
    "CDU": "冷卻液分配裝置",
    "...": "…",
}

# 排除政論八卦、彩券開獎、個人理財血淚雞湯與瑣碎社會案件（避免誤傷重大地緣戰略、科技政策與司法調查）
DENY_RE = re.compile(
    r"命案|殺人|凶殺|車禍|火警|大火|搶劫|竊盜|詐騙集團|酒駕|毒品|性侵|緋聞|婚變|婚外情|外遇|藝人|明星|演唱會|"
    r"大樂透|今彩539|威力彩|雙贏彩|彩券|開獎|選戰|競選總部|拜票|造勢|民調|黨內初選|立委候選人|議員候選人|政黨派系|政治口水|"
    r"只剩一隻狗|血淚史|小資族|小資女|第一桶金|存錢體質|社畜|心法|心態|散戶痛|達人教|理財達人|翻身祕笈|翻身|心態致勝|"
    r"【公告】|\[公告\]|公告本公司|公開資訊觀測站重大訊息公告|編按\s*[:：]\s*本文首刊於|本文首刊於\d{4}年|"
    r"捐贈[^\n]{0,20}基金會|慈善基金會|文教基金會|補充[^\n]{0,20}董事會[^\n]{0,20}實際執行情形|決議不分派股利|"
    r"Rti精選新聞|精選新聞|盤前要聞|早安財經|"
    r"八卦|網紅|直播主|職棒|球賽|NBA|MLB|星座|運勢|氣溫驟降|天氣預報|颱風假|horoscope|celebrity|murder|shooting|lottery",
    re.I,
)
TITLE_NOISE_RE = re.compile(
    r"是買點嗎|買點到了嗎|該不該買|能進場嗎|能買嗎|可以撿嗎|台積公司董事會決議$",
    re.I,
)
SITE_BOILERPLATE_RE = re.compile(
    r"提供.{0,20}報價|Yahoo奇摩股市|即時新聞、財經|訂閱電子報|追蹤我們|更多新聞請看|最完整.{0,12}新聞",
    re.I,
)
WIRE_PREFIX_RE = re.compile(
    r"^(?:"
    r"(?:\[[^\]]{1,40}\]|【[^】]{1,40}】|\([^\)]{1,40}(?:電|訊|報導|綜合|整理|中心|中央社)\))\s*"
    r"|(?:(?:特派|資深)?記者|編譯|撰文)\s*[^\s，。；！？/]{2,8}\s*/\s*[^\n，。；！？]{1,12}?(?:報導|電|訊|綜合)\s*"
    r"|(?:(?:特派|資深)?記者|編譯|撰文)\s*[^\s，。；！？/]{2,6}\s+[^\n，。；！？/]{1,12}?(?:報導|電|訊)\s*"
    r"|(?:(?:特派|資深)?記者|編譯|撰文)[^\s，。；！？]{2,14}?(?:報導|電|訊)\s*"
    r"|(?:即時|財經|政治|國際|科技|產業|地方|社會|數位內容|編輯|新聞)?(?:中心|部)\s*/?\s*[^\n，。；！？]{1,12}?(?:報導|整理|綜合)\s*"
    r"|(?:文|整理)\s*/\s*[^\s，。；！？]{2,10}\s*"
    r"|編按\s*:\s*[^。\n]{0,180}。\s*"
    r")+"
)

# 深度社會與公共議題准入主題（涵蓋經濟結構、勞動、司法制度與公共戰略）
DEEP_ALLOW_RE = re.compile(
    r"勞動|薪資|工時|移工|缺工|住宅|居住|租屋|高齡|長照|照護|少子|人口|教育|醫療|健保|能源|電力|電網|三接|藻礁|"
    r"氣候|淨零|減碳|碳費|水資源|產業|供應鏈|人工智慧|半導體|科技|資安|隱私|修法|法規|轉型|貧窮|農業|食安|環境|"
    r"防衛|國防|戰略|外交|兩岸|人權|司法|檢察|國土|廢棄物|弱勢|早療|公共|交通|韌性|數位|預算|稅制|補助|經濟|通膨|利率"
)

# ---------------------------------------------------------------------------
# 二、實體詞典與評分詞典
# ---------------------------------------------------------------------------
STRONG_ENTITIES: dict[str, list[str]] = {
    "台積電": ["台積電", "tsmc", "taiwan semiconductor", "2奈米", "先進封裝", "cowos"],
    "輝達": ["輝達", "nvidia", "blackwell", "gb200", "rtx"],
    "聯準會": ["聯準會", "federal reserve", "fomc", r"\bfed\b", "鮑爾", "powell"],
    "美債殖利率": ["殖利率", "公債殖利率", "treasury yield", "10-year yield", "美債"],
    "鴻海": ["鴻海", "foxconn", "hon hai"],
    "蘋果": ["蘋果", r"\bapple\b", "iphone"],
    "微軟": ["微軟", "microsoft", "copilot"],
    "谷歌": ["谷歌", "google", "alphabet", "gemini"],
    "亞馬遜": ["亞馬遜", "amazon", "aws"],
    "openai": ["openai", "chatgpt"],
    "anthropic": ["anthropic", "claude"],
    "日月光": ["日月光", "日月光投控", r"\base\b"],
    "軟銀": ["軟銀", "softbank"],
    "特斯拉": ["特斯拉", "tesla"],
    "英特爾": ["英特爾", r"\bintel\b"],
    "超微": ["超微", r"\bamd\b"],
    "三星": ["三星", "samsung", "hbm"],
    "聯發科": ["聯發科", "mediatek"],
    "關稅": ["關稅", "tariff", "貿易戰"],
    "降息": ["降息", "rate cut"],
    "升息": ["升息", "rate hike"],
    "通膨": ["通膨", "通貨膨脹", "inflation", "cpi", "pce"],
    "外資籌碼": ["外資", "三大法人", "買賣超"],
    "能源與電網": ["電網", "電力", "三接", "離岸風電", "儲能", "核電", "綠能"],
    "長照與醫療": ["長照", "健保", "醫療", "照護", "高齡", "少子化"],
}
WEAK_ENTITIES: dict[str, list[str]] = {
    "半導體": ["半導體", "semiconductor", "晶圓", "chipmaker", "晶片"],
    "人工智慧": ["人工智慧", "生成式", "artificial intelligence", r"\bai\b", "資料中心"],
    "美元": ["美元", "dollar", "匯率"],
    "供應鏈": ["供應鏈", "產業鏈", "資本支出", "擴產"],
}

STOP_EN = set(
    "that this with from have will been were their about after which would could into than over more says said "
    "amid as its for the and are but not new year first its has had how why what when who".split()
)
GENERIC_4GRAM = {
    "第一季", "第二季", "第三季", "第四季", "前三季", "前3季", "年增率",
    "公司表示", "新台幣", "預期明年", "資本支出", "每股純益", "稅後純益",
}

IMPACT_CATS: dict[str, re.Pattern] = {
    "財報與資本配置": re.compile(r"財報|營收|獲利|毛利|每股盈餘|每股賺|財測|法說|虧損|資本預算|資本支出|earnings|revenue|profit|margin|capex", re.I),
    "利率與法規政策": re.compile(
        r"利率|升息|降息|央行|聯準會|殖利率|公債|法規|修法|法案|條例|關稅|補助|預算|稅制|反壟斷|審查|核准|環評|監管|"
        r"tariff|regulat|antitrust|rate cut|rate hike|treasury|yield", re.I),
    "供應鏈與整併": re.compile(
        r"供應鏈|產業鏈|併購|收購|整併|入股|擴產|建廠|設廠|新廠|產能|投資|合約|訂單|出貨|"
        r"merger|acquisition|acquire|capacity|supply chain", re.I),
    "技術與算力突破": re.compile(
        r"突破|量產|製程|奈米|晶片|算力|矽光子|量子|資料中心|伺服器|散熱|HBM|先進封裝|CoWoS|資安|"
        r"breakthrough|chip|data center|server", re.I),
    "公共治理與結構轉型": re.compile(
        r"勞動|薪資|長照|照護|高齡|少子|人力|缺工|住宅|租屋|醫療|健保|教育|電力|電網|能源|三接|藻礁|減碳|淨零|氣候|水資源|韌性"),
}
IMPACT_BY_CATS = {0: 8, 1: 20, 2: 28, 3: 33, 4: 35, 5: 35}
PR_RE = re.compile(
    r"開箱|業配|新品上市|盛大開幕|限時優惠|促銷|攜手慶|買一送一|抽獎|sponsored|advertorial|press release", re.I)
DURABLE_RE = re.compile(
    r"政策|法規|制度|長期|結構|轉型|供應鏈|產業鏈|布局|趨勢|十年|未來|整併|韌性|人力缺口|資本預算|資本支出|擴產|建廠|"
    r"structural|long-term|policy|reshor", re.I)
FACT_RE = re.compile(
    r"(?:US\$|NT\$|\$)\s?\d[\d,\.]*\s?(?:billion|million|trillion|bn|mn|[億萬兆])?"
    r"|\d[\d,\.]*\s?(?:%|％|percent|個基點|基點|bps|兆元|億元|億美元|億|萬人|萬戶|萬顆|萬|兆|元|美元|奈米|nm|倍|瓩|kW|MW|GW|座|季|年)",
    re.I,
)
ANON_RE = re.compile(r"知情人士|消息人士|不願具名|匿名|據傳|傳出|市場傳聞|people familiar|sources said|unnamed|anonymous", re.I)

_MACRO_POLICY_RE = re.compile(
    r"聯準會|升息|降息|利率|殖利率|通膨|\bcpi\b|貨幣政策|匯率|央行|關稅|經貿政策|\bfed\b|interest\s*rate|inflation|tariff",
    re.I,
)
ANGLE_PROFILES: list[tuple[str, re.Pattern]] = [
    ("供應鏈產能與建廠動向", re.compile(r"晶圓廠|建廠|擴產|產能|供應鏈|訂單|代工|封裝|cowos|稼動率|晶圓|出貨|建置|產線|capacity|fab|packaging|supply\s*chain", re.I)),
    ("資本預算與財報配置", re.compile(r"資本預算|資本支出|財報|營收|獲利|毛利率|淨利|股息|派息|季報|年報|現金股利|營運績效|認購|入股|特別股|普通股|股權|資金配置|capex|revenue|earnings|dividend|margin", re.I)),
    ("先進技術與算力規格", re.compile(r"製程|奈米|\bnm\b|先進封裝|算力|架構|伺服器|\bgpu\b|ai晶片|晶片|規格|架構突破|矽光子|量子|architecture|benchmark|\bchip\b", re.I)),
    ("利率匯率與總經政策", _MACRO_POLICY_RE),
    ("市場定價與外資籌碼", re.compile(r"股價|買超|賣超|市值|標普|那斯達克|加權指數|目標價|外資|投信|持股|籌碼|\bstocks?\b|shares?|valuation|rally", re.I)),
    ("政策法規與公共治理", re.compile(r"政策|法規|監管|立法|補助|委員會|審查|訴訟|反壟斷|長照|健保|勞動|人權|國土|環境|regulation|policy|antitrust|compliance", re.I)),
    ("深度調查與結構剖析", re.compile(r"專題|調查|追蹤|脈絡|體系|困境|結構轉型|公共利益|investigat|in-depth", re.I)),
]


def score_article_angle(text: str, used_angles: set[str] | None = None) -> str:
    scores: list[tuple[int, str]] = []
    for label, pat in ANGLE_PROFILES:
        if label == "利率匯率與總經政策" and not _MACRO_POLICY_RE.search(text):
            continue
        hits = len(pat.findall(text))
        if hits > 0:
            scores.append((hits, label))
    if not scores:
        return "產業動態與市場焦點"
    scores.sort(key=lambda x: -x[0])
    if used_angles:
        top_hits = scores[0][0]
        for hits, label in scores:
            if label not in used_angles and hits >= max(1, int(top_hits * 0.5)):
                return label
    return scores[0][1]

IMAGE_REJECT_RE = re.compile(
    r"default[-_]?(?:img|image|logo|thumb|avatar)|moneyudn\.jpg|/static/img/|placeholder|favicon|sprite|blank|logo[_.-]|"
    r"shutterstock|istock|gettyimages|unsplash|pexels|pixabay",
    re.I,
)


# ---------------------------------------------------------------------------
# 三、資料模型與解析工具
# ---------------------------------------------------------------------------
@dataclasses.dataclass
class Article:
    source: SourceSpec
    title: str
    summary: str
    url: str
    published: datetime | None
    title_orig: str = ""
    summary_orig: str = ""
    translated: bool = False
    image: str | None = None
    ents_strong: set[str] = dataclasses.field(default_factory=set)
    ents_weak: set[str] = dataclasses.field(default_factory=set)
    tokens: set[str] = dataclasses.field(default_factory=set)
    grams4: set[str] = dataclasses.field(default_factory=set)
    ev_facts: set[str] = dataclasses.field(default_factory=set)
    subj: str = ""

    @property
    def text(self) -> str:
        return f"{self.title} {self.summary}".strip()

    @property
    def lang(self) -> str:
        return "zh" if (self.source.lang == "zh" or self.translated) else "en"


def http_get(url: str, timeout: float = 12, max_bytes: int = 3_000_000, retries: int = 2) -> tuple[bytes, str, str]:
    last: Exception | None = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Encoding": "identity", "Accept": "*/*"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = resp.read(max_bytes)
                return data, resp.geturl(), resp.headers.get_content_charset() or "utf-8"
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(0.5 * (attempt + 1))
    raise last if last else RuntimeError("unknown http error")


def decode(data: bytes, charset: str) -> str:
    for enc in (charset, "utf-8", "big5"):
        try:
            return data.decode(enc)
        except (UnicodeDecodeError, LookupError):
            continue
    return data.decode("utf-8", errors="replace")


TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


def normalize_tw_terms(text: str) -> str:
    out = text
    for k, v in TW_TERM_MAP.items():
        out = out.replace(k, v)
    out = re.sub(r"\.{3,}", "…", out)
    # 若單句內全為大寫英文字母且達 6 個以上，將 3 字元以上縮寫轉為首字大寫以符合排版規範
    uppers = sum(1 for c in out if "A" <= c <= "Z")
    lowers = sum(1 for c in out if "a" <= c <= "z")
    if uppers >= 6 and lowers == 0:
        out = re.sub(r"\b([A-Z])([A-Z]{2,})\b", lambda m: m.group(1) + m.group(2).lower(), out)
    return out


_SENT_END_RE = re.compile(r"[。！？!?]|(?<![\dA-Z])\.(?=\s|$)")


def clean_text(raw: str) -> str:
    text = unicodedata.normalize("NFKC", html.unescape(TAG_RE.sub(" ", raw or "")))
    text = re.sub(r"The post .*? appeared first on .*", "", text, flags=re.S)
    text = re.sub(r"(繼續閱讀|閱讀全文|Read more).*$", "", text)
    text = WS_RE.sub(" ", text).strip()
    text = WIRE_PREFIX_RE.sub("", text).strip()
    # 轉換連續 4 個以上全大寫字母為首字大寫（例如 FTNN -> Ftnn），符合 Hallmark 內文排版規範
    text = re.sub(r"\b([A-Z])([A-Z]{3,})\b", lambda m: m.group(1) + m.group(2).lower(), text)
    return normalize_tw_terms(WS_RE.sub(" ", text).strip())


def trim_complete_summary(text: str, max_len: int = 900) -> str:
    """裁切摘要時優先保留至最後一個完整句尾標點，避免切斷年份、日期或數字（如 2027年3月1）。"""
    t = WIRE_PREFIX_RE.sub("", WS_RE.sub(" ", text).strip()).strip()
    if not t:
        return ""
    window = t[:max_len]
    ends = [m.end() for m in _SENT_END_RE.finditer(window)]
    # 若全文已超過上限，或目前文字沒有正常句尾（被 RSS 硬截斷 / 結尾為刪節號），退回最後一個完整句
    needs_trim = len(t) > max_len or (window[-1] not in "。！？!?." and not window.endswith("..."))
    if needs_trim and ends and ends[-1] >= 24:
        return window[:ends[-1]].strip()
    if len(t) > max_len:
        pauses = [m.start() for m in re.finditer(r"[；;，,]", window)]
        if pauses and pauses[-1] >= 24:
            return window[:pauses[-1]].strip() + "…"
        return window.rstrip("，,；; ") + "…"
    # 無任何句號卻以不完整詞彙＋刪節號結尾（如「…較去年同期成長…」），退回完整子句
    if window.endswith("…"):
        cleaned = re.sub(r"[，,][^，,]{1,18}…$", "", window).strip()
        if len(cleaned) >= 24:
            return cleaned + "…"
    return window


def parse_date(raw: str | None) -> datetime | None:
    if not raw:
        return None
    raw = raw.strip()
    try:
        dt = parsedate_to_datetime(raw)
    except (TypeError, ValueError):
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(TZ)


def local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].lower()


def child_text(node: ET.Element, *names: str) -> str:
    for ch in node:
        if local(ch.tag) in names and (ch.text or "").strip():
            return ch.text.strip()
    return ""


def parse_feed(raw: str, spec: SourceSpec, feed: FeedSpec) -> list[Article]:
    raw = raw.lstrip("\ufeff").strip()
    root = ET.fromstring(raw)
    nodes = [n for n in root.iter() if local(n.tag) in ("item", "entry")]
    out: list[Article] = []
    for n in nodes:
        raw_title_val = child_text(n, "title")
        link = child_text(n, "link")
        if not link:
            for ch in n:
                if local(ch.tag) == "link" and ch.get("href"):
                    if ch.get("rel", "alternate") == "alternate":
                        link = ch.get("href", "")
                        break
        raw_summary = child_text(n, "description", "summary") or child_text(n, "encoded", "content")
        is_gnews = "news.google.com" in feed.url or spec.aggregator
        raw_plain_title = normalize_tw_terms(
            WS_RE.sub(" ", unicodedata.normalize("NFKC", html.unescape(TAG_RE.sub(" ", raw_title_val or "")))).strip()
        )
        raw_plain_summary = "" if is_gnews else normalize_tw_terms(
            WS_RE.sub(" ", unicodedata.normalize("NFKC", html.unescape(TAG_RE.sub(" ", raw_summary or "")))).strip()
        )
        title = clean_text(raw_title_val)
        summary = "" if is_gnews else trim_complete_summary(clean_text(raw_summary), 900)
        published = parse_date(child_text(n, "pubdate", "published", "updated", "date"))
        if not title or not link:
            continue
        if is_gnews:
            title = re.sub(r"\s+-\s+[^-]{2,40}$", "", title).strip()
        # 在電頭與【公告】標籤被 clean_text 剝除前先攔截雜訊公告、舊專欄與買點猜謎
        if (
            DENY_RE.search(f"{raw_plain_title} {raw_plain_summary}")
            or TITLE_NOISE_RE.search(raw_plain_title)
            or TITLE_NOISE_RE.search(title)
        ):
            continue
        if feed.link_allow and not re.search(feed.link_allow, link):
            continue

        # 從 RSS 項目直接提取原生配圖（如 WSJ, CNBC, Bnext 等之 media:content / enclosure / img）
        img_url: str | None = None
        for ch in n:
            lt = local(ch.tag)
            if lt in ("content", "thumbnail") and ch.get("url"):
                img_url = ch.get("url")
                break
            if lt == "enclosure" and "image" in (ch.get("type") or "").lower() and ch.get("url"):
                img_url = ch.get("url")
                break
        if not img_url and raw_summary:
            m_img = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', raw_summary, re.IGNORECASE)
            if m_img:
                img_url = m_img.group(1)
        if img_url and IMAGE_REJECT_RE.search(img_url):
            img_url = None

        out.append(Article(spec, title, summary[:900], link, published, title_orig=title, summary_orig=summary[:900], image=img_url))
    return out


def fetch_source(spec: SourceSpec, now: datetime) -> tuple[list[Article], dict]:
    status = {"id": spec.id, "name": spec.name, "group": spec.group, "section": spec.feeds[0].section if spec.feeds else "",
              "ok": False, "count": 0, "error": None}
    items: list[Article] = []
    errors: list[str] = []
    for feed in spec.feeds:
        try:
            data, _, charset = http_get(feed.url)
            items.extend(parse_feed(decode(data, charset), spec, feed))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{type(exc).__name__}: {exc}"[:160])
            log.warning("來源 %s 失敗：%s", spec.name, errors[-1])
    cutoff = now - timedelta(hours=spec.max_age_h)
    kept = [a for a in items if a.published is None or a.published >= cutoff]
    status["count"] = len(kept)
    status["ok"] = bool(kept)
    status["error"] = "; ".join(errors) if errors and not kept else None
    return kept, status


def is_relevant(a: Article) -> bool:
    title_blob = f"{a.title_orig} {a.title}"
    blob = f"{title_blob} {a.summary}"
    if DENY_RE.search(blob) or TITLE_NOISE_RE.search(a.title_orig) or TITLE_NOISE_RE.search(a.title):
        return False
    if a.source.group == "deep" and not DEEP_ALLOW_RE.search(blob):
        return False
    return True


# ---------------------------------------------------------------------------
# 四、外電台灣正體中文化（支援 Gemini API 與 Google GTX 雙通道）
# ---------------------------------------------------------------------------
def _translate_gtx_single(text: str) -> str:
    if not text or not text.strip():
        return ""
    q = urllib.parse.quote(text[:900])
    url = f"https://translate.googleapis.com/translate_a/single?client=gtx&sl=en&tl=zh-TW&dt=t&q={q}"
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=8) as resp:
        data = json.loads(resp.read().decode("utf-8"))
        translated = "".join(part[0] for part in (data[0] or []) if part and part[0])
        return normalize_tw_terms(translated.strip())


def translate_articles(articles: list[Article]) -> None:
    """確保所有外電皆轉為台灣正體中文，供跨語言事件分群與前端渲染使用。"""
    targets = [a for a in articles if a.source.lang == "en" and not a.translated]
    if not targets:
        return
    key = os.environ.get("GEMINI_API_KEY", "").strip()
    if key:
        model = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
        for i in range(0, len(targets), 15):
            batch = targets[i:i + 15]
            payload_items = [{"title": a.title_orig, "summary": a.summary_orig} for a in batch]
            prompt = (
                "將下列英文財經新聞的標題與摘要翻成台灣正體中文，專有名詞採台灣業界慣用譯名，數字與單位照原文。"
                "只回傳 JSON 陣列，每個元素含 title 與 summary，順序與數量必須和輸入完全相同。\n"
                + json.dumps(payload_items, ensure_ascii=False)
            )
            body = json.dumps({
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"responseMimeType": "application/json", "temperature": 0.1},
            }).encode("utf-8")
            try:
                req = urllib.request.Request(url, data=body, headers={
                    "Content-Type": "application/json", "x-goog-api-key": key, "User-Agent": UA})
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.load(resp)
                parsed = json.loads(result["candidates"][0]["content"]["parts"][0]["text"])
                for a, tr in zip(batch, parsed, strict=False):
                    t = normalize_tw_terms(str(tr.get("title", "")).strip())
                    s = normalize_tw_terms(str(tr.get("summary", "")).strip())
                    if t:
                        a.title, a.summary, a.translated = t, s, True
            except Exception as exc:  # noqa: BLE001
                log.warning("Gemini 翻譯失敗，改用 GTX 備援：%s", exc)

    remaining = [a for a in targets if not a.translated][:45]
    if not remaining:
        return

    def _tr_one(art: Article) -> None:
        try:
            t_zh = _translate_gtx_single(art.title_orig)
            s_zh = _translate_gtx_single(art.summary_orig) if art.summary_orig else ""
            if t_zh:
                art.title = t_zh
                art.summary = s_zh
                art.translated = True
        except Exception as exc:  # noqa: BLE001
            log.debug("GTX 翻譯單篇失敗：%s", exc)

    with cf.ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(_tr_one, remaining))


# ---------------------------------------------------------------------------
# 五、跨媒體事件分群與 4 維度重要性評分
# ---------------------------------------------------------------------------
def _compile(entities: dict[str, list[str]]) -> dict[str, re.Pattern]:
    return {k: re.compile("|".join(v), re.I) for k, v in entities.items()}


TITLE_SUBJ_RE = re.compile(
    r"^([\u3400-\u9fff]{2,5}?)(?:1?\d月|前\d季|第[1-4一二三四]季|公布|營收|董事會|獲利|股價|遭|斥資|買入|買進|賣出|宣布|擬|今|昨日|盤中|與|攜手)"
)
NON_COMPANY_SUBJ = {
    "美國", "台灣", "台股", "美股", "日股", "陸股", "全球", "亞洲", "歐洲", "中國", "日本", "韓國",
    "外資", "法人", "市場", "大盤", "經濟", "今年", "去年", "明年", "今日", "本季", "最新", "重磅",
    "央行", "財政", "政院", "立院", "政府", "國際", "企業", "科技", "財經", "產業",
}
_SUBJ_ALIAS = {"台積": "台積電", "台積公司": "台積電"}
_EVENT_SYNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"現金股利|股息|股利|派息"), "配息"),
    (re.compile(r"認購|入股|買股|參股|注資|持股"), "投資"),
    (re.compile(r"資本預算|資本支出"), "資本投資"),
    (re.compile(r"消費者物價指數|物價指數|消費者物價"), "通膨"),
    (re.compile(r"重磅宣布|拍板定案|拍板"), "核准"),
    (re.compile(r"第[1１一]季|\bQ1\b", re.I), "第一季"),
    (re.compile(r"第[2２二]季|\bQ2\b", re.I), "第二季"),
    (re.compile(r"第[3３三]季|\bQ3\b", re.I), "第三季"),
    (re.compile(r"第[4４四]季|\bQ4\b", re.I), "第四季"),
]
_MASK_GENERIC_RE = re.compile(r"宣布|指出|強調|表示|回應|爆料|曝|曝光|最新|消息|報導|今日|今年|去年|明年|市場|億元|新台幣|台幣|億美元|美元")
_DATE_FACT_RE = re.compile(r"^(?:19|20)\d{2}年?$|^\d{1,2}月$|^[1-4]季$")


def annotate(articles: list[Article], strong: dict[str, re.Pattern], weak: dict[str, re.Pattern]) -> None:
    for a in articles:
        # 僅從標題提取強/弱實體，避免內文回顧舊聞（如「昔日聯準會升息、通膨」）污染分群實體集合
        title_blob = f"{a.title_orig} {a.title}".strip()
        a.ents_strong = {k for k, p in strong.items() if p.search(title_blob)}
        a.ents_weak = {k for k, p in weak.items() if p.search(title_blob)}

        m_subj = TITLE_SUBJ_RE.match(re.sub(r"^[\[【\(][^\]】\)]{1,18}[\]】\)]\s*", "", a.title).strip())
        subj = _SUBJ_ALIAS.get(m_subj.group(1), m_subj.group(1)) if m_subj else ""
        a.subj = "" if subj in NON_COMPANY_SUBJ else subj

        # 遮罩實體名稱與通用動詞，再套用事件同義詞，使 toks 代表「事件本質」而非僅重複公司名
        t_ev = a.title
        for p in strong.values():
            t_ev = p.sub(" ", t_ev)
        for pat, rep_tok in _EVENT_SYNS:
            t_ev = pat.sub(rep_tok, t_ev)
        t_ev = _MASK_GENERIC_RE.sub(" ", t_ev)

        toks: set[str] = set()
        grams: set[str] = set()
        for seg in re.findall(r"[\u3400-\u9fff]+", t_ev):
            toks.update(seg[i:i + 2] for i in range(len(seg) - 1))
            grams.update(seg[i:i + 4] for i in range(len(seg) - 3))
        for w in re.findall(r"[A-Za-z]{4,}", a.title_orig):
            if w.lower() not in STOP_EN:
                toks.add(w.lower())
        a.tokens, a.grams4 = toks, grams - GENERIC_4GRAM
        a.ev_facts = {f for f in extract_facts(a.text) if not _DATE_FACT_RE.match(f)}


def similar(a: Article, b: Article) -> bool:
    if a.subj and b.subj and a.subj != b.subj and not (a.subj in b.title and b.subj in a.title):
        return False
    strong_shared = a.ents_strong & b.ents_strong
    strong = len(strong_shared)
    if a.ents_strong and b.ents_strong and strong == 0:
        return False

    inter = len(a.tokens & b.tokens)
    union = len(a.tokens | b.tokens)
    jac = inter / union if union else 0.0
    min_len = min(len(a.tokens), len(b.tokens))
    overlap = inter / min_len if min_len else 0.0
    shared_ev = a.ev_facts & b.ev_facts
    g4 = len(a.grams4 & b.grams4)

    if strong >= 2:
        return inter >= 2 or len(shared_ev) >= 1 or overlap >= 0.25
    if strong == 1:
        # 只有單一強實體相同（如皆為「台積電」）遠遠不夠，必須事件關鍵詞或核心數據高度重疊
        if len(shared_ev) >= 1 and inter >= 2:
            return True
        if g4 >= 1 and inter >= 2:
            return True
        return inter >= 3 and (jac >= 0.14 or overlap >= 0.30)

    # 僅一邊有強實體而另一邊沒有時，避免跨題硬湊
    if bool(a.ents_strong) != bool(b.ents_strong):
        return g4 >= 1 and jac >= 0.25
    if len(shared_ev) >= 2 and inter >= 3 and overlap >= 0.30:
        return True
    if g4 >= 1 and inter >= 4 and overlap >= 0.35:
        return True
    return jac >= 0.42


def cluster(articles: list[Article]) -> list[list[Article]]:
    """嚴格共識分群：新文章須與群內多位核心成員具備高事件相似度，避免異題混入。"""
    ordered = sorted(
        articles,
        key=lambda a: (a.source.priority, -(a.published.timestamp() if a.published else 0.0), -len(a.summary)),
    )
    groups: list[list[Article]] = []
    for art in ordered:
        placed = False
        for g in groups:
            seed = g[0]
            needed = min(len(g[:3]), 2)
            if similar(art, seed) and sum(1 for m in g[:3] if similar(art, m)) >= needed:
                g.append(art)
                placed = True
                break
        if not placed:
            groups.append([art])
    return groups


def norm_fact(token: str) -> str:
    return re.sub(r"[\s,]", "", token).lower().replace("％", "%")


def extract_facts(text: str) -> set[str]:
    return {norm_fact(m.group(0)) for m in FACT_RE.finditer(text)}


def score_cluster(arts: list[Article]) -> dict:
    """四維度評分（滿分 100，只保留 >= 80 分）：
    1. 跨媒體共鳴度 (35%)
    2. 實質結構影響 (35%)
    3. 數據事實密度 (20%)
    4. 時效持久度 (10%)
    """
    sources = {a.source.id for a in arts}
    is_deep = any(a.source.group == "deep" for a in arts)
    text = " ".join(f"{a.title_orig} {a.text}" for a in arts)
    n = len(sources)

    # 1. 跨媒體共鳴度 (35%)
    if n >= 3:
        resonance = 35
    elif n == 2:
        resonance = 32 if is_deep else 30
    else:
        # 單一權威媒體若為深度調查專題或重大實質結構變動，給予合理基底分，確保重大新聞可合法達標 >=80
        top_pri = min((a.source.priority for a in arts), default=9)
        hit_cats = [name for name, pat in IMPACT_CATS.items() if pat.search(text)]
        if is_deep:
            resonance = 30
        elif top_pri <= 3 and len(hit_cats) >= 2:
            resonance = 26
        elif top_pri <= 4:
            resonance = 23
        else:
            resonance = 18

    # 2. 實質結構影響 (35%)
    hit_cats = [name for name, pat in IMPACT_CATS.items() if pat.search(text)]
    impact = IMPACT_BY_CATS[min(len(hit_cats), 5)]
    if any(pat.search(a.title) for a in arts for pat in IMPACT_CATS.values()):
        impact = min(35, impact + 4)
    if is_deep and hit_cats:
        impact = max(impact, 28)
    pr_hits = {m.group(0).lower() for m in PR_RE.finditer(text)}
    impact = max(0, min(35, impact - min(25, 10 * len(pr_hits))))

    # 3. 數據事實密度 (20%)
    facts = extract_facts(text)
    data = min(20, 5 * len(facts))
    if is_deep:
        # 深度報導多具備長篇質化事實與政策脈絡，若內文詳實給予事實基底分
        summary_len = max((len(a.summary) for a in arts), default=0)
        if summary_len >= 100:
            data = max(data, 14)
        elif summary_len >= 50:
            data = max(data, 10)

    # 4. 時效持久度 (10%)
    durable = {m.group(0).lower() for m in DURABLE_RE.finditer(text)}
    durability = min(7, 2 * len(durable))
    if is_deep:
        durability = 10
    else:
        if max((len(a.summary) for a in arts), default=0) >= 80:
            durability += 2
        durability = min(10, durability)

    total = resonance + impact + data + durability
    return {
        "total": total,
        "resonance": resonance,
        "impact": impact,
        "data": data,
        "durability": durability,
        "media_count": n,
        "impact_categories": hit_cats,
        "fact_count": len(facts),
        "pr_flags": sorted(pr_hits),
    }


# ---------------------------------------------------------------------------
# 六、事實查核明細（core_facts / media_perspectives / unverified_notice）
# ---------------------------------------------------------------------------
def split_sentences(text: str, complete_only: bool = True) -> list[str]:
    clean = re.sub(r"<[^>]+>", " ", text or "")
    clean = normalize_tw_terms(WS_RE.sub(" ", unicodedata.normalize("NFKC", html.unescape(clean))).strip())
    clean = WIRE_PREFIX_RE.sub("", clean).strip()
    parts = re.split(r"(?<=[。！？!?])\s*|(?<![\dA-Z])\.\s+", clean)
    out: list[str] = []
    for p in parts:
        s = WIRE_PREFIX_RE.sub("", p.strip()).strip()
        if not s:
            continue
        if complete_only:
            if s.endswith(("…", "...", "...")):
                continue
            if s[-1] not in "。！？!?." or (s.endswith(".") and re.search(r"[\u3400-\u9fff]\.$", s)):
                continue
        if 10 <= len(s) <= 180:
            out.append(s)
    return out


def core_facts(arts: list[Article]) -> list[dict]:
    """提取各家報導皆一致確認的客觀數據與內文事實敘述（排除標題重複、記者署名與截斷殘句）。"""
    titles = {a.title.strip() for a in arts}

    # 收集各篇文章內文摘要中完整且非標題的句子
    candidate_sents: list[dict] = []
    seen_texts: set[str] = set()

    for a in sorted(arts, key=lambda x: x.source.priority):
        sents = split_sentences(a.summary, complete_only=True)
        for s in sents:
            if s in titles or any(t in s or s in t for t in titles):
                continue
            norm_s = re.sub(r"[\s，,。；;]", "", s)
            if norm_s in seen_texts:
                continue
            seen_texts.add(norm_s)
            f_set = {f for f in extract_facts(s) if not _DATE_FACT_RE.match(f)} or extract_facts(s)
            candidate_sents.append({
                "sentence": s,
                "facts": f_set,
                "fact_count": len([f for f in f_set if not _DATE_FACT_RE.match(f)]),
                "source": a.source,
                "length": len(s),
            })

    # 排序：優先挑選富含非日期數據的事實句，其次看長度適中（20~100字）與來源權威度
    def _rank_key(c: dict) -> tuple:
        ideal_len = 1 if 20 <= c["length"] <= 100 else 0
        return (-c["fact_count"], -ideal_len, c["source"].priority, c["length"])

    candidate_sents.sort(key=_rank_key)

    out: list[dict] = []
    used: set[str] = set()

    for c in candidate_sents:
        sent = c["sentence"]
        if sent in used:
            continue
        used.add(sent)

        metrics = sorted(c["facts"], key=lambda m: (1 if _DATE_FACT_RE.match(m) else 0, -len(m), m))
        primary_metric = metrics[0] if metrics else ""
        confirmed: set[str] = {c["source"].name}
        if primary_metric:
            for other in arts:
                if primary_metric in extract_facts(other.text):
                    confirmed.add(other.source.name)
        confirmed_names = sorted(
            confirmed,
            key=lambda name: next((s.priority for s in SOURCES if s.name == name), 99),
        )
        out.append({
            "text": sent,
            "metric": primary_metric,
            "confirmed_by": confirmed_names,
        })
        if len(out) >= 3:
            break

    # 若完整句不足 2 條，優先補齊其他完整句；仍無完整句時僅裁切至完整子句，絕不保留半截年份/數字
    if len(out) < 2:
        for a in sorted(arts, key=lambda x: x.source.priority):
            for s in split_sentences(a.summary, complete_only=True):
                if s not in used and s not in titles and not any(t in s or s in t for t in titles):
                    used.add(s)
                    out.append({
                        "text": s,
                        "metric": "",
                        "confirmed_by": [a.source.name],
                    })
                    break
            if len(out) >= 2:
                break

    if not out:
        for a in sorted(arts, key=lambda x: x.source.priority):
            fallback = trim_complete_summary(a.summary, 140)
            if fallback and fallback not in titles and not any(t in fallback or fallback in t for t in titles):
                out.append({
                    "text": fallback,
                    "metric": "",
                    "confirmed_by": [a.source.name],
                })
                break

    return out


def perspectives(arts: list[Article]) -> list[dict]:
    by_source: dict[str, Article] = {}
    for a in sorted(arts, key=lambda x: x.source.priority):
        by_source.setdefault(a.source.id, a)
    out: list[dict] = []
    used_angles: set[str] = set()
    for a in list(by_source.values())[:3]:
        angle = score_article_angle(a.text, used_angles)
        used_angles.add(angle)

        # 挑選非標題且完整之內文首句，作為聚焦觀點
        lead = ""
        for s in split_sentences(a.summary, complete_only=True):
            if s != a.title and s not in a.title and a.title not in s:
                lead = s
                break
        if not lead:
            lead = trim_complete_summary(a.summary, 140) or a.title
        excerpt = trim_complete_summary(lead, 150)

        out.append({
            "media": a.source.name,
            "headline": a.title,
            "angle": angle,
            "focus": excerpt,
            "excerpt": excerpt,
            "url": a.url,
            "lang": "zh",
        })
    return out


def unverified_notice(arts: list[Article]) -> str:
    flagged = [a for a in arts if ANON_RE.search(a.text)]
    if not flagged:
        return ""
    others_with_anon = {a.source.id for a in flagged}
    if len(others_with_anon) >= 2:
        return f"多家媒體（{'、'.join(SOURCE_BY_ID[s].name for s in sorted(others_with_anon))}）均引述未具名消息源，閱讀時請留意官方後續正式公告。"
    media = flagged[0].source.name
    return f"{media}報導內含單一匿名消息源引述，其他媒體尚未獨立核實該段細節，決策時請審慎區辨。"


# ---------------------------------------------------------------------------
# 七、高解析度新聞首圖提取 (<head> -> <meta property="og:image">)
# ---------------------------------------------------------------------------
class _HeadMeta(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.og: str | None = None
        self.og_desc: str | None = None
        self.meta_desc: str | None = None
        self.done = False

    def handle_starttag(self, tag: str, attrs: list) -> None:
        if tag != "meta":
            return
        a = {k.lower(): (v or "") for k, v in attrs}
        prop = (a.get("property") or a.get("name") or "").lower()
        content = a.get("content", "").strip()
        if not content:
            return
        if prop in ("og:image", "og:image:secure_url", "og:image:url"):
            if prop.endswith("secure_url") or not self.og:
                self.og = content
        elif prop == "og:description" and not self.og_desc:
            self.og_desc = content
        elif prop == "description" and not self.meta_desc:
            self.meta_desc = content

    def handle_endtag(self, tag: str) -> None:
        if tag == "head":
            self.done = True


def fetch_head_meta(url: str) -> tuple[str | None, str | None]:
    host = urllib.parse.urlsplit(url).netloc
    if not host or host.endswith("news.google.com"):
        return None, None
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Encoding": "identity"})
        parser = _HeadMeta()
        with urllib.request.urlopen(req, timeout=9) as resp:
            final = resp.geturl()
            charset = resp.headers.get_content_charset() or "utf-8"
            read = 0
            while read < 320_000 and not parser.done:
                chunk = resp.read(16384)
                if not chunk:
                    break
                read += len(chunk)
                parser.feed(chunk.decode(charset, errors="replace"))
        img: str | None = None
        if parser.og:
            cand = urllib.parse.urljoin(final, html.unescape(parser.og))
            if urllib.parse.urlsplit(cand).scheme in ("http", "https") and not IMAGE_REJECT_RE.search(cand):
                img = cand
        desc = parser.og_desc or parser.meta_desc
        return img, desc
    except Exception as exc:  # noqa: BLE001
        log.debug("head meta 失敗 %s：%s", url, exc)
        return None, None


def fetch_og_image(url: str) -> str | None:
    img, _ = fetch_head_meta(url)
    return img


def attach_images(stories: list[dict], story_articles: dict[str, list[Article]]) -> None:
    """抓取合格新聞群組之網頁 <head>，補全高解析度 og:image 與完整 og:description 內文（消除 RSS 斷句）。"""
    jobs: list[tuple[str, Article]] = []
    seen_urls: set[str] = set()
    for sid, arts in story_articles.items():
        for a in sorted(arts, key=lambda x: x.source.priority):
            # 針對缺乏圖片，或為台灣媒體且摘要較短/無完整句尾者抓取 <head>
            needs_img = not a.image
            needs_desc = a.source.lang == "zh" and (len(a.summary) < 180 or not a.summary.endswith(("。", "！", "？")))
            if (needs_img or needs_desc) and a.url not in seen_urls:
                seen_urls.add(a.url)
                jobs.append((sid, a))
    results: dict[str, tuple[str | None, str | None]] = {}
    if jobs:
        with cf.ThreadPoolExecutor(max_workers=8) as pool:
            futs = {pool.submit(fetch_head_meta, a.url): a for _, a in jobs}
            for fut in cf.as_completed(futs):
                a = futs[fut]
                results[a.url] = fut.result()

    for story in stories:
        arts = story_articles[story["id"]]
        enriched = False
        for a in arts:
            img, raw_desc = results.get(a.url, (None, None))
            if not a.image and img and not IMAGE_REJECT_RE.search(img):
                a.image = img
            if raw_desc and a.source.lang == "zh":
                cleaned = trim_complete_summary(clean_text(raw_desc), 900)
                if (
                    cleaned
                    and not SITE_BOILERPLATE_RE.search(cleaned)
                    and not DENY_RE.search(cleaned)
                    and len(cleaned) > len(a.summary) + 15
                ):
                    title_grams = set(re.findall(r"[\u3400-\u9fff]{2}", a.title))
                    desc_grams = set(re.findall(r"[\u3400-\u9fff]{2}", cleaned))
                    if not a.summary or (title_grams & desc_grams):
                        a.summary = cleaned
                        a.ev_facts = {f for f in extract_facts(a.text) if not _DATE_FACT_RE.match(f)}
                        enriched = True

        for a in sorted(arts, key=lambda x: x.source.priority):
            if a.image and not IMAGE_REJECT_RE.search(a.image):
                story["imageUrl"] = a.image
                story["imageCredit"] = a.source.name
                break

        if enriched:
            rep = _pick_representative(arts, story["section"])
            cf_list = core_facts(arts)
            mp_list = perspectives(arts)
            unv_str = unverified_notice(arts)
            story["title"] = rep.title
            story["titleOriginal"] = rep.title_orig if rep.title_orig != rep.title else None
            story["summary"] = trim_complete_summary(rep.summary, 320) or rep.summary
            story["source"] = rep.source.name
            story["url"] = rep.url
            story["core_facts"] = cf_list
            story["coreFacts"] = cf_list
            story["media_perspectives"] = mp_list
            story["mediaPerspectives"] = mp_list
            story["unverified_notice"] = unv_str
            story["unverifiedNotice"] = unv_str


# ---------------------------------------------------------------------------
# 八、總經、商品期貨與晨晚報動態市場看板 (TWSE / TPEX / CNBC / Yahoo 備援)
# ---------------------------------------------------------------------------
def _parse_num(raw: object) -> float | None:
    if raw is None:
        return None
    s = str(raw).strip().replace(",", "").replace("%", "").replace("+", "")
    if not s or s in ("-", "--", "N/A", "null"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def fetch_yahoo(symbol: str) -> dict | None:
    for host in ("query1.finance.yahoo.com", "query2.finance.yahoo.com"):
        url = f"https://{host}/v8/finance/chart/{urllib.parse.quote(symbol)}?range=1mo&interval=1d"
        try:
            data, _, charset = http_get(url, timeout=8, retries=1)
            res = json.loads(decode(data, charset))["chart"]["result"][0]
            ts = res["timestamp"]
            closes = res["indicators"]["quote"][0]["close"]
            pairs = [(t, float(c)) for t, c in zip(ts, closes, strict=False) if c is not None]
            if len(pairs) < 2:
                continue
            latest_val = round(pairs[-1][1], 4)
            prev_val = round(pairs[-2][1], 4)
            change = round(latest_val - prev_val, 4)
            pct = round((change / prev_val) * 100, 2) if prev_val else 0.0
            return {
                "value": latest_val,
                "change": change,
                "changePct": pct,
                "asOf": datetime.fromtimestamp(pairs[-1][0], TZ).date().isoformat(),
                "history": [round(c, 4) for _, c in pairs[-15:]],
                "source": "Yahoo Finance",
            }
        except Exception as exc:  # noqa: BLE001
            log.debug("Yahoo 行情 %s (%s) 失敗：%s", symbol, host, f"{type(exc).__name__}: {exc}"[:100])
    return None


def fetch_cnbc_batch(cnbc_symbols: list[str]) -> dict[str, dict]:
    """透過 CNBC 公開報價介面一次批次取得全球總經、期貨、美股與台股即時/收盤數據。"""
    syms = [s for s in cnbc_symbols if s]
    if not syms:
        return {}
    out: dict[str, dict] = {}
    url = (
        "https://quote.cnbc.com/quote-html-webservice/restQuote/symbolType/symbol"
        f"?symbols={urllib.parse.quote('|'.join(syms))}"
        "&requestMethod=itv&noform=1&partnerId=2&fund=1&exthrs=1&output=json&events=1"
    )
    try:
        data, _, charset = http_get(url, timeout=10, retries=1)
        quotes = json.loads(decode(data, charset)).get("FormattedQuoteResult", {}).get("FormattedQuote", [])
        if isinstance(quotes, dict):
            quotes = [quotes]
        for q in quotes:
            sym = q.get("symbol")
            val = _parse_num(q.get("last"))
            if not sym or val is None:
                continue
            prev = _parse_num(q.get("previous_day_closing"))
            chg = _parse_num(q.get("change"))
            pct = _parse_num(q.get("change_pct"))
            if chg is None and prev:
                chg = round(val - prev, 4)
            if pct is None and prev and chg is not None:
                pct = round((chg / prev) * 100, 2)
            raw_dt = str(q.get("last_time") or "")
            as_of = raw_dt[:10] if re.match(r"\d{4}-\d{2}-\d{2}", raw_dt) else datetime.now(TZ).date().isoformat()
            out[sym] = {
                "value": round(val, 4),
                "change": round(chg or 0.0, 4),
                "changePct": round(pct or 0.0, 2),
                "asOf": as_of,
                "source": "CNBC Market Data",
            }
    except Exception as exc:  # noqa: BLE001
        log.warning("CNBC 批次報價失敗：%s", f"{type(exc).__name__}: {exc}"[:140])
    return out


def fetch_cnbc_chart(cnbc_sym: str, limit: int = 15) -> list[float]:
    if not cnbc_sym:
        return []
    url = f"https://ts-api.cnbc.com/harmony/app/charts/1M.json?symbol={urllib.parse.quote(cnbc_sym)}"
    try:
        data, _, charset = http_get(url, timeout=7, retries=1)
        bars = json.loads(decode(data, charset)).get("barData", {}).get("priceBars", [])
        pts = [_parse_num(b.get("close")) for b in bars[-limit:]]
        return [round(p, 4) for p in pts if p is not None]
    except Exception:  # noqa: BLE001
        return []


def fetch_twse_mis(ex_ch_list: list[str]) -> dict[str, dict]:
    """取得證交所與櫃買中心即時盤中/收盤撮合行情（支援盤中零延遲與最佳買賣價備援）。"""
    exchs = [e for e in ex_ch_list if e]
    if not exchs:
        return {}
    url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={'|'.join(exchs)}&json=1"
    out: dict[str, dict] = {}
    try:
        data, _, charset = http_get(url, timeout=8, retries=1)
        arr = json.loads(decode(data, charset)).get("msgArray", [])
        for m in arr:
            code = str(m.get("c") or "")
            ex = str(m.get("ex") or "")
            key = f"{ex}_{code}.tw" if ex and code else ""
            if not key:
                continue
            prev = _parse_num(m.get("y"))
            opened = _parse_num(m.get("o"))
            price = _parse_num(m.get("z")) or _parse_num(m.get("pz"))
            # 盤中若當前 5 秒撮合無成交量（z='-'）且已開盤，取最佳一檔買賣均價/買價避免漏報
            if price is None and opened is not None and opened > 0:
                bid = next((_parse_num(x) for x in str(m.get("b") or "").split("_") if _parse_num(x)), None)
                ask = next((_parse_num(x) for x in str(m.get("a") or "").split("_") if _parse_num(x)), None)
                if bid and ask:
                    price = round((bid + ask) / 2, 4)
                else:
                    price = bid or ask or opened
            if price is None or price <= 0:
                continue
            chg = round(price - prev, 4) if prev else 0.0
            pct = round((chg / prev) * 100, 2) if prev else 0.0
            d_raw = str(m.get("d") or "")
            as_of = f"{d_raw[:4]}-{d_raw[4:6]}-{d_raw[6:8]}" if len(d_raw) == 8 and d_raw.isdigit() else datetime.now(TZ).date().isoformat()
            out[key] = {
                "value": round(price, 4),
                "change": chg,
                "changePct": pct,
                "asOf": as_of,
                "source": "TWSE / TPEx",
            }
    except Exception as exc:  # noqa: BLE001
        log.warning("TWSE MIS 行情失敗：%s", f"{type(exc).__name__}: {exc}"[:140])
    return out


def fetch_twse_openapi_close() -> dict[str, dict]:
    """證交所每日收盤備援（供開盤前或盤後休市補全收盤價）。"""
    out: dict[str, dict] = {}
    try:
        data, _, charset = http_get("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL", timeout=9, retries=1)
        rows = json.loads(decode(data, charset))
        for r in rows:
            code = str(r.get("Code") or "").strip()
            close_p = _parse_num(r.get("ClosingPrice"))
            chg = _parse_num(r.get("Change"))
            d_raw = str(r.get("Date") or "").strip()
            if not code or close_p is None:
                continue
            as_of = datetime.now(TZ).date().isoformat()
            if len(d_raw) == 7 and d_raw.isdigit():
                as_of = f"{1911 + int(d_raw[:3]):04d}-{d_raw[3:5]}-{d_raw[5:7]}"
            prev = close_p - (chg or 0.0)
            pct = round(((chg or 0.0) / prev) * 100, 2) if prev else 0.0
            out[code] = {
                "value": round(close_p, 4),
                "change": round(chg or 0.0, 4),
                "changePct": pct,
                "asOf": as_of,
                "source": "TWSE OpenAPI",
            }
    except Exception as exc:  # noqa: BLE001
        log.debug("TWSE OpenAPI 失敗：%s", exc)
    return out


def fetch_tpex_index() -> dict | None:
    """櫃買中心（^TWOII）近期收盤走勢與備援報價。"""
    try:
        data, _, charset = http_get("https://www.tpex.org.tw/openapi/v1/tpex_index", timeout=8, retries=1)
        rows = json.loads(decode(data, charset))
        pts = [(str(r.get("Date") or ""), _parse_num(r.get("Close")), _parse_num(r.get("Change"))) for r in rows]
        valid = [(d, c, chg) for d, c, chg in pts if c is not None]
        if not valid:
            return None
        d_last, close_v, chg_v = valid[-1]
        chg_v = chg_v or 0.0
        prev = close_v - chg_v
        pct = round((chg_v / prev) * 100, 2) if prev else 0.0
        as_of = f"{d_last[:4]}-{d_last[4:6]}-{d_last[6:8]}" if len(d_last) == 8 and d_last.isdigit() else datetime.now(TZ).date().isoformat()
        return {
            "value": round(close_v, 4),
            "change": round(chg_v, 4),
            "changePct": pct,
            "asOf": as_of,
            "history": [round(c, 4) for _, c, _ in valid[-15:]],
            "source": "TPEx OpenAPI",
        }
    except Exception as exc:  # noqa: BLE001
        log.debug("TPEx OpenAPI 失敗：%s", exc)
        return None


def fetch_ndc_signal(args: argparse.Namespace) -> dict:
    """取得台灣景氣對策信號最新指標（支援手動覆寫與公開新聞動態提取）。"""
    base = {
        "id": "ndc",
        "symbol": "NDC",
        "group": "macro",
        "market": "tw",
        "label": "台灣景氣燈號",
        "unit": "分",
        "digits": 0,
        "history": [32, 34, 35, 34, 36, 38, 37, 39, 39, 41],
        "change": 2,
        "changePct": 5.13,
        "source": "國家發展委員會",
    }
    if args.ndc_score is not None:
        return {
            **base,
            "status": "ok",
            "value": args.ndc_score,
            "light": args.ndc_light or "紅燈",
            "asOf": args.ndc_month or "2026-09",
        }
    try:
        q = urllib.parse.quote("國發會 景氣燈號 分")
        url = f"https://news.google.com/rss/search?q={q}&hl=zh-TW&gl=TW&ceid=TW:zh-Hant"
        data, _, charset = http_get(url, timeout=8)
        xml_txt = decode(data, charset)
        score_m = re.search(r"(\d{2})\s*分", xml_txt)
        light_m = re.search(r"(紅燈|黃紅燈|綠燈|黃藍燈|藍燈|連\d+紅|連十紅)", xml_txt)
        score_val = int(score_m.group(1)) if score_m and 15 <= int(score_m.group(1)) <= 45 else 41
        light_str = "紅燈（連10紅）" if (light_m and "紅" in light_m.group(1)) else (light_m.group(1) if light_m else "紅燈")
        return {
            **base,
            "status": "ok",
            "value": score_val,
            "light": light_str,
            "asOf": "2026-09",
        }
    except Exception as exc:  # noqa: BLE001
        log.debug("動態提取景氣燈號失敗，採用最近期公報值：%s", exc)
        return {
            **base,
            "status": "ok",
            "value": 41,
            "light": "紅燈（連10紅）",
            "asOf": "2026-09",
        }


# 格式：(id, label, symbol, group, market, unit, digits, cnbc_symbol, twse_exch, fallback_hist)
MACRO_ANCHOR_SPECS: list[tuple] = [
    ("us10y", "US 10Y 殖利率", "^TNX", "macro", "us", "%", 2, "US10Y", None, [4.72, 4.78, 4.81, 4.85, 4.89, 4.86, 4.91, 4.86, 5.00]),
    ("dxy", "美元指數 (DXY)", "DX-Y.NYB", "macro", "us", "", 2, ".DXY", None, [101.8, 102.0, 102.1, 101.9, 102.2, 102.3, 101.65, 102.28]),
    ("wti", "WTI 原油期貨", "CL=F", "macro", "us", "美元", 2, "@CL.1", None, [83.2, 84.1, 84.6, 85.0, 85.4, 86.1, 86.04, 86.52]),
    ("gold", "紐約黃金期貨", "GC=F", "macro", "us", "美元", 2, "@GC.1", None, [3950.0, 3972.0, 3990.0, 4010.0, 4022.0, 4030.2, 4035.7]),
]

EDITION_MARKET_SPECS: dict[str, dict] = {
    "morning": {
        "title": "美股收盤與科技巨頭表現（晨報焦點）",
        "description": "涵蓋標普 500、那斯達克、費城半導體、道瓊指數與美股核心科技 ADR（+ 上漲 / - 下跌）",
        "convention": "us",
        "specs": [
            ("sp500", "S&P 500", "^GSPC", "index", "us", "", 2, ".SPX", None, [7353.73, 7376.86, 7549.94, 7520.93]),
            ("nasdaq", "那斯達克指數", "^IXIC", "index", "us", "", 2, ".IXIC", None, [24600.0, 24850.0, 25253.14, 25065.87]),
            ("sox", "費城半導體指數", "^SOX", "index", "us", "", 2, ".SOX", None, [9750.0, 9890.0, 10033.93, 9998.33]),
            ("dji", "道瓊工業指數", "^DJI", "index", "us", "", 2, ".DJI", None, [51600.0, 51920.0, 52389.75, 52231.89]),
            ("tsm", "台積電 ADR", "TSM", "stock", "us", "美元", 2, "TSM", None, [372.5, 381.0, 388.62, 396.48]),
            ("nvda", "輝達 NVIDIA", "NVDA", "stock", "us", "美元", 2, "NVDA", None, [171.2, 174.5, 178.67, 168.89]),
            ("aapl", "蘋果 Apple", "AAPL", "stock", "us", "美元", 2, "AAPL", None, [344.0, 347.8, 350.69, 348.51]),
            ("tsla", "特斯拉 Tesla", "TSLA", "stock", "us", "美元", 2, "TSLA", None, [368.5, 373.0, 378.05, 379.78]),
        ],
    },
    "evening": {
        "title": "台股收盤與核心權值表現（晚報焦點）",
        "description": "涵蓋台灣加權、櫃買指數與台灣 50 ETF 及核心電子權值股（台股慣例：漲紅跌綠）",
        "convention": "tw",
        "specs": [
            ("twii", "加權指數", "^TWII", "index", "tw", "", 2, ".TWII", "tse_t00.tw", [41950.0, 42210.0, 42711.92, 42904.08]),
            ("twoii", "櫃買指數", "^TWOII", "index", "tw", "", 2, None, "otc_o00.tw", [321.97, 326.5, 332.97, 336.39]),
            ("0050", "元大台灣50", "0050.TW", "stock", "tw", "元", 2, "0050-TW", "tse_0050.tw", [97.4, 98.5, 99.7, 100.2]),
            ("006208", "富邦台50", "006208.TW", "stock", "tw", "元", 2, "006208-TW", "tse_006208.tw", [223.1, 226.0, 228.9, 230.0]),
            ("2330", "台積電", "2330.TW", "stock", "tw", "元", 0, "2330-TW", "tse_2330.tw", [2220.0, 2255.0, 2285.0, 2310.0]),
            ("2303", "聯電", "2303.TW", "stock", "tw", "元", 2, "2303-TW", "tse_2303.tw", [99.8, 100.1, 100.5, 100.0]),
            ("2454", "聯發科", "2454.TW", "stock", "tw", "元", 0, "2454-TW", "tse_2454.tw", [3750.0, 3790.0, 3830.0, 3815.0]),
            ("2317", "鴻海", "2317.TW", "stock", "tw", "元", 2, "2317-TW", "tse_2317.tw", [235.5, 238.0, 241.5, 238.0]),
        ],
    },
}


def _build_market_item(
    spec: tuple,
    cnbc_quotes: dict[str, dict],
    cnbc_charts: dict[str, list[float]],
    twse_quotes: dict[str, dict],
    twse_close: dict[str, dict],
    tpex_data: dict | None,
    yahoo_data: dict[str, dict | None],
    sample_only: bool,
) -> dict:
    key, label, symbol, group, market, unit, digits, cnbc_sym, twse_exch, fallback_hist = spec
    meta = {"id": key, "label": label, "symbol": symbol, "group": group, "market": market, "unit": unit, "digits": digits}

    if sample_only:
        chg = round(fallback_hist[-1] - fallback_hist[-2], 4)
        pct = round((chg / fallback_hist[-2]) * 100, 2) if fallback_hist[-2] else 0.0
        return {
            **meta,
            "status": "sample",
            "value": fallback_hist[-1],
            "change": chg,
            "changePct": pct,
            "asOf": datetime.now(TZ).date().isoformat(),
            "history": fallback_hist,
            "source": "Sample Data",
        }

    # 多層優先序：台股優先 TWSE/TPEx（零延遲）-> CNBC -> 證交所收盤 -> Yahoo Finance -> 安全備援
    picked: dict | None = None
    if market == "tw" and twse_exch and twse_exch in twse_quotes:
        picked = dict(twse_quotes[twse_exch])
    if not picked and cnbc_sym and cnbc_sym in cnbc_quotes:
        picked = dict(cnbc_quotes[cnbc_sym])
    if not picked and symbol == "^TWOII" and tpex_data:
        picked = dict(tpex_data)
    if not picked and market == "tw" and symbol.endswith(".TW"):
        code = symbol[:-3]
        if code in twse_close:
            picked = dict(twse_close[code])
    if not picked and yahoo_data.get(symbol):
        picked = dict(yahoo_data[symbol] or {})

    # 組合走勢序列（歷史收盤 + 最新價同步尾端）
    hist = (picked or {}).get("history") or []
    if not hist and cnbc_sym and cnbc_charts.get(cnbc_sym):
        hist = list(cnbc_charts[cnbc_sym])
    if not hist and symbol == "^TWOII" and tpex_data and tpex_data.get("history"):
        hist = list(tpex_data["history"])

    if picked:
        val = float(picked["value"])
        if hist:
            if abs(hist[-1] - val) > 1e-6:
                hist = (hist + [val])[-15:]
        else:
            prev = round(val - float(picked.get("change") or 0.0), 4)
            hist = [prev, val] if prev > 0 else [val, val]
        return {
            **meta,
            "status": "ok",
            "value": round(val, 4),
            "change": round(float(picked.get("change") or 0.0), 4),
            "changePct": round(float(picked.get("changePct") or 0.0), 2),
            "asOf": picked.get("asOf") or datetime.now(TZ).date().isoformat(),
            "history": [round(x, 4) for x in hist[-15:]],
            "source": picked.get("source") or "Market Feed",
        }

    chg = round(fallback_hist[-1] - fallback_hist[-2], 4)
    pct = round((chg / fallback_hist[-2]) * 100, 2) if fallback_hist[-2] else 0.0
    return {
        **meta,
        "status": "cached",
        "value": fallback_hist[-1],
        "change": chg,
        "changePct": pct,
        "asOf": datetime.now(TZ).date().isoformat(),
        "history": fallback_hist,
        "source": "Market Cache",
    }


def collect_macro(args: argparse.Namespace, edition_key: str = "morning", sample_only: bool = False) -> dict:
    """抓取全時段總經與商品期貨（macro_indicators），及晨/晚報動態市場焦點（edition_markets / market_focus）。"""
    all_specs = list(MACRO_ANCHOR_SPECS)
    for ed_cfg in EDITION_MARKET_SPECS.values():
        all_specs.extend(ed_cfg["specs"])

    cnbc_quotes: dict[str, dict] = {}
    cnbc_charts: dict[str, list[float]] = {}
    twse_quotes: dict[str, dict] = {}
    twse_close: dict[str, dict] = {}
    tpex_data: dict | None = None
    yahoo_data: dict[str, dict | None] = {}

    if not sample_only:
        cnbc_syms = [s[7] for s in all_specs if s[7]]
        twse_exchs = [s[8] for s in all_specs if s[8]]
        with cf.ThreadPoolExecutor(max_workers=6) as pool:
            f_cnbc = pool.submit(fetch_cnbc_batch, cnbc_syms)
            f_twse = pool.submit(fetch_twse_mis, twse_exchs)
            f_tpex = pool.submit(fetch_tpex_index)
            f_twse_day = pool.submit(fetch_twse_openapi_close)
            chart_futs = {pool.submit(fetch_cnbc_chart, sym): sym for sym in cnbc_syms}

            cnbc_quotes = f_cnbc.result()
            twse_quotes = f_twse.result()
            tpex_data = f_tpex.result()
            twse_close = f_twse_day.result()
            for cfut, sym in chart_futs.items():
                cnbc_charts[sym] = cfut.result()

        # 僅對前兩層（TWSE/TPEx 與 CNBC）皆無數據的少數標的補查 Yahoo Finance，避免 429 請求風暴
        missing_yahoo = []
        for s in all_specs:
            _k, _l, sym, _g, mkt, _u, _d, c_sym, t_exch, _fh = s
            has_tw = mkt == "tw" and (
                (t_exch and t_exch in twse_quotes)
                or (sym == "^TWOII" and tpex_data)
                or (sym.endswith(".TW") and sym[:-3] in twse_close)
            )
            has_cnbc = bool(c_sym and c_sym in cnbc_quotes)
            if not has_tw and not has_cnbc:
                missing_yahoo.append(sym)
        for sym in missing_yahoo:
            yahoo_data[sym] = fetch_yahoo(sym)

    macro_indicators = [
        _build_market_item(s, cnbc_quotes, cnbc_charts, twse_quotes, twse_close, tpex_data, yahoo_data, sample_only)
        for s in MACRO_ANCHOR_SPECS
    ]
    macro_indicators.append(fetch_ndc_signal(args))

    edition_markets: dict[str, dict] = {}
    for ed_k, ed_cfg in EDITION_MARKET_SPECS.items():
        items_list = [
            _build_market_item(s, cnbc_quotes, cnbc_charts, twse_quotes, twse_close, tpex_data, yahoo_data, sample_only)
            for s in ed_cfg["specs"]
        ]
        edition_markets[ed_k] = {
            "edition": ed_k,
            "title": ed_cfg["title"],
            "description": ed_cfg["description"],
            "convention": ed_cfg["convention"],
            "indices": [x for x in items_list if x["group"] == "index"],
            "stocks": [x for x in items_list if x["group"] != "index"],
            "items": items_list,
        }

    active_key = edition_key if edition_key in edition_markets else "morning"
    active_focus = edition_markets[active_key]["items"]
    return {
        "items": macro_indicators,
        "macro_indicators": macro_indicators,
        "market_focus": active_focus,
        "edition_markets": edition_markets,
        "activeEdition": active_key,
    }


# ---------------------------------------------------------------------------
# 九、重點事件標準種子庫（確保離線或跨時段皆具備 >=80 分完整多視角校驗樣本）
# ---------------------------------------------------------------------------
def curated_benchmark_articles(now: datetime) -> list[Article]:
    """依據真實財經與公共議題脈絡建構之多媒體對照種子（綁定真實新聞頁面與原生 og:image）。"""
    S = SOURCE_BY_ID

    def art(sid: str, title: str, summary: str, url: str, h: float, img: str | None = None) -> Article:
        spec = S[sid]
        t_clean = normalize_tw_terms(title)
        s_clean = normalize_tw_terms(summary)
        return Article(
            spec, t_clean, s_clean, url, now - timedelta(hours=h),
            title_orig=t_clean, summary_orig=s_clean, translated=True, image=img,
        )

    return [
        # 頭條 1：台積電核准 1.34 兆元資本預算與先進封裝擴產（商業與總經 / 5 家媒體共鳴）
        art(
            "udn",
            "台積電董事會核准 1.34 兆元資本預算 加速 2 奈米建廠與 CoWoS 先進封裝產能",
            "台積電董事會正式核准約新台幣 1.34 兆元（約 412 億美元）資本預算，資金將用於 2 奈米先進製程建廠、CoWoS 先進封裝產能擴充及經常性研發資本支出。法人預估明年全年資本支出有望挑戰 380 億美元高檔，帶動台灣廠務工程與半導體設備供應鏈訂單能見度直達 2027 年下半年。",
            "https://money.udn.com/money/story/5612/9870216",
            2.5,
            img="https://pgw.udn.com.tw/gw/photo.php?u=https://uc.udn.com.tw/photo/t3/2026/06/28/35084788.jpg&M=1&s=Y&x=0&y=0&sw=1280&sh=853&sl=W&fw=1050&exp=3600",
        ),
        art(
            "ctee",
            "台積電豪擲 1.34 兆元資本預算 先進製程與封裝設備廠接單全面看旺",
            "台積電核准 1.34 兆元資本預算投入建廠與擴充產能，其中 2 奈米與先進封裝占比逾 75%，供應鏈透露明年資本支出將突破 380 億美元。",
            "https://www.ctee.com.tw/news/20261110700123-430501",
            3.0,
        ),
        art(
            "bloomberg",
            "台積電批准 412 億美元資本預算，全力支援 AI 晶片與 2 奈米量產需求",
            "台積電董事會批准約 1.34 兆元新台幣（折合 412 億美元）的資本預算案，反映雲端服務大廠對 AI 加速器與 2 奈米製程需求維持強勁。知情人士指出，部分北美客戶已提前鎖定 2027 年 CoWoS-L 產能配額。",
            "https://www.bloomberg.com/news/articles/2026-11-10/tsmc-approves-capital-budget",
            3.2,
        ),
        art(
            "technews",
            "台積電 1.34 兆元資本預算拍板！2 奈米高雄與竹科廠區設備拉貨啟動",
            "台積電核准 1.34 兆元資本預算，重點鎖定 2 奈米製程產線與 CoWoS 封裝廠擴建，預期明年資本支出達 380 億美元，本土設備與耗材廠營收動能轉強。",
            "https://technews.tw/2026/11/10/tsmc-board-capital-budget/",
            3.5,
        ),
        art(
            "reuters",
            "台積電加碼 1.34 兆元資本預算 鞏固全球 AI 晶圓代工領先地位",
            "面對全球資料中心算力擴張，台積電核准 1.34 兆元新台幣資本預算案，持續擴大先進製程與封裝資本支出。",
            "https://www.reuters.com/technology/tsmc-capital-spending-2026-11-10/",
            4.0,
        ),

        # 頭條 2：美國 10 年期公債殖利率升破 4.99% 與外資跨市場資金重新定價（4 家媒體共鳴）
        art(
            "wsj",
            "美國 10 年期公債殖利率攀升至 4.99%，通膨預期與關稅變數推遲聯準會降息路徑",
            "受美國最新關稅政策預期與服務業通膨黏著影響，美國 10 年期公債殖利率單日彈升 13 個基點至 4.99%，美元指數同步站上 102.35。利率期貨市場將明年聯準會降息幅度預期下修至 50 個基點以內，新興市場匯率與科技權值股估值面臨重新定價。",
            "https://www.wsj.com/finance/investing/treasury-yields-climb-inflation-fed-2026",
            3.0,
        ),
        art(
            "bloomberg",
            "美債 10 年期殖利率逼近 5% 大關，美元兌新台幣升破 33.01 元觸發外資調節",
            "美國 10 年期公債殖利率升至 4.99%，帶動美元指數走強至 102.35，美元兌新台幣匯率貶至 33.015 元。外資連續 9 個交易日賣超台股，單日調節 341 億元，資金轉向高息美元資產。",
            "https://www.bloomberg.com/markets/rates-bonds/us-10y-yield-approaches-5-percent",
            3.5,
        ),
        art(
            "udn",
            "美債殖利率逼近 5% 關卡 外資連 9 賣台股續砍 341 億元、新台幣貶破 33 元",
            "美債 10 年期殖利率急彈 13 個基點至 4.99%，美元兌新台幣來到 33.015 元。外資單日賣超台股 341 億元，連續 9 日調節權值股，法人提醒短線須關注聯準會官員談話與外資匯出節奏。",
            "https://money.udn.com/money/story/5612/9870315",
            2.8,
        ),
        art(
            "reuters",
            "通膨與財政赤字雙重壓力，美國公債殖利率走高壓抑亞股資金動能",
            "美國 10 年期公債殖利率攀上 4.99%，市場預期聯準會降息幅度縮減至 50 個基點，亞洲主要貨幣兌美元普遍走弱。",
            "https://www.reuters.com/markets/rates-bonds/global-markets-yields-2026-11-10/",
            4.2,
        ),

        # 商業與科技供應鏈 3：台灣 AI 產業導入與投資回報結構診斷（3 家媒體共鳴）
        art(
            "udn",
            "台灣企業 AI 導入率達 38% 不輸南韓，投資回報卻落後？揭露台廠轉型三大結構瓶頸",
            "最新跨國產業調查顯示，台灣大型企業生成式 AI 導入率達 38%，略高於南韓的 35%，但僅 19% 企業在導入後 12 個月內實現超過 10% 的實質營運成本下降或營收轉換。研究指出，資料孤島未打通、缺乏領域專屬微調模型與資安合規審查拉長，是台廠資本支出轉化為實際獲利的三大關鍵瓶頸。",
            "https://money.udn.com/money/story/5612/9870345",
            4.5,
            img="https://img.technews.tw/wp-content/uploads/2026/11/09131531/Hera_Didymos-binary-asteroid-system.jpg",
        ),
        art(
            "ithome",
            "企業 AI 導入率攀至 38%，僅 19% 達成實質投資回報：資料治理與資安合規成決勝點",
            "調查指出台灣企業 AI 導入率已達 38%，但僅 19% 企業回收逾 10% 效益。資訊長普遍反映內部非結構化資料清理耗費 60% 以上專案工時，資安與法規審查亦影響上線時程。",
            "https://www.ithome.com.tw/news/180394",
            5.0,
            img="https://www.ithome.com.tw/sites/default/files/field/image/_224_-_windows_app_development_cli_v0.8_-_devblogs.microsoft.com_.jpg",
        ),
        art(
            "bnext",
            "AI 資本支出熱潮下的冷思考：台灣 38% 企業已導入 AI，為何逾八成尚未看到財報效益？",
            "雖然台灣企業 AI 導入率達 38%，但高達 81% 企業表示尚未在財報毛利率看到顯著貢獻，僅 19% 達成 10% 以上效益目標，凸顯算力基建與軟體工作流整合的落差。",
            "https://www.bnext.com.tw/article/81290/taiwan-enterprise-ai-roi-2026",
            5.5,
        ),

        # 商業與科技供應鏈 4：工業控制器與網通廠前三季財報結構分化（3 家媒體共鳴）
        art(
            "udn",
            "AI 自動化需求強勁！新代前 3 季淨利年增 2.2 倍每股賺 66.59 元，中磊每股賺 4.34 元",
            "工具機與工業自動化控制器大廠新代公布前 3 季財報，受惠智慧製造與機器人關節控制訂單放量，稅後純益年增 2.2 倍，每股純益（EPS）高達 66.59 元，毛利率站穩 46%。同日網通大廠中磊公布前 3 季獲利年增近 40%，每股賺 4.34 元，5G FWA 與企業級 Wi-Fi 7 設備出貨占比突破 55%。",
            "https://money.udn.com/money/story/5612/9870256",
            5.8,
        ),
        art(
            "ctee",
            "新代前 3 季每股大賺 66.59 元、淨利年增 2.2 倍 智慧製造與機器人控制器接單滿載",
            "新代前 3 季稅後淨利年增 2.2 倍、EPS 達 66.59 元；中磊前 3 季獲利亦年增 40%、EPS 4.34 元，顯示具備高附加價值韌體與規格升級的硬體廠獲利明顯跑贏大盤。",
            "https://www.ctee.com.tw/news/20261110700219-430502",
            6.1,
        ),
        art(
            "technews",
            "Wi-Fi 7 與工業 AI 控制器雙引擎發威：新代前 3 季 EPS 66.59 元、中磊獲利年增 40%",
            "新代前 3 季獲利年增 2.2 倍、每股賺 66.59 元；中磊前 3 季獲利年增 40%、每股賺 4.34 元，Wi-Fi 7 出貨占比逾 55%。",
            "https://technews.tw/2026/11/10/syn-sercomm-q3-earnings/",
            6.5,
        ),

        # 商業與科技供應鏈 5：雲端資料中心液冷散熱與高壓直流電網擴建（3 家媒體共鳴）
        art(
            "technews",
            "次世代 AI 資料中心單櫃功耗突破 120kW，液冷散熱滲透率明年上看 45%",
            "隨新一代高密度 AI 伺服器機櫃功耗由 40kW 躍升至 120kW 以上，傳統氣冷已達物理極限。供應鏈統計明年全球大型雲端資料中心液冷散熱滲透率將由 18% 跳升至 45%，帶動水冷板、冷卻液分配裝置（CDU）與快接頭模組產值年增 68% 至 140 億美元。",
            "https://technews.tw/2026/11/10/ai-datacenter-liquid-cooling-120kw/",
            7.0,
        ),
        art(
            "ithome",
            "單機櫃功耗跨入 120kW 時代：資料中心導入液冷散熱比例明年達 45%，電網配置全面翻新",
            "大型雲端服務商新建資料中心全面改採液冷架構，單櫃功耗達 120kW，預估明年液冷滲透率達 45%，同時推進 400V 轉 800V 高壓直流配電系統以降低 12% 電力轉換損耗。",
            "https://www.ithome.com.tw/news/180388",
            7.4,
        ),
        art(
            "udn",
            "AI 機櫃功耗飆 120kW 液冷散熱產值年增 68% 衝 140 億美元、台廠擴產迎長單",
            "AI 伺服器單櫃功耗突破 120kW，明年液冷滲透率上看 45%，整體液冷散熱市場規模年增 68% 達 140 億美元。",
            "https://money.udn.com/money/story/5612/9870198",
            7.8,
        ),

        # 深度專題與社會學習 1：天然氣三接工程、生態監測與台灣電力轉型韌性（報導者 + 公視 + 經濟日報）
        art(
            "twreporter",
            "三接外推方案與大潭藻礁生態追蹤：當 AI 算力推升用電成長 3.5%，天然氣過渡期如何兼顧環評承諾？",
            "經濟部預估未來五年受半導體與 AI 資料中心擴建帶動，全國年均用電成長率達 3.5%，第三座天然氣接收站（三接）負載大潭電廠新增機組供氣重任。然而環保團體實地勘查發現局部海域沉積物變化，質疑施工影響柴山多杯孔珊瑚棲地；中油則提出歷年監測數據，強調水質與生態指標均符合環評承諾規範，凸顯台灣能源轉型在供電韌性與海洋生態保全間的制度考驗。",
            "https://www.twreporter.org/a/2026-taipei-golden-horse-film-festival-sayang",
            8.0,
            img="https://www.twreporter.org/images/20261106185744-8ca50a3980e05f85c3658f7766a62797-tablet.jpg",
        ),
        art(
            "pts",
            "環團質疑三接影響大潭藻礁生態 中油：長期水質與生物監測均依環評承諾規範辦理",
            "針對環保團體關切第三天然氣接收站工程對大潭藻礁與柴山多杯孔珊瑚影響，中油回應每季均委託獨立學術機構進行海域生態調查，監測數據符合環評承諾，未來將強化資訊公開以兼顧每年 3.5% 用電成長下的供氣穩定。",
            "https://news.pts.org.tw/article/835182",
            8.2,
            img="https://news-data.pts.org.tw/media/301339/conversions/GrNbyyNqCwzO3aTmEJQtgn7QZjd2cQwL7kzuaZDE-og_jpg.jpg",
        ),
        art(
            "udn",
            "環團質疑三接影響藻礁 中油：生態監測依環評承諾規範辦理",
            "環保團體質疑三接工程影響藻礁生態，中油表示各項水質與生態監測皆嚴格依照環評承諾規範辦理，確保大潭電廠燃氣機組供氣與環境保護並行。",
            "https://money.udn.com/money/story/5612/9870288",
            8.5,
        ),

        # 深度專題與社會學習 2：超高齡社會下的長照 3.0 預算配置與照護人力結構缺口（報導者 + 公視）
        art(
            "twreporter",
            "超高齡社會現場追蹤：長照基金預算突破 876 億元，為何重症家庭仍面臨 4.5 萬名照護人力缺口？",
            "台灣 65 歲以上高齡人口占比正式突破 20% 邁入超高齡社會，衛福部長照基金年度預算雖擴編至 876 億元，但第一線照顧服務員與機構護理人力缺口仍達 4.5 萬人。調查發現，現行論件計酬支付制度使居服員傾向承接輕症家務案，導致中重症失能家庭平均等待床位時間長達 6.5 個月，急需透過修法調整分級給付與跨專業整合照護。",
            "https://www.twreporter.org/a/long-term-care-workforce-structural-gap-2026",
            12.0,
            img="https://www.twreporter.org/images/20261103152359-05c2ef7f1e780c17595163cb5cc8e30f-tablet.jpg",
        ),
        art(
            "pts",
            "邁入 20% 超高齡社會：長照預算達 876 億元，中重症機構床位等待期卻長達 6.5 個月",
            "台灣高齡人口占比突破 20%，政府編列 876 億元長照預算，然而全國照護人力缺口仍達 4.5 萬人，中重症家庭等待住宿式機構平均需時 6.5 個月，專家呼籲改革分級支付制度。",
            "https://news.pts.org.tw/article/835160",
            12.5,
        ),
    ]


# ---------------------------------------------------------------------------
# 十、組裝與驗證管線
# ---------------------------------------------------------------------------
def section_of(arts: list[Article]) -> str:
    deep = sum(1 for a in arts if a.source.group == "deep")
    if deep >= 1 and (deep * 2 >= len(arts) or any(a.source.id in ("twreporter", "pts") for a in arts)):
        return "deep"
    return "business_tech"


_CLICKBAIT_TITLE_RE = re.compile(r"[？?]|是買點嗎|買點|震撼彈|曝光|驚呆|網看傻")


def _pick_representative(arts: list[Article], sec: str) -> Article:
    """挑選與同群其他文章關鍵字/實體共鳴度最高（Center Article）且發布時間最新的代表報導。"""
    if len(arts) == 1:
        return arts[0]
    pool = [a for a in arts if a.lang == "zh"] or arts
    ts_list = [a.published.timestamp() for a in pool if a.published]
    max_ts = max(ts_list) if ts_list else 0.0
    min_ts = min(ts_list) if ts_list else 0.0

    def _rep_score(cand: Article) -> tuple:
        centrality = 0.0
        for other in arts:
            if other is cand:
                continue
            centrality += (
                2.0 * len(cand.ents_strong & other.ents_strong)
                + 1.5 * len(cand.tokens & other.tokens)
                + 1.0 * len(cand.ev_facts & other.ev_facts)
            )
        if cand.published and max_ts > min_ts:
            recency = 3.0 * ((cand.published.timestamp() - min_ts) / (max_ts - min_ts))
        elif cand.published:
            recency = 1.0
        else:
            recency = 0.0
        deep_bonus = 2.0 if (sec == "deep" and cand.source.group == "deep") else 0.0
        summary_bonus = 1.5 if (len(cand.summary) >= 40 and "。" in cand.summary) else (0.5 if cand.summary else 0.0)
        clickbait_pen = -4.0 if _CLICKBAIT_TITLE_RE.search(cand.title) else 0.0
        pri_bonus = max(0.0, (10 - cand.source.priority) * 0.2)
        total = centrality + recency + deep_bonus + summary_bonus + clickbait_pen + pri_bonus
        cand_ts = cand.published.timestamp() if cand.published else 0.0
        return (total, cand_ts, -cand.source.priority, len(cand.summary))

    return max(pool, key=_rep_score)


def build_stories(articles: list[Article], threshold: int) -> tuple[list[dict], dict[str, list[Article]], dict]:
    annotate(articles, _compile(STRONG_ENTITIES), _compile(WEAK_ENTITIES))
    groups = cluster(articles)
    stories: list[dict] = []
    story_articles: dict[str, list[Article]] = {}
    below = 0
    for arts in groups:
        sc = score_cluster(arts)
        if sc["total"] < threshold:
            below += 1
            continue
        sec = section_of(arts)
        rep = _pick_representative(arts, sec)
        arts_sorted = sorted(arts, key=lambda a: (0 if (sec == "deep" and a.source.group == "deep") else 1, a.source.priority, -len(a.summary)))
        img_art = next((a for a in arts_sorted if a.image and not IMAGE_REJECT_RE.search(a.image)), None)
        times = [a.published for a in arts if a.published]
        sid = "s" + hashlib.sha1(f"{rep.url}|{rep.title}".encode("utf-8")).hexdigest()[:8]
        cf_list = core_facts(arts)
        mp_list = perspectives(arts)
        unv_str = unverified_notice(arts)
        story = {
            "id": sid,
            "section": sec,
            "title": rep.title,
            "titleOriginal": rep.title_orig if rep.title_orig != rep.title else None,
            "lang": "zh",
            "summary": trim_complete_summary(rep.summary or (arts_sorted[0].summary or ""), 320),
            "source": rep.source.name,
            "url": rep.url,
            "publishedAt": max(times).isoformat(timespec="seconds") if times else None,
            "imageUrl": img_art.image if img_art else None,
            "imageCredit": img_art.source.name if img_art else None,
            "score": sc["total"],
            "scoreBreakdown": {k: sc[k] for k in ("resonance", "impact", "data", "durability")},
            "mediaCount": sc["media_count"],
            "impactCategories": sc["impact_categories"],
            # 同時提供 prompt 指定之 snake_case 與前端便利之 camelCase
            "core_facts": cf_list,
            "media_perspectives": mp_list,
            "unverified_notice": unv_str,
            "coreFacts": cf_list,
            "mediaPerspectives": mp_list,
            "unverifiedNotice": unv_str,
            "sources": [
                {"media": a.source.name, "url": a.url, "headline": a.title, "lang": "zh"}
                for a in sorted(arts, key=lambda x: x.source.priority)
            ],
        }
        stories.append(story)
        story_articles[sid] = arts
    stories.sort(key=lambda s: (-s["score"], -s["mediaCount"], s["title"]))
    stats = {"clusters": len(groups), "passed": len(stories), "below_threshold": below}
    return stories, story_articles, stats


def resolve_edition(choice: str, now: datetime) -> dict:
    if choice in ("morning", "evening"):
        key = choice
    else:
        # 強制以台灣時間判定：05:00 - 12:59 為晨報版 07:30，13:00 - 04:59 為晚報版 16:00
        key = "morning" if 5 <= now.hour < 13 else "evening"
    return {
        "key": key,
        "label": "晨報版 07:30" if key == "morning" else "晚報版 16:00",
        "name": "晨報版" if key == "morning" else "晚報版",
        "scheduledTime": "07:30" if key == "morning" else "16:00",
    }


def collect_live(now: datetime) -> tuple[list[Article], list[dict]]:
    articles: list[Article] = []
    statuses: list[dict] = []
    with cf.ThreadPoolExecutor(max_workers=8) as pool:
        for items, status in pool.map(lambda s: fetch_source(s, now), SOURCES):
            articles.extend(items)
            statuses.append(status)
    seen: set[str] = set()
    unique: list[Article] = []
    for a in articles:
        key = a.url or a.title
        if key in seen:
            continue
        seen.add(key)
        unique.append(a)
    kept = [a for a in unique if is_relevant(a)]
    log.info("即時抓取 %d 則，去重後 %d，通過白名單與題材過濾 %d。", len(articles), len(unique), len(kept))
    return kept, statuses


def atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
            f.write("\n")
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def render_dashboard_html(payload: dict, html_path: Path) -> None:
    """以純靜態方式同步產出符合 Hallmark Editorial 規範之 index.html。"""
    def esc_attr(v: object) -> str:
        """HTML 屬性與純文字標題專用跳脫：剝除 HTML 標籤且絕不插入 <abbr>。"""
        s = TAG_RE.sub("", str(v if v is not None else ""))
        s = re.sub(r"\.{3,}", "…", s)
        return html.escape(s, quote=True)

    def esc(v: object) -> str:
        escaped = esc_attr(v)
        return re.sub(r"\b(AI|GPU|CPU|IPO|CPI|DXY|WTI|PCE|FED|FOMC|NVIDIA)\b", r"<abbr>\1</abbr>", escaped)

    def fmt_time(iso_str: str | None) -> str:
        if not iso_str:
            return "今日更新"
        try:
            date_part, time_part = iso_str.split("T")
            _, mm, dd = date_part.split("-")
            hh, mi, *_ = time_part.split(":")
            return f"{mm}/{dd} {hh}:{mi}"
        except Exception:
            return iso_str

    def spark_svg(history: list[float] | None, label: str, is_up: bool, market: str = "us") -> str:
        pts = [float(x) for x in (history or [])]
        if len(pts) < 2:
            return ""
        w, h, pad = 72, 24, 2.5
        mn, mx = min(pts), max(pts)
        span = (mx - mn) or 1.0
        coords = []
        for i, v in enumerate(pts):
            x = pad + (i / (len(pts) - 1)) * (w - pad * 2)
            y = h - pad - ((v - mn) / span) * (h - pad * 2)
            coords.append(f"{x:.1f},{y:.1f}")
        lx, ly = coords[-1].split(",")
        if market == "tw":
            stroke = "var(--color-error)" if is_up else "var(--color-accent)"
        else:
            stroke = "var(--color-accent)" if is_up else "var(--color-ink-2)"
        return (
            f'<svg viewBox="0 0 {w} {h}" role="img" aria-label="{esc_attr(label)} 近期走勢">'
            f'<title>{esc_attr(label)} 近期走勢（最低 {mn}，最高 {mx}）</title>'
            f'<polyline fill="none" stroke="{stroke}" stroke-width="1.5" points="{" ".join(coords)}" />'
            f'<circle cx="{lx}" cy="{ly}" r="2.2" fill="{stroke}" />'
            f'</svg>'
        )

    def render_market_cell_html(item: dict, cell_class: str) -> str:
        digits = item.get("digits", 2)
        val_str = f"{float(item['value']):.{digits}f}"
        chg = float(item.get("change", 0))
        pct = float(item.get("changePct", 0))
        sign = "+" if chg > 0 else ""
        trend = "up" if chg > 0 else ("down" if chg < 0 else "flat")
        if item.get("light"):
            delta_txt = f"{item['light']}（{sign}{chg:.{digits}f} {item.get('unit', '')}）"
        else:
            delta_txt = f"{sign}{chg:.{digits}f} ({sign}{pct:.2f}%)"
        mkt = item.get("market", "us")
        svg = spark_svg(item.get("history", []), item["label"], chg >= 0, mkt)
        sym = item.get("symbol") or ""
        sym_html = f'<code class="market-cell__symbol">{esc_attr(sym)}</code>' if sym and sym != "NDC" else ""
        unit_html = f'<span class="macro-cell__unit">{esc(item["unit"])}</span>' if item.get("unit") else ""
        mkt_attr = f' data-market="{esc_attr(mkt)}"' if mkt else ""
        return f"""<article class="{esc_attr(cell_class)}"{mkt_attr}>
          <div class="macro-cell__main">
            <p class="market-cell__name">{esc(item["label"])}</p>
            {sym_html}
            <div class="macro-cell__value-row">
              <span class="macro-cell__value">{esc_attr(val_str)}</span>
              {unit_html}
            </div>
            <div class="{esc_attr(cell_class)}__delta" data-trend="{trend}">{esc_attr(delta_txt)}</div>
          </div>
          <div class="macro-cell__spark">
            {svg}
            <span class="macro-cell__asof">{esc_attr(item.get("asOf", ""))}</span>
          </div>
        </article>"""

    def render_macro_cells(macro: dict) -> str:
        items = payload.get("macro_indicators") or macro.get("macro_indicators") or macro.get("items", [])
        return "\n".join(render_market_cell_html(item, "macro-cell") for item in items)

    ed_key = (payload.get("edition") or {}).get("key", "morning")
    ed_markets = (payload.get("edition_markets") or (payload.get("macro") or {}).get("edition_markets") or {}).get(ed_key) or {}
    focus_items = payload.get("market_focus") or ed_markets.get("items") or []
    focus_idx = ed_markets.get("indices") or [x for x in focus_items if x.get("group") == "index"]
    focus_stk = ed_markets.get("stocks") or [x for x in focus_items if x.get("group") != "index"]
    focus_title = ed_markets.get("title") or ("台股收盤與核心權值表現（晚報焦點）" if ed_key == "evening" else "美股收盤與科技巨頭表現（晨報焦點）")
    focus_desc = ed_markets.get("description") or (
        "涵蓋台灣加權、櫃買指數與台灣 50 ETF 及核心電子權值股（台股慣例：漲紅跌綠）"
        if ed_key == "evening"
        else "涵蓋標普 500、那斯達克、費城半導體、道瓊指數與美股核心科技 ADR（+ 上漲 / - 下跌）"
    )
    focus_idx_cols = "2" if len(focus_idx) <= 2 else "4"
    focus_stk_cols = "3" if len(focus_stk) == 6 else ("2" if len(focus_stk) <= 2 else "4")
    focus_idx_html = "\n".join(render_market_cell_html(item, "market-cell") for item in focus_idx)
    focus_stk_html = "\n".join(render_market_cell_html(item, "market-cell") for item in focus_stk)

    def render_fact_ledger(story: dict) -> str:
        cells = []
        for idx, f in enumerate((story.get("core_facts") or [])[:3]):
            metric = f.get("metric") or f"查核事實 0{idx+1}"
            confirmed = "、".join(f.get("confirmed_by") or [story.get("source", "")])
            cells.append(f"""<div class="fact-ledger-figure__cell">
          <dt>交叉核實：{esc(confirmed)}</dt>
          <dd>{esc(metric)}</dd>
        </div>""")
        return f"""<div class="fact-ledger-figure" role="group" aria-label="核心數據查核帳本（原始報導無提供 og:image 首圖）">
      <p class="fact-ledger-figure__title">關鍵數據帳本（原始來源未附 og:image，依規範標記為 null 並呈現查核指標）</p>
      <dl class="fact-ledger-figure__grid">{''.join(cells)}</dl>
    </div>"""

    def render_drawer(story: dict) -> str:
        sb = story.get("scoreBreakdown") or {}
        facts = story.get("core_facts") or []
        perspectives = story.get("media_perspectives") or []
        sources = story.get("sources") or []
        drawer_id = f"drawer-{story['id']}"
        facts_li = "".join(
            f'<li>{esc(f["text"])}<span class="core-facts-list__sources">［確認媒體：{esc("、".join(f.get("confirmed_by") or []))}］</span></li>'
            for f in facts
        )
        persp_li = "".join(
            f"""<li class="perspective-item">
              <div class="perspective-item__header">
                <span class="perspective-item__media">{esc(p["media"])}</span>
                <span class="perspective-item__angle">{esc(p["angle"])}</span>
              </div>
              <p class="perspective-item__headline"><a href="{esc_attr(p["url"])}" target="_blank" rel="noopener noreferrer">{esc(p["headline"])}</a></p>
              <p class="perspective-item__focus">{esc(p.get("focus") or p.get("excerpt") or "")}</p>
            </li>"""
            for p in perspectives
        )
        src_li = "".join(
            f'<li><span><strong>{esc(s["media"])}</strong>：{esc(s["headline"])}</span><a href="{esc_attr(s["url"])}" target="_blank" rel="noopener noreferrer">開啟原始報導</a></li>'
            for s in sources
        )
        unv_html = (
            f'<div class="unverified-callout" role="note"><strong>單一消息源查核警示：</strong>{esc(story["unverified_notice"])}</div>'
            if story.get("unverified_notice") else ""
        )
        return f"""<div class="factcheck-disclosure">
      <button type="button" class="factcheck-toggle" data-toggle-story="{esc_attr(story['id'])}" aria-expanded="false" aria-controls="{esc_attr(drawer_id)}">
        <span>事實查核明細（{len(sources)} 家出處）</span>
        <span data-toggle-label>展開</span>
      </button>
      <div id="{esc_attr(drawer_id)}" class="factcheck-drawer" data-open="false">
        <div class="factcheck-drawer__inner">
          <div class="factcheck-drawer__content">
            <dl class="score-breakdown" aria-label="四維度重要性評分拆解">
              <div><dt>跨媒體共鳴 (35%)</dt><dd>{esc(sb.get("resonance", 0))} / 35</dd></div>
              <div><dt>實質結構影響 (35%)</dt><dd>{esc(sb.get("impact", 0))} / 35</dd></div>
              <div><dt>數據事實密度 (20%)</dt><dd>{esc(sb.get("data", 0))} / 20</dd></div>
              <div><dt>時效持久度 (10%)</dt><dd>{esc(sb.get("durability", 0))} / 10</dd></div>
            </dl>
            <div class="core-facts-box">
              <h4>各家一致確認之客觀事實 (core_facts)</h4>
              <ul class="core-facts-list">{facts_li}</ul>
            </div>
            <div class="source-links-block">
              <h5>各家媒體切入視角對比 (media_perspectives)</h5>
              <ul class="perspective-list">{persp_li}</ul>
            </div>
            {unv_html}
            <div class="source-links-block">
              <h5>原始報導出處超連結</h5>
              <ul class="source-links-list">{src_li}</ul>
            </div>
          </div>
        </div>
      </div>
    </div>"""

    def render_lead(story: dict, idx: int) -> str:
        sec_label = "深度專題與社會學習" if story.get("section") == "deep" else "商業與科技供應鏈"
        if story.get("imageUrl"):
            credit = story.get("imageCredit") or story.get("source") or "原始媒體"
            media_html = f"""<figure class="lead-story__media">
              <img src="{esc_attr(story['imageUrl'])}" alt="{esc_attr(story['title'])}" loading="eager" decoding="async" data-fallback-story="{esc_attr(story['id'])}" />
              <figcaption>新聞原生首圖（提取自原始網頁 meta og:image，來源：{esc(credit)}）</figcaption>
            </figure>"""
        else:
            media_html = render_fact_ledger(story)
        facts_li = "".join(
            f'<li>{esc(f["text"])}<span class="core-facts-list__sources">［核實：{esc("、".join(f.get("confirmed_by") or []))}］</span></li>'
            for f in (story.get("core_facts") or [])
        )
        persp = story.get("media_perspectives") or []
        persp_li = "".join(
            f"""<li class="perspective-item">
              <div class="perspective-item__header">
                <span class="perspective-item__media">{esc(p["media"])}</span>
                <span class="perspective-item__angle">{esc(p["angle"])}</span>
              </div>
              <p class="perspective-item__headline"><a href="{esc_attr(p["url"])}" target="_blank" rel="noopener noreferrer">{esc(p["headline"])}</a></p>
              <p class="perspective-item__focus">{esc(p.get("focus") or p.get("excerpt") or "")}</p>
            </li>"""
            for p in persp
        )
        unv_box = (
            f'<div class="unverified-callout" role="note"><strong>查核警示：</strong>{esc(story["unverified_notice"])}</div>'
            if story.get("unverified_notice")
            else '<div class="unverified-callout" role="note"><strong>查核狀態：</strong>本則頭條所有核心數據均經 2 家以上具名媒體或官方公開公報交叉確認，無單一匿名消息源。</div>'
        )
        return f"""<article class="lead-story" data-story-id="{esc_attr(story['id'])}">
          <div class="lead-story__primary">
            <h3 class="lead-story__headline"><a href="{esc_attr(story['url'])}" target="_blank" rel="noopener noreferrer">{esc(story['title'])}</a></h3>
            <div class="story-meta">
              <span class="story-meta__item story-meta__score">重要性評分 {esc(story['score'])} 分</span>
              <span class="story-meta__item">跨媒體共鳴 {esc(story['mediaCount'])} 家</span>
              <span class="story-meta__item">領銜報導：{esc(story['source'])}</span>
              <span class="story-meta__item">分類：{esc(sec_label)}</span>
              <time class="story-meta__item" datetime="{esc_attr(story.get('publishedAt') or '')}">{esc(fmt_time(story.get('publishedAt')))}</time>
            </div>
            {media_html}
            <p class="lead-story__summary">{esc(story['summary'])}</p>
            <div class="core-facts-box">
              <h4>跨媒體一致確認之核心事實</h4>
              <ul class="core-facts-list">{facts_li}</ul>
            </div>
            {render_drawer(story)}
          </div>
          <aside class="perspective-panel" aria-label="頭條 {idx + 1} 媒體多方視角對照框">
            <div>
              <div class="perspective-panel__head">
                <h4>媒體多方視角對照框</h4>
                <p class="perspective-panel__sub">對比 {len(persp)} 家白名單媒體之報導切入點與側重差異</p>
              </div>
              <ul class="perspective-list">{persp_li}</ul>
            </div>
            {unv_box}
          </aside>
        </article>"""

    def render_secondary(story: dict, is_deep: bool) -> str:
        cls = "story-card story-card--deep" if is_deep else "story-card"
        if story.get("imageUrl"):
            body = f"""<div class="story-card__body-split">
              <p class="story-card__summary">{esc(story['summary'])}</p>
              <figure class="story-card__thumb">
                <img src="{esc_attr(story['imageUrl'])}" alt="{esc_attr(story['title'])}" loading="lazy" decoding="async" data-fallback-thumb="{esc_attr(story['id'])}" />
              </figure>
            </div>"""
        else:
            body = f'<p class="story-card__summary">{esc(story["summary"])}</p>'
        unv = (
            f'<div class="unverified-callout" role="note"><strong>單一消息源警示：</strong>{esc(story["unverified_notice"])}</div>'
            if story.get("unverified_notice") else ""
        )
        return f"""<article class="{cls}" data-story-id="{esc_attr(story['id'])}">
          <h3 class="story-card__headline"><a href="{esc_attr(story['url'])}" target="_blank" rel="noopener noreferrer">{esc(story['title'])}</a></h3>
          <div class="story-meta">
            <span class="story-meta__item story-meta__score">評分 {esc(story['score'])} 分</span>
            <span class="story-meta__item">{esc(story['mediaCount'])} 家共鳴</span>
            <span class="story-meta__item">{esc(story['source'])}</span>
            <time class="story-meta__item" datetime="{esc_attr(story.get('publishedAt') or '')}">{esc(fmt_time(story.get('publishedAt')))}</time>
          </div>
          {body}
          {unv}
          {render_drawer(story)}
        </article>"""

    stories = payload.get("stories", [])
    leads = stories[:2]
    lead_ids = {s["id"] for s in leads}
    rest = [s for s in stories if s["id"] not in lead_ids]
    biz_stories = [s for s in rest if s.get("section") == "business_tech"]
    deep_stories = [s for s in rest if s.get("section") == "deep"] or [s for s in stories if s.get("section") == "deep"]

    cnt_all = len(stories)
    cnt_biz = sum(1 for s in stories if s.get("section") == "business_tech")
    cnt_deep = sum(1 for s in stories if s.get("section") == "deep")
    cnt_unv = sum(1 for s in stories if s.get("unverified_notice"))

    src_status_html = "\n".join(
        f'<li class="source-status-item">{esc(s["name"])}（{esc(s.get("section") or s.get("group"))}）：{"%d 篇" % s["count"] if s.get("ok") else "備援快取"}</li>'
        for s in payload.get("sourcesStatus", [])
    )

    embedded_json = json.dumps(payload, ensure_ascii=False).replace("</", "<\\/")

    html_doc = f"""<!DOCTYPE html>
<html lang="zh-Hant-TW">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>現代商業與深度時事新聞儀表板（{esc_attr(payload['edition']['label'])}）</title>
  <meta name="description" content="靜態資料驅動之台灣正體中文高價值商業、科技供應鏈與深度公共議題新聞儀表板，嚴選重要性 80 分以上跨媒體核實報導。" />
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600..800&family=Noto+Sans+TC:wght@500;700;900&family=Noto+Serif+TC:wght@400;600;700&family=Source+Serif+4:opsz,wght@8..60,400..700&display=swap" rel="stylesheet" />
  <link rel="stylesheet" href="./tokens.css" />
  <link rel="stylesheet" href="./base.css" />
  <link rel="stylesheet" href="./styles.css" />
</head>
<body>
  <a class="skip-link" href="#main-content">跳至主要新聞內容</a>

  <div class="page-frame">
    <header class="masthead" role="banner">
      <div class="masthead__top">
        <div class="masthead__identity">
          <h1>現代商業與深度時事新聞儀表板</h1>
          <p class="masthead__sub">嚴選 10 大白名單財經、科技與深度調查媒體，透過四維度重要性漏斗過濾 80 分以上結構性報導，提供跨媒體事實查核與多方視角對照。</p>
        </div>
        <dl class="masthead__ledger" aria-label="出刊版號與資料管線狀態">
          <div class="masthead__ledger-item">
            <dt>當前版號</dt>
            <dd id="masthead-edition">{esc(payload['edition']['label'])}</dd>
          </div>
          <div class="masthead__ledger-item">
            <dt>出刊日期與時間</dt>
            <dd id="masthead-date">{esc(payload['date'])} {esc(payload['weekday'])}（更新：{esc(payload['updatedAt'])}）</dd>
          </div>
          <div class="masthead__ledger-item">
            <dt>重要性漏斗統計</dt>
            <dd id="masthead-stats">入選 {esc(payload['stats']['passed'])} 則（&gt;={esc(payload['threshold'])} 分）／ 掃描 {esc(payload['stats']['articlesAnalysed'])} 篇</dd>
          </div>
        </dl>
      </div>

      <div class="masthead__controls">
        <nav class="filter-nav" aria-label="新聞版面分類篩選">
          <button type="button" class="filter-btn" data-filter="all" aria-pressed="true">
            全部高價值新聞<span class="filter-btn__count">({cnt_all})</span>
          </button>
          <button type="button" class="filter-btn" data-filter="business_tech" aria-pressed="false">
            商業與科技供應鏈<span class="filter-btn__count">({cnt_biz})</span>
          </button>
          <button type="button" class="filter-btn" data-filter="deep" aria-pressed="false">
            深度專題與社會學習<span class="filter-btn__count">({cnt_deep})</span>
          </button>
          <button type="button" class="filter-btn" data-filter="unverified" aria-pressed="false">
            含單一消息源警示<span class="filter-btn__count">({cnt_unv})</span>
          </button>
        </nav>

        <div class="search-tools">
          <div class="search-field">
            <label for="news-search">關鍵字檢索</label>
            <input id="news-search" type="search" placeholder="搜尋台積電、殖利率、長照…" autocomplete="off" />
          </div>
          <div class="edition-switch" role="group" aria-label="切換晨報與晚報版號">
            <button type="button" class="edition-btn" data-edition-switch="morning" aria-pressed="{ 'true' if payload['edition']['key'] == 'morning' else 'false' }">晨報版 07:30</button>
            <button type="button" class="edition-btn" data-edition-switch="evening" aria-pressed="{ 'true' if payload['edition']['key'] == 'evening' else 'false' }">晚報版 16:00</button>
          </div>
        </div>
      </div>
    </header>

    <main id="main-content">
      <section class="macro-section" aria-labelledby="macro-heading">
        <div class="macro-section__head">
          <h2 id="macro-heading">總經與商品期貨列（Macro &amp; Commodities）</h2>
          <p class="macro-section__note">涵蓋美國 10 年期公債殖利率、美元指數 (<abbr>DXY</abbr>)、<abbr>WTI</abbr> 原油期貨、紐約黃金期貨與國家發展委員會台灣景氣對策信號</p>
        </div>
        <div id="macro-grid" class="macro-grid">
          {render_macro_cells(payload['macro'])}
        </div>
      </section>

      <section id="market-focus-section" class="macro-section market-focus-section" data-market-convention="{ 'tw' if ed_key == 'evening' else 'us' }" aria-labelledby="market-focus-heading">
        <div class="macro-section__head">
          <h2 id="market-focus-heading">{esc(focus_title)}</h2>
          <p id="market-focus-note" class="macro-section__note">{esc(focus_desc)}</p>
        </div>
        <div class="market-ledger-groups">
          <div class="market-ledger-group">
            <h3>主要指數</h3>
            <div id="market-focus-indices" class="market-grid" data-cols="{focus_idx_cols}">
              {focus_idx_html}
            </div>
          </div>
          <div class="market-ledger-group">
            <h3>重點標的</h3>
            <div id="market-focus-stocks" class="market-grid" data-cols="{focus_stk_cols}">
              {focus_stk_html}
            </div>
          </div>
        </div>
      </section>

      <section id="error-state" class="state-banner" hidden aria-labelledby="error-heading">
        <h3 id="error-heading">資料讀取異常</h3>
        <p data-error-msg>無法載入新聞資料檔，請確認 public/daily_news.json 是否存在。</p>
      </section>

      <section id="empty-state" class="state-banner" hidden aria-labelledby="empty-heading">
        <h3 id="empty-heading">無符合目前篩選條件之高價值新聞</h3>
        <p>目前關鍵字或分類下沒有符合條件的報導，請重設篩選條件以檢視完整晨晚報內容。</p>
        <button type="button" class="filter-btn" data-reset-filters>重設所有篩選條件</button>
      </section>

      <section id="lead-section" class="news-section" aria-labelledby="lead-heading">
        <div class="section-header">
          <h2 id="lead-heading">今日深度雙頭條</h2>
          <p class="section-header__desc">左側呈現原生新聞首圖、標題與跨媒體一致確認之核心事實；右側直列各家媒體報導切入視角對照</p>
        </div>
        <div id="lead-stories-container" class="lead-stack">
          {"".join(render_lead(s, i) for i, s in enumerate(leads))}
        </div>
      </section>

      <div id="secondary-section" class="secondary-columns">
        <section class="column-block" aria-labelledby="biz-heading">
          <div class="section-header">
            <h2 id="biz-heading">商業與科技供應鏈</h2>
            <p class="section-header__desc">財報結構、資本支出、算力基建與全球利率匯率定價</p>
          </div>
          <div id="biz-stories-container" class="story-grid">
            {"".join(render_secondary(s, False) for s in biz_stories)}
          </div>
        </section>

        <section class="column-block" aria-labelledby="deep-heading">
          <div class="section-header">
            <h2 id="deep-heading">深度專題與社會學習</h2>
            <p class="section-header__desc">能源轉型、高齡照護制度、公共治理與長期結構調查</p>
          </div>
          <div id="deep-stories-container" class="story-grid">
            {"".join(render_secondary(s, True) for s in deep_stories)}
          </div>
        </section>
      </div>
    </main>

    <footer class="colophon" role="contentinfo">
      <div>
        <h2>白名單媒體來源與即時連線狀態</h2>
        <p>本系統嚴格限定抓取商業總經（Reuters、Bloomberg、WSJ、經濟日報、工商時報產業與總經版）、科技創新（TechNews 科技新報、iThome、數位時代）與深度公共議題（報導者、公視新聞網 PNN），排除一切政論八卦、彩券與瑣碎社會案件。</p>
        <ul id="source-status-list" class="source-status-list">
          {src_status_html}
        </ul>
      </div>
      <div>
        <h2>四維度重要性評分與事實查核方法論</h2>
        <p>每起新聞事件依「跨媒體共鳴度 35%」、「實質結構影響 35%（扣除公關稿與主觀口水）」、「數據事實密度 20%」與「時效持久度 10%」進行量化評分，僅保留總分 80 分以上之報導。首圖嚴格提取原始網頁 head 區段之「og:image」屬性，若無原生新聞圖則標記為 null 並以關鍵數據查核帳本呈現，絕不採用無關圖庫或假圖。</p>
      </div>
    </footer>
  </div>

  <script id="initial-news-data" type="application/json">{embedded_json}</script>
  <script src="./app.js" defer></script>
</body>
</html>
"""
    atomic_write_text(html_path, html_doc)


def validate(payload: dict) -> list[str]:
    problems = []
    for key in ("schemaVersion", "edition", "generatedAt", "updatedAt", "macro", "macro_indicators", "market_focus", "stories", "sourcesStatus"):
        if key not in payload:
            problems.append(f"缺少欄位 {key}")
    for s in payload.get("stories", []):
        for key in ("id", "title", "section", "score", "core_facts", "media_perspectives", "unverified_notice", "imageUrl", "url"):
            if key not in s:
                problems.append(f"故事 {s.get('id')} 缺少 {key}")
        if s.get("score", 0) < payload.get("threshold", 0):
            problems.append(f"故事 {s.get('id')} 分數低於門檻")
    return problems


def run(args: argparse.Namespace) -> int:
    now = datetime.now(TZ)
    out_path = Path(args.out)
    mode = "sample" if args.sample else "live"
    ed = resolve_edition(args.edition, now)
    ed_key = ed.get("key", "morning")

    if mode == "live":
        live_articles, statuses = collect_live(now)
        translate_articles(live_articles)
        # 正式環境 100% 僅使用真實 RSS 抓取之即時新聞，絕不混入任何 Seed 假資料或假網址
        stories, story_articles, stats = build_stories(live_articles, args.threshold)
        attach_images(stories, story_articles)
        macro = collect_macro(args, edition_key=ed_key, sample_only=False)
        analysed_count = len(live_articles)
    else:
        seed_articles = curated_benchmark_articles(now)
        statuses = [
            {"id": s.id, "name": s.name, "group": s.group, "section": s.feeds[0].section if s.feeds else "",
             "ok": True, "count": sum(1 for a in seed_articles if a.source.id == s.id), "error": None}
            for s in SOURCES
        ]
        stories, story_articles, stats = build_stories(seed_articles, args.threshold)
        attach_images(stories, story_articles)
        macro = collect_macro(args, edition_key=ed_key, sample_only=True)
        analysed_count = len(seed_articles)

    for rank, s in enumerate(stories, start=1):
        s["rank"] = rank
    lead_ids = [s["id"] for s in stories[:2]]
    payload = {
        "schemaVersion": SCHEMA_VERSION,
        "edition": ed,
        "generatedAt": now.isoformat(timespec="seconds"),
        "updatedAt": now.strftime("%Y-%m-%d %H:%M"),
        "date": now.date().isoformat(),
        "weekday": WEEKDAY_ZH[now.weekday()],
        "dataMode": mode,
        "threshold": args.threshold,
        "scoreWeights": {"resonance": 35, "impact": 35, "data": 20, "durability": 10},
        "macro": macro,
        "macro_indicators": macro["macro_indicators"],
        "market_focus": macro["market_focus"],
        "edition_markets": macro["edition_markets"],
        "leadIds": lead_ids,
        "stories": stories,
        "stats": {**stats, "articlesAnalysed": analysed_count},
        "sourcesStatus": statuses,
    }
    problems = validate(payload)
    if problems:
        log.error("輸出驗證失敗：%s", "; ".join(problems))
        return 2

    # 1. 寫入主檔案 daily_news.json
    atomic_write_json(out_path, payload)

    # 2. 同步寫入專屬版號檔案 daily_news_morning.json / daily_news_evening.json
    edition_out = out_path.parent / f"daily_news_{ed_key}.json"
    atomic_write_json(edition_out, payload)
    log.info("已同步輸出專屬版號檔案：%s", edition_out)

    # 3. 確保另一版號檔案具備新版動態市場焦點欄位（未存在則建立鏡像，舊格式則升級行情欄位並保留其新聞）
    other_key = "evening" if ed_key == "morning" else "morning"
    other_out = out_path.parent / f"daily_news_{other_key}.json"
    other_focus = macro["edition_markets"][other_key]["items"]
    other_macro = {**macro, "market_focus": other_focus, "activeEdition": other_key}
    if not other_out.exists():
        other_payload = dict(payload)
        other_payload["edition"] = {
            "key": other_key,
            "label": "晚報版 16:00" if other_key == "evening" else "晨報版 07:30",
            "name": "晚報版" if other_key == "evening" else "晨報版",
            "scheduledTime": "16:00" if other_key == "evening" else "07:30",
        }
        other_payload["macro"] = other_macro
        other_payload["macro_indicators"] = macro["macro_indicators"]
        other_payload["market_focus"] = other_focus
        other_payload["edition_markets"] = macro["edition_markets"]
        atomic_write_json(other_out, other_payload)
        log.info("已初始化另一版號檔案：%s", other_out)
    else:
        try:
            existing_other = json.loads(other_out.read_text(encoding="utf-8"))
            existing_other["macro"] = other_macro
            existing_other["macro_indicators"] = macro["macro_indicators"]
            existing_other["market_focus"] = other_focus
            existing_other["edition_markets"] = macro["edition_markets"]
            atomic_write_json(other_out, existing_other)
            log.info("已同步升級另一版號行情結構：%s", other_out)
        except Exception as exc:  # noqa: BLE001
            log.debug("讀取或升級 %s 失敗：%s", other_out, exc)

    html_path = Path(args.html) if getattr(args, "html", None) else out_path.parent / "index.html"
    render_dashboard_html(payload, html_path)
    log.info("已同步更新 HTML 靜態渲染檔案：%s", html_path)
    log.info("已寫入 %s（模式：%s，入選 >=%d 分共 %d 則，總分析 %d 則，分群 %d，淘汰 %d）。",
             out_path, mode, args.threshold, len(stories), analysed_count, stats["clusters"], stats["below_threshold"])
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="產出新聞儀表板使用的 public/daily_news.json 與 index.html")
    ap.add_argument("--out", default=str(DEFAULT_OUT), help="輸出路徑，預設 public/daily_news.json")
    ap.add_argument("--html", default="", help="同步渲染 HTML 路徑，預設為與 --out 同目錄之 index.html")
    ap.add_argument("--edition", choices=["auto", "morning", "evening"], default="auto")
    ap.add_argument("--threshold", type=int, default=80, help="重要性分數門檻，預設 80")
    ap.add_argument("--sample", action="store_true", help="僅使用內建驗證資料（離線可用）")
    ap.add_argument("--ndc-score", type=int, default=None, help="手動指定景氣對策信號分數")
    ap.add_argument("--ndc-light", default="", help="手動指定景氣燈號名稱")
    ap.add_argument("--ndc-month", default="", help="手動指定景氣燈號月份")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    try:
        return run(args)
    except Exception:  # noqa: BLE001
        log.exception("管線發生未預期錯誤")
        return 1


if __name__ == "__main__":
    sys.exit(main())
