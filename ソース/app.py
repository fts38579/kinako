# -*- coding: utf-8 -*-
"""
カワウソマネージャー きなこ – 統合アプリ  v2.5  (PyQt6)
=======================================================
修正・改善内容:
  [Bug1] ライブ監視: print()出力をGUIログに転送（stdout リダイレクト）
  [Bug2] 監視停止ボタン押下時もインサイト自動取得を実行
  [Bug3] insights.py で webdriver-manager 対応（Chrome自動検出）
  [Bug4] tkcalendar の月選択不具合 → PyQt6 QDateEdit に置換
  [改善] GUI 全体を tkinter → PyQt6 に移行
  [Bug2/3] live_bot に stop_event を渡して停止ボタンで即座にループ終了
  [Bug3] insights.py で webdriver-manager 対応（Chrome自動検出）
  [v2.2] config.validate() の tkinter 依存を除去（PyQt6 クラッシュ修正）
  [v2.2] on_stream_end 3分待機中も stop_event をチェックして即時終了可能に
  [v2.2] LiveBot インスタンス生成後に importlib.reload(config) で config 再注入
  [v2.2] asyncio イベントループのクリーンアップを強化
  [v2.3] Windows で SelectorEventLoop を使用（PyQt6 + asyncio 競合修正）
  [v2.3] asyncio カスタム例外ハンドラー追加（--windowed EXE クラッシュ修正）
  [v2.3] asyncio.set_event_loop() 削除（グローバル変数汚染防止）
  [v2.3] __main__ に multiprocessing.freeze_support() と stderr 対策追加
  [v2.4] live_bot v9.4: connect() → start()+await task に変更（二重await競合解消）
  [v2.4] live_bot v9.4: finally の二重 disconnect を1回に統合
  [v2.4] 実際の配信（teketeke1205）で停止フリーズなしを確認済み
  [v2.5] グラフエンジンを matplotlib → PyQtGraph に完全移行
        ズーム・パン・右クリックメニューがマウスで直接操作可能に
"""

import os
import sys
import re
import time
import threading
import traceback
from datetime import datetime, timedelta, date

# ── プロジェクトルート解決 ────────────────────────────────────────
if getattr(sys, "frozen", False):
    _PROJECT_ROOT = os.path.dirname(sys.executable)
else:
    _PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

_SRC = os.path.join(_PROJECT_ROOT, "ソース")
if not getattr(sys, "frozen", False) and _SRC not in sys.path:
    sys.path.insert(0, _SRC)

DATA_DIR     = os.path.join(_PROJECT_ROOT, "data")
CONFIG_FILE  = os.path.join(_PROJECT_ROOT, "config.py")
CSV_FILE     = os.path.join(DATA_DIR, "gift_timeline.csv")
VIEWERS_FILE = os.path.join(DATA_DIR, "viewers.csv")

# ── PyQt6 ────────────────────────────────────────────────────────
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QTabWidget,
    QVBoxLayout, QHBoxLayout, QGridLayout,
    QLabel, QLineEdit, QPushButton, QTextEdit,
    QFileDialog, QMessageBox, QFrame, QDateEdit,
    QSizePolicy, QScrollArea, QSplitter, QProgressBar
)
from PyQt6.QtCore import (
    Qt, QThread, pyqtSignal, QObject, QDate, QTimer, pyqtSlot, QRectF
)
from PyQt6.QtGui import QFont, QColor, QPalette, QTextCursor, QPainter

# ── PyQtGraph ────────────────────────────────────────────────────
import pyqtgraph as pg
from pyqtgraph import BarGraphItem, PlotWidget, mkPen, mkBrush
import numpy as np
import platform

try:
    import pandas as pd
    _HAS_PANDAS = True
except ImportError:
    _HAS_PANDAS = False

# ── PyQtGraph / フォント初期化（QApplication生成後に必ず呼ぶ） ──
_JP_FONT = "MS Gothic"  # デフォルト。_init_pyqtgraph() で上書きされる

def _init_pyqtgraph():
    """QApplication 生成後に一度だけ呼ぶ。
    pg.setConfigOption は QApplication より前に呼ぶとクラッシュする。"""
    global _JP_FONT
    # PyQtGraph グローバル設定
    pg.setConfigOption("background", "#1e1b2e")
    pg.setConfigOption("foreground", "#e5e7eb")
    pg.setConfigOptions(antialias=True)
    # 日本語フォント選択
    sys_fonts = {
        "Windows": ["Meiryo", "Yu Gothic", "MS Gothic"],
        "Darwin":  ["Hiragino Sans", "Hiragino Kaku Gothic ProN"],
        "Linux":   ["Noto Sans CJK JP", "IPAGothic", "TakaoGothic"],
    }
    candidates = sys_fonts.get(platform.system(), []) + ["DejaVu Sans"]
    from PyQt6.QtGui import QFontDatabase
    available = set(QFontDatabase.families())
    for name in candidates:
        if name in available:
            _JP_FONT = name
            return
    _JP_FONT = "DejaVu Sans"

# ────────────────────────────────────────────────────────────────
#  カラーパレット（ダークテーマ）
# ────────────────────────────────────────────────────────────────
C_BG       = "#1e1b2e"
C_PANEL    = "#2d2a45"
C_ACCENT   = "#7c3aed"
C_ACCENT2  = "#5b21b6"
C_TEXT     = "#e5e7eb"
C_SUBTEXT  = "#9ca3af"
C_GREEN    = "#16a34a"
C_RED      = "#dc2626"
C_BLUE     = "#1d4ed8"
C_TEAL     = "#059669"
C_BORDER   = "#4b5563"

STYLE_SHEET = f"""
QMainWindow, QWidget {{
    background-color: {C_BG};
    color: {C_TEXT};
    font-family: "Meiryo", "Yu Gothic", "MS Gothic", sans-serif;
    font-size: 10pt;
}}
QTabWidget::pane {{
    border: 1px solid {C_BORDER};
    background: {C_BG};
}}
QTabBar::tab {{
    background: {C_PANEL};
    color: {C_SUBTEXT};
    padding: 8px 20px;
    font-weight: bold;
    border: none;
}}
QTabBar::tab:selected {{
    background: {C_ACCENT};
    color: white;
}}
QTabBar::tab:hover:!selected {{
    background: #3d3a55;
}}
QLineEdit {{
    background: {C_PANEL};
    color: {C_TEXT};
    border: 1px solid {C_BORDER};
    border-radius: 4px;
    padding: 6px 10px;
    font-size: 10pt;
}}
QLineEdit:focus {{
    border: 1px solid {C_ACCENT};
}}
QTextEdit {{
    background: #0f0d1a;
    color: {C_TEXT};
    border: 1px solid {C_BORDER};
    font-family: "Consolas", "Courier New", monospace;
    font-size: 9pt;
}}
QPushButton {{
    border-radius: 5px;
    padding: 8px 18px;
    font-weight: bold;
    font-size: 10pt;
    border: none;
}}
QPushButton:hover {{ opacity: 0.85; }}
QPushButton:disabled {{ background: #4b5563; color: #6b7280; }}
QDateEdit {{
    background: {C_PANEL};
    color: {C_TEXT};
    border: 1px solid {C_BORDER};
    border-radius: 4px;
    padding: 5px 8px;
}}
QDateEdit::drop-down {{
    border: none;
    background: {C_ACCENT};
    border-radius: 3px;
    width: 20px;
}}
QCalendarWidget {{
    background: {C_PANEL};
    color: {C_TEXT};
}}
QCalendarWidget QAbstractItemView {{
    background: {C_PANEL};
    color: {C_TEXT};
    selection-background-color: {C_ACCENT};
}}
QCalendarWidget QToolButton {{
    background: {C_ACCENT};
    color: white;
    border-radius: 3px;
    padding: 4px;
}}
QCalendarWidget QMenu {{
    background: {C_PANEL};
    color: {C_TEXT};
}}
QScrollBar:vertical {{
    background: {C_PANEL};
    width: 8px;
    border-radius: 4px;
}}
QScrollBar::handle:vertical {{
    background: {C_ACCENT};
    border-radius: 4px;
    min-height: 20px;
}}
QFrame#statusFrame {{
    background: {C_PANEL};
    border: 1px solid {C_BORDER};
    border-radius: 6px;
    padding: 6px;
}}
QLabel#sectionTitle {{
    font-size: 13pt;
    font-weight: bold;
    color: #c4b5fd;
}}
QLabel#subText {{
    color: {C_SUBTEXT};
    font-size: 9pt;
}}
"""

def btn(text, color, callback, min_w=140):
    b = QPushButton(text)
    b.setStyleSheet(
        f"QPushButton {{ background:{color}; color:white; }}"
        f"QPushButton:hover {{ background:{color}cc; }}"
    )
    b.setMinimumWidth(min_w)
    b.clicked.connect(callback)
    return b

# ────────────────────────────────────────────────────────────────
#  stdout → Qt シグナル リダイレクター  [Bug1修正]
# ────────────────────────────────────────────────────────────────
class _StdoutRedirector(QObject):
    text_written = pyqtSignal(str)

    def __init__(self, original_stdout):
        super().__init__()
        self._original = original_stdout

    def write(self, text):
        if text and text.strip():
            try:
                self.text_written.emit(text.rstrip())
            except Exception:
                pass
        # ★ v2.3: frozen --windowed では _original が None の場合があるので安全化
        if self._original is not None:
            try:
                self._original.write(text)
            except Exception:
                pass

    def flush(self):
        if self._original is not None:
            try:
                self._original.flush()
            except Exception:
                pass

# ────────────────────────────────────────────────────────────────
#  設定ユーティリティ
# ────────────────────────────────────────────────────────────────
CHROME_PATHS = [
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    os.path.join(os.environ.get("LOCALAPPDATA", ""),
                 r"Google\Chrome\Application\chrome.exe"),
]
FIXED_ANALYTICS_URL   = "https://livecenter.tiktok.com/analytics/live_video?lang=ja-JP"

def find_chrome():
    for p in CHROME_PATHS:
        if p and os.path.isfile(p):
            return p
    return None

def validate_tiktok_id(v):
    if not v: return "TikTok ID を入力してください。"
    if len(v) > 24: return "TikTok ID は 24 文字以内で入力してください。"
    if not re.fullmatch(r"[a-zA-Z0-9_.]{1,24}", v):
        return "TikTok ID に使えない文字が含まれています。\n（英数字・アンダースコア・ピリオドのみ）"
    return None

def read_config_value(key):
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            for line in f:
                m = re.match(rf'^{re.escape(key)}\s*=\s*["\'](.+?)["\']', line)
                if m: return m.group(1)
    except Exception:
        pass
    return ""

def update_config(tiktok_id):
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        content = f.read()
    def replace_var(text, key, val):
        pat = rf'^({re.escape(key)}\s*=\s*)["\'].*?["\']'
        new, n = re.subn(pat, lambda m: m.group(1) + repr(val),
                         text, flags=re.MULTILINE)
        if n == 0: new += f"\n{key} = {repr(val)}\n"
        return new
    content = replace_var(content, "MY_TIKTOK_USERNAME", tiktok_id)
    content = replace_var(content, "ANALYTICS_URL", FIXED_ANALYTICS_URL)
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        f.write(content)

# ────────────────────────────────────────────────────────────────
#  データ読み込み
# ────────────────────────────────────────────────────────────────
def find_col(df, *kws):
    for kw in kws:
        for col in df.columns:
            if kw in col: return col
    return None

def _parse_watch_time_to_minutes(val):
    """「4分」「2時間11分」「6秒」「1分48秒」などを分単位の float に変換する。
    変換できない場合は 0.0 を返す。"""
    if pd.isna(val): return 0.0
    s = str(val).strip()
    if not s or s in ("N/A", "-", ""): return 0.0
    # まず純粋な数値なら分としてそのまま返す
    try: return float(s)
    except ValueError: pass
    total = 0.0
    m = re.search(r"(\d+)\s*時間", s)
    if m: total += int(m.group(1)) * 60
    m = re.search(r"(\d+)\s*分", s)
    if m: total += int(m.group(1))
    m = re.search(r"(\d+)\s*秒", s)
    if m: total += int(m.group(1)) / 60
    return total

def _parse_recommend_pct(val):
    """「60%」→ 60.0、「N/A」→ 0.0、「-」→ 0.0 に変換する。"""
    if pd.isna(val): return 0.0
    s = str(val).strip()
    if not s or s in ("N/A", "-", ""): return 0.0
    s = s.replace("%", "").strip()
    try: return float(s)
    except ValueError: return 0.0

def _insights_csv_path():
    try:
        import config as _cfg
        raw = getattr(_cfg, "CSV_INSIGHTS_FILE", "data/insights.csv")
    except Exception:
        raw = "data/insights.csv"
    return os.path.join(DATA_DIR, os.path.splitext(os.path.basename(raw))[0] + ".csv")

def load_insights():
    if not _HAS_PANDAS: return None, "pandas が必要です"
    path = _insights_csv_path()
    if not os.path.exists(path): return None, f"insights.csv が見つかりません\n{path}"
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
        date_col = next((c for c in df.columns
                         if any(k in c for k in ["日","date","時","取得"])), None)
        df["_date"] = pd.to_datetime(
            df[date_col] if date_col else df.iloc[:,0], errors="coerce")
        df = df.dropna(subset=["_date"]).sort_values("_date").reset_index(drop=True)
        return df, None
    except Exception as e:
        return None, str(e)

def load_gifts():
    if not _HAS_PANDAS: return None, "pandas が必要です"
    if not os.path.exists(CSV_FILE): return None, "gift_timeline.csv が見つかりません"
    try:
        df = pd.read_csv(CSV_FILE, encoding="utf-8-sig")
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.dropna(subset=["timestamp"])
        df_gift = df[df["type"] == "gift"].copy()
        if df_gift.empty: return df_gift, None
        def parse_detail(s):
            m = re.match(r"^(.+?)\s*[×x×](\d+)$", str(s).strip())
            return (m.group(1).strip(), int(m.group(2))) if m else (str(s).strip(), 1)
        parsed = df_gift["detail"].apply(parse_detail)
        df_gift["gift_name"] = [p[0] for p in parsed]
        df_gift["count"]     = [p[1] for p in parsed]
        df_gift["_date"]     = df_gift["timestamp"]
        return df_gift, None
    except Exception as e:
        return None, str(e)

def load_viewers():
    if not _HAS_PANDAS: return None, "pandas が必要です"
    if not os.path.exists(VIEWERS_FILE): return None, "viewers.csv が見つかりません"
    try:
        df = pd.read_csv(VIEWERS_FILE, encoding="utf-8-sig")
        df.columns = [c.strip().lower() for c in df.columns]
        if "session_date" not in df.columns:
            df = df.rename(columns={df.columns[0]: "session_date"})
        df["session_date"] = pd.to_datetime(df["session_date"], errors="coerce").dt.date
        return df.dropna(subset=["session_date"]), None
    except Exception as e:
        return None, str(e)

# ────────────────────────────────────────────────────────────────
#  PyQtGraph グラフ共通ユーティリティ
# ────────────────────────────────────────────────────────────────

def _make_plot_widget(title: str = "") -> pg.PlotWidget:
    """ダークテーマ済み PlotWidget を生成"""
    pw = pg.PlotWidget(title=f"<span style='color:#c4b5fd;font-size:10pt'>{title}</span>")
    pw.setBackground("#1e1b2e")
    pw.getPlotItem().getAxis("bottom").setPen(pg.mkPen(C_BORDER))
    pw.getPlotItem().getAxis("left").setPen(pg.mkPen(C_BORDER))
    pw.getPlotItem().getAxis("bottom").setTextPen(pg.mkPen(C_SUBTEXT))
    pw.getPlotItem().getAxis("left").setTextPen(pg.mkPen(C_SUBTEXT))
    pw.showGrid(x=False, y=True, alpha=0.2)
    return pw


def _set_x_labels(pw: pg.PlotWidget, labels: list):
    """棒グラフの X 軸に文字列ラベルを設定（日付など）"""
    ticks = [(i, lbl) for i, lbl in enumerate(labels)]
    ax = pw.getPlotItem().getAxis("bottom")
    ax.setTicks([ticks])


def _bar_graph(pw: pg.PlotWidget, values: list, color: str,
               labels: list | None = None, show_mean: bool = True):
    """縦棒グラフを描画してデータポイントを重ねる"""
    pw.clear()
    n = len(values)
    if n == 0:
        return

    x = list(range(n))
    bar = pg.BarGraphItem(x=x, height=values, width=0.6,
                          brush=pg.mkBrush(color + "cc"),
                          pen=pg.mkPen(color))
    pw.addItem(bar)

    # データポイント
    pw.plot(x, values, pen=None,
            symbol="o", symbolSize=7,
            symbolBrush=pg.mkBrush("white"),
            symbolPen=pg.mkPen("white"))

    # 値ラベル
    for xi, v in zip(x, values):
        txt = pg.TextItem(text=f"{int(v)}", color="white", anchor=(0.5, 1.0))
        txt.setFont(pg.QtGui.QFont(_JP_FONT, 7))
        txt.setPos(xi, v)
        pw.addItem(txt)

    # 平均線
    if show_mean and n > 0:
        mean_v = float(np.mean(values))
        inf_line = pg.InfiniteLine(
            pos=mean_v, angle=0,
            pen=pg.mkPen("red", width=1.5, style=pg.QtCore.Qt.PenStyle.DashLine),
            label=f"平均: {mean_v:.1f}",
            labelOpts={"color": "red", "position": 0.95,
                       "font": pg.QtGui.QFont(_JP_FONT, 8)}
        )
        pw.addItem(inf_line)

    if labels:
        _set_x_labels(pw, labels)

    # Y軸は正値のみ表示（下限0固定）
    max_v = max(values) if values else 0
    pw.setYRange(0, max_v * 1.15 if max_v > 0 else 1)
    pw.setLimits(yMin=0)


def _barh_graph(pw: pg.PlotWidget, values: list, labels: list, color: str):
    """横棒グラフを描画（Top10 ランキング用）"""
    pw.clear()
    n = len(values)
    if n == 0:
        return

    y = list(range(n))
    for yi, v in zip(y, values):
        bar = pg.BarGraphItem(x=[0], x1=[v], y=[yi - 0.3], y1=[yi + 0.3],
                              brush=pg.mkBrush(color + "cc"),
                              pen=pg.mkPen(color))
        pw.addItem(bar)

    # 値ラベル
    for yi, v in zip(y, values):
        txt = pg.TextItem(text=str(int(v)), color="white", anchor=(0.0, 0.5))
        txt.setFont(pg.QtGui.QFont(_JP_FONT, 7))
        txt.setPos(v * 1.02 if v > 0 else 0.1, yi)
        pw.addItem(txt)

    ticks = [(i, lbl) for i, lbl in enumerate(labels)]
    pw.getPlotItem().getAxis("left").setTicks([ticks])
    pw.getPlotItem().getAxis("bottom").setLabel("回数", color=C_SUBTEXT)

    # X軸（値軸）は正値のみ（下限0固定）
    max_v = max(values) if values else 0
    pw.setXRange(0, max_v * 1.2 if max_v > 0 else 1)
    pw.setLimits(xMin=0)


def _line_graph(pw: pg.PlotWidget, values: list, color: str,
                labels: list | None = None):
    """折れ線グラフを描画（データポイント＋マーカー付き）"""
    pw.clear()
    n = len(values)
    if n == 0:
        return
    x = list(range(n))
    # 折れ線
    pw.plot(x, values,
            pen=pg.mkPen(color, width=2),
            symbol="o", symbolSize=8,
            symbolBrush=pg.mkBrush(color),
            symbolPen=pg.mkPen("white", width=1))
    # 値ラベル
    for xi, v in zip(x, values):
        txt = pg.TextItem(text=f"{v:.1f}" if isinstance(v, float) and v != int(v)
                          else str(int(v)),
                          color="white", anchor=(0.5, 1.2))
        txt.setFont(pg.QtGui.QFont(_JP_FONT, 7))
        txt.setPos(xi, v)
        pw.addItem(txt)
    # 平均線
    if n > 0:
        mean_v = float(np.mean(values))
        inf_line = pg.InfiniteLine(
            pos=mean_v, angle=0,
            pen=pg.mkPen("red", width=1.5, style=pg.QtCore.Qt.PenStyle.DashLine),
            label=f"平均: {mean_v:.1f}",
            labelOpts={"color": "red", "position": 0.95,
                       "font": pg.QtGui.QFont(_JP_FONT, 8)}
        )
        pw.addItem(inf_line)
    if labels:
        _set_x_labels(pw, labels)

    # Y軸は正値のみ表示（下限0固定）
    max_v = max(values) if values else 0
    pw.setYRange(0, max_v * 1.2 if max_v > 0 else 1)
    pw.setLimits(yMin=0)


# ────────────────────────────────────────────────────────────────
#  円グラフウィジェット（PyQtGraph は円グラフ非対応のため QPainter で描画）
# ────────────────────────────────────────────────────────────────
class PieChartWidget(QWidget):
    """リピーター比率用シンプル円グラフ"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._slices: list[tuple[float, str, str]] = []  # (value, label, color)
        self._title: str = ""
        self.setMinimumSize(200, 200)

    def set_data(self, slices: list[tuple[float, str, str]], title: str = ""):
        """slices: [(value, label, color), ...]. value は実数（比率計算は内部で行う）"""
        self._slices = slices
        self._title  = title
        self.update()

    def paintEvent(self, event):  # noqa: N802
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)

        w, h = self.width(), self.height()
        bg_color = QColor(C_BG)
        painter.fillRect(0, 0, w, h, bg_color)

        if not self._slices:
            painter.setPen(QColor(C_SUBTEXT))
            painter.drawText(QRectF(0, 0, w, h),
                             Qt.AlignmentFlag.AlignCenter, "データなし")
            painter.end()
            return

        total = sum(v for v, _, _ in self._slices)
        if total <= 0:
            painter.setPen(QColor(C_SUBTEXT))
            painter.drawText(QRectF(0, 0, w, h),
                             Qt.AlignmentFlag.AlignCenter, "データなし")
            painter.end()
            return

        # タイトル描画
        title_h = 24 if self._title else 0
        if self._title:
            painter.setPen(QColor("#c4b5fd"))
            fnt = QFont(_JP_FONT, 9)
            fnt.setBold(True)
            painter.setFont(fnt)
            painter.drawText(QRectF(0, 2, w, title_h),
                             Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignTop,
                             self._title)

        # 凡例エリアの高さ
        legend_h = 20 * len(self._slices) + 8
        pie_size = min(w, h - title_h - legend_h) - 20
        if pie_size < 40:
            pie_size = 40
        pie_x = (w - pie_size) // 2
        pie_y = title_h + 8
        pie_rect = QRectF(pie_x, pie_y, pie_size, pie_size)

        # 円グラフ描画
        start_angle = 90 * 16  # 12時方向から開始（Qt の単位は 1/16 度）
        for val, lbl, color in self._slices:
            span = int(round(val / total * 360 * 16))
            painter.setPen(QColor(C_BG))
            painter.setBrush(QColor(color))
            painter.drawPie(pie_rect, start_angle, -span)
            # パーセント表示（スライス中心に）
            mid_angle_deg = (start_angle / 16) - (span / 16) / 2
            import math
            mid_rad = math.radians(mid_angle_deg)
            r = pie_size / 2 * 0.62
            cx = pie_x + pie_size / 2 + r * math.cos(mid_rad)
            cy = pie_y + pie_size / 2 - r * math.sin(mid_rad)
            pct = val / total * 100
            if pct >= 5:
                painter.setPen(QColor("white"))
                painter.setFont(QFont(_JP_FONT, 8))
                painter.drawText(QRectF(cx - 22, cy - 10, 44, 20),
                                 Qt.AlignmentFlag.AlignCenter,
                                 f"{pct:.1f}%")
            start_angle -= span

        # 凡例
        legend_y = pie_y + pie_size + 10
        painter.setFont(QFont(_JP_FONT, 8))
        for i, (val, lbl, color) in enumerate(self._slices):
            ly = legend_y + i * 20
            painter.setBrush(QColor(color))
            painter.setPen(Qt.PenStyle.NoPen)
            painter.drawRect(int(pie_x), int(ly + 2), 12, 12)
            painter.setPen(QColor(C_TEXT))
            cnt_str = f"{int(val)}人  {val/total*100:.1f}%"
            painter.drawText(int(pie_x + 18), int(ly + 13), f"{lbl}: {cnt_str}")

        painter.end()


# ────────────────────────────────────────────────────────────────
#  ライブ監視ワーカースレッド  [Bug1 / Bug2 修正]
# ────────────────────────────────────────────────────────────────
class LiveWorker(QThread):
    log_signal        = pyqtSignal(str)
    status_signal     = pyqtSignal(str, str)   # (text, color)
    finished_signal   = pyqtSignal()
    insight_started   = pyqtSignal()           # インサイト取得開始
    insight_finished  = pyqtSignal()           # インサイト取得完了

    def __init__(self, stop_event: threading.Event):
        super().__init__()
        self._stop = stop_event
        self._insight_thread = None

    def run(self):
        # ★ v2.3: クラッシュログをファイルに書き出す（デバッグ用）
        _log_path = os.path.join(_PROJECT_ROOT, "crash_log.txt")
        def _write_crash(msg):
            try:
                with open(_log_path, "a", encoding="utf-8") as _f:
                    import datetime
                    _f.write(f"[{datetime.datetime.now()}] {msg}\n")
            except Exception:
                pass
        _write_crash("LiveWorker.run() 開始")

        try:
            import asyncio
            import importlib
            import config
            # ★ Bug-A 修正: スレッド内でも config を最新状態に再読込
            _write_crash("config import 前")
            importlib.reload(config)
            _write_crash(f"config reload 完了: username={config.MY_TIKTOK_USERNAME}")
            from modules import live_bot as _lb_mod
            _write_crash("live_bot import 前")
            importlib.reload(_lb_mod)  # live_bot のモジュールレベル config も更新
            LiveBot = _lb_mod.LiveBot
            _write_crash("LiveBot クラス取得 OK")

            self.log_signal.emit(f"監視ボットを起動しました")
            self.log_signal.emit(f"監視対象: @{config.MY_TIKTOK_USERNAME}")
            self.status_signal.emit("🟢 監視中", C_GREEN)

            stop = self._stop  # ローカル参照（クロージャ用）

            def on_stream_end():
                """配信側が配信終了したときに自動でインサイト取得を実行"""
                self.log_signal.emit("配信終了を検知。3分後にインサイトを自動取得します…")
                # 3分待機（stop_event がセットされた＝手動停止の場合はスキップ）
                waited = 0
                while waited < 180:
                    if stop.is_set():
                        self.log_signal.emit("手動停止のためインサイト自動取得をスキップします")
                        return
                    time.sleep(5)
                    waited += 5
                self.insight_started.emit()
                self._run_insights()
                self.insight_finished.emit()

            bot = LiveBot(on_stream_end_callback=on_stream_end,
                          stop_event=self._stop)
            _write_crash("LiveBot インスタンス生成 OK")

            # ★ v2.2 修正: LiveBot生成後に config の値を注入（モジュールレベルのキャッシュ対策）
            bot.username = config.MY_TIKTOK_USERNAME
            _write_crash(f"bot.username = {bot.username}")

            # ★ v2.3 修正: Windows では SelectorEventLoop を使う（PyQt6 + ProactorEventLoop の競合対策）
            # ★ v2.3 修正: asyncio.set_event_loop() を削除（グローバル変数を汚染しない）
            if sys.platform == 'win32':
                loop = asyncio.SelectorEventLoop()
            else:
                loop = asyncio.new_event_loop()

            # ★ v2.3 修正: asyncio 例外ハンドラーをカスタム設定
            # --windowed ビルドでは sys.stderr=None のためデフォルトハンドラーがクラッシュする
            def _safe_exception_handler(loop_ref, context):
                msg = context.get('message', 'asyncio エラー')
                exc = context.get('exception')
                err_text = f"[asyncio] {msg}"
                if exc:
                    err_text += f": {exc}"
                try:
                    self.log_signal.emit(f"⚠️ {err_text}")
                except Exception:
                    pass

            loop.set_exception_handler(_safe_exception_handler)
            _write_crash("asyncio ループ準備完了。bot.start() 呼び出します")

            try:
                loop.run_until_complete(bot.start())
                _write_crash("bot.start() 正常終了")
            finally:
                # ★ 残タスクをキャンセルしてループをクリーンに閉じる
                try:
                    pending = {t for t in asyncio.all_tasks(loop)
                               if not t.done()}
                    if pending:
                        for t in pending:
                            t.cancel()
                        loop.run_until_complete(
                            asyncio.gather(*pending, return_exceptions=True))
                except Exception:
                    pass
                try:
                    loop.close()
                except Exception:
                    pass

        except ImportError as ex:
            # ★ Bug-B: 依存ライブラリ未インストール時の詳細メッセージ
            tb = traceback.format_exc()
            _write_crash(f"ImportError: {ex}\n{tb}")
            self.log_signal.emit(f"❌ インポートエラー: {ex}")
            self.log_signal.emit("ℹ️ 依存ライブラリが不足しています。")
            self.log_signal.emit("   pip install TikTokLive selenium webdriver-manager を実行してください")
            self.log_signal.emit(f"詳細:\n{tb[:500]}")
        except Exception as ex:
            tb = traceback.format_exc()
            _write_crash(f"Exception: {ex}\n{tb}")
            self.log_signal.emit(f"❌ ボットエラー: {ex}")
            self.log_signal.emit(f"詳細:\n{tb[:800]}")
        finally:
            _write_crash("LiveWorker.run() finally ブロック")
            self.status_signal.emit("⏹ 停止中", C_RED)
            self.finished_signal.emit()

    def _run_insights(self):
        """インサイト取得をこのスレッド内で実行"""
        self.log_signal.emit("インサイト取得中…")
        try:
            from modules.insights import collect_insights
            ok = collect_insights()
            if ok:
                self.log_signal.emit("✅ インサイト取得完了！")
            else:
                self.log_signal.emit("❌ インサイト取得失敗。debug_page.html を確認してください。")
        except Exception as ex:
            self.log_signal.emit(f"❌ インサイト取得エラー: {ex}")

    def trigger_insight_now(self):
        """停止ボタン押下時に手動でインサイト取得を起動 [Bug2修正]"""
        t = threading.Thread(target=self._run_insights, daemon=True)
        self._insight_thread = t
        t.start()
        self.log_signal.emit("停止要求を受け付けました。インサイト取得を開始します…")

# ────────────────────────────────────────────────────────────────
#  メインウィンドウ
# ────────────────────────────────────────────────────────────────
class KinakoApp(QMainWindow):

    def __init__(self):
        super().__init__()
        self.setWindowTitle("🦦 カワウソマネージャー きなこ")
        self.resize(1100, 800)

        # stdout リダイレクト [Bug1]
        self._redirector = _StdoutRedirector(sys.stdout)
        self._redirector.text_written.connect(self._on_stdout)
        sys.stdout = self._redirector

        self._bot_stop_event = threading.Event()
        self._live_worker: LiveWorker | None = None

        # グラフキャッシュ（PyQtGraph は PlotWidget を直接保持するため fig は不要）
        self._insight_df = None
        self._gift_df    = None
        self._repeat_df  = None

        self._build_ui()

    def _build_ui(self):
        self.setStyleSheet(STYLE_SHEET)

        central = QWidget()
        self.setCentralWidget(central)
        root_layout = QVBoxLayout(central)
        root_layout.setContentsMargins(0, 0, 0, 0)
        root_layout.setSpacing(0)

        # ── ヘッダー ──
        header = QWidget()
        header.setStyleSheet(f"background:{C_ACCENT};")
        header.setFixedHeight(54)
        h_lay = QHBoxLayout(header)
        h_lay.setContentsMargins(16, 0, 16, 0)

        title_lbl = QLabel("🦦 カワウソマネージャー きなこ")
        title_lbl.setStyleSheet("color:white; font-size:15pt; font-weight:bold;")
        h_lay.addWidget(title_lbl)
        h_lay.addStretch()

        self._btn_excel = btn("📊 Excelエクスポート", C_BLUE,  self._on_export_excel, 160)
        self._btn_csv   = btn("📥 CSVエクスポート",   C_TEAL, self._on_export_csv,   140)
        h_lay.addWidget(self._btn_excel)
        h_lay.addSpacing(8)
        h_lay.addWidget(self._btn_csv)

        root_layout.addWidget(header)

        # ── タブ ──
        self._tabs = QTabWidget()
        root_layout.addWidget(self._tabs)

        self._tab_live    = QWidget()
        self._tab_report  = QWidget()
        self._tab_setup   = QWidget()

        self._tabs.addTab(self._tab_live,    "📡  ライブ監視")
        self._tabs.addTab(self._tab_report,  "📊  レポート")
        self._tabs.addTab(self._tab_setup,   "⚙️  設定")

        self._build_setup_tab()
        self._build_live_tab()
        self._build_report_tab()

        # フッター
        footer = QLabel(f"データフォルダ: {DATA_DIR}")
        footer.setObjectName("subText")
        footer.setAlignment(Qt.AlignmentFlag.AlignCenter)
        footer.setStyleSheet(f"color:{C_SUBTEXT}; padding:4px;")
        root_layout.addWidget(footer)

    # ─────────────────────────────────────────────────────────
    #  ① セットアップタブ
    # ─────────────────────────────────────────────────────────
    def _build_setup_tab(self):
        lay = QVBoxLayout(self._tab_setup)
        lay.setContentsMargins(40, 24, 40, 24)
        lay.setSpacing(10)

        t = QLabel("⚙️  設定"); t.setObjectName("sectionTitle"); lay.addWidget(t)

        sub = QLabel("TikTok ID を入力して「保存して設定完了」を押してください")
        sub.setObjectName("subText"); lay.addWidget(sub)

        lay.addSpacing(10)

        lay.addWidget(QLabel("TikTok ID（@ なし）"))
        self._setup_id = QLineEdit(read_config_value("MY_TIKTOK_USERNAME"))
        self._setup_id.setPlaceholderText("例: kinako_tiktok")
        lay.addWidget(self._setup_id)

        lay.addSpacing(16)
        save_btn = btn("✅  保存して設定完了", C_ACCENT, self._on_setup_save)
        save_btn.setFixedHeight(44)
        lay.addWidget(save_btn)

        foot = QLabel("Chrome は自動検出・永続プロファイルで起動します")
        foot.setStyleSheet(f"color:{C_SUBTEXT}; font-size:8pt;")
        foot.setAlignment(Qt.AlignmentFlag.AlignCenter)
        lay.addWidget(foot)
        lay.addStretch()

    def _on_setup_save(self):
        tid = self._setup_id.text().strip().lstrip("@")
        # インサイトページURLはアプリ内で固定（ユーザー入力不要）
        url = FIXED_ANALYTICS_URL
        for err in (validate_tiktok_id(tid),):
            if err:
                QMessageBox.warning(self, "入力エラー", err); return
        # ★ Bug-C 修正: Chrome が見つからなくても設定保存は続行（警告のみ）
        # webdriver-manager が ChromeDriver を自動管理するため、
        # Chrome本体のパス確認に失敗しても実際の動作には支障がない場合がある
        chrome_warning = ""
        if not find_chrome():
            chrome_warning = ("\n\n⚠️ Google Chrome のパスが自動検出できませんでした。\n"
                              "インストール済みの場合は問題ありません。\n"
                              "Chrome がない場合は https://www.google.com/chrome/ からインストールしてください。")
        try:
            update_config(tid)
            # ★ Bug-A 修正③: config.py 書き換え後にキャッシュを破棄
            # 次に import config または importlib.reload(config) した時に
            # 必ず最新の config.py が読まれるようにする
            import sys as _sys
            _sys.modules.pop("config", None)
            QMessageBox.information(self, "設定完了 🎉",
                f"設定を保存しました！\n\n  ・TikTok ID : @{tid}\n\n"
                "「📡 ライブ監視」タブから配信監視を開始できます！" + chrome_warning)
        except PermissionError:
            QMessageBox.critical(self, "書き込みエラー",
                f"config.py に書き込めませんでした。\n{CONFIG_FILE}")
        except Exception as e:
            QMessageBox.critical(self, "エラー", str(e))

    # ─────────────────────────────────────────────────────────
    #  ② ライブ監視タブ
    # ─────────────────────────────────────────────────────────
    def _build_live_tab(self):
        lay = QVBoxLayout(self._tab_live)
        lay.setContentsMargins(24, 16, 24, 16)
        lay.setSpacing(10)

        t = QLabel("📡  ライブ監視ボット"); t.setObjectName("sectionTitle"); lay.addWidget(t)

        # 使い方の説明
        guide = QLabel(
            "【使い方】\n"
            "1. 「⚙️ 設定」タブで TikTok ID を登録してください\n"
            "2. 「▶ 監視開始」を押すとバックグラウンドで配信開始を待機します\n"
            "3. 配信を検知すると自動でギフト・視聴者の記録を開始します\n"
            "4. 配信終了後、3分待ってから自動でインサイトを取得します\n"
            "5. 取得完了後は「📊 レポート」タブでグラフを確認できます\n"
            "※ 緊急停止を押すと即座に監視を中断します（インサイト取得はスキップされます）"
        )
        guide.setObjectName("subText")
        guide.setWordWrap(True)
        lay.addWidget(guide)

        # ステータス行
        status_frame = QFrame(); status_frame.setObjectName("statusFrame")
        sf_lay = QHBoxLayout(status_frame)
        sf_lay.addWidget(QLabel("ステータス:"))
        self._status_lbl = QLabel("⏹ 停止中")
        self._status_lbl.setStyleSheet(f"color:{C_RED}; font-weight:bold; font-size:11pt;")
        sf_lay.addWidget(self._status_lbl)
        sf_lay.addStretch()
        lay.addWidget(status_frame)

        # ボタン行
        btn_row = QHBoxLayout()
        self._btn_start = btn("▶  監視開始", C_GREEN, self._on_live_start)
        self._btn_stop  = btn("🚨  緊急停止", C_RED,   self._on_live_stop)
        self._btn_stop.setEnabled(False)
        btn_row.addWidget(self._btn_start)
        btn_row.addWidget(self._btn_stop)
        btn_row.addStretch()
        lay.addLayout(btn_row)

        # インサイト取得中インジケーター（非表示で待機）
        self._insight_indicator = QFrame()
        ind_lay = QHBoxLayout(self._insight_indicator)
        ind_lay.setContentsMargins(8, 6, 8, 6)
        self._insight_spinner = QProgressBar()
        self._insight_spinner.setRange(0, 0)   # 不確定モード（グルグル）
        self._insight_spinner.setFixedHeight(18)
        self._insight_spinner.setFixedWidth(120)
        ind_lay.addWidget(self._insight_spinner)
        ind_lbl = QLabel("インサイト情報取得中…")
        ind_lbl.setStyleSheet(f"color:#f5a623; font-weight:bold;")
        ind_lay.addWidget(ind_lbl)
        ind_lay.addStretch()
        self._insight_indicator.setVisible(False)
        lay.addWidget(self._insight_indicator)

        lay.addWidget(QLabel("ログ"))
        self._log_view = QTextEdit()
        self._log_view.setReadOnly(True)
        lay.addWidget(self._log_view, stretch=1)

    @pyqtSlot(str)
    def _on_stdout(self, text):
        """stdout出力をログビューに転送 [Bug1修正]"""
        self._append_log(text)

    def _append_log(self, msg):
        ts = datetime.now().strftime("%H:%M:%S")
        self._log_view.append(f'<span style="color:{C_SUBTEXT};">[{ts}]</span> {msg}')
        self._log_view.moveCursor(QTextCursor.MoveOperation.End)

    def _on_live_start(self):
        try:
            import importlib
            import config
            # ★ Bug-A 修正: セットアップ後の config.py 書き換えを確実に反映させる
            # Python は一度 import したモジュールをキャッシュするため、
            # アプリ起動後にファイルを書き換えても古い値のまま → reload() で強制再読込
            importlib.reload(config)
            if not config.MY_TIKTOK_USERNAME:
                raise ValueError("MY_TIKTOK_USERNAME が未設定です")
        except ValueError as e:
            QMessageBox.critical(self, "設定未完了",
                f"設定に問題があります。\n\n{e}\n\n「⚙️ 設定」タブで設定してください。")
            return
        except Exception as e:
            QMessageBox.critical(self, "設定読み込みエラー",
                f"config.py の読み込みに失敗しました。\n\n{e}")
            return

        self._bot_stop_event.clear()
        self._live_worker = LiveWorker(self._bot_stop_event)
        self._live_worker.log_signal.connect(self._append_log)
        self._live_worker.status_signal.connect(self._set_status)
        self._live_worker.finished_signal.connect(self._on_bot_finished)
        self._live_worker.insight_started.connect(self._on_insight_started)
        self._live_worker.insight_finished.connect(self._on_insight_finished)
        self._live_worker.start()

        self._btn_start.setEnabled(False)
        self._btn_stop.setEnabled(True)

    @pyqtSlot(str, str)
    def _set_status(self, text, color):
        self._status_lbl.setText(text)
        self._status_lbl.setStyleSheet(
            f"color:{color}; font-weight:bold; font-size:11pt;")

    def _on_live_stop(self):
        """緊急停止ボタン：警告ポップアップを出してから stop_event をセット"""
        result = QMessageBox.warning(
            self, "⚠️ 緊急停止",
            "監視を緊急停止します。\n\n"
            "・現在の配信データの記録は中断されます\n"
            "・インサイトの自動取得は行われません\n\n"
            "本当に停止しますか？",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        if result != QMessageBox.StandardButton.Yes:
            return
        self._bot_stop_event.set()
        self._btn_stop.setEnabled(False)
        self._set_status("⏸ 停止中…", "#f59e0b")
        self._append_log("🚨 緊急停止リクエストを送信しました（インサイト取得はスキップ）")

    @pyqtSlot()
    def _on_insight_started(self):
        """インサイト取得開始 → インジケーターを表示"""
        self._insight_indicator.setVisible(True)
        self._set_status("🔄 インサイト取得中…", "#f5a623")

    @pyqtSlot()
    def _on_insight_finished(self):
        """インサイト取得完了 → インジケーターを非表示"""
        self._insight_indicator.setVisible(False)
        self._set_status("✅ インサイト取得完了", C_GREEN)

    @pyqtSlot()
    def _on_bot_finished(self):
        self._btn_start.setEnabled(True)
        self._btn_stop.setEnabled(False)
        self._insight_indicator.setVisible(False)
        self._set_status("⏹ 停止中", C_RED)
        self._append_log("監視ボットが停止しました")

    # ─────────────────────────────────────────────────────────
    #  ③ インサイト手動取得タブ
    # ─────────────────────────────────────────────────────────
    def _build_insight_fetch_tab(self, parent_widget):
        """レポートタブ内の「📥 インサイト取得」サブタブのコンテンツを構築"""
        lay = QVBoxLayout(parent_widget)
        lay.setContentsMargins(40, 24, 40, 24)
        lay.setSpacing(10)

        t = QLabel("📥  インサイト手動取得"); t.setObjectName("sectionTitle"); lay.addWidget(t)
        sub = QLabel("ボタンを押すと Chrome が自動起動し、TikTok LiveCenter から\n"
                     "最新の配信インサイトデータを取得して保存します。")
        sub.setObjectName("subText"); lay.addWidget(sub)

        lay.addSpacing(16)
        get_btn = btn("📥  今すぐインサイトを取得", "#0369a1", self._on_insight_get)
        get_btn.setFixedHeight(48)
        get_btn.setMaximumWidth(320)
        lay.addWidget(get_btn)

        lay.addSpacing(20)
        guide = QLabel(
            "【手動取得の使い方】\n"
            "1. 「⚙️ 設定」タブで TikTok ID を登録してください\n"
            "2. 「今すぐインサイトを取得」ボタンを押します\n"
            "3. Chrome が自動起動します（初回は TikTok ログインが必要）\n"
            "4. 取得完了後、「📊 インサイト」タブでグラフを確認できます\n\n"
            "※ ChromeDriver は自動インストールされます（webdriver-manager）"
        )
        guide.setObjectName("subText")
        lay.addWidget(guide)
        lay.addStretch()

    def _on_insight_get(self):
        try:
            import importlib, config
            importlib.reload(config)
            if not config.MY_TIKTOK_USERNAME:
                raise ValueError("TikTok ID が設定されていません。「⚙️ 設定」タブで設定してください。")
        except Exception as e:
            QMessageBox.critical(self, "設定エラー", str(e))
            return
        if QMessageBox.question(self, "インサイト手動取得",
            "TikTok LiveCenter のインサイトを今すぐ取得します。\n\n"
            "Chrome が自動的に起動します。OK で開始しますか？") \
                != QMessageBox.StandardButton.Yes:
            return

        def run():
            try:
                from modules.insights import collect_insights
                ok = collect_insights()
                if ok:
                    self._show_info_later("✅ 取得完了",
                        "インサイトの取得が完了しました！\n保存先: data/insights.csv")
                else:
                    self._show_warn_later("⚠️ 取得失敗",
                        "インサイトの取得に失敗しました。\n"
                        "・TikTok にログインしているか確認\n"
                        "・data/debug_page.html で詳細確認")
            except Exception as ex:
                self._show_err_later("❌ エラー", str(ex))
                traceback.print_exc()

        threading.Thread(target=run, daemon=True).start()

    def _show_info_later(self, t, m):
        QTimer.singleShot(0, lambda: QMessageBox.information(self, t, m))
    def _show_warn_later(self, t, m):
        QTimer.singleShot(0, lambda: QMessageBox.warning(self, t, m))
    def _show_err_later(self, t, m):
        QTimer.singleShot(0, lambda: QMessageBox.critical(self, t, m))

    # ─────────────────────────────────────────────────────────
    #  ④ レポートタブ  [Bug4修正 + グラフ改善]
    # ─────────────────────────────────────────────────────────
    def _build_report_tab(self):
        if not _HAS_PANDAS:
            lay = QVBoxLayout(self._tab_report)
            lay.addWidget(QLabel("pandas が必要です: pip install pandas"))
            return

        lay = QVBoxLayout(self._tab_report)
        lay.setContentsMargins(8, 8, 8, 8)
        lay.setSpacing(4)

        sub_tabs = QTabWidget()
        sub_tabs.setStyleSheet(f"""
            QTabBar::tab {{ padding:6px 16px; font-size:9pt; }}
            QTabBar::tab:selected {{ background:{C_ACCENT2}; color:white; }}
        """)
        lay.addWidget(sub_tabs)

        self._rtab_insight      = QWidget()
        self._rtab_gift         = QWidget()
        self._rtab_repeat       = QWidget()
        self._rtab_ranking      = QWidget()
        self._rtab_fetch        = QWidget()
        sub_tabs.addTab(self._rtab_insight,      "📊  インサイト")
        sub_tabs.addTab(self._rtab_gift,         "🎁  ギフト")
        sub_tabs.addTab(self._rtab_repeat,       "👥  リピート率")
        sub_tabs.addTab(self._rtab_ranking,      "🏆  ユーザーランキング")
        sub_tabs.addTab(self._rtab_fetch,        "📥  インサイト取得")
        self._report_sub_tabs = sub_tabs

        self._build_insight_report()
        self._build_gift_report()
        self._build_repeat_report()
        self._build_ranking_tab()
        self._build_insight_fetch_tab(self._rtab_fetch)

        # 初期描画（遅延）
        QTimer.singleShot(400, self._on_show_insights)
        QTimer.singleShot(500, self._on_show_gift)
        QTimer.singleShot(600, self._on_show_repeat)
        QTimer.singleShot(700, self._on_show_ranking)

    def _make_ctrl_row(self, parent_lay, on_show):
        """日付範囲コントロール行を生成し、(date_start, date_end) を返す"""
        row = QHBoxLayout()
        row.setSpacing(8)

        today     = QDate.currentDate()
        one_month = today.addDays(-30)

        row.addWidget(QLabel("開始日"))
        de_start = QDateEdit(one_month)   # [Bug4修正] QDateEdit使用
        de_start.setCalendarPopup(True)
        de_start.setDisplayFormat("yyyy-MM-dd")
        de_start.setMinimumWidth(120)
        row.addWidget(de_start)

        row.addSpacing(10)
        row.addWidget(QLabel("終了日"))
        de_end = QDateEdit(today)
        de_end.setCalendarPopup(True)
        de_end.setDisplayFormat("yyyy-MM-dd")
        de_end.setMinimumWidth(120)
        row.addWidget(de_end)

        show_btn = btn("グラフを表示", C_ACCENT, on_show, 130)
        show_btn.setFixedHeight(34)
        row.addWidget(show_btn)
        row.addStretch()

        parent_lay.addLayout(row)
        return de_start, de_end

    # ── インサイトレポート ──
    def _build_insight_report(self):
        lay = QVBoxLayout(self._rtab_insight)
        lay.setContentsMargins(8, 8, 8, 8)
        lay.setSpacing(4)
        self._de_ins_start, self._de_ins_end = self._make_ctrl_row(lay, self._on_show_insights)

        # インサイトをサブタブで「トレンド（折れ線）」「集計（棒グラフ）」に分割
        ins_sub = QTabWidget()
        ins_sub.setStyleSheet(f"""
            QTabBar::tab {{ padding:4px 12px; font-size:8pt; }}
            QTabBar::tab:selected {{ background:{C_ACCENT2}; color:white; }}
        """)
        lay.addWidget(ins_sub, stretch=1)

        self._ins_tab_trend = QWidget()
        self._ins_tab_bar   = QWidget()
        ins_sub.addTab(self._ins_tab_trend, "📈  トレンド")
        ins_sub.addTab(self._ins_tab_bar,   "📊  集計")

        # ── トレンドタブ（折れ線）: LIVEおすすめ / 最高同時接続 / ギフト贈呈者数 / ユニーク視聴者数 ──
        trend_lay = QVBoxLayout(self._ins_tab_trend)
        trend_lay.setContentsMargins(4, 4, 4, 4)
        trend_grid = QWidget()
        tg = QGridLayout(trend_grid)
        tg.setSpacing(6)
        self._pw_trend = [
            _make_plot_widget("LIVEおすすめ率（%）"),     # [0]
            _make_plot_widget("最高同時視聴者数（人）"),  # [1]
            _make_plot_widget("ギフト贈呈者数（人）"),    # [2]
            _make_plot_widget("ユニーク視聴者数（人）"),  # [3]
        ]
        for i, pw in enumerate(self._pw_trend):
            tg.addWidget(pw, i // 2, i % 2)
        trend_lay.addWidget(trend_grid, stretch=1)

        # ── 集計タブ（棒グラフ）: ダイヤ数 / 平均視聴時間 ──
        bar_lay = QVBoxLayout(self._ins_tab_bar)
        bar_lay.setContentsMargins(4, 4, 4, 4)
        bar_row = QWidget()
        br = QHBoxLayout(bar_row)
        br.setSpacing(6)
        self._pw_bar_ins = [
            _make_plot_widget("ダイヤ数"),           # [0]
            _make_plot_widget("平均視聴時間（分）"), # [1]
        ]
        for pw in self._pw_bar_ins:
            br.addWidget(pw)
        bar_lay.addWidget(bar_row, stretch=1)

    def _on_show_insights(self):
        df, err = load_insights()
        if err or df is None: return
        s = pd.to_datetime(self._de_ins_start.date().toString("yyyy-MM-dd"))
        e = pd.to_datetime(self._de_ins_end.date().toString("yyyy-MM-dd")) \
            + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        df = df[(df["_date"] >= s) & (df["_date"] <= e)]
        # ★ 空でも先に代入（エクスポートで「未表示」エラーを防ぐ）
        self._insight_df = df
        if df.empty: return

        col_peak    = "最高同時視聴者数" if "最高同時視聴者数" in df.columns else find_col(df,"最高同時","peak")
        col_diamond = "ダイヤ合計"      if "ダイヤ合計"      in df.columns else find_col(df,"diamond")
        col_gifter  = "ギフト贈呈者数"  if "ギフト贈呈者数"  in df.columns else find_col(df,"ギフト贈呈","gifter")
        col_watch   = "平均視聴時間"    if "平均視聴時間"    in df.columns else find_col(df,"平均視聴","watch")
        col_rec     = "LIVEおすすめ"    if "LIVEおすすめ"    in df.columns else find_col(df,"おすすめ","recommend")
        col_unique  = "ユニーク視聴者数" if "ユニーク視聴者数" in df.columns else find_col(df,"ユニーク","unique")

        # 数値変換
        for col in [col_peak, col_diamond, col_gifter, col_unique]:
            if col: df[col] = pd.to_numeric(df[col], errors="coerce")
        if col_watch:
            df[col_watch] = df[col_watch].apply(_parse_watch_time_to_minutes)
        if col_rec:
            df[col_rec] = df[col_rec].apply(_parse_recommend_pct)

        # X軸ラベルを日付（MM/DD）で統一
        labels = (df["_date"].dt.strftime("%m/%d").tolist()
                  if "_date" in df.columns else [str(i) for i in range(len(df))])

        # ── 折れ線グラフ（トレンドタブ）──
        trend_cfgs = [
            (self._pw_trend[0], col_rec,    "#c084fc"),
            (self._pw_trend[1], col_peak,   "#4f86c6"),
            (self._pw_trend[2], col_gifter, "#7ed321"),
            (self._pw_trend[3], col_unique, "#f5a623"),
        ]
        for pw, col, color in trend_cfgs:
            try:
                pw.clear()
                if col and col in df.columns:
                    vals = df[col].fillna(0).tolist()
                    _line_graph(pw, vals, color, labels)
                else:
                    pw.addItem(pg.TextItem("データなし", color=C_SUBTEXT, anchor=(0.5, 0.5)))
            except Exception:
                pass

        # ── 棒グラフ（集計タブ）──
        bar_cfgs = [
            (self._pw_bar_ins[0], col_diamond, "#f5a623"),
            (self._pw_bar_ins[1], col_watch,   "#e87c7c"),
        ]
        for pw, col, color in bar_cfgs:
            try:
                pw.clear()
                if col and col in df.columns:
                    vals = df[col].fillna(0).tolist()
                    _bar_graph(pw, vals, color, labels)
                else:
                    pw.addItem(pg.TextItem("データなし", color=C_SUBTEXT, anchor=(0.5, 0.5)))
            except Exception:
                pass

    # ── ギフトレポート ──
    def _build_gift_report(self):
        lay = QVBoxLayout(self._rtab_gift)
        lay.setContentsMargins(8, 8, 8, 8)
        lay.setSpacing(4)
        self._de_gift_start, self._de_gift_end = self._make_ctrl_row(lay, self._on_show_gift)

        # 上段：時間帯別ギフト回数（横長）
        self._pw_gift_hourly = _make_plot_widget("時間帯別ギフト回数（0〜23時）")
        lay.addWidget(self._pw_gift_hourly, stretch=1)

        # 下段：ギフト種別 Top10（横長・縦棒で横軸にギフト名）
        self._pw_gift_type = _make_plot_widget("ギフト種別 Top10")
        lay.addWidget(self._pw_gift_type, stretch=1)

    def _on_show_gift(self):
        df, err = load_gifts()
        if err or df is None or df.empty: return
        s = pd.to_datetime(self._de_gift_start.date().toString("yyyy-MM-dd")).date()
        e = (pd.to_datetime(self._de_gift_end.date().toString("yyyy-MM-dd"))
             + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)).date()
        df_r = df[(df["_date"].dt.date >= s) & (df["_date"].dt.date <= e)].copy()
        # ★ 空でも先に代入（エクスポートで「未表示」エラーを防ぐ）
        self._gift_df = df_r
        if df_r.empty: return

        # 時間帯別（0〜23時でデフォルト表示・マイナス不要）
        try:
            self._pw_gift_hourly.clear()
            # 0〜23 の全時間帯を用意（データがない時間は 0）
            all_hours = list(range(24))
            if "_date" in df_r.columns:
                df_r["hour"] = df_r["_date"].dt.hour
                hourly_raw = df_r.groupby("hour").size()
                vals = [int(hourly_raw.get(h, 0)) for h in all_hours]
                bar = pg.BarGraphItem(x=all_hours, height=vals, width=0.6,
                                      brush=pg.mkBrush("#f5a623cc"),
                                      pen=pg.mkPen("#f5a623"))
                self._pw_gift_hourly.addItem(bar)
                for xi, v in zip(all_hours, vals):
                    if v > 0:
                        t = pg.TextItem(str(v), color="white", anchor=(0.5, 1.0))
                        t.setFont(pg.QtGui.QFont(_JP_FONT, 7))
                        t.setPos(xi, v)
                        self._pw_gift_hourly.addItem(t)
                # Y軸の下限を 0 に固定（マイナス非表示）
                self._pw_gift_hourly.setYRange(0, max(vals) * 1.15 if max(vals) > 0 else 1)
                self._pw_gift_hourly.setLimits(yMin=0)
                self._pw_gift_hourly.getPlotItem().getAxis("bottom").setLabel("時刻（時）", color=C_SUBTEXT)
                self._pw_gift_hourly.getPlotItem().getAxis("left").setLabel("ギフト回数", color=C_SUBTEXT)
                # X 軸ティック（0〜23）
                ticks = [(h, str(h)) for h in all_hours]
                self._pw_gift_hourly.getPlotItem().getAxis("bottom").setTicks([ticks])
        except Exception:
            pass

        # ギフト種別 Top10（縦棒・横軸にギフト名を表示）
        try:
            self._pw_gift_type.clear()
            if "gift_name" in df_r.columns:
                col_count = "count" if "count" in df_r.columns else None
                tgt = (df_r.groupby("gift_name")[col_count].sum()
                       if col_count else df_r.groupby("gift_name").size())
                tgt = tgt.nlargest(10).sort_values()   # 小さい順（棒グラフ左から）
                gift_labels = tgt.index.tolist()
                gift_vals   = [int(v) for v in tgt.values.tolist()]
                # 縦棒グラフ（横軸＝ギフト名）
                x = list(range(len(gift_labels)))
                bar = pg.BarGraphItem(x=x, height=gift_vals, width=0.6,
                                      brush=pg.mkBrush("#4f86c6cc"),
                                      pen=pg.mkPen("#4f86c6"))
                self._pw_gift_type.addItem(bar)
                for xi, v in zip(x, gift_vals):
                    t = pg.TextItem(str(v), color="white", anchor=(0.5, 1.0))
                    t.setFont(pg.QtGui.QFont(_JP_FONT, 7))
                    t.setPos(xi, v)
                    self._pw_gift_type.addItem(t)
                # 横軸にギフト名を設定
                _set_x_labels(self._pw_gift_type, gift_labels)
                self._pw_gift_type.setYRange(0, max(gift_vals) * 1.15 if gift_vals else 1)
                self._pw_gift_type.setLimits(yMin=0)
                self._pw_gift_type.getPlotItem().getAxis("left").setLabel("個数", color=C_SUBTEXT)
        except Exception:
            pass

    # ── リピート率レポート ──
    def _build_repeat_report(self):
        lay = QVBoxLayout(self._rtab_repeat)
        lay.setContentsMargins(8, 8, 8, 8)
        lay.setSpacing(6)
        self._de_rep_start, self._de_rep_end = self._make_ctrl_row(lay, self._on_show_repeat)

        # 円グラフウィジェット（中央に大きく表示）
        self._pie_repeat = PieChartWidget()
        self._pie_repeat.setMinimumHeight(320)
        lay.addWidget(self._pie_repeat, stretch=1)

        # サマリーラベル
        self._lbl_rep_summary = QLabel("グラフを表示してください")
        self._lbl_rep_summary.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._lbl_rep_summary.setStyleSheet(
            f"color:{C_SUBTEXT}; font-size:11pt; padding:6px;")
        lay.addWidget(self._lbl_rep_summary)

    def _on_show_repeat(self):
        df, err = load_viewers()
        if err or df is None or df.empty: return
        s = pd.to_datetime(self._de_rep_start.date().toString("yyyy-MM-dd")).date()
        e = (pd.to_datetime(self._de_rep_end.date().toString("yyyy-MM-dd"))
             + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)).date()
        df = df[(df["session_date"] >= s) & (df["session_date"] <= e)]
        # ★ 空でも先に代入（エクスポートで「未表示」エラーを防ぐ）
        self._repeat_df = df
        if df.empty:
            self._pie_repeat.set_data([], "データなし")
            self._lbl_rep_summary.setText("期間内のデータがありません")
            return

        uid_col  = "uid" if "uid" in df.columns else \
                   (df.columns[2] if len(df.columns) > 2 else None)
        if uid_col is None: return

        sc      = df.groupby(uid_col)["session_date"].nunique()
        total   = len(sc)
        repeats = int((sc >= 2).sum())
        firsts  = total - repeats
        rate    = repeats / total * 100 if total > 0 else 0.0

        # 円グラフデータをセット
        slices = []
        if repeats > 0:
            slices.append((float(repeats), "リピーター",  C_ACCENT))
        if firsts > 0:
            slices.append((float(firsts),  "初回のみ",    "#c4b5fd"))
        title = f"リピーター比率  ({self._de_rep_start.date().toString('yyyy/MM/dd')} ～ {self._de_rep_end.date().toString('yyyy/MM/dd')})"
        self._pie_repeat.set_data(slices, title)

        # サマリーラベル
        self._lbl_rep_summary.setText(
            f"ユニーク視聴者: {total}人  ／  "
            f"リピーター: {repeats}人 ({rate:.1f}%)  ／  "
            f"初回のみ: {firsts}人"
        )
        self._lbl_rep_summary.setStyleSheet(
            f"color:{C_TEXT}; font-size:11pt; font-weight:bold; padding:6px;")

    # ── ユーザーランキングタブ ──
    def _build_ranking_tab(self):
        lay = QVBoxLayout(self._rtab_ranking)
        lay.setContentsMargins(8, 8, 8, 8)
        lay.setSpacing(6)
        self._de_rank_start, self._de_rank_end = self._make_ctrl_row(lay, self._on_show_ranking)

        # スプリッター: 上段=棒グラフ2本, 下段=テーブル
        splitter = QSplitter(Qt.Orientation.Vertical)
        lay.addWidget(splitter, stretch=1)

        # 棒グラフエリア（リピーター数 / ギフト額）
        graph_widget = QWidget()
        graph_lay    = QHBoxLayout(graph_widget)
        graph_lay.setContentsMargins(0, 0, 0, 0)
        graph_lay.setSpacing(6)
        self._pw_rank_repeat = _make_plot_widget("リピーター数 Top20")
        self._pw_rank_gift   = _make_plot_widget("ギフト合計 Top20（ダイヤ）")
        graph_lay.addWidget(self._pw_rank_repeat)
        graph_lay.addWidget(self._pw_rank_gift)
        splitter.addWidget(graph_widget)

        # テーブルエリア（QTextEdit でシンプルHTML表示）
        self._rank_table = QTextEdit()
        self._rank_table.setReadOnly(True)
        self._rank_table.setStyleSheet(
            f"background:{C_PANEL}; color:{C_TEXT}; "
            f"font-family:'Meiryo','MS Gothic',monospace; font-size:9pt; "
            f"border:1px solid {C_BORDER}; border-radius:4px;")
        splitter.addWidget(self._rank_table)
        splitter.setSizes([340, 200])

        self._ranking_df = None

    def _on_show_ranking(self):
        """viewers.csv と gift_timeline.csv を統合してユーザーランキングを表示"""
        s_str = self._de_rank_start.date().toString("yyyy-MM-dd")
        e_str = self._de_rank_end.date().toString("yyyy-MM-dd")
        s_date = pd.to_datetime(s_str).date()
        e_date = (pd.to_datetime(e_str) + pd.Timedelta(days=1)
                  - pd.Timedelta(seconds=1)).date()

        # ── viewers.csv からリピーター数（参加セッション数）を集計 ──
        rep_map: dict = {}   # uid -> (display_name, session_count)
        try:
            vdf, verr = load_viewers()
            if vdf is not None and not verr:
                vdf = vdf[(vdf["session_date"] >= s_date) & (vdf["session_date"] <= e_date)]
                uid_col  = next((c for c in vdf.columns
                                  if c.lower() in ("unique_id","uid","user_id")),
                                 vdf.columns[2] if len(vdf.columns) > 2 else None)
                name_col = "display_name" if "display_name" in vdf.columns else None
                if uid_col:
                    sc = vdf.groupby(uid_col)["session_date"].nunique()
                    for uid, cnt in sc.items():
                        name = uid
                        if name_col:
                            row_match = vdf[vdf[uid_col] == uid]
                            if not row_match.empty:
                                name = row_match.iloc[0][name_col]
                        rep_map[uid] = (str(name), int(cnt))
        except Exception:
            pass

        # ── gift_timeline.csv からギフト合計（ダイヤ）を集計 ──
        gift_map: dict = {}  # uid -> (display_name, total_diamonds)
        try:
            gdf, gerr = load_gifts()
            if gdf is not None and not gerr:
                gdf["_gdate"] = gdf["_date"].dt.date
                gdf = gdf[(gdf["_gdate"] >= s_date) & (gdf["_gdate"] <= e_date)]
                if not gdf.empty:
                    # ユーザー列を探す（unique_id / uid / user_id 等）
                    uid_g   = next((c for c in gdf.columns
                                    if c.lower() in ("unique_id","uid","user_id","username")), None)
                    name_g  = next((c for c in gdf.columns
                                    if c.lower() in ("display_name","name","nickname","user")), None)
                    # ダイヤ数列を探す（diamonds / diamond / reward 等）
                    dia_col = next((c for c in gdf.columns
                                    if c.lower() in ("diamonds","diamond","reward","ダイヤ","ダイヤ合計")), None)
                    if uid_g:
                        if dia_col:
                            gdf[dia_col] = pd.to_numeric(gdf[dia_col], errors="coerce").fillna(0)
                            gs = gdf.groupby(uid_g)[dia_col].sum()
                        else:
                            # ダイヤ列がなければ count 列を代用
                            count_c = "count" if "count" in gdf.columns else None
                            if count_c:
                                gdf[count_c] = pd.to_numeric(gdf[count_c], errors="coerce").fillna(0)
                                gs = gdf.groupby(uid_g)[count_c].sum()
                            else:
                                gs = gdf.groupby(uid_g).size()
                        for uid, total in gs.items():
                            name = uid
                            if name_g:
                                row_match = gdf[gdf[uid_g] == uid]
                                if not row_match.empty:
                                    name = row_match.iloc[0][name_g]
                            gift_map[uid] = (str(name), float(total))
        except Exception:
            pass

        # ── 統合 DataFrame を作成 ──
        all_uids = set(rep_map.keys()) | set(gift_map.keys())
        if not all_uids:
            self._rank_table.setHtml(
                f"<p style='color:{C_SUBTEXT};padding:12px;'>期間内のデータがありません</p>")
            self._pw_rank_repeat.clear()
            self._pw_rank_gift.clear()
            self._ranking_df = None
            return

        rows = []
        for uid in all_uids:
            rep_name, rep_cnt = rep_map.get(uid, (uid, 0))
            gift_name, gift_total = gift_map.get(uid, (uid, 0.0))
            display = rep_name if rep_name != uid else gift_name
            rows.append({
                "ユーザーID":    str(uid),
                "表示名":        str(display),
                "参加セッション数": int(rep_cnt),
                "ギフト合計(ダイヤ)": float(gift_total),
            })

        rank_df = pd.DataFrame(rows).sort_values(
            ["ギフト合計(ダイヤ)", "参加セッション数"], ascending=False
        ).reset_index(drop=True)
        rank_df.index = rank_df.index + 1  # 1始まりランク
        # エクスポート用に「順位」列を先頭に追加したコピーを保持
        self._ranking_df = rank_df.reset_index().rename(columns={"index": "順位"})

        # ── 棒グラフ: リピーター数 Top20 ──
        top_rep = rank_df.nlargest(20, "参加セッション数")
        if not top_rep.empty:
            labels_r = top_rep["表示名"].tolist()
            vals_r   = top_rep["参加セッション数"].tolist()
            self._pw_rank_repeat.clear()
            x = list(range(len(labels_r)))
            bar_r = pg.BarGraphItem(x=x, height=vals_r, width=0.6,
                                    brush=pg.mkBrush(C_ACCENT + "cc"),
                                    pen=pg.mkPen(C_ACCENT))
            self._pw_rank_repeat.addItem(bar_r)
            for xi, v in zip(x, vals_r):
                t = pg.TextItem(str(v), color="white", anchor=(0.5, 1.0))
                t.setFont(pg.QtGui.QFont(_JP_FONT, 7))
                t.setPos(xi, v)
                self._pw_rank_repeat.addItem(t)
            _set_x_labels(self._pw_rank_repeat, labels_r)
            self._pw_rank_repeat.setYRange(0, max(vals_r) * 1.15 if vals_r else 1)
            self._pw_rank_repeat.setLimits(yMin=0)
            self._pw_rank_repeat.getPlotItem().getAxis("left").setLabel("セッション数", color=C_SUBTEXT)
        else:
            self._pw_rank_repeat.clear()

        # ── 棒グラフ: ギフト合計 Top20 ──
        top_gift = rank_df.nlargest(20, "ギフト合計(ダイヤ)")
        if not top_gift.empty and top_gift["ギフト合計(ダイヤ)"].sum() > 0:
            labels_g = top_gift["表示名"].tolist()
            vals_g   = [int(v) for v in top_gift["ギフト合計(ダイヤ)"].tolist()]
            self._pw_rank_gift.clear()
            x = list(range(len(labels_g)))
            bar_g = pg.BarGraphItem(x=x, height=vals_g, width=0.6,
                                    brush=pg.mkBrush("#f5a623cc"),
                                    pen=pg.mkPen("#f5a623"))
            self._pw_rank_gift.addItem(bar_g)
            for xi, v in zip(x, vals_g):
                t = pg.TextItem(str(v), color="white", anchor=(0.5, 1.0))
                t.setFont(pg.QtGui.QFont(_JP_FONT, 7))
                t.setPos(xi, v)
                self._pw_rank_gift.addItem(t)
            _set_x_labels(self._pw_rank_gift, labels_g)
            self._pw_rank_gift.setYRange(0, max(vals_g) * 1.15 if vals_g else 1)
            self._pw_rank_gift.setLimits(yMin=0)
            self._pw_rank_gift.getPlotItem().getAxis("left").setLabel("ダイヤ合計", color=C_SUBTEXT)
        else:
            self._pw_rank_gift.clear()

        # ── テーブル表示 (HTML) ──
        html = (
            f"<table width='100%' cellspacing='0' cellpadding='4' "
            f"style='border-collapse:collapse;'>"
            f"<tr style='background:{C_ACCENT2};color:white;font-weight:bold;'>"
            f"<th>順位</th><th>表示名</th><th>ユーザーID</th>"
            f"<th>参加セッション数</th><th>ギフト合計(ダイヤ)</th></tr>"
        )
        for i, (idx_val, row) in enumerate(rank_df.iterrows()):
            bg = C_PANEL if i % 2 == 0 else C_BG
            rank_no = idx_val  # 1始まりのランク
            html += (
                f"<tr style='background:{bg};'>"
                f"<td align='center'>{rank_no}</td>"
                f"<td>{row['表示名']}</td>"
                f"<td style='color:{C_SUBTEXT};font-size:8pt;'>{row['ユーザーID']}</td>"
                f"<td align='center'>{int(row['参加セッション数'])}</td>"
                f"<td align='center'>{int(row['ギフト合計(ダイヤ)'])}</td>"
                f"</tr>"
            )
        html += "</table>"
        self._rank_table.setHtml(html)

    # ─────────────────────────────────────────────────────────
    #  エクスポート
    # ─────────────────────────────────────────────────────────
    def _get_current_report(self):
        idx = self._report_sub_tabs.currentIndex() if hasattr(self, "_report_sub_tabs") else -1
        if idx == 0: return self._insight_df, \
            f"インサイト_{self._de_ins_start.date().toString('yyyyMMdd')}"
        if idx == 1: return self._gift_df, \
            f"ギフト_{self._de_gift_start.date().toString('yyyyMMdd')}"
        if idx == 2: return self._repeat_df, "リピート率レポート"
        if idx == 3: return getattr(self, "_ranking_df", None), "ユーザーランキング"
        return None, ""

    def _on_export_excel(self):
        df, title = self._get_current_report()
        if df is None:
            QMessageBox.warning(self, "未表示",
                "先にレポートタブでグラフを表示してください。"); return
        # df が空（期間内データなし）でも空ファイルとして保存する
        try:
            import openpyxl
        except ImportError:
            QMessageBox.critical(self, "ライブラリエラー",
                "openpyxl が必要です: pip install openpyxl"); return
        path, _ = QFileDialog.getSaveFileName(
            self, "Excelを保存", f"{title}.xlsx", "Excel (*.xlsx)")
        if not path: return
        try:
            wb = openpyxl.Workbook()
            ws = wb.active; ws.title = "データ"
            out = df.copy()
            for c in out.columns:
                if pd.api.types.is_datetime64_any_dtype(out[c]):
                    out[c] = out[c].astype(str)
            ws.append(list(out.columns))
            for row in out.itertuples(index=False): ws.append(list(row))
            wb.save(path)
            QMessageBox.information(self, "保存完了", f"保存しました:\n{path}")
        except Exception as e:
            QMessageBox.critical(self, "保存エラー", str(e))

    def _on_export_csv(self):
        df, title = self._get_current_report()
        if df is None:
            QMessageBox.warning(self, "未表示",
                "先にレポートタブでグラフを表示してください。"); return
        # df が空（期間内データなし）でも空ファイルとして保存する
        path, _ = QFileDialog.getSaveFileName(
            self, "CSVを保存", f"{title}.csv", "CSV (*.csv)")
        if not path: return
        try:
            out = df.copy()
            for c in out.columns:
                if pd.api.types.is_datetime64_any_dtype(out[c]):
                    out[c] = out[c].astype(str)
            out.to_csv(path, index=False, encoding="utf-8-sig")
            QMessageBox.information(self, "保存完了", f"保存しました:\n{path}")
        except Exception as e:
            QMessageBox.critical(self, "保存エラー", str(e))

    # ─────────────────────────────────────────────────────────
    #  終了処理
    # ─────────────────────────────────────────────────────────
    def closeEvent(self, event):
        if self._live_worker and self._live_worker.isRunning():
            r = QMessageBox.question(self, "終了確認",
                "ライブ監視ボットが動作中です。\n終了してよいですか？")
            if r != QMessageBox.StandardButton.Yes:
                event.ignore(); return
        self._bot_stop_event.set()
        sys.stdout = self._redirector._original
        event.accept()


# ────────────────────────────────────────────────────────────────
#  エントリポイント
# ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # ★ v2.3 修正: PyInstaller frozen exe での multiprocessing 対応
    # WebDriver (ChromeDriver) が subprocess を使う場合に必要
    if getattr(sys, 'frozen', False):
        import multiprocessing
        multiprocessing.freeze_support()

    # ★ v2.3 修正: Windows で SelectorEventLoop をデフォルトに設定
    # PyQt6 と asyncio (ProactorEventLoop) の競合を防ぐ
    if sys.platform == 'win32':
        import asyncio
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # ★ v2.3 修正: --windowed ビルドでの sys.stderr=None 対策
    # asyncio / TikTokLive 内部が stderr に書き込もうとしてクラッシュするのを防ぐ
    if sys.stderr is None:
        sys.stderr = open(os.devnull, 'w')
    if sys.stdout is None:
        sys.stdout = open(os.devnull, 'w')

    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    _init_pyqtgraph()   # ★ QApplication 生成後に PyQtGraph を初期化
    window = KinakoApp()
    window.show()
    sys.exit(app.exec())
