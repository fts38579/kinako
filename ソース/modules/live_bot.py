# -*- coding: utf-8 -*-
"""
カワウソマネージャー きなこ – LiveBot  v9.4
変更:
  v8.8: _gift_last を __init__ に移動（競合リスク解消）
  v8.9: stop_event 連携追加（GUI停止ボタンでループ終了）
  v9.0: [重要修正] client.start() → client.connect() に変更
        start() は非ブロッキング Task を返すだけで WebSocket が動かない
        connect() は接続が切れるまでブロックする正しい API
        gift.name の取得方法を修正（event.gift.name）
  v9.1: [修正] _fire_end_callback を daemon=True に変更
        [修正] モジュールレベルの config 参照を遅延ロードに変更
        [修正] _init_csv/_init_viewers_csv を None ガード付きに変更
  v9.2: [根本修正] stop_event セット時に connect() を即座に中断する仕組みを追加
        ─ 問題: connect() は WebSocket が物理的に切断されるまでブロックし続ける
                stop_event をセットしても connect() は気づかないので永遠に戻らない
                → 監視停止ボタンを押してもアプリが固まる（強制終了の真の原因）
        ─ 修正: _connect_with_stop() を追加
                asyncio.create_task(connect()) と並行して
                stop_watchdog() タスクを走らせ、stop_event 検知時に
                client.disconnect() を呼んで connect() を正常終了させる
  v9.3: [修正] TikTokLiveClient を毎回 NEW インスタンスで作成
        (再利用すると内部状態が残り接続が不安定になる)
        [修正] start() の直前に stop_event チェックを追加（高速応答）
        [修正] _connect_with_stop() の watchdog 間隔を 0.3 秒に短縮
        [修正] start()内の finally で client を確実にクリーンアップ
  v9.4: [根本修正2] connect() の代わりに start() + await task を使用
        ─ 問題: connect() は内部で start() + await task を行い、
                watchdog が disconnect() を呼ぶと disconnect() も await task する
                → 同じ task を 2 箇所から await する二重 await 競合が発生し不安定
        ─ 修正: _connect_with_stop() で client.start() を直接呼び
                返ってきた task を await する。watchdog は disconnect() ではなく
                task.cancel() で停止（より安全）
        [修正] finally の二重 disconnect を1回にまとめ (close_client=True)
"""

import sys
import os
import time
import asyncio
import threading
import traceback
import csv
import importlib
from typing import Optional, Callable

from TikTokLive import TikTokLiveClient
from TikTokLive.events import ConnectEvent, DisconnectEvent, GiftEvent, JoinEvent

# ── 定数 ──────────────────────────────────────────────────────────
_OFFLINE_SEC     = 30
_BLOCKED_SEC     = 600
_RETRY_BASE_SEC  = 5
_RETRY_MAX_SEC   = 120
_MAX_RETRIES     = 5
_GIFT_DEDUP_SEC  = 10

# ── プロジェクトルート ────────────────────────────────────────────
if getattr(sys, "frozen", False):
    _PROJECT_ROOT = os.path.dirname(sys.executable)
else:
    _PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def _data_path(rel: str) -> str:
    return os.path.join(_PROJECT_ROOT, rel)

# ★ v9.1: モジュールレベルでは一旦インポートするだけ
# 実際の値参照は LiveBot.__init__ 内で reload() 後に行う
try:
    import config
except ImportError:
    config = None  # type: ignore

# ── ユーティリティ ────────────────────────────────────────────────
def _safe_str(v) -> str:
    try:    return str(v) if v is not None else ""
    except: return ""

def _extract_user(event):
    """イベントからユーザー名・UID を安全に取得"""
    try:
        u    = event.user
        name = _safe_str(getattr(u, "display_name", "") or getattr(u, "nickname", ""))
        uid  = _safe_str(getattr(u, "unique_id",   "") or getattr(u, "uniqueId",  ""))
        return name or "不明", uid or "不明"
    except:
        return "不明", "不明"

# ── エラー分類 ────────────────────────────────────────────────────
def _is_offline_error(e: Exception) -> bool:
    msgs = ("hosting", "offline", "is not online", "is not live",
            "not currently live", "UserOffline", "LIVE_NOT_FOUND",
            "userofflineerror", "usernotfounderror")
    s    = str(e).lower()
    name = type(e).__name__.lower()
    return any(m.lower() in s for m in msgs) or name in msgs

def _is_blocked_error(e: Exception) -> bool:
    names = ("WebcastBlocked200Error", "DeviceBlocked", "DEVICE_BLOCKED")
    s = str(e)
    return any(n in s for n in names) or type(e).__name__ in names

def _is_rate_limit_error(e: Exception) -> bool:
    if _is_blocked_error(e):
        return False
    names = ("RateLimitError", "TooManyRequests", "rate_limit", "RateLimit")
    s = str(e)
    return any(n in s for n in names) or type(e).__name__ in names

# ── カウントダウン付きスリープ ────────────────────────────────────
async def _sleep_cd(seconds: int, label: str,
                    stop_event: Optional[threading.Event] = None):
    end      = time.time() + seconds
    log_step = 60 if seconds >= 600 else 10
    next_log = 0.0
    while True:
        remaining = end - time.time()
        if remaining <= 0:
            break
        if stop_event and stop_event.is_set():
            print(f"[LiveBot] ⏭ {label} – 停止要求により待機をスキップ")
            break
        if time.time() >= next_log:
            m, s = divmod(int(remaining), 60)
            print(f"[LiveBot] ⏳ {label} – 残り {m}分{s:02d}秒")
            next_log = time.time() + log_step
        await asyncio.sleep(min(5, max(0.1, remaining)))

# ── CSV（ギフトタイムライン） ────────────────────────────────────
# ★ v9.1: パスは None で初期化し、_resolve_paths() で設定
_CSV_FILE    = None
_CSV_HEADERS = ["timestamp", "type", "user", "unique_id", "detail"]

# ── viewers.csv（入室ログ） ───────────────────────────────────────
_VIEWERS_FILE    = None
_VIEWERS_HEADERS = ["session_date", "session_start", "unique_id", "display_name"]


def _resolve_paths():
    """config から CSV パスを取得する（遅延して呼び出し）"""
    global _CSV_FILE, _VIEWERS_FILE
    _CSV_FILE     = _data_path(getattr(config, "CSV_FILE",     "data/gift_timeline.csv"))
    _VIEWERS_FILE = _data_path(getattr(config, "VIEWERS_FILE", "data/viewers.csv"))


def _init_csv():
    if not _CSV_FILE:
        return
    os.makedirs(os.path.dirname(_CSV_FILE), exist_ok=True)
    if not os.path.exists(_CSV_FILE):
        with open(_CSV_FILE, "w", newline="", encoding="utf-8-sig") as f:
            csv.writer(f).writerow(_CSV_HEADERS)

def _append_csv(row_type, user, uid, detail):
    if not _CSV_FILE:
        return
    try:
        with open(_CSV_FILE, "a", newline="", encoding="utf-8-sig") as f:
            csv.writer(f).writerow([
                time.strftime("%Y-%m-%d %H:%M:%S"),
                row_type, user, uid, detail
            ])
    except Exception as e:
        print(f"[CSV] 書き込みエラー: {e}")


def _init_viewers_csv():
    if not _VIEWERS_FILE:
        return
    os.makedirs(os.path.dirname(_VIEWERS_FILE), exist_ok=True)
    if not os.path.exists(_VIEWERS_FILE):
        with open(_VIEWERS_FILE, "w", newline="", encoding="utf-8-sig") as f:
            csv.writer(f).writerow(_VIEWERS_HEADERS)

def _append_viewer(session_date: str, session_start: str, uid: str, name: str):
    if not _VIEWERS_FILE:
        return
    try:
        with open(_VIEWERS_FILE, "a", newline="", encoding="utf-8-sig") as f:
            csv.writer(f).writerow([session_date, session_start, uid, name])
    except Exception as e:
        print(f"[Viewers] 書き込みエラー: {e}")

# ── リピート率計算 ───────────────────────────────────────────────
def _calc_repeat_rate() -> tuple:
    if not _VIEWERS_FILE or not os.path.exists(_VIEWERS_FILE):
        return 0, 0, 0.0
    try:
        uid_sessions: dict = {}
        with open(_VIEWERS_FILE, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                uid  = row.get("unique_id", "").strip()
                date = row.get("session_date", "").strip()
                if uid and uid != "不明" and date:
                    uid_sessions.setdefault(uid, set()).add(date)
        total   = len(uid_sessions)
        repeats = sum(1 for s in uid_sessions.values() if len(s) >= 2)
        rate    = (repeats / total * 100) if total > 0 else 0.0
        return total, repeats, rate
    except Exception as e:
        print(f"[Repeat] 計算エラー: {e}")
        return 0, 0, 0.0

# ── LiveBot ───────────────────────────────────────────────────────
class LiveBot:
    def __init__(self,
                 on_stream_end_callback: Optional[Callable] = None,
                 stop_event: Optional[threading.Event] = None):
        # ★ v9.1: __init__ 内で config を再ロードして最新値を保証
        global config
        if config is not None:
            try:
                importlib.reload(config)
            except Exception:
                pass
        else:
            try:
                import config as _cfg
                config = _cfg
            except ImportError:
                pass

        # ★ v9.1: config 値初期化後にパス解決
        _resolve_paths()

        self.username          = getattr(config, 'MY_TIKTOK_USERNAME', '') if config else ''
        self._on_stream_end_cb = on_stream_end_callback
        self._stop_event       = stop_event  # GUI 停止ボタンと連携
        self.client: Optional[TikTokLiveClient] = None

        self._stream_started   = False
        self._stream_end_fired = False
        self._should_stop      = False
        self._end_cb_thread: Optional[threading.Thread] = None

        self._start_time         = None
        self._session_date       = ""
        self._session_start_str  = ""
        self._gift_last: dict    = {}

        _init_csv()
        _init_viewers_csv()
        print(f"[LiveBot] 初期化完了 – 監視対象: @{self.username}")

    def _is_stop_requested(self) -> bool:
        if self._stop_event and self._stop_event.is_set():
            return True
        return self._should_stop

    def _fire_end_callback(self):
        """on_stream_end_callback をスレッドで安全に一度だけ発火
        ★ v9.1: daemon=True（アプリ終了をブロックしない）
        """
        if self._on_stream_end_cb:
            if self._end_cb_thread is None or not self._end_cb_thread.is_alive():
                self._end_cb_thread = threading.Thread(
                    target=self._on_stream_end_cb, daemon=True)
                self._end_cb_thread.start()

    # ── イベントハンドラ ──────────────────────────────────────────

    async def _on_connect(self, event: ConnectEvent):
        self._start_time        = time.time()
        self._stream_end_fired  = False
        self._stream_started    = True
        self._session_date      = time.strftime("%Y-%m-%d")
        self._session_start_str = time.strftime("%H:%M:%S")
        print(f"[LiveBot] ✅ 配信開始検知: {self._session_start_str}")
        _append_csv("connect", self.username, "", "配信開始")

    async def _on_disconnect(self, event: DisconnectEvent):
        if self._stream_end_fired:
            return
        self._stream_end_fired = True

        duration = ""
        if self._start_time:
            s = int(time.time() - self._start_time)
            duration = f"{s // 60}分{s % 60}秒"

        print(f"[LiveBot] 📴 配信終了検知 ({duration}): {time.strftime('%H:%M:%S')}")
        _append_csv("disconnect", self.username, "", f"配信終了 {duration}")

        total, repeats, rate = _calc_repeat_rate()
        print("=" * 50)
        print(f"[リピート率] 累計ユニーク視聴者: {total}人")
        print(f"[リピート率] リピーター(2回以上): {repeats}人")
        print(f"[リピート率] リピート率: {rate:.1f}%")
        print("=" * 50)

        self._should_stop = True
        self._fire_end_callback()

    async def _on_gift(self, event: GiftEvent):
        try:
            name, uid = _extract_user(event)
            try:
                gift_name = _safe_str(event.gift.name) if event.gift else "不明"
            except Exception:
                gift_name = _safe_str(getattr(event, "gift_name", "不明"))
            if not gift_name:
                gift_name = "不明"

            if hasattr(event, 'streaking') and event.streaking:
                return

            count = getattr(event, "repeat_count", 1) or 1

            now    = time.time()
            key    = (uid, gift_name)
            last_t = self._gift_last.get(key, 0)
            if now - last_t < _GIFT_DEDUP_SEC:
                return
            self._gift_last[key] = now

            print(f"[Gift] 🎁 {name} が {gift_name} ×{count} を送りました")
            _append_csv("gift", name, uid, f"{gift_name} ×{count}")

        except Exception as e:
            print(f"[Gift] 処理エラー: {e}")
            traceback.print_exc()

    async def _on_join(self, event: JoinEvent):
        try:
            name, uid = _extract_user(event)
            print(f"[Join] 👋 {name} が入室しました")
            _append_csv("join", name, uid, "入室")
            if uid and uid != "不明":
                _append_viewer(
                    self._session_date,
                    self._session_start_str,
                    uid, name
                )
        except Exception as e:
            print(f"[Join] 処理エラー: {e}")

    # ── ★ v9.4 核心修正2: stop_event 対応の接続メソッド ─────────────────
    async def _connect_with_stop(self) -> None:
        """
        start() で Task を取得し、その Task を await しながら
        stop_watchdog で stop_event を監視する。

        stop_event がセットされたら:
          1. ws を直接 disconnect して WS ループを終わらせる
          2. task が自然に完了するのを待つ

        ★ v9.4 の変更点:
          connect() を廃止し start() + await task に置き換え。
          connect() 内部も start() + await task を行うため、
          watchdog から disconnect() を呼ぶと「同じ task を 2 箇所から await」
          する二重 await 競合が発生し不安定になる問題を修正。
        """
        assert self.client is not None

        stop = self._stop_event

        # start() は非ブロッキングで _event_loop_task(Task) を返す
        task = await self.client.start()
        print(f"[LiveBot] start() 完了 – WebSocket 接続中")

        async def stop_watchdog():
            """stop_event を 0.3 秒ごとにポーリングし、セットされたら WS を切断"""
            while True:
                await asyncio.sleep(0.3)
                if stop and stop.is_set():
                    print("[LiveBot] 🛑 停止要求を検知 → WebSocket を切断します")
                    try:
                        # ws を直接切断して _ws_client_loop を終わらせる
                        await self.client._ws.disconnect()
                    except Exception as e:
                        print(f"[LiveBot] ws.disconnect エラー（無視）: {e}")
                    break

        watchdog_task = asyncio.create_task(stop_watchdog())
        try:
            # task (_ws_client_loop) が終わるまでブロック
            try:
                await task
            except asyncio.CancelledError:
                pass  # 外部から cancel された場合は無視
            except Exception as e:
                raise  # 他の例外は上位に伝播
        finally:
            # task が終わったら watchdog も不要なのでキャンセル
            if not watchdog_task.done():
                watchdog_task.cancel()
                try:
                    await watchdog_task
                except asyncio.CancelledError:
                    pass

    # ── メインループ ──────────────────────────────────────────────

    async def start(self):
        retry    = 0
        rl_count = 0

        while True:
            # ── 停止チェック ──────────────────────────────────────
            if self._is_stop_requested():
                print("[LiveBot] 🛑 停止要求 – 監視ループを終了します")
                if self._stream_started and not self._stream_end_fired:
                    self._stream_end_fired = True
                    _append_csv("disconnect", self.username, "", "手動停止")
                    self._fire_end_callback()
                break

            print(f"[LiveBot] 🔄 @{self.username} への接続を試みます (試行 {retry + 1})")
            self._stream_started   = False
            self._stream_end_fired = False

            try:
                # ★ v9.3: 毎回新規インスタンスを作成（再利用すると内部状態が残り不安定）
                self.client = TikTokLiveClient(unique_id=f"@{self.username}")
                self.client.add_listener(ConnectEvent,    self._on_connect)
                self.client.add_listener(DisconnectEvent, self._on_disconnect)
                self.client.add_listener(GiftEvent,       self._on_gift)
                self.client.add_listener(JoinEvent,       self._on_join)

                print(f"[LiveBot] 接続中… (@{self.username})")
                # ★ v9.2: connect() → _connect_with_stop() に変更
                # stop_event がセットされると watchdog が disconnect() を呼ぶ
                await self._connect_with_stop()
                print(f"[LiveBot] 接続終了 (@{self.username})")
                rl_count = 0

            except Exception as e:
                err   = str(e)
                ename = type(e).__name__

                if _is_rate_limit_error(e):
                    rl_count += 1
                    wait = min(1800 * (2 ** (rl_count - 1)), 7200)
                    if "account_hour" in err:  wait = 3600
                    elif "room_id_day" in err: wait = 7200
                    print(f"[LiveBot] ⏳ レートリミット ({rl_count}回目) – {wait}秒待機")
                    retry = 0
                    await _sleep_cd(wait, "レートリミット待機", self._stop_event)

                elif _is_blocked_error(e):
                    rl_count = 0
                    print(f"[LiveBot] 🚫 ブロック ({ename}: {err[:80]}) – {_BLOCKED_SEC}秒待機")
                    retry = 0
                    await _sleep_cd(_BLOCKED_SEC, "ブロック待機", self._stop_event)

                elif _is_offline_error(e):
                    rl_count = 0
                    print(f"[LiveBot] 📴 @{self.username} はオフラインです – {_OFFLINE_SEC}秒後に再確認")
                    retry = 0
                    await _sleep_cd(_OFFLINE_SEC, "オフライン待機", self._stop_event)

                else:
                    rl_count = 0
                    print(f"[LiveBot] ❌ 予期しないエラー ({ename}): {err[:120]}")
                    traceback.print_exc()

                    if self._stream_started and not self._stream_end_fired:
                        self._stream_end_fired = True
                        duration = ""
                        if self._start_time:
                            s = int(time.time() - self._start_time)
                            duration = f"{s // 60}分{s % 60}秒"
                        _append_csv("disconnect", self.username, "", f"配信終了(例外) {duration}")
                        total, repeats, rate = _calc_repeat_rate()
                        print(f"[リピート率] {total}人中 {repeats}人リピーター ({rate:.1f}%)")
                        self._fire_end_callback()
                        self._should_stop = True
                    else:
                        retry += 1
                        wait = min(_RETRY_BASE_SEC * (2 ** (retry - 1)), _RETRY_MAX_SEC)
                        print(f"[LiveBot] リトライ {retry}/{_MAX_RETRIES} – {wait}秒後")
                        if retry >= _MAX_RETRIES:
                            retry = 0
                            print(f"[LiveBot] 最大リトライ到達 – しばらく待機")
                            await _sleep_cd(60, "リトライ超過待機", self._stop_event)
                        else:
                            await _sleep_cd(wait, "リトライ待機", self._stop_event)

            finally:
                # ★ v9.4: 後片付け: クライアントを確実にクリーンアップ（1回のみ）
                if self.client is not None:
                    try:
                        # close_client=True で HTTP クライアントも一緒に閉じる
                        await self.client.disconnect(close_client=True)
                    except Exception:
                        pass
                    self.client = None

            # _on_disconnect で _should_stop が立った場合もここで終了
            if self._should_stop:
                print("[LiveBot] ✅ 配信終了 – 監視ループ終了")
                break

            # 停止要求があればここでも終了
            if self._is_stop_requested():
                print("[LiveBot] 🛑 ループ終了（接続後停止要求）")
                break

        print("[LiveBot] 監視終了")
