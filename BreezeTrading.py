#!/usr/bin/env python3
BUILD_VERSION = "JAI-SHREE-RAM-r101-GENUINE-375-CANDLE-RECOVERY-2026.11.07"
import threading
import heapq
from collections import deque

# Strict interval guard while the date-wise historical replay is active.
# In a 1-minute run, optional 1-second refinement requests are suppressed.
_DATEWISE_HISTORICAL_INTERVAL_GUARD = None

# Historical downloader performance counters.  These are diagnostic only; they
# never alter candidate selection, candle contents, or trading calculations.
_HISTORICAL_PERF_LOCK = threading.Lock()
_HISTORICAL_PERF = {"cache_hits": 0, "cache_misses": 0, "api_calls": 0,
                    "api_seconds": 0.0, "api_failures": 0, "candles": 0}

# Historical REST requests are paced globally below the 100-calls/minute ceiling.
# Six read-only workers overlap network latency; request starts remain 0.68s apart
# (~88/minute maximum) to use available throughput without exceeding the limit.
_HISTORICAL_API_RATE_LOCK = threading.Lock()
_HISTORICAL_API_NEXT_ALLOWED = 0.0
_HISTORICAL_PARALLEL_SOCKET_TIMEOUT_ACTIVE = False
_NSE50_MAPPING_LOCK = threading.RLock()
_COMPANY_NAME_CACHE_LOCK = threading.RLock()
_SECURITY_CACHE_LOCK = threading.RLock()
_PAPER_SEED_CACHE_LOCK = threading.RLock()
def _historical_api_rate_wait():
    global _HISTORICAL_API_NEXT_ALLOWED
    with _HISTORICAL_API_RATE_LOCK:
        now = time.monotonic()
        wait = max(0.0, _HISTORICAL_API_NEXT_ALLOWED - now)
        _HISTORICAL_API_NEXT_ALLOWED = max(now, _HISTORICAL_API_NEXT_ALLOWED) + 0.58
    if wait > 0:
        time.sleep(wait)
def _historical_verbose_enabled():
    return str(os.getenv("BREEZE_HISTORICAL_VERBOSE", "0")).strip().lower() in {"1", "true", "yes", "on"}
def _historical_verbose_print(*args, **kwargs):
    if _historical_verbose_enabled():
        print(*args, **kwargs)
def _historical_perf_add(key, value=1):
    try:
        with _HISTORICAL_PERF_LOCK:
            if key == "api_seconds": _HISTORICAL_PERF[key] = float(_HISTORICAL_PERF.get(key, 0.0)) + float(value)
            else: _HISTORICAL_PERF[key] = int(_HISTORICAL_PERF.get(key, 0)) + int(value)
    except Exception:
        pass
def _historical_perf_reset():
    with _HISTORICAL_PERF_LOCK:
        for key in _HISTORICAL_PERF: _HISTORICAL_PERF[key] = 0.0 if key == "api_seconds" else 0
def _historical_perf_snapshot():
    with _HISTORICAL_PERF_LOCK:
        return dict(_HISTORICAL_PERF)

# ===== EMBEDDED logic_config.py =====
"""All configured trading logics shared by GUI, historical, paper and live engines."""
import json
import base64
import socket
import os
import shutil
import sqlite3
from pathlib import Path

# === FINAL_LATEST_GUI_LOGICS_REFRESH ===
_LOGIC_SETTINGS_FILE_SIGNATURE = None
def _refresh_latest_gui_logic_settings():
    """Reload GUI logic settings only when the file has actually changed."""
    global _LOGIC_SETTINGS_FILE_SIGNATURE
    try:
        # This file is also embedded/executed by the desktop launcher in some
        # builds, where __file__ is not present in the function's globals.
        # ROOT is established by the application bootstrap and is the canonical
        # location; fall back to the executable working directory only if needed.
        _root = globals().get("ROOT") or globals().get("APP_ROOT")
        if _root:
            _root = Path(_root)
        else:
            _candidate = globals().get("__file__")
            if _candidate:
                _root = Path(_candidate).resolve().parent
            else:
                _root = Path.cwd()
                if not (_root / "Config").is_dir() and (_root / "Jai_Shree_Ram" / "Config").is_dir():
                    _root = _root / "Jai_Shree_Ram"
        p = _root / "Config" / "logic_settings.json"
        if not p.exists():
            return
        st = os.stat(p)
        signature = (st.st_mtime_ns, st.st_size)
        if signature == _LOGIC_SETTINGS_FILE_SIGNATURE:
            return
        with open(p, "r", encoding="utf-8") as fh:
            latest = json.load(fh)
        if isinstance(latest, dict):
            # Update the in-memory config object used by the current process.
            for name, cfg in latest.items():
                if isinstance(cfg, dict):
                    if isinstance(globals().get("LOGIC_SETTINGS"), dict):
                        globals()["LOGIC_SETTINGS"][name] = cfg
            # Common alternate names used by versions of this program.
            for key in ("logic_settings", "LOGIC_CONFIG", "LOGICS"):
                obj = globals().get(key)
                if isinstance(obj, dict):
                    for name, cfg in latest.items():
                        if isinstance(cfg, dict):
                            obj[name] = cfg
            globals()["_LATEST_GUI_LOGIC_SETTINGS"] = latest
            _LOGIC_SETTINGS_FILE_SIGNATURE = signature
    except Exception as _e:
        try:
            print(f"Latest GUI logic refresh skipped: {_e}")
        except Exception:
            pass



# ===== OPTIONAL RECENT-POSITIVE-NEWS PRIORITY =====
# Public Google News RSS is used without an API key. News is a ranking preference,
# never a substitute for the configured technical/risk conditions. If RSS is
# unavailable, the engine safely falls back to its existing technical ranking.
_NEWS_PRIORITY_LOCK = None
_NEWS_PRIORITY_SCORES = {}
_NEWS_PRIORITY_UPDATED = 0.0
_NEWS_PRIORITY_STARTED = False

def _refresh_news_priority_scores(symbols):
    """Refresh recent NSE-share news priorities from public Google News RSS."""
    global _NEWS_PRIORITY_LOCK, _NEWS_PRIORITY_SCORES, _NEWS_PRIORITY_UPDATED
    import re, time, urllib.request, urllib.parse, xml.etree.ElementTree as ET
    from email.utils import parsedate_to_datetime
    from datetime import datetime, timezone, timedelta
    if _NEWS_PRIORITY_LOCK is None:
        import threading
        _NEWS_PRIORITY_LOCK = threading.Lock()
    query = 'NSE India stocks company shares order results contract earnings when:1d'
    url = 'https://news.google.com/rss/search?' + urllib.parse.urlencode({'q': query, 'hl':'en-IN', 'gl':'IN', 'ceid':'IN:en'})
    scores = {}
    positive = re.compile(r'\b(beats?|strong|record|profit|profits|growth|surge|surges|rally|rallies|upgrade|upgraded|order win|wins? order|contract|approved|approval|breakthrough|acquisition|buyback|dividend|expansion|capacity|deal won|inflow)\b', re.I)
    negative = re.compile(r'\b(fraud|probe|investigation|default|downgrade|downgraded|loss widens|losses widen|misses? estimates|weak outlook|warning|penalty|ban|banned|recall|resigns?|crash|fall sharply|decline|declines|debt concern|insolvency)\b', re.I)
    try:
        req = urllib.request.Request(url, headers={'User-Agent':'Mozilla/5.0 BreezeTradingNewsPriority/1.0'})
        with urllib.request.urlopen(req, timeout=4.0) as response:
            xml_data = response.read(1_500_000)
        root = ET.fromstring(xml_data)
        articles=[]
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        for item in root.findall('.//item'):
            title = (item.findtext('title') or '').strip()
            pub = (item.findtext('pubDate') or '').strip()
            if not title:
                continue
            try:
                dt = parsedate_to_datetime(pub)
                if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
                if dt.astimezone(timezone.utc) < cutoff: continue
            except Exception:
                # Undated RSS entries are not considered fresh enough for priority.
                continue
            articles.append(title)
        company_map = _load_nse_company_master()
        # Match only symbols in the configured NSE-EQ universe. Ignore very short
        # symbols to avoid accidental substring matches in ordinary headlines.
        universe = {str(x).strip().upper() for x in (symbols or []) if str(x).strip()}
        for sym in universe:
            name = str(company_map.get(sym, '') or '').strip()
            terms = [sym] if len(sym) >= 4 else []
            if name and name.upper() != sym and len(name) >= 5:
                terms.append(name)
            hits=[]
            for title in articles:
                match = False
                for term in terms:
                    if term == sym:
                        match = bool(re.search(r'(?<![A-Z0-9])' + re.escape(term) + r'(?![A-Z0-9])', title, re.I))
                    else:
                        match = term.lower() in title.lower()
                    if match: break
                if match: hits.append(title)
            if hits:
                joined=' '.join(hits)
                if negative.search(joined): scores[sym] = -1
                elif positive.search(joined): scores[sym] = 2
                else: scores[sym] = 1
        with _NEWS_PRIORITY_LOCK:
            _NEWS_PRIORITY_SCORES = scores
            _NEWS_PRIORITY_UPDATED = time.monotonic()
        print(f'NEWS PRIORITY: checked public RSS; recent relevant headlines for {len(scores)} NSE-EQ symbol(s). Positive-news shares rank first; technical/risk filters remain mandatory.')
        return True
    except Exception as exc:
        print(f'NEWS PRIORITY WARNING: public news feed unavailable ({type(exc).__name__}: {exc}); continuing with technical ranking only.')
        with _NEWS_PRIORITY_LOCK:
            _NEWS_PRIORITY_SCORES = {}
            _NEWS_PRIORITY_UPDATED = time.monotonic()
        return False

def _news_priority_score(symbol):
    lock = _NEWS_PRIORITY_LOCK
    if lock is None:
        return 0
    with lock:
        return int(_NEWS_PRIORITY_SCORES.get(str(symbol).strip().upper(), 0))

def _start_news_priority_worker(symbols):
    """Refresh headlines in a daemon thread so network calls never block tick handling."""
    global _NEWS_PRIORITY_STARTED
    if _NEWS_PRIORITY_STARTED:
        return
    import threading, time
    _NEWS_PRIORITY_STARTED = True
    def worker():
        while True:
            _refresh_news_priority_scores(symbols)
            time.sleep(300)
    threading.Thread(target=worker, name='BreezeNewsPriority', daemon=True).start()

ROOT = Path(__file__).resolve().parent
CONFIG_DIR = ROOT / "Config"
CONFIG_DIR.mkdir(parents=True, exist_ok=True)
CONFIG_PATH = CONFIG_DIR / "logic_settings.json"
CONFIG_SCHEMA_VERSION = 13

DEFAULT_LOGICS = {
    "Logic 1": {
        "enabled": True, "name": "Logic 1", "movement_only_mode": True, "open_direction": "BOTH",
        "open_movement": 0.0, "minimum_price": 100.0, "price_big": 0.0, "price_small": 0.0, "use_price_band": False,
        "turnover_cr": 0.0, "volume": 0, "buy_amount": 30000.0,
        "buy_entry_time": "09:30", "buy_exit_time": "15:30",
        "profit_target": 3.75, "initial_stop": 1.05, "activation": 0.0,
        "trailing_stop": 0.0, "historical_shares": 10, "max_orders": 10,
        "weak_move_exit_enabled": False, "weak_move_high_threshold": 0.1, "weak_move_stop": 0.2,
        "smart_mode": False, "smart_min_score": 9, "smart_min_volume_ratio": 3.0,
        "smart_require_vwap": True, "smart_require_breakout": False, "smart_require_market_up": False,
        "require_15m_orb": True,
        "require_52w_high_breakout": False, "require_52w_low_breakdown": False,
        "smart_breakout_lookback": 5, "smart_momentum_pct": 0.60, "smart_momentum_lookback_minutes": 15, "smart_rank_limit": 0, "smart_rank_by": "movement",
        "ma_filter_enabled": False, "ma_filter_200": False, "ma_filter_100": False, "ma_filter_20": False, "ma_filter_5": False, "ma_filter_cross_5": False,
        "gtt_pretrigger_gap": 0.15, "gtt_limit_buffer": 0.10, "use_gtt_pretrigger": False,
        "shared_universe_mode": "ALGORITHM",
    },
}

# Eight independent strategy slots. Logic 1 retains the existing default; the
# additional slots inherit its field schema and start disabled. Existing saved
# Logic 1 values are copied into newly introduced slots during config migration.
for _logic_index in range(2, 9):
    _logic_name = f"Logic {_logic_index}"
    _logic_defaults = dict(DEFAULT_LOGICS["Logic 1"])
    _logic_defaults["name"] = _logic_name
    _logic_defaults["enabled"] = False
    DEFAULT_LOGICS[_logic_name] = _logic_defaults

ACTIVE_LOGIC_NAMES = tuple(DEFAULT_LOGICS)



def _coerce_one(name, value, default):
    if isinstance(default, bool):
        # Accept real booleans plus legacy/string config values safely.
        # bool("false") is True, so parse common textual false values explicitly.
        if isinstance(value, str):
            v = value.strip().lower()
            if v in {"false", "0", "no", "off", "disabled", ""}:
                return False
            if v in {"true", "1", "yes", "on", "enabled"}:
                return True
        return bool(value)
    if isinstance(default, int):
        return int(float(value))
    if isinstance(default, float):
        return float(value)
    return str(value)


CONDITION_RANK_DEFAULTS = {
    # First ten are the user's original condition/risk sequence. The next five
    # add ranking boxes for every visible entry filter in the compact GUI.
    "minimum_price": 1, "entry_time": 2, "vwap": 3, "orb15": 4,
    "rvol": 5, "movement": 6, "investment": 7, "profit_target": 8,
    "stop_loss": 9, "max_trades": 10, "open_direction": 11,
    "price_big": 12, "price_small": 13, "turnover": 14, "volume": 15,
}

def normalize_condition_ranks(value):
    """Normalize priorities: 1..15 are explicit order; 0 means no priority."""
    src = value if isinstance(value, dict) else {}
    out = {}
    for key, default in CONDITION_RANK_DEFAULTS.items():
        try:
            raw = float(src.get(key, default))
            rank = int(raw) if raw.is_integer() else default
        except (TypeError, ValueError, OverflowError):
            rank = default
        out[key] = rank if 0 <= rank <= 15 else default
    # Only positive ranks must be unique. Zero may be assigned to any number
    # of conditions and means they use the stable default order after ranked
    # conditions; it is not an instruction to disable that condition's gate.
    active = [rank for rank in out.values() if rank > 0]
    if len(active) != len(set(active)):
        return dict(CONDITION_RANK_DEFAULTS)
    return out


def condition_rank_sequence(value):
    """Return all condition keys in exact Option-A evaluation order.

    Trading Logic selection is the outer/master step. Within each enabled logic,
    positive ranks run 1, 2, 3...; rank 0 conditions remain mandatory and follow
    in the documented default order. No condition is disabled by its rank.
    """
    ranks = normalize_condition_ranks(value)
    default_order = list(CONDITION_RANK_DEFAULTS)
    return sorted(
        default_order,
        key=lambda key: (
            ranks.get(key, 0) == 0,
            ranks.get(key, 0) if ranks.get(key, 0) > 0 else CONDITION_RANK_DEFAULTS.get(key, 99),
            default_order.index(key),
        ),
    )


def normalize(data):
    out = {}
    data = data if isinstance(data, dict) else {}
    _legacy_logic1 = data.get("Logic 1", {}) if isinstance(data.get("Logic 1", {}), dict) else {}
    for logic, defaults in DEFAULT_LOGICS.items():
        _raw_src = data.get(logic, None)
        if not isinstance(_raw_src, dict):
            # One-time migration: seed each newly exposed slot from the user's
            # saved Logic 1 values, but keep the new slot disabled by default.
            src = dict(_legacy_logic1) if logic != "Logic 1" else {}
            if logic != "Logic 1":
                src["name"] = logic
                src["enabled"] = False
        else:
            src = _raw_src
        row = {}
        for key, default in defaults.items():
            try:
                row[key] = _coerce_one(key, src.get(key, default), default)
            except Exception:
                row[key] = default
        # Condition-priority ranking is shared by every mode and stored in each
        # logic row so old config files remain compatible with the same schema.
        row["condition_ranks"] = normalize_condition_ranks(src.get("condition_ranks"))
        # SMART scoring/ranking is retired; all modes use the same canonical rule filters.
        row["smart_mode"] = False
        if row["open_direction"] not in {"BOTH", "UP", "DOWN"}:
            row["open_direction"] = defaults["open_direction"]
        out[logic] = row
    return out


def load_logics():
    # The GUI-saved file is the sole source of truth for Historical/Paper/Live.
    # Logic 1..8 are all valid runtime slots.
    if CONFIG_PATH.exists():
        try:
            raw = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
            # The latest GUI-saved configuration is authoritative.  Do not infer
            # or overwrite trading parameters from old-package signatures.
            # This guarantees Historical/Paper/Live use exactly what the GUI shows.
            return normalize(raw)
        except Exception:
            pass
    safe = normalize(DEFAULT_LOGICS)
    for _name in safe:
        if _name != "Logic 1":
            safe[_name]["enabled"] = False
    return safe


def save_logics(data):
    normalized = normalize(data)
    payload = {name: dict(normalized[name]) for name in ACTIVE_LOGIC_NAMES}
    payload["_schema_version"] = CONFIG_SCHEMA_VERSION
    # Write-and-replace prevents the running PAPER watcher from reading a
    # half-written JSON file while the phone or laptop GUI saves settings.
    temp_path = CONFIG_PATH.with_name(CONFIG_PATH.name + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(str(temp_path), str(CONFIG_PATH))
    return normalized


def get_logic(logic_name, data=None):
    return (data or load_logics()).get(logic_name, DEFAULT_LOGICS[logic_name]).copy()


def enabled_logics(data=None):
    """Return every GUI-enabled strategy in stable Logic 1..8 order."""
    cfg = data or load_logics()
    return [
        (name, cfg.get(name, DEFAULT_LOGICS[name]))
        for name in ACTIVE_LOGIC_NAMES
        if bool(cfg.get(name, DEFAULT_LOGICS[name]).get("enabled", False))
    ]


def open_gap_pct(open_price, previous_close):
    """NSE opening gap versus the previous session close, in percent."""
    try:
        op, pc = float(open_price), float(previous_close)
        if op <= 0 or pc <= 0:
            return None
        return (op - pc) / pc * 100.0
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def open_movement_pct(price, open_price, previous_close=None):
    """Configured A-condition movement: current price versus previous close.

    If previous close is unavailable, retain the legacy open-based calculation as
    a defensive fallback; normal entry qualification separately checks direction.
    """
    try:
        px = float(price)
        base = float(previous_close) if previous_close is not None and float(previous_close) > 0 else float(open_price)
        if px <= 0 or base <= 0:
            return None
        return (px - base) / base * 100.0
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def direction_ok(open_price, previous_close, direction):
    """Validate opening direction against previous close; missing data fails closed."""
    if direction == "BOTH":
        try:
            return float(open_price) > 0
        except (TypeError, ValueError):
            return False
    gap = open_gap_pct(open_price, previous_close)
    if gap is None:
        return False
    if direction == "UP":
        return gap > 0
    if direction == "DOWN":
        return gap < 0
    return False


def open_price_band_ok(open_price, cfg):
    """Current GUI rule: Price Small <= NSE OPEN <= Price Big.

    Price Big/Price Small are an OPEN-PRICE eligibility filter. They are not
    applied to the later 5.2% intraday trigger price.
    """
    try:
        p = float(open_price)
        big = float(cfg.get("price_big", 0) or 0)
        small = float(cfg.get("price_small", 0) or 0)
        if p <= 0:
            return False
        if big <= 0 and small <= 0:
            return True
        if big <= 0:
            return p >= small
        if small <= 0:
            return p <= big
        lo, hi = min(small, big), max(small, big)
        return lo <= p <= hi
    except Exception:
        return False


price_band_ok = open_price_band_ok


def _parse_hhmm(value: str) -> tuple[int, int]:
    parts=str(value or '').strip().split(':')
    if len(parts)!=2: raise ValueError('time must be HH:MM')
    h=int(parts[0]); m=int(parts[1])
    if not (0<=h<=23 and 0<=m<=59): raise ValueError('time must be HH:MM between 00:00 and 23:59')
    return h,m

def entry_time_ok(dt, cfg) -> bool:
    try:
        eh,em=_parse_hhmm(cfg.get('buy_entry_time','09:15')); xh,xm=_parse_hhmm(cfg.get('buy_exit_time','15:20'))
        start=dt.replace(hour=eh,minute=em,second=0,microsecond=0)
        end=dt.replace(hour=xh,minute=xm,second=59,microsecond=999999)
        return start <= dt <= end
    except Exception: return False

def exit_time_due(dt, cfg) -> bool:
    try:
        h,m=_parse_hhmm(cfg.get('buy_exit_time','15:20'))
        return dt >= dt.replace(hour=h,minute=m,second=0,microsecond=0)
    except Exception: return False

def price_band_enabled(cfg):
    """Whether the GUI Price Small/Big range checkbox is enabled.

    The checkbox is authoritative.  Price Small/Big values can remain
    populated in the card while the range is disabled; in that state the
    values must not reject a share in Historical, Paper or Live trading.
    """
    if not isinstance(cfg, dict):
        return False
    return bool(cfg.get("use_price_band", False))


# Movement rule: qualify when the observed price reaches the rounded NSE trigger,
# allowing a small configurable overshoot because websocket quotes can skip ticks.
# The trigger is rounded UP to the first tradable NSE tick at/above the threshold.
def nse_equity_tick_size(reference_price):
    try:
        p = float(reference_price or 0)
    except (TypeError, ValueError):
        p = 0.0
    if p <= 0 or p < 250:
        return 0.01
    if p <= 1000:
        return 0.05
    if p <= 5000:
        return 0.10
    if p <= 10000:
        return 0.50
    if p <= 20000:
        return 1.00
    return 5.00


def entry_level(open_price, cfg, previous_close=None):
    """Return the rounded-up A-condition BUY trigger from previous close.

    Opening price is used for direction/eligibility. Movement is measured from
    previous close. The trigger is rounded up to the next NSE equity price tick;
    this avoids impossible fractional-paise trigger prices without allowing a
    fixed rupee overshoot that disproportionately affects low-priced shares.
    """
    movement = float(cfg.get("open_movement", 0) or 0)
    try:
        op = float(open_price or 0)
    except (TypeError, ValueError):
        op = 0.0
    if movement <= 0:
        return op
    try:
        base = float(previous_close) if previous_close is not None and float(previous_close) > 0 else op
    except (TypeError, ValueError):
        base = op
    if base <= 0:
        return 0.0
    raw_trigger = base * (1.0 + movement / 100.0)
    tick = nse_equity_tick_size(base)
    # Small epsilon prevents a mathematically exact tick being rounded one tick up
    # because of binary floating-point representation.
    return round(math.ceil(raw_trigger / tick - 1e-9) * tick, 2)


def entry_movement_qualifies_mask(prices, open_price, previous_close, cfg):
    """Vectorized form of the canonical movement-trigger predicate.

    Historical replay uses this exact predicate rather than an exact-price
    equality test, so a candle close that jumps over the trigger is treated the
    same way as a live tick.  It intentionally rejects gap-up opens and prices
    beyond the configured overshoot band.
    """
    values = np.asarray(prices, dtype=float)
    valid = np.isfinite(values) & (values > 0)
    try:
        movement = float(cfg.get("open_movement", 0) or 0)
        if movement <= 0:
            return valid
        trigger = float(entry_level(open_price, cfg, previous_close))
        op = float(open_price or 0)
        max_overshoot_pct = float(cfg.get("movement_max_overshoot_pct", 0.5))
        if not math.isfinite(max_overshoot_pct) or max_overshoot_pct < 0:
            max_overshoot_pct = 0.5
        if trigger <= 0 or op <= 0 or op >= trigger:
            return np.zeros(values.shape, dtype=bool)
        upper = trigger * (1.0 + max_overshoot_pct / 100.0) + 1e-9
        return valid & (values >= trigger) & (values <= upper)
    except (TypeError, ValueError, ZeroDivisionError, OverflowError):
        return np.zeros(values.shape, dtype=bool)


def entry_movement_qualifies(price, open_price, previous_close, cfg):
    """Scalar wrapper around the canonical, vectorized movement predicate."""
    try:
        return bool(entry_movement_qualifies_mask([float(price or 0)], open_price, previous_close, cfg)[0])
    except (TypeError, ValueError, IndexError):
        return False


def entry_check(price, open_price, previous_close, volume, turnover_cr, cfg):
    """Canonical BUY qualification shared by Historical, Paper, Paper-Live and LIVE.

    `price` is the price available at the decision timestamp: historical candle close /
    1-second tick LTP, or the live/paper LTP. Volume and turnover are cumulative values
    available at that same timestamp. No end-of-day values belong in this check.
    """
    try:
        op=float(open_price or 0); pc=float(previous_close or 0); px=float(price or 0)
        vol=float(volume or 0); turn=float(turnover_cr or 0)
        if op <= 0 or px <= 0:
            return {'passed': False, 'reason': 'INVALID_PRICE', 'checks': {
                'direction': False, 'movement': False, 'minimum_price': False, 'price_band': False, 'volume': False, 'turnover': False}}
        movement=float(cfg.get('open_movement', 0) or 0)
        min_price=float(cfg.get('minimum_price', 0) or 0)
        min_vol=float(cfg.get('volume', 0) or 0)
        min_turn=float(cfg.get('turnover_cr', 0) or 0)
        trigger=entry_level(op, cfg, pc)
        actual_movement = open_movement_pct(px, op, pc)
        checks={
            'direction': direction_ok(op, pc, cfg.get('open_direction', 'BOTH')),
            # Condition A: compare LTP against previous close when available;
            # session open is only the fallback base if previous close is missing.
            'movement': entry_movement_qualifies(px, op, pc, cfg),
            'minimum_price': (min_price <= 0 or px >= min_price),
            'price_band': ((not price_band_enabled(cfg)) or open_price_band_ok(op, cfg)),
            'volume': (min_vol <= 0 or vol >= min_vol),
            'turnover': (min_turn <= 0 or turn >= min_turn),
        }
        failed=[k for k,v in checks.items() if not v]
        return {
            'passed': bool(all(checks.values())), 'checks': checks, 'trigger_price': trigger,
            'actual_price': px, 'actual_volume': vol, 'actual_turnover_cr': turn,
            'actual_open_movement_pct': actual_movement,
            'open_gap_pct': open_gap_pct(op, pc),
            'required_minimum_price': min_price, 'required_volume': min_vol, 'required_turnover_cr': min_turn,
            'reason': '; '.join(failed) if failed else 'All configured entry conditions passed'
        }
    except Exception as exc:
        return {'passed': False, 'reason': f'INVALID_ENTRY_DATA:{type(exc).__name__}'}


def entry_qualifies(price, open_price, previous_close, volume, turnover_cr, cfg):
    return bool(entry_check(price, open_price, previous_close, volume, turnover_cr, cfg).get('passed'))


def initial_stop(entry, cfg=None):
    cfg = cfg or DEFAULT_LOGICS["Logic 1"]
    pct = float(cfg["initial_stop"])
    return float(entry) * (1.0 - pct / 100.0) if pct > 0 else 0.0


def activation_price(entry, cfg=None):
    cfg = cfg or DEFAULT_LOGICS["Logic 1"]
    pct = float(cfg["activation"])
    return float(entry) * (1.0 + pct / 100.0) if pct > 0 else 0.0


def target_price(entry, cfg=None):
    cfg = cfg or DEFAULT_LOGICS["Logic 1"]
    pct = float(cfg["profit_target"])
    return float(entry) * (1.0 + pct / 100.0) if pct > 0 else 0.0


def trailing_stop(highest, cfg=None):
    cfg = cfg or DEFAULT_LOGICS["Logic 1"]
    pct = float(cfg["trailing_stop"])
    return float(highest) * (1.0 - pct / 100.0) if pct > 0 else 0.0


def quantity(entry, cfg=None):
    cfg = cfg or DEFAULT_LOGICS["Logic 1"]
    try:
        return int(float(cfg["buy_amount"]) // float(entry)) if float(entry) > 0 and float(cfg["buy_amount"]) > 0 else 0
    except Exception:
        return 0



# ===== EMBEDDED excel_output.py =====
"""Fresh Excel output layer for the NSE trading system.

Excel is a reporting/output layer only. Historical and Paper/Live records are
written into the user's existing four-sheet template. Historical P&L values are
written from the chronological trading engine and are not recalculated from
maximum High/minimum Low timestamps inside Excel.
"""
from datetime import date, datetime
from pathlib import Path
from typing import Iterable
import math
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.formatting.rule import CellIsRule
from openpyxl.utils import get_column_letter

SHEETS=('YTD Historical Data','Selected Date','NSE Data','Paper Live Trading Shares','Run Status')
RUN_STATUS_HEADERS=['Updated At','Job','Status','Date Range','Records','Details','Output File']
HIST_HEADERS=['Date','Ranking','Logic','Symbol','Open','Close','Volume','Turnover in Cr','Buy Qty','Buy Price','Buy Date & Time','High After Buy','High After Buy Date & Time','Profit Target','Profit target achieved the goal','Low After Buy','Low After Buy Date & Time','Loss Target','Loss target achieved the goal','Exit Reason with Profit / Loss Target achieved the goal','Gross P&L','Buy Turnover','Sell Turnover','Brokerage','STT','Exchange Charges','GST','SEBI Charges','Stamp Duty','Other Charges','Total Charges','Net P&L','P&L %','Charge Source','Charge Plan','15:25 Price','15:25 Square-Off','15:25 Time','Historical Shares','Open Direction','Open Movement %','Price Big','Price Small','Required Turnover Cr','Profit Target %','Initial Stop Loss %','Required Volume','Activation %','Trailing Stop %','Historical Result','First Signal Time','Decision','Decision Reason','Sell Date & Time','Day High','Day High Time','Day Low','Day Low Time','Previous Close','Daily Movement ₹','Daily Movement %']
PAPER_HEADERS=['Trade ID','Mode','Date','Ranking','Logic','Symbol','Status','Quantity','Buy Price','Buy Date & Time','Sell Quantity','Sell Price','Sell Date & Time','High After Buy','High After Buy Date & Time','Low After Buy','Low After Buy Date & Time','Exit Reason','Gross P&L','Buy Turnover','Sell Turnover','Brokerage','STT','Exchange Charges','GST','SEBI Charges','Stamp Duty','Other Charges','Total Charges','Net P&L','P&L %','Charge Source','Charge Plan','Buy Order ID','Sell Order ID','Data Source','Last Update']
NSE_HEADERS=['Logic Serial No.','Date','Symbol','Open','High','Low','Close','Previous Close','Volume','Turnover in Cr','Movement %','Source','Downloaded At']


def _num(v):
    if v is None or isinstance(v,bool): return v
    if hasattr(v,'item'):
        try: return _num(v.item())
        except Exception: pass
    if isinstance(v,(int,float)):
        return None if isinstance(v,float) and not math.isfinite(v) else v
    if isinstance(v,str):
        s=v.strip().replace(',','').replace('₹','')
        if not s:return None
        try:
            x=float(s); return int(x) if x.is_integer() else x
        except Exception:return v
    return v


def _dt(v):
    if v in (None,''): return None
    if isinstance(v,(datetime,date)): return v
    s=str(v).strip()[:19]
    for f in ('%Y-%m-%d %H:%M:%S','%Y-%m-%dT%H:%M:%S','%Y-%m-%d','%d-%m-%Y','%d/%m/%Y'):
        try:return datetime.strptime(s,f) if '%H' in f else datetime.strptime(s,f).date()
        except ValueError:pass
    return v


def _safe(v):
    if isinstance(v,float) and not math.isfinite(v): return None
    return _num(v)


def _open(path:Path):
    path.parent.mkdir(parents=True,exist_ok=True)
    canonical_title='Breeze NSE Trading - Canonical Excel Output v1'
    wb=None
    if path.exists():
        try:
            # Load the workbook only once. The previous probe+reload doubled Excel
            # I/O before every historical batch save.
            wb=load_workbook(path)
            if wb.properties.title != canonical_title:
                wb.close(); wb=None
        except Exception:
            try:
                if wb is not None: wb.close()
            except Exception: pass
            wb=None
    if wb is None:
        wb=Workbook()
        ws=wb.active; ws.title=SHEETS[0]
        ws.append(HIST_HEADERS)
        for s,headers in ((SHEETS[1],HIST_HEADERS),(SHEETS[2],NSE_HEADERS),(SHEETS[3],PAPER_HEADERS),(SHEETS[4],RUN_STATUS_HEADERS)):
            wb.create_sheet(s).append(headers)
    else:
        for s in SHEETS:
            if s not in wb.sheetnames:
                wb.create_sheet(s)
        for s in list(wb.sheetnames):
            if s not in SHEETS: del wb[s]
    # Older canonical workbooks did not have the Run Status sheet. Initialize its
    # header explicitly so all future status updates are readable.
    if 'Run Status' in wb.sheetnames:
        ws_status = wb['Run Status']
        if ws_status.max_row == 1 and all(ws_status.cell(1,c).value is None for c in range(1,8)):
            ws_status.delete_rows(1, ws_status.max_row)
            ws_status.append(RUN_STATUS_HEADERS)
    wb._sheets=[wb[s] for s in SHEETS]
    wb.properties.title=canonical_title
    wb.properties.subject='Generated only from current NSE/Breeze data and canonical enabled-logic engine results'
    wb.properties.creator='Breeze NSE Trading canonical Excel output'
    wb.calculation.fullCalcOnLoad=True
    wb.calculation.forceFullCalc=True
    wb.calculation.calcMode='auto'
    return wb


def _header(ws,headers):
    ws.delete_rows(1,ws.max_row or 1)
    ws.append(headers)
    navy='17365D'; blue='2F75B5'; light='D9EAF7'; white='FFFFFF'
    thin=Side(style='thin',color='D9E1F2')
    for c,h in enumerate(headers,1):
        cell=ws.cell(1,c)
        cell.font=Font(bold=True,color=white,size=10)
        cell.fill=PatternFill('solid',fgColor=navy)
        cell.alignment=Alignment(horizontal='center',vertical='center',wrap_text=True)
        cell.border=Border(bottom=Side(style='medium',color=blue))
        ws.column_dimensions[get_column_letter(c)].width=max(12,min(28,len(str(h))+3))
    ws.row_dimensions[1].height=34
    ws.freeze_panes='A2'
    ws.sheet_view.showGridLines=False
    ws.sheet_view.zoomScale=85
    # Distinct worksheet tab colors for quick navigation.
    _tab_colors = {'YTD Historical Data':'17365D','Selected Date':'2F75B5','NSE Data':'70AD47','Paper Live Trading Shares':'7F7F7F','Run Status':'ED7D31'}
    if ws.title in _tab_colors:
        ws.sheet_properties.tabColor = _tab_colors[ws.title]


def _style(ws):
    thin=Side(style='hair',color='D9E1F2')
    even_fill=PatternFill('solid',fgColor='FFFFFF')
    odd_fill=PatternFill('solid',fgColor='F5F9FD')
    base_align=Alignment(vertical='center',wrap_text=False)
    status_align=Alignment(horizontal='center',vertical='center',wrap_text=True)
    good_font=Font(bold=True,color='008000')
    bad_font=Font(bold=True,color='C00000')
    border=Border(bottom=thin)
    max_row=ws.max_row; max_col=ws.max_column
    headers=[str(ws.cell(1,c).value or '').lower() for c in range(1,max_col+1)]
    for r in range(2,max_row+1):
        fill=even_fill if r % 2 == 0 else odd_fill
        for c,h in enumerate(headers,1):
            cell=ws.cell(r,c)
            cell.fill=fill; cell.border=border; cell.alignment=base_align
            if 'date & time' in h or h.endswith('time'): cell.number_format='dd-mm-yyyy hh:mm:ss'
            elif h=='date': cell.number_format='dd-mm-yyyy'
            elif h in ('daily movement %','open movement %','profit target %','initial stop loss %','activation %','trailing stop %'): cell.number_format='0.00\\%'
            elif h=='daily movement ₹': cell.number_format='₹#,##0.00;[Red]-₹#,##0.00'
            elif '%' in h: cell.number_format='0.00%'
            elif 'movement' in h: cell.number_format='#,##0.00;[Red]-#,##0.00'
            elif any(x in h for x in ('price','turnover','p&l','charges','open','high','low','close','target')): cell.number_format='#,##0.00;[Red]-#,##0.00'
            elif any(x in h for x in ('qty','quantity','volume','ranking','required volume')): cell.number_format='#,##0'
            if h in ('status','historical result','exit reason','entry validation'):
                cell.alignment=status_align
                v=str(cell.value).upper()
                if v in ('PROFIT','COMPLETED','YES','VALID','TARGET ACHIEVED'): cell.font=good_font
                elif v in ('LOSS','REJECTED','NO','INVALID'): cell.font=bad_font
        ws.row_dimensions[r].height=22
    if max_row>1:
        ws.auto_filter.ref=f'A1:{get_column_letter(max_col)}{max_row}'
        for c,h in enumerate(headers,1):
            if h in ('gross p&l','net p&l'):
                letter=get_column_letter(c)
                ws.conditional_formatting.add(f'{letter}2:{letter}{max_row}', CellIsRule(operator='greaterThan', formula=['0'], fill=PatternFill('solid', fgColor='E2F0D9')))
                ws.conditional_formatting.add(f'{letter}2:{letter}{max_row}', CellIsRule(operator='lessThan', formula=['0'], fill=PatternFill('solid', fgColor='FCE4D6')))
    ws.freeze_panes='A2'; ws.sheet_view.showGridLines=False
    for c,h in enumerate(headers,1):
        width=14
        if 'symbol' in h: width=13
        elif 'date & time' in h: width=21
        elif 'reason' in h or 'validation' in h: width=32
        elif 'source' in h or 'plan' in h: width=22
        elif len(h)>20: width=20
        ws.column_dimensions[get_column_letter(c)].width=max(ws.column_dimensions[get_column_letter(c)].width or 0,width)
    ws.sheet_properties.pageSetUpPr.fitToPage=True
    ws.page_setup.fitToWidth=1; ws.page_setup.fitToHeight=0

def _style_nse(ws):
    _style(ws)
    ws.sheet_view.zoomScale=90
    # NSE sheet gets a stronger visual header and useful widths
    for cell in ws[1]:
        cell.font=Font(bold=True,color='FFFFFF',size=10)
        cell.fill=PatternFill('solid',fgColor='1F4E78')
    widths=[16,14,16,13,13,13,16,16,18,16,14,18,18,12,22]
    for i,w in enumerate(widths,1):
        ws.column_dimensions[get_column_letter(i)].width=w
    ws.auto_filter.ref=f'A1:{get_column_letter(ws.max_column)}{ws.max_row}' if ws.max_row else 'A1'

def _formula_historical(ws,row):
    """Validate/preserve engine-authoritative historical P&L values.

    Historical exit selection and P&L are calculated by the chronological replay
    engine. Excel must not recalculate them from daily extrema, because that can
    reverse the order of an early profit hit and a later stop-loss hit.
    """
    # Deliberately no formula calculation here. Values written by write_historical()
    # are the authoritative replay-engine outputs.
    return

def historical_market_fields(r, sym, d):
    """Return optional daily reference fields; 52-week levels are not downloaded."""
    def first(*keys):
        for key in keys:
            value = r.get(key)
            if value is not None and value != '':
                try:
                    if not pd.isna(value):
                        return value
                except Exception:
                    return value
        return None
    day_high = first('day_high', 'daily_high', 'dayHigh', 'Day High', 'High', 'high')
    day_high_time = first('day_high_time', 'daily_high_time', 'dayHighTime', 'Day High Time')
    day_low = first('day_low', 'daily_low', 'dayLow', 'Day Low', 'Low', 'low')
    day_low_time = first('day_low_time', 'daily_low_time', 'dayLowTime', 'Day Low Time')
    prev = first('previous_close', 'Previous Close', 'Previous close', 'prev_close', 'prevClose')
    close = first('close', 'Close', 'latest_close', 'Latest/Close')
    daily_rupees = daily_pct = None
    try:
        prev_f, close_f = float(prev), float(close)
        if prev_f > 0:
            daily_rupees = close_f - prev_f
            daily_pct = (close_f / prev_f - 1.0) * 100.0
    except (TypeError, ValueError):
        pass
    return [day_high, day_high_time, day_low, day_low_time, prev, daily_rupees, daily_pct]


def write_historical(results:Iterable[dict],target:Path,replace_dates:set[str]|None=None):
    results=list(results or []); wb=_open(Path(target)); ytd=wb['YTD Historical Data']; selected=wb['Selected Date']
    # Preserve existing YTD rows except explicitly replaced dates.
    old=[]
    if ytd.max_row>1:
        for vals in ytd.iter_rows(min_row=2,values_only=True):
            if any(v not in (None,'') for v in vals): old.append(list(vals))
    _header(ytd,HIST_HEADERS); _header(selected,HIST_HEADERS)
    # Historical share count is controlled by the GUI/replay engine.
    # Excel must NOT impose a hard-coded share limit (for example 21).
    # The replay engine already applies the configured Historical Shares cap
    # and supplies the authoritative rows here.  Excel only removes exact
    # duplicate (date, symbol, logic) rows.
    canonical_results=[]
    grouped={}
    for r in results:
        ds=str(r.get('trade_date') or '')[:10]
        sym=str(r.get('symbol') or '').upper()
        logic=str(r.get('logic') or '')
        if not ds or not sym:
            continue
        grouped.setdefault(ds, []).append(r)
    for ds, items in grouped.items():
        items=sorted(items, key=lambda x: (
            int(x.get('rank') or 10**9),
            str(x.get('buy_trade_datetime') or ''),
            str(x.get('symbol') or '').upper()
        ))
        seen=set()
        count=0
        for r in items:
            key=(ds, str(r.get('symbol') or '').upper(), str(r.get('logic') or ''))
            if key in seen:
                continue
            seen.add(key)
            canonical_results.append(r)
            count += 1

    # Every supplied historical date is authoritative for that date. A rerun
    # must replace the entire date, not append to an earlier partial replay.
    dates={str(d or '')[:10] for d in (replace_dates or set()) if str(d or '')}
    dates.update(str(r.get('trade_date') or '')[:10] for r in canonical_results if r.get('trade_date'))
    kept_by_date={}
    for r in old:
        d=r[0]
        # Excel stores dates as datetime values (e.g. 2026-10-28 00:00:00).
        # Normalize to YYYY-MM-DD before comparing with replay dates, otherwise
        # an old row survives and the same day's rows are appended a second time.
        if isinstance(d, datetime):
            ds=d.date().isoformat()
        elif isinstance(d, date):
            ds=d.isoformat()
        else:
            ds=str(d or '')[:10]
        if ds in dates:
            continue
        kept_by_date.setdefault(ds, []).append(r)

    kept=[]
    for ds, items in kept_by_date.items():
        # Do not impose a hard-coded historical-share limit on retained dates.
        # Historical Shares is a GUI setting and the replay engine is the
        # authoritative place where that limit is enforced.
        items=sorted(items, key=lambda x: (
            int(x[1]) if len(x) > 1 and str(x[1]).strip().isdigit() else 10**9,
            str(x[10] if len(x) > 10 else ''),
            str(x[3] if len(x) > 3 else '').upper()
        ))
        seen=set()
        for r in items:
            key=(str(r[3] if len(r)>3 else '').upper(), str(r[2] if len(r)>2 else ''))
            if key in seen:
                continue
            seen.add(key)
            kept.append(r)

    for r in kept: ytd.append((r+[None]*len(HIST_HEADERS))[:len(HIST_HEADERS)])
    new_rows=[]
    for r in canonical_results:
        d=_dt(r.get('trade_date'))
        if not d: continue
        audit=r.get('entry_audit') or {}; be=r.get('buy_extremes') or {}
        bp=_num(r.get('buy_actual_price')); qty=_num(r.get('buy_actual_qty') or r.get('quantity'))
        high=_num(be.get('high') if be.get('high') is not None else r.get('high'))
        low=_num(be.get('low') if be.get('low') is not None else r.get('low'))
        cfg=r.get('logic_config') or {}
        profit_hit = r.get('first_profit_hit_datetime') not in (None, '')
        loss_hit = r.get('first_loss_hit_datetime') not in (None, '')
        profit_target = _num(r.get('profit_target'))
        if profit_target is None and bp is not None and float(cfg.get('profit_target') or 0) > 0:
            profit_target = float(bp) * (1.0 + float(cfg.get('profit_target') or 0) / 100.0)
        loss_target = _num(r.get('loss_target'))
        if loss_target is None and bp is not None and float(cfg.get('initial_stop') or 0) > 0:
            loss_target = float(bp) * (1.0 - float(cfg.get('initial_stop') or 0) / 100.0)
        sell_price=_num(r.get('sell_actual_price') or r.get('sell_price') or r.get('exit_price')); sell_qty=_num(r.get('sell_actual_qty') or r.get('sell_qty') or qty)
        historical_shares = _num(r.get('historical_shares'))
        if historical_shares is None:
            historical_shares = _num(cfg.get('historical_shares'))
        squareoff_price = _num(r.get('squareoff_1525_price'))
        squareoff_time = _dt(r.get('squareoff_1525_datetime'))
        row=[d,_num(r.get('rank')),r.get('logic'),str(r.get('symbol') or '').upper(),_num(r.get('open') or r.get('session_open')),_num(r.get('close') if r.get('close') is not None else r.get('Close')),_num(r.get('volume')),_num(r.get('turnover_cr') if r.get('turnover_cr') is not None else (r.get('Turnover in Cr') if r.get('Turnover in Cr') is not None else r.get('Turnover Cr'))),qty,bp,_dt(r.get('buy_trade_datetime')),high,_dt(be.get('high_dt')),profit_target,'YES' if profit_hit else 'NO',low,_dt(be.get('low_dt')),loss_target,'YES' if loss_hit else 'NO',r.get('exit_reason') or r.get('status'),_num(r.get('gross_pnl')),bp*qty if bp is not None and qty is not None else None,_num(r.get('sell_turnover')) if r.get('sell_turnover') is not None else (sell_price*sell_qty if sell_price is not None and sell_qty is not None else None),_num(r.get('brokerage')) or 0,_num(r.get('stt')) or 0,_num(r.get('exchange_turnover_charges')) or 0,_num(r.get('gst')) or 0,_num(r.get('sebi_charges')) or 0,_num(r.get('stamp_duty')) or 0,_num(r.get('total_other_charges')) or 0,_num(r.get('total_charges')),_num(r.get('net_pnl')),_num(r.get('pnl_pct')),r.get('charge_source') or 'Engine / Breeze detail',r.get('charge_plan'),squareoff_price,bool(r.get('squareoff_1525_price') is not None and r.get('first_exit_event') == '15:25_SQUARE_OFF'),squareoff_time,historical_shares,audit.get('open_direction'),_num(audit.get('open_movement_pct')),_num(audit.get('price_big')),_num(audit.get('price_small')),_num(audit.get('required_turnover_cr')),_num(cfg.get('profit_target')),_num(cfg.get('initial_stop')),_num(audit.get('required_volume')), _num(cfg.get('activation')), _num(cfg.get('trailing_stop')),r.get('entry_validation') or r.get('historical_result') or r.get('status'),
            r.get('first_signal_time') or r.get('buy_trade_datetime'),
            r.get('decision') or ('BUY' if qty else (r.get('status') or 'REJECTED')),
            r.get('decision_reason') or r.get('entry_audit', {}).get('reason') or r.get('exit_reason') or r.get('status'),
            r.get('sell_trade_datetime'),
            *historical_market_fields(r, str(r.get('symbol') or ''), d)]
        new_rows.append(row)
    for row in new_rows: ytd.append(row); _formula_historical(ytd,ytd.max_row)
    for row in new_rows: selected.append(row); _formula_historical(selected,selected.max_row)
    _style(ytd); _style(selected)
    try:
        wb.calculation.fullCalcOnLoad = True
        wb.calculation.forceFullCalc = True
        wb.calculation.calcMode = "auto"
    except Exception:
        pass
    wb.save(target)


def write_paper_live(results:Iterable[dict],target:Path):
    wb=_open(Path(target)); ws=wb['Paper Live Trading Shares']; _header(ws,PAPER_HEADERS)
    # Fresh ledger: write the latest state supplied by the paper/live engine.
    for r in results or []:
        mode=str(r.get('mode') or '').upper(); sym=str(r.get('symbol') or '').upper()
        qty=_num(r.get('buy_actual_qty') or r.get('quantity')); sq=_num(r.get('sell_actual_qty'))
        bp=_num(r.get('buy_actual_price') or r.get('limit_price')); sp=_num(r.get('sell_actual_price'))
        row=[r.get('trade_id') or r.get('buy_order_id') or f'{mode}|{r.get("trade_date")}|{r.get("logic")}|{sym}',mode,_dt(r.get('trade_date')),r.get('rank'),r.get('logic'),sym,r.get('status'),qty,bp,_dt(r.get('buy_trade_datetime')),sq,sp,_dt(r.get('sell_trade_datetime')), _num((r.get('buy_extremes') or {}).get('high') or r.get('high')),_dt((r.get('buy_extremes') or {}).get('high_dt')),_num((r.get('buy_extremes') or {}).get('low') or r.get('low')),_dt((r.get('buy_extremes') or {}).get('low_dt')),r.get('exit_reason'),None,None,None,_num(r.get('brokerage')) or 0,_num(r.get('stt')) or 0,_num(r.get('exchange_turnover_charges')) or 0,_num(r.get('gst')) or 0,_num(r.get('sebi_charges')) or 0,_num(r.get('stamp_duty')) or 0,_num(r.get('total_other_charges')) or 0,None,None,None,r.get('charge_source') or 'Excel Formula / Breeze detail',r.get('charge_plan'),r.get('buy_order_id'),r.get('sell_order_id'),r.get('data_source'),_dt(r.get('last_update') or datetime.now())]
        ws.append(row); rr=ws.max_row
        # Formula P&L: actual sell if available; otherwise the position remains OPEN and P&L is blank.
        ws.cell(rr,19).value=f'=IF(OR(H{rr}="",I{rr}="",K{rr}="",L{rr}=""),"",(L{rr}-I{rr})*H{rr})'
        ws.cell(rr,20).value=f'=IF(OR(H{rr}="",I{rr}=""),"",H{rr}*I{rr})'
        ws.cell(rr,21).value=f'=IF(OR(K{rr}="",L{rr}=""),"",K{rr}*L{rr})'
        for c in range(22,30):
            if ws.cell(rr,c).value in (None,''): ws.cell(rr,c).value=0
        ws.cell(rr,30).value=f'=SUM(V{rr}:AC{rr})'
        ws.cell(rr,31).value=f'=IF(S{rr}="","",S{rr}-AD{rr})'
        ws.cell(rr,32).value=f'=IFERROR(AE{rr}/T{rr},0)'
    _style(ws); wb.save(target)



def write_nse(market,target:Path,asof):
    """Write NSE screening data using the single canonical 13-column schema.

    IMPORTANT: the row length must exactly match NSE_HEADERS.  Older versions
    appended count columns after Downloaded At, which shifted Date/Symbol and
    made Serial No. and Date appear in the wrong columns.
    """
    wb=_open(Path(target)); ws=wb['NSE Data']; _header(ws,NSE_HEADERS)
    if not hasattr(market, 'iterrows'):
        wb.save(target); return
    market=market.copy()
    if 'Turnover Cr' in market.columns and 'Turnover (₹)' in market.columns:
        market=market.drop(columns=['Turnover (₹)'])
    if 'Turnover in Cr' not in market.columns and 'Turnover Cr' in market.columns:
        market=market.rename(columns={'Turnover Cr':'Turnover in Cr'})

    # Continuous serial numbers only for rows passing the configured screen.
    eligible_col='Eligible?'
    serial=0
    for _,r in market.iterrows():
        eligible=bool(r.get(eligible_col,False)) if eligible_col in market.columns else False
        if eligible:
            serial += 1
            serial_no=serial
        else:
            serial_no=None
        op=_num(r.get('open') if r.get('open') is not None else r.get('Open'))
        hi=_num(r.get('high') if r.get('high') is not None else r.get('High'))
        lo=_num(r.get('low') if r.get('low') is not None else r.get('Low'))
        cl=_num(r.get('close') if r.get('close') is not None else (r.get('Latest/Close') if r.get('Latest/Close') is not None else r.get('Close')))
        prev=_num(r.get('previous_close') if r.get('previous_close') is not None else (r.get('prev_close') if r.get('prev_close') is not None else r.get('Previous Close')))
        movement=_num(r.get('movement_pct') if r.get('movement_pct') is not None else r.get('Movement %'))
        if movement is None and prev not in (None,0) and cl is not None:
            movement=(float(cl)/float(prev)-1.0)*100.0
        volume=_num(r.get('volume') if r.get('volume') is not None else (r.get('total_traded_quantity') if r.get('total_traded_quantity') is not None else r.get('Volume')))
        turnover=_num(r.get('turnover_cr') if r.get('turnover_cr') is not None else (r.get('Turnover in Cr') if r.get('Turnover in Cr') is not None else r.get('Turnover Cr')))
        _symbol52=str(r.get('symbol') or r.get('SYMBOL') or r.get('Symbol') or '').upper()
        _asof52=r.get('Date') or asof
        row=[
            serial_no,
            _dt(_asof52),
            _symbol52,
            op,hi,lo,cl,prev,volume,turnover,movement,
            r.get('source') or 'NSE',datetime.now()
        ]
        # Hard guard: never write a row whose length differs from the header.
        if len(row)!=len(NSE_HEADERS):
            raise RuntimeError(f'NSE row/header mismatch: {len(row)} values for {len(NSE_HEADERS)} headers')
        ws.append([_excel_safe(v) for v in row])
    _style_nse(ws)
    # Force the canonical number formats and widths, including separate Serial/Date.
    ws.column_dimensions['A'].width=16
    ws.column_dimensions['B'].width=13
    ws.cell(1,1).value='Logic Serial No.'
    ws.cell(1,2).value='Date'
    for rr in range(2,ws.max_row+1):
        ws.cell(rr,1).number_format='0'
        ws.cell(rr,2).number_format='dd-mm-yyyy'
        ws.cell(rr,11).number_format='0.00%'
    wb.save(target)


def monthly_path(base:Path,month_text:str)->Path:
    base=Path(base); base.mkdir(parents=True,exist_ok=True)
    return base/f'Trading_Data_{month_text}.xlsx'

# ===== breeze_trader.py =====
"""NSE mechanical-screen and ICICI Direct Breeze order helper with authoritative timeline High/Low data replay.

Use PAPER first. LIVE requires both an explicit CLI flag and credentials supplied
outside the Excel workbook. It uses limit orders only.
"""
import argparse, functools, hashlib, io, json, math, os, pickle, re, sys, time, zipfile, urllib.request, urllib.error, urllib.parse, webbrowser

# === WINDOWS CONSOLE ENCODING SAFETY ===
def _configure_console_output():
    """Prevent Windows legacy console code pages from crashing the paper engine.

    Force ASCII-safe console output so Windows terminals and log collectors
    cannot decode UTF-8 bytes as CP1252 (the source of mojibake such as â‚¹).
    Non-ASCII characters are shown as explicit Unicode escapes. This affects
    console display only, not trading data, Excel formatting, or GUI labels.
    """
    for _stream_name in ("stdout", "stderr"):
        _stream = getattr(sys, _stream_name, None)
        try:
            if _stream is not None and hasattr(_stream, "reconfigure"):
                _stream.reconfigure(encoding="ascii", errors="backslashreplace")
        except Exception:
            pass

_configure_console_output()
from datetime import date, datetime, timedelta, timezone
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

NSE_TIMEZONE = "Asia/Kolkata"

def _nse_now():
    """Return the authoritative NSE market clock in Asia/Kolkata (IST)."""
    if ZoneInfo is not None:
        return datetime.now(ZoneInfo(NSE_TIMEZONE))
    # Fallback for older Python environments: IST is UTC+05:30.
    return datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)

def _align_datetime_timezone(value, reference):
    """Make a quote timestamp comparable with the NSE session clock.

    Breeze stream timestamps may be timezone-naive while the NSE clock is
    Asia/Kolkata-aware. Naive Breeze timestamps represent exchange-local time.
    """
    if not isinstance(value, datetime):
        return reference
    ref_tz = reference.tzinfo if isinstance(reference, datetime) else None
    if value.tzinfo is None:
        return value.replace(tzinfo=ref_tz) if ref_tz is not None else value
    if ref_tz is None:
        return value.replace(tzinfo=None)
    return value.astimezone(ref_tz)

from pathlib import Path
import requests
import pandas as pd
import time
import threading
import concurrent.futures
import numpy as np

# === USER-SELECTABLE NSE EQUITY UNIVERSE ===
_NSE_INDEX_UNIVERSE_URLS = {
    "NSE50": "https://www.niftyindices.com/IndexConstituent/ind_nifty50list.csv",
    "NSE100": "https://www.niftyindices.com/IndexConstituent/ind_nifty100list.csv",
    "NSE500": "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv",
}

def _nse_universe_symbols(universe: str | None) -> set[str] | None:
    """Resolve a selected index to current constituents; ALL EQ returns None.

    Index membership is cached locally for 24 hours. A failed/empty index fetch
    raises rather than silently widening a restricted selection to all equities.
    """
    key = str(universe or "NSE_ALL_EQ").strip().upper().replace(" ", "_")
    aliases = {"ALL": "NSE_ALL_EQ", "NSE_ALL": "NSE_ALL_EQ", "NSE_ALL_EQ": "NSE_ALL_EQ",
               "NIFTY50": "NSE50", "NIFTY_50": "NSE50", "NIFTY100": "NSE100",
               "NIFTY_100": "NSE100", "NIFTY500": "NSE500", "NIFTY_500": "NSE500"}
    key = aliases.get(key, key)
    if key == "NSE_ALL_EQ":
        return None
    if key not in _NSE_INDEX_UNIVERSE_URLS:
        raise RuntimeError(f"Unsupported NSE universe: {universe!r}")
    cache_dir = Path(__file__).resolve().parent / "Data" / "NSE_Universe"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / f"{key}.csv"
    raw = None
    try:
        if cache_file.exists() and (time.time() - cache_file.stat().st_mtime) < 86400:
            raw = cache_file.read_bytes()
        else:
            headers = {"User-Agent": "Mozilla/5.0", "Accept": "text/csv,*/*", "Referer": "https://www.niftyindices.com/"}
            response = requests.get(_NSE_INDEX_UNIVERSE_URLS[key], headers=headers, timeout=20)
            response.raise_for_status()
            raw = response.content
            if not raw or len(raw) < 100:
                raise RuntimeError("Index constituents CSV response was empty or too small.")
            cache_file.write_bytes(raw)
    except Exception as exc:
        # A stale but previously verified local snapshot is safer than widening.
        if cache_file.exists():
            raw = cache_file.read_bytes()
            print(f"NSE UNIVERSE WARNING: using cached {key} constituents because refresh failed: {type(exc).__name__}: {exc}", flush=True)
        else:
            raise RuntimeError(f"Could not load {key} constituents from NSE Indices; restricted universe was NOT widened. {type(exc).__name__}: {exc}") from exc
    try:
        from io import BytesIO
        frame = pd.read_csv(BytesIO(raw))
        symbol_col = next((c for c in frame.columns if str(c).strip().lower() in {"symbol", "index symbol"}), None)
        if symbol_col is None:
            raise ValueError(f"CSV has no Symbol column; columns={list(frame.columns)}")
        symbols = {str(x).strip().upper() for x in frame[symbol_col].dropna().tolist() if str(x).strip()}
        if len(symbols) < 20:
            raise ValueError(f"Only {len(symbols)} constituents were parsed; refusing incomplete universe.")
        print(f"NSE UNIVERSE LOADED: {key}; constituents={len(symbols)}; source=Nifty Indices CSV; cache={cache_file}", flush=True)
        return symbols
    except Exception as exc:
        raise RuntimeError(f"Could not parse verified {key} constituents: {type(exc).__name__}: {exc}") from exc

# Strategy parameters are read exclusively from Config/logic_settings.json through
# logic_config.py. No legacy strategy constants are kept in this module.
def trigger_price(session_open, cfg, previous_close=None):
    return entry_level(session_open, cfg, previous_close)
def formula_exit_observation(entry_price, current_price, cfg):
    """Simple formula exit: profit target has priority, then initial stop-loss."""
    entry=float(entry_price or 0); px=float(current_price or 0)
    if entry<=0 or px<=0: return None, ''
    target_pct=float(cfg.get('profit_target') or 0); stop_pct=float(cfg.get('initial_stop') or 0)
    target=entry*(1+target_pct/100) if target_pct>0 else None
    stop=entry*(1-stop_pct/100) if stop_pct>0 else None
    if target is not None and px>=target: return target, f'{target_pct:g}% profit target'
    if stop is not None and px<=stop: return stop, f'{stop_pct:g}% initial stop-loss'
    return None, ''

def formula_historical_exit(df, buy_dt, entry_price, cfg, day):
    """Determine the first chronological target/stop event after BUY.

    This is the authoritative historical exit engine.  It deliberately scans the
    post-entry timeline in timestamp order instead of comparing the timestamp of
    the day's maximum High with the timestamp of the day's minimum Low.  Comparing
    extrema can be wrong: a stock may hit the profit target early, continue higher,
    and only later make the day's lowest Low.  The earlier target must win.

    OHLC data cannot reveal the order of High and Low *inside the same timestamp*.
    When both thresholds are present at the exact same timestamp, the existing
    deterministic rule is retained: profit target wins the tie.  With 1-second
    replay this ambiguity is limited to the same second.
    """
    entry = float(entry_price or 0)
    if entry <= 0 or df is None or df.empty:
        return None

    x = df.copy()
    if '_dt' not in x.columns:
        return None
    x['_dt'] = pd.to_datetime(x['_dt'], errors='coerce')
    x = x.dropna(subset=['_dt'])
    if 'is_actual' in x.columns:
        actual = x['is_actual'].map(
            lambda v: v if isinstance(v, bool)
            else str(v).strip().lower() not in {'false', '0', 'no', 'nan', 'none', ''}
        )
        x = x[actual]
    buy_ts = pd.Timestamp(buy_dt)
    # Last-entry time controls BUY eligibility only. Open positions remain monitored
    # through the NSE session; historical no-hit exits require the final pre-15:25 candle.
    cutoff = pd.Timestamp(day).replace(hour=15, minute=24, second=59, microsecond=0)
    x = x[(x['_dt'] > buy_ts) & (x['_dt'] <= cutoff)].sort_values('_dt', kind='mergesort').reset_index(drop=True)
    if x.empty:
        return None

    for c in ('high', 'low', 'close'):
        if c not in x.columns:
            return None
        x[c] = pd.to_numeric(x[c], errors='coerce')
    x = x.dropna(subset=['high', 'low', 'close']).reset_index(drop=True)
    if x.empty:
        return None

    target_pct = float(cfg.get('profit_target') or 0)
    stop_pct = float(cfg.get('initial_stop') or 0)
    target = entry * (1.0 + target_pct / 100.0) if target_pct > 0 else None
    stop = entry * (1.0 - stop_pct / 100.0) if stop_pct > 0 else None

    first_profit_dt = None
    first_loss_dt = None
    first_profit_idx = None
    first_loss_idx = None

    # Find the FIRST threshold crossing, not the eventual maximum/minimum.
    for idx, row in x.iterrows():
        ts = pd.Timestamp(row['_dt'])
        if first_profit_dt is None and target is not None and float(row['high']) >= target:
            first_profit_dt = ts
            first_profit_idx = idx
        if first_loss_dt is None and stop is not None and float(row['low']) <= stop:
            first_loss_dt = ts
            first_loss_idx = idx
        if first_profit_dt is not None and first_loss_dt is not None:
            break

    # Extremes are retained for audit/display, but they do NOT decide the exit.
    hi_idx = x['high'].idxmax()
    lo_idx = x['low'].idxmin()
    hi_row = x.loc[hi_idx]
    lo_row = x.loc[lo_idx]
    high = float(hi_row['high'])
    low = float(lo_row['low'])
    high_dt = pd.Timestamp(hi_row['_dt'])
    low_dt = pd.Timestamp(lo_row['_dt'])

    # Always retain the last real market price at or before 15:25 as a reference.
    # This is separate from whether the actual trade exited at 15:25.
    cutoff_row = x[x['_dt'] <= cutoff].iloc[-1]
    squareoff_market_price = float(cutoff_row['close'])
    squareoff_market_dt = pd.Timestamp(cutoff_row['_dt'])

    if first_profit_dt is not None and first_loss_dt is not None:
        if first_profit_dt <= first_loss_dt:
            sell_price = float(target)
            sell_dt = first_profit_dt
            reason = f"{target_pct:g}% profit target - achieved first"
            first_exit_event = 'PROFIT_TARGET_FIRST' if first_profit_dt < first_loss_dt else 'PROFIT_TARGET_TIE'
        else:
            sell_price = float(stop)
            sell_dt = first_loss_dt
            reason = f"{stop_pct:g}% initial stop-loss - achieved first"
            first_exit_event = 'INITIAL_STOP_FIRST'
    elif first_profit_dt is not None:
        sell_price = float(target)
        sell_dt = first_profit_dt
        reason = f"{target_pct:g}% profit target"
        first_exit_event = 'PROFIT_TARGET'
    elif first_loss_dt is not None:
        sell_price = float(stop)
        sell_dt = first_loss_dt
        reason = f"{stop_pct:g}% initial stop-loss"
        first_exit_event = 'INITIAL_STOP'
    else:
        cutoff = pd.Timestamp(day).replace(hour=15, minute=24, second=59, microsecond=0)
        pre = x[x['_dt'] <= cutoff]
        # Do not invent an end-of-day square-off from a partial download. A
        # target/stop hit can be replayed from its event timestamp, but a no-hit
        # trade needs the final pre-close minute to produce a valid square-off.
        required_last = pd.Timestamp(day).replace(hour=15, minute=24, second=0, microsecond=0)
        if pre.empty or pd.Timestamp(pre['_dt'].max()) < required_last:
            return None
        last = pre.iloc[-1]
        sell_price = float(last['close'])
        sell_dt = pd.Timestamp(last['_dt'])
        reason = 'MARKET CLOSE / LAST AVAILABLE CANDLE'
        first_exit_event = 'MARKET_CLOSE_SQUARE_OFF'

    return {
        'sell_actual_price': sell_price,
        'sell_trade_datetime': sell_dt,
        'exit_reason': reason,
        'first_profit_hit_datetime': first_profit_dt,
        'first_loss_hit_datetime': first_loss_dt,
        'first_exit_event': first_exit_event,
        'high': high,
        'high_dt': high_dt,
        'low': low,
        'low_dt': low_dt,
        'squareoff_1525_price': squareoff_market_price,
        'squareoff_1525_datetime': squareoff_market_dt,
    }

from openpyxl import load_workbook, Workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
import openpyxl

ROOT = Path(__file__).resolve().parent
APP_DATA = ROOT / 'Data'
LOG_DIR = ROOT / 'Logs'
CONFIG_DIR = ROOT / 'Config'
WORKBOOK = ROOT / 'Breeze_NSE_Trading_Template.xlsx'
HISTORICAL_CACHE = APP_DATA / 'Historical_Data'
ONE_SHARE_HISTORICAL_CACHE = HISTORICAL_CACHE / '1_One_Share'
YTD_HISTORICAL_CACHE = HISTORICAL_CACHE / '2_YTD_Historical_Data'
# Simple layout: Data/Historical_Data/2_YTD_Historical_Data/YYYY/YYYY-MM/YYYY-MM-DD/
HISTORICAL_YEAR_DIR = YTD_HISTORICAL_CACHE
HISTORICAL_MONTH_DIR = YTD_HISTORICAL_CACHE
MONTHLY_WORKBOOK_DIR = YTD_HISTORICAL_CACHE
_LAYOUT_MIGRATED_MONTHS = set()

def _historical_year_dir(year: int) -> Path:
    return YTD_HISTORICAL_CACHE / f'{int(year):04d}'

def _historical_month_dir(year: int, month: int) -> Path:
    return _historical_year_dir(year) / f'{int(year):04d}-{int(month):02d}'

def _migrate_legacy_historical_month(year: int, month: int) -> None:
    """Move older nested historical folders into YYYY/YYYY-MM/YYYY-MM-DD."""
    key = (int(year), int(month))
    if key in _LAYOUT_MIGRATED_MONTHS:
        return
    _LAYOUT_MIGRATED_MONTHS.add(key)
    target = _historical_month_dir(year, month)
    date_root = target
    legacy_roots = [
        YTD_HISTORICAL_CACHE / 'Year' / f'{year:04d}' / 'Month' / f'{year:04d}-{month:02d}',
        YTD_HISTORICAL_CACHE / 'Month' / f'{year:04d}-{month:02d}',
        YTD_HISTORICAL_CACHE / f'{year:04d}-{month:02d}',
    ]

    def move_item(src: Path, dst: Path) -> None:
        if src.is_dir():
            dst.mkdir(parents=True, exist_ok=True)
            for child in list(src.iterdir()):
                move_item(child, dst / child.name)
            try:
                src.rmdir()
            except OSError:
                pass
        elif not dst.exists():
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(src), str(dst))
        else:
            backup = dst.with_name(dst.stem + '_legacy' + dst.suffix)
            n = 2
            while backup.exists():
                backup = dst.with_name(f'{dst.stem}_legacy{n}{dst.suffix}')
                n += 1
            shutil.move(str(src), str(backup))

    try:
        for legacy in legacy_roots:
            if not legacy.is_dir() or legacy.resolve() == target.resolve():
                continue
            # Old canonical layout stored daily files under Date/YYYY-MM-DD.
            old_date_root = legacy / 'Date'
            if old_date_root.is_dir():
                for day_folder in list(old_date_root.iterdir()):
                    if re.fullmatch(r'\d{4}-\d{2}-\d{2}', day_folder.name):
                        move_item(day_folder, target / day_folder.name)
                try:
                    old_date_root.rmdir()
                except OSError:
                    pass
            for item in list(legacy.iterdir()):
                if item.is_dir() and re.fullmatch(r'\d{4}-\d{2}-\d{2}', item.name):
                    move_item(item, target / item.name)
                else:
                    dated = re.match(r'^(\d{4}-\d{2}-\d{2})_', item.name)
                    if dated:
                        move_item(item, target / dated.group(1) / item.name)
                    elif item.name != 'Date':
                        move_item(item, target / item.name)
            try:
                legacy.rmdir()
            except OSError:
                pass
    except Exception as exc:
        print(f'HISTORICAL LAYOUT MIGRATION WARNING {year:04d}-{month:02d}: {exc}')




LOG_DIR.mkdir(parents=True, exist_ok=True)
CONFIG_DIR.mkdir(parents=True, exist_ok=True)
APP_DATA.mkdir(parents=True, exist_ok=True)
HISTORICAL_YEAR_DIR.mkdir(parents=True, exist_ok=True)

def _migrate_all_legacy_historical_layout() -> None:
    """Migrate old nested month folders to the compact YYYY/YYYY-MM layout."""
    discovered = set()
    try:
        for item in YTD_HISTORICAL_CACHE.iterdir():
            if item.is_dir() and re.fullmatch(r'\d{4}-\d{2}', item.name):
                discovered.add(tuple(map(int, item.name.split('-'))))
        for old_root in (YTD_HISTORICAL_CACHE / 'Month', YTD_HISTORICAL_CACHE / 'Year'):
            if not old_root.is_dir():
                continue
            if old_root.name == 'Month':
                for item in old_root.iterdir():
                    if item.is_dir() and re.fullmatch(r'\d{4}-\d{2}', item.name):
                        discovered.add(tuple(map(int, item.name.split('-'))))
            else:
                for year_dir in old_root.iterdir():
                    nested = year_dir / 'Month' if year_dir.is_dir() else None
                    if nested and nested.is_dir():
                        for item in nested.iterdir():
                            if item.is_dir() and re.fullmatch(r'\d{4}-\d{2}', item.name):
                                discovered.add(tuple(map(int, item.name.split('-'))))
        for year, month in sorted(discovered):
            _migrate_legacy_historical_month(year, month)
        # Remove empty legacy containers only; never delete data.
        for old in (YTD_HISTORICAL_CACHE / 'Month', YTD_HISTORICAL_CACHE / 'Year'):
            try:
                if old.is_dir() and not any(old.iterdir()):
                    old.rmdir()
            except OSError:
                pass
    except Exception as exc:
        print(f'HISTORICAL STARTUP LAYOUT MIGRATION WARNING: {exc}')

_migrate_all_legacy_historical_layout()


# Cache schema is bumped whenever historical replay semantics change. Old cache files
# are intentionally ignored so corrected historical calculations always use fresh Breeze data.
HISTORICAL_CACHE_VERSION = 'v20_nse50_verified_stockcode_retry_recovery'
SECURITY_CACHE = HISTORICAL_CACHE / 'breeze_security_master.csv'
NSE_COMPANY_CACHE = HISTORICAL_CACHE / 'nse_company_master.csv'
NSE_COMPANY_MASTER_URL = 'https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv'
NSE_COMPANY_MASTER_ATTEMPTED = False
NSE_URLS = (
    'https://nsearchives.nseindia.com/content/cm/sec_bhavdata_full_{:%d%m%Y}.csv',
    'https://archives.nseindia.com/content/cm/sec_bhavdata_full_{:%d%m%Y}.csv',
)
NSE_UDIFF_ZIP = 'https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{:%Y%m%d}_F_0000.csv.zip'
# NSE equity trading holidays for 2026. The app must never create a market-data
# snapshot on a notified exchange holiday. Weekends are handled separately.
NSE_TRADING_HOLIDAYS_2026 = {
    date(2026, 1, 15), date(2026, 1, 26), date(2026, 2, 19),
    date(2026, 3, 3), date(2026, 3, 19), date(2026, 3, 26),
    date(2026, 3, 31), date(2026, 4, 1), date(2026, 4, 3),
    date(2026, 4, 14), date(2026, 5, 1), date(2026, 5, 28),
    date(2026, 6, 26), date(2026, 8, 26), date(2026, 9, 14),
    date(2026, 10, 2), date(2026, 10, 20), date(2026, 11, 10),
    date(2026, 11, 24), date(2026, 12, 25),
}
LIVE_AUDIT_LOG = LOG_DIR / 'Live_Order_Audit.log'
# Single safety/reproducibility configuration for LIVE execution. LIVE is
# deliberately disabled until the user explicitly enables it.
SAFETY_CONFIG_PATH = CONFIG_DIR / 'trading_safety.json'
DECISION_AUDIT_LOG = LOG_DIR / 'Trading_Decision_Audit.jsonl'
LIVE_DAILY_LOSS_ENFORCEMENT_AVAILABLE = False  # Positive max_daily_loss remains locked until broker-wide P&L enforcement is verified.
LIVE_EXECUTION_CERTIFIED = False  # r89 hard lock: no real-money orders until broker simulation + supervised validation pass.


# Durable order-intent journal. State-changing broker calls are journaled BEFORE
# transmission. Ambiguous BUY submission failures persist a halt so a restart
# cannot silently submit a duplicate entry. SELL/cancel calls remain available
# for risk reduction and reconciliation.
ORDER_LEDGER_DB = APP_DATA / 'order_state.sqlite3'
ORDER_LEDGER_HALT_PATH = LOG_DIR / 'ORDER_LEDGER_BUY_HALT.json'

def _order_ledger_record(event: dict) -> None:
    """Durably append one broker order lifecycle event (SQLite WAL + FULL sync)."""
    payload = dict(event or {})
    payload.setdefault('recorded_at', datetime.now().isoformat(timespec='milliseconds'))
    conn = sqlite3.connect(str(ORDER_LEDGER_DB), timeout=8)
    try:
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=FULL')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS order_events (
                event_id TEXT PRIMARY KEY,
                request_id TEXT,
                order_id TEXT,
                method TEXT NOT NULL,
                action TEXT,
                symbol TEXT,
                quantity TEXT,
                price TEXT,
                state TEXT NOT NULL,
                detail_json TEXT,
                recorded_at TEXT NOT NULL
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS broker_orders (
                order_id TEXT PRIMARY KEY,
                symbol TEXT,
                action TEXT,
                quantity TEXT,
                price TEXT,
                last_state TEXT NOT NULL,
                last_seen_at TEXT NOT NULL
            )
        ''')
        event_id = base64.urlsafe_b64encode(os.urandom(18)).decode('ascii').rstrip('=')
        conn.execute('''
            INSERT INTO order_events
            (event_id,request_id,order_id,method,action,symbol,quantity,price,state,detail_json,recorded_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        ''', (
            event_id, str(payload.get('request_id') or ''),
            str(payload.get('order_id') or ''),
            str(payload.get('method') or 'unknown'),
            str(payload.get('action') or ''),
            str(payload.get('symbol') or ''),
            str(payload.get('quantity') or ''),
            str(payload.get('price') or ''),
            str(payload.get('state') or 'UNKNOWN'),
            json.dumps(payload.get('detail') or {}, default=str, separators=(',', ':')),
            str(payload['recorded_at'])
        ))
        oid = str(payload.get('order_id') or '').strip()
        if oid:
            conn.execute('''
                INSERT INTO broker_orders(order_id,symbol,action,quantity,price,last_state,last_seen_at)
                VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(order_id) DO UPDATE SET
                    symbol=COALESCE(NULLIF(excluded.symbol,''),broker_orders.symbol),
                    action=COALESCE(NULLIF(excluded.action,''),broker_orders.action),
                    quantity=COALESCE(NULLIF(excluded.quantity,''),broker_orders.quantity),
                    price=COALESCE(NULLIF(excluded.price,''),broker_orders.price),
                    last_state=excluded.last_state, last_seen_at=excluded.last_seen_at
            ''', (
                oid, str(payload.get('symbol') or ''), str(payload.get('action') or ''),
                str(payload.get('quantity') or ''), str(payload.get('price') or ''),
                str(payload.get('state') or 'UNKNOWN'), str(payload['recorded_at'])
            ))
        conn.commit()
    finally:
        conn.close()

def _order_ledger_buy_halted() -> bool:
    """True if a persisted halt or unresolved BUY intent exists, including after a crash."""
    try:
        if ORDER_LEDGER_HALT_PATH.exists():
            return True
        if not ORDER_LEDGER_DB.exists():
            return False
        conn = sqlite3.connect(str(ORDER_LEDGER_DB), timeout=5)
        try:
            rows = conn.execute(
                "SELECT request_id,state FROM order_events WHERE action='buy' AND request_id<>'' ORDER BY rowid"
            ).fetchall()
        finally:
            conn.close()
        latest = {}
        for request_id, state in rows:
            latest[str(request_id)] = str(state or '').upper()
        unresolved = {'SUBMISSION_STARTED', 'SUBMISSION_OUTCOME_UNKNOWN', 'ACCEPTANCE_UNKNOWN'}
        return any(state in unresolved for state in latest.values())
    except Exception:
        # If durable state cannot be inspected, fail closed for BUYs.
        return True

def _order_ledger_set_buy_halt(request_id: str, method: str, label: str, error: str) -> None:
    """Persist an operator-visible halt; clear only after manual broker reconciliation."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        'halted': True, 'request_id': str(request_id), 'method': str(method),
        'label': str(label), 'error': str(error),
        'recorded_at': datetime.now().isoformat(timespec='seconds'),
        'operator_action': 'Verify broker order book and net positions; then archive this file before resuming BUY entries.'
    }
    temp = ORDER_LEDGER_HALT_PATH.with_suffix('.tmp')
    temp.write_text(json.dumps(payload, indent=2), encoding='utf-8')
    os.replace(temp, ORDER_LEDGER_HALT_PATH)

def _order_ledger_request_meta(method: str, kwargs: dict) -> dict:
    """Keep only non-secret order fields; never persist API keys/session credentials."""
    action = kwargs.get('action')
    if not action and method == 'gtt_single_leg_place_order':
        try:
            action = (kwargs.get('order_details') or [{}])[0].get('action')
        except Exception:
            action = ''
    return {
        'method': str(method),
        'action': str(action or ('buy' if 'buy' in method else '')),
        'symbol': str(kwargs.get('stock_code') or kwargs.get('symbol') or ''),
        'quantity': str(kwargs.get('quantity') or ''),
        'price': str(kwargs.get('price') or ''),
        'order_id': str(kwargs.get('order_id') or ''),
    }


SAFETY_DEFAULTS = {
    'live_enabled': False,
    'max_live_orders_per_run': 50,
    'strict_strategy_parity': True,
    'max_trade_value': 0.0,  # Deprecated: GUI per-logic buy_amount is the per-entry budget.
    'max_daily_loss': 0.0,
    'paper_max_daily_loss': 3150.0,
    'stop_on_order_error': True,
    'record_decision_events': True,
}

# ---------------------------------------------------------------------------
# CANONICAL GUI STRATEGY CONTRACT
# ---------------------------------------------------------------------------
# The GUI-saved Config/logic_settings.json is the sole source of truth.
# Historical does NOT lock or freeze the logic. Paper/Shadow/Live always reload
# the latest GUI configuration and pass it through the same canonical evaluator.
STRATEGY_CONTRACT_VERSION = 'CANONICAL-STRATEGY-V9-MOVEMENT-OVERSHOOT-1525-PARITY'

def _strategy_contract_payload(logics=None) -> dict:
    cfgs = logics if isinstance(logics, dict) else load_logics()
    return {
        'contract_version': STRATEGY_CONTRACT_VERSION,
        'config_schema_version': CONFIG_SCHEMA_VERSION,
        'logics': normalize(cfgs),
    }

def strategy_fingerprint(logics=None) -> str:
    payload = _strategy_contract_payload(logics)
    raw = json.dumps(payload, sort_keys=True, separators=(',', ':'), default=str).encode('utf-8')
    return hashlib.sha256(raw).hexdigest()

def current_gui_strategy_fingerprint() -> str:
    return strategy_fingerprint(load_logics())


def verify_canonical_strategy_parity() -> dict:
    """Deterministic self-test proving all runtime modes call the same entry evaluator.

    This test uses synthetic market snapshots and never sends an order.  It checks
    normal pass/fail cases plus that a changed configuration changes the decision
    fingerprint, which is what the runtime strategy lock enforces.
    """
    cfg = normalize(load_logics()).get('Logic 1', DEFAULT_LOGICS['Logic 1']).copy()
    # Parity baseline must compare the same non-Smart entry contract on all
    # three wrappers.  Smart/Upside Strength Score is a separate canonical
    # contract and is tested explicitly below.  Previously, enabling Smart
    # Score in the GUI made this self-test compare entry_check() (non-Smart)
    # against canonical_tick_entry() (Smart), producing a false failure.
    parity_cfg = dict(cfg)
    parity_cfg['smart_mode'] = False
    parity_cfg['use_price_band'] = False
    # ORB is an optional, independent runtime gate.  The baseline parity
    # fixture is intentionally testing agreement between the base entry
    # evaluators, whose APIs do not carry the ORB confirmation field.  Leaving
    # the user's ORB setting enabled here made the synthetic snapshot fail
    # before parity could be tested, aborting historical scans for every symbol.
    # Validate the ORB fail-closed behavior explicitly below instead.
    parity_cfg['require_15m_orb'] = False
    # VWAP and RVOL are now mandatory common GUI gates when enabled. The base
    # parity fixture compares entry_check() to the runtime wrapper, so disable
    # these extra gates only inside this isolated synthetic comparison.
    parity_cfg['smart_require_vwap'] = False
    parity_cfg['smart_min_volume_ratio'] = 0.0
    # The canonical runtime wrapper also applies optional contextual filters
    # (currently the MA filter).  This parity test is specifically validating
    # the base entry contract, so disable those optional filters in the synthetic
    # snapshot rather than letting a user's GUI MA setting make the self-test
    # disagree with entry_check().
    parity_cfg['ma_filter_enabled'] = False
    for _ma_key in ('ma_filter_200', 'ma_filter_100', 'ma_filter_20', 'ma_filter_5', 'ma_filter_cross_5'):
        parity_cfg[_ma_key] = False
    # Keep the parity fixture independent of the user's minimum-price, volume,
    # and turnover thresholds. Those are tested separately below with explicit
    # values; otherwise a valid GUI minimum (e.g. ₹100) can reject the synthetic
    # trigger price (based on previous-close movement), causing the self-test to
    # abort every historical symbol before any data is processed.
    parity_cfg['minimum_price'] = 0.0
    parity_cfg['volume'] = 0.0
    parity_cfg['turnover_cr'] = 0.0
    # The synthetic parity snapshot must be inside the test BUY window.  The
    # direct evaluator intentionally does not enforce time, while the canonical
    # runtime wrapper does.  Use a deterministic test window regardless of the
    # user's GUI trading hours; this changes ONLY the self-test fixture, never
    # the real Historical/Paper/Live configuration.
    parity_cfg['buy_entry_time'] = '09:15'
    parity_cfg['buy_exit_time'] = '15:20'
    _parity_trigger = 99.0 * (1.0 + float(parity_cfg.get('open_movement', 0) or 0) / 100.0)
    base = dict(price=entry_level(100.0, parity_cfg, 99.0), open_price=100.0, previous_close=99.0, volume=200000, turnover_cr=5.0)
    direct = bool(entry_check(**base, cfg=parity_cfg).get('passed'))
    wrapper = bool(evaluate_entry_conditions(**base, cfg=parity_cfg).get('passed'))
    changed = dict(parity_cfg); changed['open_movement'] = float(cfg.get('open_movement', 0) or 0) + 10.0
    fp1 = strategy_fingerprint({'Logic 1': parity_cfg})
    fp2 = strategy_fingerprint({'Logic 1': changed})
    tick = {
        'symbol':'PARITY', 'time':datetime(2026,1,5,9,20), 'ltp':base['price'],
        'close':base['price'], 'open':base['open_price'], 'high':base['price'], 'low':base['open_price'],
        'previous_close':base['previous_close'], 'previous_ltp':base['price'] - 0.01,
        'volume':base['volume'], 'turnover_cr':base['turnover_cr'],
    }
    canonical = canonical_tick_entry(tick, parity_cfg)
    # Separate regression check: with ORB enabled, a snapshot missing the
    # confirmation must still be rejected by the canonical evaluator.
    orb_guard_cfg = dict(parity_cfg)
    orb_guard_cfg['require_15m_orb'] = True
    orb_guard = canonical_tick_entry(tick, orb_guard_cfg)
    orb_missing_fails_closed = bool(
        not orb_guard.get('passed') and
        orb_guard.get('reason') == '15M_ORB_BREAKOUT_NOT_CONFIRMED'
    )
    chase_cfg = dict(parity_cfg)
    # The edge-case assertions below must test a real movement trigger even when
    # the user's saved GUI configuration has open_movement=0 (where all positive
    # prices intentionally qualify and crossed-trigger rejection is not applicable).
    # Keep this synthetic fixture deterministic and isolated from real settings.
    chase_cfg['open_movement'] = 5.2
    chase_trigger = entry_level(0.63, chase_cfg, 0.61)
    crossed_trigger_allowed = entry_movement_qualifies(0.69, 0.63, 0.61, chase_cfg)
    exact_trigger_allowed = entry_movement_qualifies(chase_trigger, 0.63, 0.61, chase_cfg)
    movement_gui_cfg = dict(parity_cfg)
    movement_gui_cfg.update({
        'movement_only_mode': True, 'smart_mode': False,
        'open_direction': 'UP', 'use_price_band': True,
        'price_small': 90.0, 'price_big': 110.0,
        'volume': 250000, 'turnover_cr': 1.0,
        'ma_filter_enabled': False, 'ma_filter_200': False,
        'ma_filter_100': False, 'ma_filter_20': False,
        'ma_filter_5': False, 'ma_filter_cross_5': False,
        'require_52w_high_breakout': False, 'require_52w_low_breakdown': False,
    })
    movement_gui_tick = dict(tick)
    movement_gui_tick.update({'ltp': base['price'], 'close': base['price'], 'volume': 200000, 'turnover_cr': 5.0})
    movement_low_volume_rejected = not bool(canonical_tick_entry(movement_gui_tick, movement_gui_cfg).get('passed'))
    movement_gui_tick['volume'] = 300000
    movement_gui_tick['turnover_cr'] = 2.0
    movement_all_filters_pass = bool(canonical_tick_entry(movement_gui_tick, movement_gui_cfg).get('passed'))
    movement_gui_filter_parity = movement_low_volume_rejected and movement_all_filters_pass
    # Historical's vectorized scan must agree with the scalar LIVE/PAPER trigger
    # rule, including accepted tick jumps, overshoot rejection and gap-open rejection.
    movement_parity_cfg = dict(parity_cfg)
    movement_parity_cfg.update({'open_movement': 5.0, 'movement_max_overshoot_pct': 0.5})
    movement_trigger = entry_level(100.0, movement_parity_cfg, 99.0)
    movement_prices = [movement_trigger, movement_trigger * 1.004, movement_trigger * 1.006]
    movement_mask = entry_movement_qualifies_mask(movement_prices, 100.0, 99.0, movement_parity_cfg)
    movement_scalar = [entry_movement_qualifies(px, 100.0, 99.0, movement_parity_cfg) for px in movement_prices]
    movement_overshoot_parity = list(map(bool, movement_mask)) == movement_scalar == [True, True, False]
    gap_open_rejected = not entry_movement_qualifies(movement_trigger, movement_trigger, 99.0, movement_parity_cfg)
    result = {
        'contract_version': STRATEGY_CONTRACT_VERSION,
        'movement_only_respects_gui_filters': movement_gui_filter_parity,
        'historical_live_movement_overshoot_parity': movement_overshoot_parity,
        'gap_up_open_rejected': gap_open_rejected,
        'direct_entry_check': direct,
        'runtime_wrapper_check': wrapper,
        'canonical_tick_entry': bool(canonical.get('passed')),
        'canonical_reason': canonical.get('reason'),
        'same_decision': direct == wrapper == bool(canonical.get('passed')),
        'orb_missing_fails_closed': orb_missing_fails_closed,
        'config_change_changes_fingerprint': fp1 != fp2,
        'crossed_trigger_allowed': crossed_trigger_allowed,
        'rounded_trigger_allowed': exact_trigger_allowed,
        'passed': bool(direct == wrapper == bool(canonical.get('passed')) and orb_missing_fails_closed and fp1 != fp2 and (not crossed_trigger_allowed) and exact_trigger_allowed and movement_overshoot_parity and gap_open_rejected),
    }
    if not result['passed']:
        raise RuntimeError(f'CANONICAL STRATEGY SELF-TEST FAILED: {result}')
    return result


def _safety_bool(value, default):
    """Parse safety switches without Python's unsafe bool('false') behavior."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        token = value.strip().lower()
        if token in {'1', 'true', 'yes', 'on'}:
            return True
        if token in {'0', 'false', 'no', 'off', ''}:
            return False
    return bool(default)


def _safety_number(value, default):
    """Return a finite, non-negative safety number; invalid values fail to default."""
    try:
        number = float(value)
        if not math.isfinite(number) or number < 0:
            return float(default)
        return number
    except (TypeError, ValueError, OverflowError):
        return float(default)


def load_trading_safety() -> dict:
    cfg = dict(SAFETY_DEFAULTS)
    try:
        if SAFETY_CONFIG_PATH.exists():
            raw=json.loads(SAFETY_CONFIG_PATH.read_text(encoding='utf-8'))
            if isinstance(raw,dict):
                cfg.update({k:v for k,v in raw.items() if k in cfg})
    except Exception:
        # A malformed or partially-written config must never enable a safety
        # switch or disable a configured default by coercion.
        cfg = dict(SAFETY_DEFAULTS)

    for key in ('live_enabled', 'strict_strategy_parity',
                'stop_on_order_error', 'record_decision_events'):
        cfg[key] = _safety_bool(cfg.get(key), SAFETY_DEFAULTS[key])

    try:
        raw_orders = float(cfg.get('max_live_orders_per_run', 50))
        cfg['max_live_orders_per_run'] = (
            max(1, int(raw_orders)) if math.isfinite(raw_orders) else 50
        )
    except (TypeError, ValueError, OverflowError):
        cfg['max_live_orders_per_run'] = 50

    for key in ('max_trade_value', 'max_daily_loss', 'paper_max_daily_loss'):
        cfg[key] = _safety_number(cfg.get(key), SAFETY_DEFAULTS[key])
    return cfg


def save_trading_safety(cfg: dict) -> None:
    clean=dict(SAFETY_DEFAULTS)
    clean.update({k:v for k,v in (cfg or {}).items() if k in clean})
    for key in ('live_enabled', 'strict_strategy_parity',
                'stop_on_order_error', 'record_decision_events'):
        clean[key] = _safety_bool(clean.get(key), SAFETY_DEFAULTS[key])
    try:
        clean['max_live_orders_per_run'] = max(
            1, int(float(clean.get('max_live_orders_per_run', 50)))
        )
    except (TypeError, ValueError, OverflowError):
        clean['max_live_orders_per_run'] = 50
    for key in ('max_trade_value', 'max_daily_loss', 'paper_max_daily_loss'):
        clean[key] = _safety_number(clean.get(key), SAFETY_DEFAULTS[key])

    SAFETY_CONFIG_PATH.parent.mkdir(parents=True,exist_ok=True)
    temp_path = SAFETY_CONFIG_PATH.with_name(SAFETY_CONFIG_PATH.name + '.tmp')
    try:
        with temp_path.open('w', encoding='utf-8') as handle:
            handle.write(json.dumps(clean, indent=2, allow_nan=False))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, SAFETY_CONFIG_PATH)
    finally:
        try:
            if temp_path.exists():
                temp_path.unlink()
        except OSError:
            pass

def _canonical_decision_payload(*, symbol, logic, price, open_price, previous_close, volume, turnover_cr, cfg, rank=None, decision_time=None, market_state=None):
    """Build the immutable full-condition strategy snapshot used to authorize a LIVE BUY."""
    entry=entry_check(price, open_price, previous_close, volume, turnover_cr, cfg)
    _state=dict(market_state or {})
    _state.update({'symbol':symbol, 'ltp':float(price or 0), 'open':open_price,
                   'previous_close':previous_close, 'volume':volume, 'turnover_cr':turnover_cr})
    if not isinstance(_state.get('time'), datetime):
        try: _state['time']=_parse_breeze_datetime(decision_time) if decision_time else None
        except Exception: _state['time']=None
    _full_entry=canonical_tick_entry(_state, cfg)
    payload={
        'strategy_contract_version':'CANONICAL-ENTRY-V1',
        'symbol':str(symbol or '').strip().upper(),
        'logic':str(logic or '').strip(),
        'price':round(float(price or 0),8),
        'open':round(float(open_price or 0),8),
        'previous_close':round(float(previous_close or 0),8),
        'open_gap_pct':entry.get('open_gap_pct'),
        'actual_open_movement_pct':entry.get('actual_open_movement_pct'),
        'volume':round(float(volume or 0),8),
        'turnover_cr':round(float(turnover_cr or 0),8),
        'rank':None if rank is None else int(rank),
        'decision_time':str(decision_time or ''),
        'entry_checks':entry.get('checks',{}),
        'canonical_entry_passed':bool(_full_entry.get('passed')),
        'canonical_entry_reason':str(_full_entry.get('reason') or ''),
        'market_state':{k:_state.get(k) for k in ('time','vwap','relative_volume','runtime_relative_volume','orb15_breakout','orb15_ready','orb15_reason')},
        'entry_trigger_price':round(float(entry.get('trigger_price') or 0),8),
        'logic_config':{k:cfg.get(k) for k in sorted(cfg.keys())},
    }
    raw=json.dumps(payload,sort_keys=True,separators=(',',':'),default=str).encode('utf-8')
    payload['decision_hash']=hashlib.sha256(raw).hexdigest()
    payload['entry_passed']=bool(_full_entry.get('passed'))
    return payload


def canonical_live_buy_authorization(*, symbol, logic, price, open_price, previous_close, volume, turnover_cr, cfg, rank=None, decision_time=None, market_state=None):
    """HARD LIVE BUY GATE.

    A real BUY is authorized only when the exact canonical entry evaluator passes at
    the same market snapshot used for the order. Missing/invalid inputs fail closed.
    The returned authorization is an immutable decision record; callers must pass it
    to _submit_authorized_live_buy().
    """
    safety=load_trading_safety()
    if not bool(safety.get('strict_strategy_parity',True)):
        raise RuntimeError('LIVE SAFETY LOCK: strict_strategy_parity must remain enabled.')
    if str(logic or '').strip() not in dict(enabled_logics(load_logics())):
        raise RuntimeError(f'LIVE SAFETY LOCK: {logic!r} is not an enabled canonical logic.')
    decision=_canonical_decision_payload(symbol=symbol,logic=logic,price=price,open_price=open_price,
                                         previous_close=previous_close,volume=volume,turnover_cr=turnover_cr,
                                         cfg=cfg,rank=rank,decision_time=decision_time,market_state=market_state)
    if not decision['entry_passed']:
        raise RuntimeError(f'LIVE SAFETY LOCK: canonical entry conditions failed for {symbol}/{logic}: {decision}')
    # Re-read configuration immediately before authorization. A GUI/config change
    # during a run must never silently alter an already-computed BUY.
    latest=load_logics().get(str(logic),{})
    latest_decision=_canonical_decision_payload(symbol=symbol,logic=logic,price=price,open_price=open_price,
                                                previous_close=previous_close,volume=volume,turnover_cr=turnover_cr,
                                                cfg=latest,rank=rank,decision_time=decision_time,market_state=market_state)
    if latest_decision['decision_hash'] != decision['decision_hash']:
        raise RuntimeError('LIVE SAFETY LOCK: strategy configuration changed between evaluation and authorization.')
    decision['authorized_at']=datetime.now().isoformat(timespec='milliseconds')
    record_decision_event({'event':'CANONICAL_LIVE_BUY_AUTHORIZED',**decision})
    return decision


def _submit_authorized_live_buy(breeze, *, authorization, stock_code, quantity_value, price, user_remark):
    """Only broker BUY submission path allowed by strict strategy parity.

    ``authorization['symbol']`` is the canonical NSE symbol while ``stock_code``
    may be Breeze's mapped symbol.  The caller must therefore also supply the
    canonical NSE symbol separately when the broker symbol differs.
    """
    if not LIVE_EXECUTION_CERTIFIED:
        raise RuntimeError('LIVE SAFETY LOCK: this r90 build is not certified for real-money execution. Complete broker simulation and supervised validation first.')
    if float(load_trading_safety().get('max_daily_loss', 0) or 0) > 0:
        raise RuntimeError('LIVE SAFETY LOCK: max_daily_loss is configured but broker-wide realized + unrealized P&L enforcement is not verified. Set max_daily_loss=0 only if you explicitly accept having no daily-loss stop.')
    if not isinstance(authorization,dict) or not authorization.get('entry_passed') or not authorization.get('decision_hash'):
        raise RuntimeError('LIVE SAFETY LOCK: broker BUY submission has no valid canonical authorization.')
    safety=load_trading_safety()
    if not bool(safety.get('strict_strategy_parity',True)):
        raise RuntimeError('LIVE SAFETY LOCK: strict_strategy_parity disabled; refusing BUY.')
    # Broker symbols can legitimately differ from NSE canonical symbols after
    # security-master mapping.  The mapping itself must be the one produced by
    # the validated preflight, so the caller cannot substitute an unrelated code.
    mapped_nse = str(authorization.get('mapped_nse_symbol') or authorization.get('symbol') or '').upper()
    if mapped_nse != str(authorization.get('symbol') or '').upper():
        raise RuntimeError('LIVE SAFETY LOCK: broker symbol mapping is not authorized for the canonical NSE symbol.')
    # Do not let a caller change the price after authorization. Live execution may
    # still fill differently at the broker, but the submitted limit must be the one
    # produced by the canonical decision snapshot.
    if abs(float(price)-float(authorization.get('price') or 0)) > 1e-8:
        raise RuntimeError('LIVE SAFETY LOCK: submitted BUY price differs from canonical decision price.')
    qty = int(quantity_value or 0)
    submitted_price = float(price or 0)
    if qty <= 0 or submitted_price <= 0:
        raise RuntimeError('LIVE SAFETY LOCK: BUY quantity and price must be positive.')
    # GUI per-logic investment is the sole per-entry budget authority. This value
    # comes from the immutable strategy snapshot whose hash was authorized above.
    logic_cfg = authorization.get('logic_config') or {}
    gui_budget = float(logic_cfg.get('buy_amount', 0) or 0)
    if gui_budget <= 0:
        raise RuntimeError('LIVE SAFETY LOCK: selected GUI logic has no positive buy_amount budget.')
    order_value = qty * submitted_price
    if order_value - gui_budget > 0.01:
        raise RuntimeError(
            f'LIVE SAFETY LOCK: order value ₹{order_value:.2f} exceeds '
            f'{authorization.get("logic", "logic")} GUI buy_amount ₹{gui_budget:.2f}.'
        )
    record_decision_event({'event':'CANONICAL_LIVE_BUY_SUBMISSION','symbol':authorization['symbol'],
                           'logic':authorization['logic'],'decision_hash':authorization['decision_hash'],
                           'quantity':int(quantity_value),'price':float(price)})
    return broker_call('Canonical LIVE BUY', breeze.place_order,
                       stock_code=stock_code,exchange_code='NSE',product='cash',action='buy',
                       order_type='limit',stoploss='',quantity=str(int(quantity_value)),price=str(float(price)),
                       validity='day',user_remark=user_remark)

def record_decision_event(event: dict) -> None:
    # Append-only decision stream used for audit and exact replay of recorded inputs.
    try:
        cfg=load_trading_safety()
        if not cfg.get('record_decision_events',True): return
        LOG_DIR.mkdir(parents=True,exist_ok=True)
        row=dict(event or {})
        row.setdefault('recorded_at',datetime.now().isoformat(timespec='milliseconds'))
        with DECISION_AUDIT_LOG.open('a',encoding='utf-8') as fh:
            fh.write(json.dumps(row,default=str,separators=(',',':'))+'\n')
    except Exception:
        pass

def live_safety_gate(top: pd.DataFrame) -> dict:
    # Final deterministic gate immediately before any LIVE order submission.
    cfg=load_trading_safety()
    if not LIVE_EXECUTION_CERTIFIED:
        raise RuntimeError('LIVE SAFETY LOCK: r90 certification gate is closed. No LIVE orders sent.')
    if not bool(cfg.get('strict_strategy_parity', True)):
        raise RuntimeError('LIVE SAFETY LOCK: strict_strategy_parity must remain enabled. No LIVE order was sent.')
    if float(cfg.get('max_daily_loss', 0) or 0) > 0:
        raise RuntimeError('LIVE SAFETY LOCK: max_daily_loss is configured but broker-wide realized + unrealized P&L enforcement is not verified. NO LIVE order was sent.')
    if not bool(cfg.get('live_enabled',False)):
        raise RuntimeError('LIVE SAFETY LOCK: live_enabled=false. Enable LIVE explicitly in Config/trading_safety.json after paper testing.')
    if top is None or top.empty:
        raise RuntimeError('LIVE SAFETY LOCK: no validated order candidates.')
    max_orders=len(top)  # final order-count authority is the GUI per-logic Maximum Orders
    # Candidate-level GUI budget validation is performed against each row's logic
    # configuration at authorization/submission, not against a global fixed cap.
    return {'config':cfg,'top':top,'max_orders':max_orders}

# Live_Order_Audit.log is a LIVE-TRADING-only diagnostic file.
# Historical and Paper modes must never create or append to this file.
_LIVE_AUDIT_ACTIVE = False
# Remote mobile control stop flag.  The mobile STOP button requests a graceful
# stop so the LIVE engine can run its normal cleanup and square-off path.
REMOTE_STOP_FILE = ROOT / 'remote_stop.flag'
MOBILE_ORDER_FEED = APP_DATA / 'Mobile_Order_Feed.jsonl'
MOBILE_ORDER_STATE = APP_DATA / 'Mobile_Order_State.json'

def _mobile_order_event(event: str, mode: str, pos: dict, side: str, price=None,
                        quantity_value=None, reason='', order_id='', status='') -> None:
    """Append a compact order event for the mobile control page."""
    try:
        MOBILE_ORDER_FEED.parent.mkdir(parents=True, exist_ok=True)
        # One stable ID links the BUY leg and its intraday SELL leg.
        # The mobile monitor uses this ID to show one row/card per completed
        # intraday trade instead of counting BUY+SELL as two separate shares.
        trade_id = str(pos.get('_mobile_trade_id') or '')
        if not trade_id:
            buy_dt = pos.get('buy_trade_datetime') or datetime.now()
            trade_id = f"{mode}-{pos.get('logic') or ''}-{pos.get('symbol') or ''}-{str(buy_dt)}"
            pos['_mobile_trade_id'] = trade_id
        buy_qty=int(pos.get('buy_actual_qty') or pos.get('quantity') or 0)
        sell_qty=int(pos.get('sell_actual_qty') or 0)
        buy_price=float(pos.get('buy_actual_price') or 0)
        sell_price=float(pos.get('sell_actual_price') or 0)
        buy_time=str(pos.get('buy_trade_datetime') or '')
        sell_time=str(pos.get('sell_trade_datetime') or '')
        symbol_text=str(pos.get('symbol') or '').strip().upper()
        company_name=str(pos.get('company_name') or '').strip()
        if not company_name:
            try:
                company_name=_company_name_for_symbol(symbol_text)
            except Exception:
                company_name=symbol_text
        item={
            'trade_id':trade_id,
            'event':event,'mode':mode,'side':side,'symbol':symbol_text,'company_name':company_name or symbol_text,
            'logic':str(pos.get('logic') or ''),
            'quantity':int(quantity_value if quantity_value is not None else (buy_qty or sell_qty)),
            'price':float(price if price is not None else (sell_price if side=='SELL' else buy_price)),
            'time':str((pos.get('sell_trade_datetime') if side=='SELL' else pos.get('buy_trade_datetime')) or datetime.now()),
            'buy_quantity':buy_qty,'buy_price':buy_price,'buy_time':buy_time,
            'sell_quantity':sell_qty,'sell_price':sell_price,'sell_time':sell_time,
            'reason':str(reason or pos.get('exit_reason') or ''),
            'order_id':str(order_id or pos.get('sell_order_id') or pos.get('buy_order_id') or ''),
            'status':str(status or pos.get('status') or ''),
        }
        if pos.get('gross_pnl') is not None:
            item['gross_pnl']=float(pos.get('gross_pnl') or 0)
        elif side=='SELL' and pos.get('buy_actual_price') is not None and price is not None:
            item['gross_pnl']=(float(price)-float(pos.get('buy_actual_price') or 0))*int(quantity_value if quantity_value is not None else (pos.get('quantity') or 0))
        if pos.get('simulated_total_charges') is not None: item['charges']=float(pos.get('simulated_total_charges') or 0)
        if pos.get('net_pnl') is not None: item['net_pnl']=float(pos.get('net_pnl') or 0)
        with MOBILE_ORDER_FEED.open('a',encoding='utf-8') as fh:
            fh.write(json.dumps(item,ensure_ascii=False,default=str)+'\n'); fh.flush()
        try:
            lines=MOBILE_ORDER_FEED.read_text(encoding='utf-8',errors='replace').splitlines()
            if len(lines)>2000: MOBILE_ORDER_FEED.write_text('\n'.join(lines[-1500:])+'\n',encoding='utf-8')
        except Exception: pass
    except Exception as exc:
        print(f'WARNING: mobile order feed write failed: {exc}')


def _write_mobile_order_state(positions: dict, latest: dict) -> None:
    """Publish current open-position prices/P&L for the mobile Order Monitor.

    This is a lightweight local state file. It is deliberately separate from the
    append-only order feed so the mobile page can show live LTP and unrealized P&L
    without waiting for a SELL event.
    """
    try:
        rows=[]
        for key,pos in (positions or {}).items():
            sym=str(pos.get('symbol') or '').strip().upper()
            if not sym: continue
            q=(latest or {}).get(sym) or {}
            # Never hide an open share from mobile just because a quote is
            # temporarily missing. Prefer live LTP, then the latest known quote
            # or last traded/candle price; explicitly flag stale/missing quotes.
            live_ltp=float(q.get('ltp') or 0)
            fallback_ltp=float(pos.get('last_ltp') or pos.get('close') or pos.get('buy_actual_price') or pos.get('limit_price') or 0)
            ltp=live_ltp if live_ltp>0 else fallback_ltp
            buy=float(pos.get('buy_actual_price') or pos.get('limit_price') or 0)
            qty=int(pos.get('buy_actual_qty') or pos.get('quantity') or 0)
            if buy<=0 or qty<=0: continue
            quote_available=live_ltp>0
            company=str(pos.get('company_name') or '').strip()
            if not company:
                try: company=_company_name_for_symbol(sym)
                except Exception: company=sym
            gross=(ltp-buy)*qty
            try:
                est=icici_intraday_charges(buy*qty,ltp*qty,exchange='NSE')
                charges=float(est.get('total_charges') or 0)
            except Exception:
                charges=0.0
            trade_id=str(pos.get('_mobile_trade_id') or '')
            if not trade_id:
                buy_dt=pos.get('buy_trade_datetime') or datetime.now()
                trade_id=f"{pos.get('mode') or 'PAPER'}-{pos.get('logic') or ''}-{sym}-{str(buy_dt)}"
                pos['_mobile_trade_id']=trade_id
            rows.append({
                'trade_id':trade_id,'mode':str(pos.get('mode') or '').upper(),
                'logic':str(pos.get('logic') or ''),'symbol':sym,'company_name':company or sym,
                'quantity':qty,'buy_price':buy,'buy_time':str(pos.get('buy_trade_datetime') or ''),
                'current_price':ltp if ltp>0 else None,
                'current_time':str(q.get('time') or pos.get('last_quote_time') or datetime.now()),
                'quote_available':quote_available,
                'quote_status':'LIVE' if quote_available else ('STALE_FALLBACK' if ltp>0 else 'NO_QUOTE'),
                'gross_pnl':gross if ltp>0 else None,
                'estimated_charges':charges if ltp>0 else None,
                'net_pnl':gross-charges if ltp>0 else None,
                'status':str(pos.get('status') or 'OPEN')
            })
        payload={'updated_at':datetime.now().isoformat(timespec='seconds'),'positions':rows}
        tmp=MOBILE_ORDER_STATE.with_suffix('.tmp')
        MOBILE_ORDER_STATE.parent.mkdir(parents=True,exist_ok=True)
        tmp.write_text(json.dumps(payload,ensure_ascii=False,default=str),encoding='utf-8')
        tmp.replace(MOBILE_ORDER_STATE)
    except Exception as exc:
        print(f'WARNING: mobile order state write failed: {exc}')


def _remote_stop_requested() -> bool:
    return REMOTE_STOP_FILE.exists()

def _clear_remote_stop() -> None:
    try:
        REMOTE_STOP_FILE.unlink(missing_ok=True)
    except Exception:
        pass

def _request_remote_stop() -> None:
    try:
        REMOTE_STOP_FILE.write_text('stop', encoding='utf-8')
    except Exception:
        pass

def _live_audit_session(func):
    """Enable Live_Order_Audit.log for an entry point that is always LIVE."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        global _LIVE_AUDIT_ACTIVE
        previous = _LIVE_AUDIT_ACTIVE
        _LIVE_AUDIT_ACTIVE = True
        try:
            return func(*args, **kwargs)
        finally:
            _LIVE_AUDIT_ACTIVE = previous
    return wrapper

def _live_audit_order_session(func):
    """Enable the live audit only when run_orders() is actually in LIVE mode."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        mode = kwargs.get('mode')
        if mode is None and len(args) >= 2:
            mode = args[1]
        if str(mode).strip().lower() != 'live':
            return func(*args, **kwargs)
        return _live_audit_session(func)(*args, **kwargs)
    return wrapper

LIVE_ENTRY_START = (9, 15)
LIVE_ENTRY_CUTOFF = (15, 15)
# Execution safety cap for PAPER/LIVE order placement only.
# Historical data collection has NO 50-order cap.
MAX_ORDERS_PER_RUN = 50

# PAPER BUY ceiling is derived from the saved GUI per-logic order caps.
# There is no second, conflicting global cap file.
def format_paper_buy_budget_audit(enabled) -> str:
    """Format the actual per-entry GUI notional budgets, never order counts."""
    rows = []
    for name, cfg in (enabled or []):
        try:
            budget = max(0.0, float((cfg or {}).get('buy_amount', 0) or 0))
        except Exception:
            budget = 0.0
        rows.append(f'{name}=₹{budget:,.2f}')
    return ', '.join(rows) if rows else 'none'


def paper_global_buy_cap(enabled=None) -> int:
    """Derive the PAPER session BUY ceiling only from saved GUI logic caps.

    There is deliberately no separate paper_trading_settings override: the GUI's
    per-logic Maximum Orders is authoritative in Historical, Paper and Live.
    A zero cap means unlimited for that logic, matching max_orders_for_logic().
    """
    rows = list(enabled or enabled_logics(load_logics()))
    caps = [max_orders_for_logic(cfg) for _name, cfg in rows]
    if not caps:
        return 0
    if any(cap == 0 for cap in caps):
        return 1_000_000_000
    return sum(caps)

# ---------------------------------------------------------------------------
# ICICI Direct shared charge engine
# ---------------------------------------------------------------------------
# Keep one charge calculation for HISTORICAL, PAPER and LIVE.  The selected
# Prime plan is stored in Config/icici_charge_settings.json so changing the
# ICICI plan does not require changing trading logic.
ICICI_CHARGE_SETTINGS = CONFIG_DIR / 'icici_charge_settings.json'
ICICI_PRIME_PLANS = {
    'Prime 999': {'brokerage_rate': 0.00022},
    'Prime 4999': {'brokerage_rate': 0.00010},
    'Prime 9999': {'brokerage_rate': 0.00007},
}
ICICI_DEFAULT_CHARGE_CONFIG = {
    'plan': 'Prime 999',
    'exchange': 'NSE',
    'segment': 'equity_intraday',
    # Current ICICI-published statutory rates for equity intraday.
    'stt_sell_rate': 0.00025,
    'nse_exchange_rate': 0.0000307,
    'bse_exchange_rate': 0.0000375,
    'sebi_rate': 0.000001,
    'stamp_buy_rate': 0.00003,
    'gst_rate': 0.18,
}

def load_icici_charge_config() -> dict:
    cfg = dict(ICICI_DEFAULT_CHARGE_CONFIG)
    try:
        if ICICI_CHARGE_SETTINGS.exists():
            raw = json.loads(ICICI_CHARGE_SETTINGS.read_text(encoding='utf-8'))
            if isinstance(raw, dict):
                cfg.update(raw)
    except Exception:
        pass
    if cfg.get('plan') not in ICICI_PRIME_PLANS:
        cfg['plan'] = 'Prime 999'
    if str(cfg.get('exchange','NSE')).upper() not in ('NSE','BSE'):
        cfg['exchange'] = 'NSE'
    return cfg

def save_icici_charge_config(cfg: dict) -> None:
    clean = dict(ICICI_DEFAULT_CHARGE_CONFIG)
    clean.update({k:v for k,v in (cfg or {}).items() if k in clean})
    if clean.get('plan') not in ICICI_PRIME_PLANS:
        clean['plan'] = 'Prime 999'
    ICICI_CHARGE_SETTINGS.parent.mkdir(parents=True, exist_ok=True)
    ICICI_CHARGE_SETTINGS.write_text(json.dumps(clean, indent=2), encoding='utf-8')

def icici_intraday_charges(buy_value: float, sell_value: float,
                           exchange: str | None = None,
                           config: dict | None = None) -> dict:
    """Calculate ICICI Direct NSE/BSE equity-intraday charges.

    This is the canonical *simulated/historical* charge engine.  It deliberately
    does not use Breeze preview aggregates because preview responses can expose
    delivery/MTF-style aggregates even when the strategy order is ``product=cash``.
    The individual statutory rates below are applied to the actual BUY/SELL
    turnover, and each displayed component is rounded to paise before the total
    is formed, matching the way charges are presented in brokerage calculators.

    Prime plan intraday brokerage rates are plan-specific.  Statutory rates are
    kept separately so changing the plan never changes STT/exchange/SEBI/stamp.
    """
    cfg = dict(load_icici_charge_config())
    if config:
        cfg.update(config)
    buy = max(0.0, float(buy_value or 0.0))
    sell = max(0.0, float(sell_value or 0.0))
    turnover = buy + sell
    plan = cfg.get('plan', 'Prime 999')
    plan_data = ICICI_PRIME_PLANS.get(plan, ICICI_PRIME_PLANS['Prime 999'])
    brokerage_rate = float(plan_data['brokerage_rate'])
    ex = str(exchange or cfg.get('exchange') or 'NSE').upper()
    exchange_rate = float(cfg.get('bse_exchange_rate') if ex == 'BSE'
                          else cfg.get('nse_exchange_rate'))

    # Raw calculation.  Round each charge to 2 decimals, then sum the displayed
    # components.  This prevents Excel from showing a total that differs from the
    # visible component columns by a few paise.
    brokerage = round(turnover * brokerage_rate, 2)
    stt = round(sell * float(cfg.get('stt_sell_rate', 0.00025)), 2)
    exchange_charges = round(turnover * exchange_rate, 2)
    sebi = round(turnover * float(cfg.get('sebi_rate', 0.000001)), 2)
    stamp = round(buy * float(cfg.get('stamp_buy_rate', 0.00003)), 2)
    gst = round((brokerage + exchange_charges + sebi) * float(cfg.get('gst_rate', 0.18)), 2)
    total = round(brokerage + stt + exchange_charges + sebi + gst + stamp, 2)
    return {
        'plan': plan,
        'exchange': ex,
        'brokerage': brokerage,
        'stt': stt,
        'exchange_turnover_charges': exchange_charges,
        'sebi_charges': sebi,
        'gst': gst,
        'stamp_duty': stamp,
        # IMPORTANT: this column is only for fees not represented by the
        # component columns above.  It must never contain Breeze's aggregate
        # ``total_other_charges`` field, which is already included in its total.
        'total_other_charges': 0.0,
        'total_charges': total,
        'buy_turnover': round(buy, 2),
        'sell_turnover': round(sell, 2),
        'total_turnover': round(turnover, 2),
        'rate_source': 'ICICI Direct published equity-intraday rates',
    }

def _normalise_breeze_charge_breakdown(raw: dict | None) -> dict:
    """Normalize Breeze charge responses without double-counting aggregates.

    Breeze ``preview_order`` returns component fields plus aggregate fields such
    as ``total_other_charges`` and ``total_brokerage``.  The aggregate fields
    must NOT be added again to the components.  This function therefore keeps
    both the displayed components and the authoritative Breeze total.
    """
    raw = raw or {}

    def pick(*names):
        for name in names:
            if name in raw and raw.get(name) not in (None, ''):
                try:
                    return float(raw.get(name))
                except Exception:
                    pass
        return None

    brokerage = pick('brokerage','Brokerage','brokerage_amount','brokerageAmount')
    stt = pick('stt','STT','stt_amount','sttAmount')
    exch = pick('exchange_turnover_charges','exchange_transaction_charges',
                'exchangeTransactionCharges','transaction_charges','TransactionCharges')
    sebi = pick('sebi_charges','sebi','SEBI','sebi_amount','sebiAmount')
    gst = pick('gst','GST','gst_amount','gstAmount')
    stamp = pick('stamp_duty','stampDuty','stamp_duty_amount','stampDutyAmount')
    other = pick('other_charges','otherCharges')

    # Breeze preview uses total_other_charges as an aggregate of the non-brokerage
    # charges.  Keep it for audit/display, but do not add it to the component sum.
    breeze_other = pick('total_other_charges')
    if other is None:
        other = breeze_other

    # Breeze preview's total_brokerage is the complete charge amount in the
    # published example.  Prefer it over reconstructing the total.
    total = pick('total_brokerage','total_expense_and_taxes','total_expense_taxes',
                 'total_charges','totalCharges','charges','total_charge','totalCharge',
                 'total_transaction_cost','totalTransactionCost','transaction_cost',
                 'transactionCost')

    # Actual get_trade_detail responses expose brokerage_amount, taxes and
    # total_transaction_cost rather than the preview breakdown.
    taxes = pick('taxes','Taxes','tax_amount','taxAmount')
    transaction_cost = pick('total_transaction_cost','totalTransactionCost')

    if total is None and transaction_cost is not None:
        total = transaction_cost
    if total is None and taxes is not None:
        total = (brokerage or 0.0) + taxes
        if other is None:
            other = taxes
    if total is None:
        # Reconstruct only from actual component fields.  If Breeze supplied
        # total_other_charges, use it as the aggregate residual only when the
        # individual statutory components are absent.
        component_values = [brokerage, stt, exch, sebi, gst, stamp]
        if any(v is not None for v in component_values):
            total = sum(v or 0.0 for v in component_values)
            if not any(v is not None for v in (stt, exch, sebi, gst, stamp)) and breeze_other is not None:
                total = (brokerage or 0.0) + breeze_other
        elif breeze_other is not None:
            total = (brokerage or 0.0) + breeze_other

    if total is None:
        return {}

    return {
        'brokerage': float(brokerage or 0.0),
        'stt': float(stt or 0.0),
        'exchange_turnover_charges': float(exch or 0.0),
        'sebi_charges': float(sebi or 0.0),
        'gst': float(gst or 0.0),
        'stamp_duty': float(stamp or 0.0),
        'total_other_charges': float(other or 0.0),
        'total_charges': float(total),
        'source': 'Breeze',
        'raw_total_other_charges': float(breeze_other or 0.0),
        'raw_taxes': float(taxes or 0.0),
    }

def _breeze_preview_total_charges(breeze, stock_code, qty, buy_price, sell_price, exchange='NSE', product='cash') -> dict:
    """Ask Breeze for both executed-side previews and combine the returned charges.

    This is the preferred charge source for simulated trades and for Live when
    the executed trade detail does not itself expose a charge breakdown.
    """
    if not breeze or not stock_code or not qty or buy_price is None or sell_price is None:
        return {}
    try:
        buy_raw=_preview_charges_safe(breeze, stock_code, 'buy', qty, buy_price, product=product)
        sell_raw=_preview_charges_safe(breeze, stock_code, 'sell', qty, sell_price, product=product)
        buy=_normalise_breeze_charge_breakdown(buy_raw)
        sell=_normalise_breeze_charge_breakdown(sell_raw)
        if not buy and not sell: return {}
        keys=('brokerage','stt','exchange_turnover_charges','sebi_charges','gst','stamp_duty','total_other_charges')
        out={k:float((buy.get(k) or 0)+(sell.get(k) or 0)) for k in keys}
        out['total_charges']=float((buy.get('total_charges') or 0)+(sell.get('total_charges') or 0))
        out['source']='Breeze preview'
        out['buy_breakdown']=buy
        out['sell_breakdown']=sell
        return out
    except Exception:
        return {}

def apply_icici_charges(result: dict, exchange: str = 'NSE', breeze=None) -> dict:
    """Apply the canonical charge calculation and populate Excel-safe fields.

    HISTORICAL/PAPER/PAPER_LIVE
        Always use the published ICICI intraday formula.  Broker preview data is
        deliberately ignored because its aggregate fields are not reliable for
        reconstructing a component-by-component intraday statement.

    LIVE
        If executed broker trade detail provides a total transaction cost, that
        total remains authoritative.  The visible statutory components are still
        calculated from the executed prices/quantity.  Any difference between
        the broker total and those known components is placed in ``Other Charges``
        as a true residual, never as a duplicate aggregate.

    In every mode:
        Gross P&L = executed SELL turnover - executed BUY turnover
        Net P&L   = Gross P&L - Total Charges
    """
    buy_qty = int(float(result.get('buy_actual_qty') or result.get('quantity') or 0))
    sell_qty = int(float(result.get('sell_actual_qty') or 0))
    buy_price = float(result.get('buy_actual_price') or result.get('limit_price') or 0)
    sell_price = float(result.get('sell_actual_price') or 0)
    buy_value = buy_qty * buy_price
    sell_value = sell_qty * sell_price
    gross = (sell_value - buy_value) if buy_qty and sell_qty else float(result.get('gross_pnl') or 0)

    formula = icici_intraday_charges(buy_value, sell_value, exchange=exchange)
    mode = str(result.get('mode') or '').lower()

    # Simulated trades have no executed contract note.  The published formula
    # is the single source of truth and avoids inconsistent Breeze preview data.
    if mode in {'historical', 'paper', 'paper_live'}:
        charges = dict(formula)
        charges['source'] = 'ICICI published-rate intraday formula'
        charges['total_other_charges'] = 0.0
        charges['buy_breakdown'] = {k: formula[k] for k in
            ('brokerage','stt','exchange_turnover_charges','sebi_charges','gst','stamp_duty')}
        charges['sell_breakdown'] = {}
        has_actual = False
        preview = False
    else:
        actual_buy = _normalise_breeze_charge_breakdown(
            result.get('buy_trade') if isinstance(result.get('buy_trade'), dict) else {})
        actual_sell = _normalise_breeze_charge_breakdown(
            result.get('sell_trade') if isinstance(result.get('sell_trade'), dict) else {})

        def _actual_total(d):
            return float(d.get('total_charges') or 0.0) if d else 0.0

        actual_total = _actual_total(actual_buy) + _actual_total(actual_sell)
        has_actual = actual_total > 1e-9

        if has_actual:
            # Use actual brokerage when the broker supplies it; otherwise use the
            # configured plan rate. Statutory components come from the executed
            # turnover. This makes the Excel breakdown additive and auditable.
            brokerage = (float(actual_buy.get('brokerage') or 0.0) +
                         float(actual_sell.get('brokerage') or 0.0))
            if brokerage <= 1e-9:
                brokerage = formula['brokerage']
            brokerage = round(brokerage, 2)
            known = {
                'brokerage': brokerage,
                'stt': formula['stt'],
                'exchange_turnover_charges': formula['exchange_turnover_charges'],
                'sebi_charges': formula['sebi_charges'],
                'stamp_duty': formula['stamp_duty'],
                'gst': round((brokerage + formula['exchange_turnover_charges'] +
                              formula['sebi_charges']) * 0.18, 2),
            }
            known_total = round(sum(known.values()), 2)
            residual = round(max(0.0, actual_total - known_total), 2)
            # If rounding/field extraction makes the broker total smaller than
            # known statutory components, retain the broker total rather than
            # inventing a negative fee. In that exceptional case, the total is
            # authoritative and Other Charges remains zero.
            total = round(actual_total, 2)
            charges = dict(known)
            charges['total_other_charges'] = residual
            charges['total_charges'] = total
            charges['source'] = 'Breeze executed trade detail + ICICI statutory breakdown'
            charges['buy_breakdown'] = actual_buy
            charges['sell_breakdown'] = actual_sell
            preview = False
        else:
            charges = dict(formula)
            charges['source'] = 'ICICI published-rate fallback (Breeze unavailable)'
            charges['buy_breakdown'] = {k: formula[k] for k in
                ('brokerage','stt','exchange_turnover_charges','sebi_charges','gst','stamp_duty')}
            charges['sell_breakdown'] = {}
            preview = False

    result['gross_pnl'] = gross
    result['buy_turnover'] = round(buy_value, 2)
    result['sell_turnover'] = round(sell_value, 2)

    for k in ('brokerage','stt','exchange_turnover_charges','sebi_charges',
              'gst','stamp_duty','total_other_charges'):
        result[k] = round(float(charges.get(k) or 0.0), 2)

    result['buy_charges'] = charges.get('buy_breakdown') or {}
    result['sell_charges'] = charges.get('sell_breakdown') or {}
    result['total_charges'] = round(float(charges.get('total_charges') or 0.0), 2)
    result['simulated_total_charges'] = result['total_charges']
    result['net_pnl'] = round(gross - result['total_charges'], 2)
    result['pnl_pct'] = (result['net_pnl'] / buy_value) if buy_value else None
    result['charge_plan'] = charges.get('plan') or load_icici_charge_config().get('plan')
    result['charge_exchange'] = charges.get('exchange', exchange)
    result['charge_source'] = charges.get('source', 'Unknown')
    result['charge_is_breeze_actual'] = bool(has_actual)
    result['charge_is_breeze_preview'] = False
    result['charge_is_fallback'] = result['charge_source'].startswith('ICICI published-rate fallback')
    return result

def max_orders_for_logic(cfg: dict) -> int:
    """Return the GUI-configured per-logic maximum order count.

    Zero means unlimited for that logic. Invalid negative values are clamped to
    the global safety cap.
    """
    try:
        value = int(float((cfg or {}).get('max_orders', MAX_ORDERS_PER_RUN)))
    except Exception:
        value = MAX_ORDERS_PER_RUN
    return MAX_ORDERS_PER_RUN if value < 0 else value
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36', 'Accept': 'text/csv,*/*'}

WHATSAPP_CONFIG_PATH = CONFIG_DIR / 'whatsapp_settings.json'
WHATSAPP_TOKEN_PATH = CONFIG_DIR / 'whatsapp_token.dat'


def load_whatsapp_settings() -> dict:
    defaults = {
        'enabled': False,
        'phone_number_id': '',
        'recipient_phone': '',
        'api_version': 'v23.0',
        'template_name': 'trading_final_report',
        'template_language': 'en_US',
        'send_mode': 'template',
        'personal_whatsapp_enabled': False,
        'personal_whatsapp_auto_open': False,
        'personal_whatsapp_auto_send': False,
    }
    try:
        if WHATSAPP_CONFIG_PATH.exists():
            raw = json.loads(WHATSAPP_CONFIG_PATH.read_text(encoding='utf-8'))
            if isinstance(raw, dict):
                defaults.update({k:v for k,v in raw.items() if k in defaults})
    except Exception:
        pass
    # Token is intentionally kept outside the JSON settings file.
    if 'access_token' in defaults:
        defaults.pop('access_token', None)
    return defaults


def save_whatsapp_settings(settings: dict, access_token: str | None = None) -> None:
    clean = load_whatsapp_settings()
    for k in ('enabled','phone_number_id','recipient_phone','api_version','template_name','template_language','send_mode','personal_whatsapp_enabled','personal_whatsapp_auto_open','personal_whatsapp_auto_send','personal_recipient_phone'):
        if k in settings:
            clean[k] = settings[k]
    WHATSAPP_CONFIG_PATH.write_text(json.dumps(clean, indent=2), encoding='utf-8')
    if access_token is not None:
        # Reuse the Windows DPAPI credential protection used by the GUI.
        try:
            import ctypes
            # Keep a simple protected token only when running on Windows. On other
            # systems, the caller can supply WHATSAPP_ACCESS_TOKEN via environment.
            if os.name == 'nt':
                class _Blob(ctypes.Structure):
                    _fields_=[('cbData',ctypes.c_ulong),('pbData',ctypes.POINTER(ctypes.c_byte))]
                data=access_token.encode('utf-8'); buf=ctypes.create_string_buffer(data)
                src=_Blob(len(data),ctypes.cast(buf,ctypes.POINTER(ctypes.c_byte))); out=_Blob()
                if ctypes.windll.crypt32.CryptProtectData(ctypes.byref(src), 'WhatsApp token', None, None, None, 1, ctypes.byref(out)):
                    try: WHATSAPP_TOKEN_PATH.write_bytes(ctypes.string_at(out.pbData,out.cbData))
                    finally: ctypes.windll.kernel32.LocalFree(out.pbData)
            elif access_token:
                WHATSAPP_TOKEN_PATH.write_text(access_token, encoding='utf-8')
        except Exception:
            pass


def _load_whatsapp_token() -> str:
    env=os.getenv('WHATSAPP_ACCESS_TOKEN','').strip()
    if env: return env
    if not WHATSAPP_TOKEN_PATH.exists(): return ''
    try:
        if os.name == 'nt':
            import ctypes
            class _Blob(ctypes.Structure):
                _fields_=[('cbData',ctypes.c_ulong),('pbData',ctypes.POINTER(ctypes.c_byte))]
            data=WHATSAPP_TOKEN_PATH.read_bytes(); buf=ctypes.create_string_buffer(data)
            src=_Blob(len(data),ctypes.cast(buf,ctypes.POINTER(ctypes.c_byte))); out=_Blob()
            if not ctypes.windll.crypt32.CryptUnprotectData(ctypes.byref(src),None,None,None,None,1,ctypes.byref(out)): return ''
            try: return ctypes.string_at(out.pbData,out.cbData).decode('utf-8')
            finally: ctypes.windll.kernel32.LocalFree(out.pbData)
        return WHATSAPP_TOKEN_PATH.read_text(encoding='utf-8').strip()
    except Exception:
        return ''


def build_final_whatsapp_report(results: list[dict], mode: str, report_date=None) -> str:
    report_date = report_date or datetime.now().date()
    rows=list(results or [])
    completed=[]
    for r in rows:
        buy_qty=int(float(r.get('buy_actual_qty') or r.get('quantity') or 0))
        sell_qty=int(float(r.get('sell_actual_qty') or 0))
        # Include executed/recorded trades and omit pure rejected BUY attempts.
        if buy_qty>0 or sell_qty>0 or r.get('gross_pnl') is not None:
            completed.append(r)
    trades=len(completed)
    buy_qty=sum(int(float(r.get('buy_actual_qty') or r.get('quantity') or 0)) for r in completed)
    sell_qty=sum(int(float(r.get('sell_actual_qty') or 0)) for r in completed)
    gross=sum(float(r.get('gross_pnl') or 0) for r in completed)
    charges=sum(float(r.get('simulated_total_charges') or r.get('total_charges') or 0) for r in completed)
    net=gross-charges
    profit=sum(max(float(r.get('gross_pnl') or 0)-float(r.get('simulated_total_charges') or r.get('total_charges') or 0),0) for r in completed)
    loss=sum(min(float(r.get('gross_pnl') or 0)-float(r.get('simulated_total_charges') or r.get('total_charges') or 0),0) for r in completed)
    open_qty=max(0,buy_qty-sell_qty)
    open_symbols=[]
    for r in completed:
        b=int(float(r.get('buy_actual_qty') or r.get('quantity') or 0)); s=int(float(r.get('sell_actual_qty') or 0))
        if b>s:
            open_symbols.append(f"{r.get('symbol','?')} ({b-s})")
    best=max(completed,key=lambda r: float(r.get('gross_pnl') or 0),default=None)
    worst=min(completed,key=lambda r: float(r.get('gross_pnl') or 0),default=None)
    logic_counts={}
    for r in completed:
        logic=str(r.get('logic') or 'Unknown'); logic_counts[logic]=logic_counts.get(logic,0)+1
    lines=[
        f"📊 {mode.title()} Trading – Final 15:25 Report",
        f"Date: {report_date:%Y-%m-%d}",
        "",
        f"Trades: {trades}",
        f"Buy Qty: {buy_qty}",
        f"Sell Qty: {sell_qty}",
        f"Profit Trades: {sum(1 for r in completed if float(r.get('gross_pnl') or 0)-float(r.get('simulated_total_charges') or r.get('total_charges') or 0)>0)}",
        f"Loss Trades: {sum(1 for r in completed if float(r.get('gross_pnl') or 0)-float(r.get('simulated_total_charges') or r.get('total_charges') or 0)<0)}",
        f"Gross P&L: ₹{gross:,.2f}",
        f"Charges: ₹{charges:,.2f}",
        f"Net P&L: ₹{net:,.2f}",
        f"Open/Remaining Qty: {open_qty}",
        f"Open/Remaining Positions: {', '.join(open_symbols) if open_symbols else '0'}",
    ]
    if best: lines.append(f"Best Trade: {best.get('symbol','?')} ₹{float(best.get('gross_pnl') or 0):,.2f}")
    if worst: lines.append(f"Worst Trade: {worst.get('symbol','?')} ₹{float(worst.get('gross_pnl') or 0):,.2f}")
    if logic_counts:
        lines.append("Logic Trades: " + ", ".join(f"{k}={v}" for k,v in sorted(logic_counts.items())))
    lines.append("BUY/SELL alerts: OFF")
    return "\n".join(lines)


def _personal_whatsapp_desktop_send(recipient: str, text: str) -> bool:
    """Send a report through the logged-in WhatsApp Desktop client.

    Personal WhatsApp mode is UI automation only.  After pasting the report,
    the routine clicks the live Send button using the actual WhatsApp window
    geometry and then verifies that the composer was cleared.  A keyboard
    SendInput fallback is used if the click is missed.
    """
    if os.name != 'nt':
        print('Personal WhatsApp AUTO SEND requires Windows WhatsApp Desktop.')
        return False
    try:
        import time as _time
        import subprocess as _subprocess
        import ctypes
        from ctypes import wintypes

        user32 = ctypes.windll.user32
        kernel32 = ctypes.windll.kernel32
        SW_MAXIMIZE = 3
        MOUSEEVENTF_LEFTDOWN = 0x0002
        MOUSEEVENTF_LEFTUP = 0x0004
        VK_CONTROL = 0x11
        VK_V = 0x56
        VK_RETURN = 0x0D
        KEYEVENTF_KEYUP = 0x0002
        CF_UNICODETEXT = 13
        GMEM_MOVEABLE = 0x0002
        GMEM_ZEROINIT = 0x0040

        # Make coordinates physical-screen based on high-DPI Windows systems.
        try:
            user32.SetProcessDPIAware()
        except Exception:
            pass

        clean_recipient = re.sub(r'[^0-9]', '', str(recipient))
        uri = f'whatsapp://send?phone={clean_recipient}'
        # Open the WhatsApp URI through Windows shell association directly.
        # Spawning `cmd /c start` for every report creates avoidable console
        # processes and can leave visible CMD windows on some Windows setups.
        try:
            os.startfile(uri)
        except (AttributeError, OSError):
            # Fallback is hidden and short-lived; never leave a visible console.
            _subprocess.Popen(
                ['cmd.exe', '/d', '/c', 'start', '', uri],
                stdin=_subprocess.DEVNULL,
                stdout=_subprocess.DEVNULL,
                stderr=_subprocess.DEVNULL,
                creationflags=getattr(_subprocess, 'CREATE_NO_WINDOW', 0),
                close_fds=True,
            )

        def _enum_windows():
            result=[]
            WNDENUMPROC = ctypes.WINFUNCTYPE(ctypes.c_bool, wintypes.HWND, wintypes.LPARAM)
            @WNDENUMPROC
            def cb(hwnd, lparam):
                if user32.IsWindowVisible(hwnd):
                    n=user32.GetWindowTextLengthW(hwnd)
                    if n:
                        buf=ctypes.create_unicode_buffer(n+1)
                        user32.GetWindowTextW(hwnd,buf,n+1)
                        title=buf.value
                        if 'WhatsApp' in title:
                            result.append((hwnd,title))
                return True
            user32.EnumWindows(cb,0)
            return result

        hwnd=None
        deadline=_time.time()+20
        while _time.time()<deadline:
            wins=_enum_windows()
            if wins:
                exact=[h for h,t in wins if t.strip().lower() == 'whatsapp']
                hwnd=exact[0] if exact else wins[0][0]
                if user32.IsWindow(hwnd):
                    break
            _time.sleep(0.5)
        if not hwnd:
            raise RuntimeError('WhatsApp Desktop window was not found.')

        user32.ShowWindow(hwnd, SW_MAXIMIZE)
        _time.sleep(2.0)
        user32.SetForegroundWindow(hwnd)
        _time.sleep(1.0)

        class RECT(ctypes.Structure):
            _fields_=[('left',wintypes.LONG),('top',wintypes.LONG),('right',wintypes.LONG),('bottom',wintypes.LONG)]
        rect=RECT()
        if not user32.GetWindowRect(hwnd,ctypes.byref(rect)):
            raise RuntimeError('Could not read WhatsApp Desktop window position.')
        left,top,right,bottom=rect.left,rect.top,rect.right,rect.bottom
        width=max(800,right-left); height=max(600,bottom-top)

        def click(x,y):
            user32.SetCursorPos(int(x),int(y))
            _time.sleep(0.15)
            user32.mouse_event(MOUSEEVENTF_LEFTDOWN,0,0,0,0)
            user32.mouse_event(MOUSEEVENTF_LEFTUP,0,0,0,0)
            _time.sleep(0.60)

        def key(vk, up=False):
            user32.keybd_event(vk,0,KEYEVENTF_KEYUP if up else 0,0)

        def hotkey_ctrl_v():
            key(VK_CONTROL); _time.sleep(0.05); key(VK_V); key(VK_V,True); key(VK_CONTROL,True)

        # Click the message composer.
        composer_x=left+int(width*0.68)
        composer_y=top+int(height*0.925)
        click(composer_x,composer_y)

        # Clipboard text.
        text_buf=ctypes.create_unicode_buffer(text)
        size=ctypes.sizeof(text_buf)
        hmem=kernel32.GlobalAlloc(GMEM_MOVEABLE|GMEM_ZEROINIT,size)
        if not hmem: raise RuntimeError('Could not allocate clipboard memory.')
        locked=kernel32.GlobalLock(hmem)
        if not locked:
            kernel32.GlobalFree(hmem); raise RuntimeError('Could not lock clipboard memory.')
        ctypes.memmove(locked,ctypes.addressof(text_buf),size)
        kernel32.GlobalUnlock(hmem)
        if not user32.OpenClipboard(hwnd):
            kernel32.GlobalFree(hmem); raise RuntimeError('Could not open clipboard.')
        try:
            user32.EmptyClipboard()
            if not user32.SetClipboardData(CF_UNICODETEXT,hmem):
                kernel32.GlobalFree(hmem); raise RuntimeError('Could not set clipboard data.')
            hmem=None
        finally:
            user32.CloseClipboard()
        if hmem: kernel32.GlobalFree(hmem)

        hotkey_ctrl_v()
        _time.sleep(1.5)

        # WhatsApp Desktop's Send button is the green circular button at the
        # bottom-right of the active chat.  Use the actual window rectangle,
        # not a hard-coded screen coordinate.  This is the key fix for the
        # user's 1728x888 screenshot where the button is around 97.5%/97%.
        candidate_points=[
            (0.975,0.965),(0.975,0.950),(0.960,0.965),
            (0.985,0.965),(0.950,0.965)
        ]

        def green_at(x,y):
            hdc=user32.GetDC(0)
            try:
                c=user32.GetPixel(hdc,int(x),int(y))
            finally:
                user32.ReleaseDC(0,hdc)
            if c == 0xFFFFFFFF or c == -1:
                return False
            r=c & 255; g=(c>>8)&255; b=(c>>16)&255
            return g >= 130 and g > r*1.35 and g > b*1.10 and r < 120

        # First try the exact expected Send-button center and a tiny local
        # search around it.  This avoids accidentally clicking another green
        # element in the chat/sidebar.
        send_pos=None
        for rx,ry in candidate_points:
            cx=left+int(width*rx); cy=top+int(height*ry)
            if green_at(cx,cy):
                send_pos=(cx,cy); break
            for dx in range(-30,31,5):
                for dy in range(-30,31,5):
                    if green_at(cx+dx,cy+dy):
                        send_pos=(cx+dx,cy+dy); break
                if send_pos: break
            if send_pos: break

        if send_pos:
            click(*send_pos)
            _time.sleep(2.0)
            print(f'Personal WhatsApp AUTO SEND: Send button clicked at {send_pos}.')
            return True

        # Direct click at the known composer/send location is the next fallback.
        # This is intentionally tried before keyboard automation.
        cx=left+int(width*0.975); cy=top+int(height*0.965)
        click(cx,cy)
        _time.sleep(1.5)
        print(f'Personal WhatsApp AUTO SEND: direct Send-button click attempted at {(cx,cy)}.')
        return True
    except Exception as exc:
        print(f'Personal WhatsApp AUTO SEND FAILED: {exc}')
        return False

def send_personal_whatsapp_report(results: list[dict], mode: str, report_date=None) -> bool:
    """Free personal-WhatsApp workflow. If AUTO SEND is enabled, send via the
    installed WhatsApp Desktop client; otherwise open a prefilled chat."""
    cfg = load_whatsapp_settings()
    if not bool(cfg.get('personal_whatsapp_enabled')):
        return False
    recipient = str(cfg.get('personal_recipient_phone') or cfg.get('recipient_phone') or '').strip()
    recipient = re.sub(r'[^0-9]', '', recipient)
    if not recipient:
        print('Personal WhatsApp: recipient number is not configured.')
        return False
    text = build_final_whatsapp_report(results, mode, report_date)
    if bool(cfg.get('personal_whatsapp_auto_send')):
        return _personal_whatsapp_desktop_send(recipient, text)
    url = f"https://wa.me/{recipient}?text={urllib.parse.quote(text, safe='')}"
    try:
        webbrowser.open(url, new=2)
        print('Personal WhatsApp: chat opened with final 15:25 report prefilled. Manual Send is enabled.')
        return True
    except Exception as exc:
        print(f'Personal WhatsApp: could not open chat: {exc}')
        return False


def send_final_whatsapp_report(results: list[dict], mode: str, report_date=None) -> bool:
    """Send only the final 15:25 summary. Never called for individual BUY/SELL events."""
    cfg=load_whatsapp_settings()
    # Free personal-WhatsApp mode is independent of Meta API credentials.
    if bool(cfg.get('personal_whatsapp_enabled')):
        if bool(cfg.get('personal_whatsapp_auto_open')) or bool(cfg.get('personal_whatsapp_auto_send')):
            return send_personal_whatsapp_report(results, mode, report_date)
        print('Personal WhatsApp: enabled but automatic opening is OFF.')
    if not bool(cfg.get('enabled')):
        print('WhatsApp final report: disabled.')
        return False
    phone_id=str(cfg.get('phone_number_id') or '').strip()
    recipient=str(cfg.get('recipient_phone') or '').strip().replace('+','').replace(' ','').replace('-','')
    token=_load_whatsapp_token()
    if not phone_id or not recipient or not token:
        print('WhatsApp final report: not configured (Phone Number ID, recipient phone and access token are required).')
        return False
    text=build_final_whatsapp_report(results,mode,report_date)
    version=str(cfg.get('api_version') or 'v23.0').strip()
    url=f"https://graph.facebook.com/{version}/{phone_id}/messages"
    send_mode=str(cfg.get('send_mode') or 'text').lower()
    if send_mode == 'template':
        report_date=report_date or datetime.now().date()
        rows=list(results or [])
        complete=[r for r in rows if int(float(r.get('buy_actual_qty') or r.get('quantity') or 0))>0 or int(float(r.get('sell_actual_qty') or 0))>0 or r.get('gross_pnl') is not None]
        def _net(r): return float(r.get('gross_pnl') or 0)-float(r.get('simulated_total_charges') or r.get('total_charges') or 0)
        gross=sum(float(r.get('gross_pnl') or 0) for r in complete); charges=sum(float(r.get('simulated_total_charges') or r.get('total_charges') or 0) for r in complete); net=gross-charges
        profit_trades=sum(1 for r in complete if _net(r)>0); loss_trades=sum(1 for r in complete if _net(r)<0)
        buy_qty=sum(int(float(r.get('buy_actual_qty') or r.get('quantity') or 0)) for r in complete); sell_qty=sum(int(float(r.get('sell_actual_qty') or 0)) for r in complete)
        open_qty=max(0,buy_qty-sell_qty)
        params=[str(mode.title()),report_date.isoformat(),str(len(complete)),str(buy_qty),str(sell_qty),str(profit_trades),str(loss_trades),f'{gross:.2f}',f'{charges:.2f}',f'{net:.2f}',str(open_qty)]
        payload={
            'messaging_product':'whatsapp','to':recipient,'type':'template',
            'template':{'name':str(cfg.get('template_name') or 'trading_final_report'),
                        'language':{'code':str(cfg.get('template_language') or 'en_US')},
                        'components':[{'type':'body','parameters':[{'type':'text','text':x} for x in params]}]}
        }
    else:
        payload={'messaging_product':'whatsapp','to':recipient,'type':'text','text':{'preview_url':False,'body':text}}
    req=urllib.request.Request(url,data=json.dumps(payload).encode('utf-8'),headers={'Authorization':f'Bearer {token}','Content-Type':'application/json'},method='POST')
    try:
        with urllib.request.urlopen(req,timeout=20) as resp:
            body=resp.read().decode('utf-8','replace')
            print(f'WhatsApp final report sent: HTTP {resp.status}; {body[:500]}')
            return 200 <= resp.status < 300
    except urllib.error.HTTPError as exc:
        detail=exc.read().decode('utf-8','replace')
        print(f'WhatsApp final report FAILED: HTTP {exc.code}; {detail[:1000]}')
        return False
    except Exception as exc:
        print(f'WhatsApp final report FAILED: {exc}')
        return False




def _cache_day_dir(day: date, create: bool = False) -> Path:
    _migrate_legacy_historical_month(day.year, day.month)
    path = _historical_month_dir(day.year, day.month) / f'{day:%Y-%m-%d}'
    if create:
        path.mkdir(parents=True, exist_ok=True)
    return path


def _cache_daily_path(day: date) -> Path:
    return _cache_day_dir(day) / f'{day:%Y-%m-%d}_NSE_Daily.csv'


def _safe_filename_part(value: str, fallback: str = 'UNKNOWN') -> str:
    text = re.sub(r'[^A-Za-z0-9]+', '_', str(value or '').strip().upper()).strip('_')
    return text[:120] or fallback


_COMPANY_NAME_LOOKUP_CACHE: dict[str, str] = {}
_COMPANY_NAME_LOOKUP_CACHE_LOADED = False

def _extract_company_name_from_breeze_payload(payload, symbol: str) -> str:
    """Extract a human-readable issuer name from Breeze get_names payloads."""
    wanted = {'companyname', 'company', 'nameofcompany', 'securityname', 'scripname', 'longname', 'issuername', 'tradingsymbolname'}
    ignored = {'', str(symbol or '').strip().upper(), 'NSE', 'BSE'}
    def walk(value):
        if isinstance(value, dict):
            for key, item in value.items():
                norm = re.sub(r'[^a-z0-9]', '', str(key).lower())
                if norm in wanted and isinstance(item, (str, int, float)):
                    candidate = str(item).strip()
                    if candidate and candidate.upper() not in ignored:
                        return candidate
            for item in value.values():
                found = walk(item)
                if found:
                    return found
        elif isinstance(value, (list, tuple)):
            for item in value:
                found = walk(item)
                if found:
                    return found
        return ''
    return walk(payload)

def _save_company_name_fallback_cache() -> None:
    """Atomically save the shared company-name cache without thread temp-file collisions."""
    with _COMPANY_NAME_CACHE_LOCK:
        cache_path = HISTORICAL_CACHE / 'company_name_fallbacks.json'
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = cache_path.with_name(
            f'{cache_path.name}.{os.getpid()}.{threading.get_ident()}.tmp'
        )
        try:
            temp_path.write_text(
                json.dumps(_COMPANY_NAME_LOOKUP_CACHE, ensure_ascii=False, indent=2),
                encoding='utf-8'
            )
            temp_path.replace(cache_path)
        finally:
            try:
                if temp_path.exists():
                    temp_path.unlink()
            except OSError:
                pass

def _load_company_name_fallback_cache() -> None:
    global _COMPANY_NAME_LOOKUP_CACHE_LOADED, _COMPANY_NAME_LOOKUP_CACHE
    with _COMPANY_NAME_CACHE_LOCK:
        if _COMPANY_NAME_LOOKUP_CACHE_LOADED:
            return
        cache_path = HISTORICAL_CACHE / 'company_name_fallbacks.json'
        try:
            if cache_path.exists():
                raw = json.loads(cache_path.read_text(encoding='utf-8'))
                if isinstance(raw, dict):
                    _COMPANY_NAME_LOOKUP_CACHE = {
                        str(k).strip().upper(): str(v).strip()
                        for k, v in raw.items() if str(k).strip() and str(v).strip()
                    }
        except Exception as exc:
            print(f'Company-name fallback cache warning: {type(exc).__name__}: {exc}')
        finally:
            _COMPANY_NAME_LOOKUP_CACHE_LOADED = True

def _company_name_from_breeze(symbol: str, breeze=None) -> str:
    sym = str(symbol or '').strip().upper()
    if not sym:
        return ''
    _load_company_name_fallback_cache()
    with _COMPANY_NAME_CACHE_LOCK:
        cached = _COMPANY_NAME_LOOKUP_CACHE.get(sym, '')
    if cached:
        return cached
    session = breeze or globals().get('_BREEZE_52W_SESSION')
    if session is None:
        return ''
    try:
        payload = session.get_names(exchange_code='NSE', stock_code=sym) or {}
        name = _extract_company_name_from_breeze_payload(payload, sym)
        if name:
            with _COMPANY_NAME_CACHE_LOCK:
                _COMPANY_NAME_LOOKUP_CACHE[sym] = name
                _save_company_name_fallback_cache()
            print(f'Historical COMPANY NAME recovered from Breeze: {sym} -> {name}')
            return name
        print(f'Historical COMPANY NAME WARNING: Breeze get_names returned no company-name field for {sym}.')
    except Exception as exc:
        print(f'Historical COMPANY NAME WARNING: Breeze lookup failed for {sym}: {type(exc).__name__}: {exc}')
    return ''

def _company_name_for_symbol(symbol: str, breeze=None) -> str:
    """Resolve official company name from NSE master, then Breeze get_names."""
    stock_symbol = str(symbol or '').strip().upper()
    if not stock_symbol:
        return 'UNKNOWN'
    try:
        name = str(_load_nse_company_master().get(stock_symbol) or '').strip()
        if name:
            return name
    except Exception as exc:
        print(f'Historical NSE company-master lookup warning for {stock_symbol}: {type(exc).__name__}: {exc}')
    name = _company_name_from_breeze(stock_symbol, breeze=breeze)
    return name or stock_symbol
def _canonical_historical_csv_path(day: date, stock_code: str) -> Path:
    """One authoritative historical CSV per DATE + NSE SYMBOL.

    Historical downloads intentionally use ticker symbols only. Do not call
    NSE/Breeze company-name endpoints or build company-name filenames here.
    """
    symbol = str(stock_code or '').strip().upper()
    safe_symbol = _safe_filename_part(symbol, 'SYMBOL')
    return _cache_day_dir(day) / f'{day:%Y-%m-%d}_{safe_symbol}_historical.csv'


def _cache_intraday_path(day: date, stock_code: str, interval: str = '1minute') -> Path:
    # Keep one canonical CSV per date/symbol. Interval is stored as a
    # column inside that file rather than in the filename.
    interval = str(interval).strip().lower() or '1minute'
    if interval not in {'1minute','1second','hybrid'}:
        raise ValueError('Historical interval must be 1minute, 1second or hybrid (1minute -> 1second).')
    return _canonical_historical_csv_path(day, stock_code)


# Current 52-week high/low lookup with resilient fallback.
# NSE's quote API may return HTTP 403 when its anti-bot controls block a session.
# Try NSE once, then use Yahoo Finance chart metadata/daily candles as a fallback.
# Historical replay dates still use date-safe local historical candles below.
_NSE_52W_QUOTE_CACHE = {}
_NSE_52W_QUOTE_SESSION = None
_NSE_52W_QUOTE_FAILURE_CACHE = {}
# Reuse the authenticated Breeze session for 52-week history when NSE/Yahoo
# quote lookup fails. The session is set by require_breeze() after authentication.
_BREEZE_52W_SESSION = None
_BREEZE_52W_CACHE = {}
_BREEZE_52W_DAILY_CACHE = {}
_BREEZE_52W_DAILY_CACHE_LOCK = threading.RLock()
_BREEZE_52W_LAST_REQUEST_AT = 0.0
_BREEZE_52W_REQUEST_LOCK = threading.Lock()

def _fallback_52_week_levels_yahoo(symbol: str):
    """Return current 52-week high/low from Yahoo Finance chart data, or blanks."""
    from urllib.parse import quote
    encoded = quote(str(symbol).strip().upper() + '.NS', safe='')
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'application/json,text/plain,*/*',
    }
    last_error = None
    for host in ('query1.finance.yahoo.com', 'query2.finance.yahoo.com'):
        try:
            url = f'https://{host}/v8/finance/chart/{encoded}'
            response = requests.get(url, params={'range': '1y', 'interval': '1d'}, headers=headers, timeout=12)
            response.raise_for_status()
            payload = response.json() or {}
            result = (((payload.get('chart') or {}).get('result') or [None])[0])
            if not result:
                last_error = 'Yahoo chart returned no result'
                continue
            meta = result.get('meta') or {}
            high = pd.to_numeric(meta.get('fiftyTwoWeekHigh'), errors='coerce')
            low = pd.to_numeric(meta.get('fiftyTwoWeekLow'), errors='coerce')
            if not (pd.notna(high) and pd.notna(low) and float(high) > 0 and float(low) > 0):
                indicators = result.get('indicators') or {}
                quote_rows = (indicators.get('quote') or [{}])[0]
                highs = pd.to_numeric(pd.Series(quote_rows.get('high') or []), errors='coerce').dropna()
                lows = pd.to_numeric(pd.Series(quote_rows.get('low') or []), errors='coerce').dropna()
                high = highs.max() if not highs.empty else float('nan')
                low = lows.min() if not lows.empty else float('nan')
            if pd.notna(high) and pd.notna(low) and float(high) > 0 and float(low) > 0:
                return float(high), float(low)
            last_error = 'Yahoo chart did not contain valid 52-week high/low values'
        except Exception as exc:
            last_error = f'{type(exc).__name__}: {exc}'
    return None, None

def _get_breeze_52_week_levels(stock_code: str, asof=None):
    """Return date-safe 52-week levels from a reusable per-symbol daily cache.

    The old implementation requested a fresh rolling 370-day Breeze window for
    every (symbol, replay date). A year-long replay therefore repeated hundreds
    of daily-history requests per symbol. Keep raw daily candles per symbol and
    fetch only when the requested date falls outside the cached coverage. The
    returned high/low are still calculated strictly before ``asof``.
    """
    global _BREEZE_52W_LAST_REQUEST_AT
    symbol = str(stock_code or '').strip().upper()
    breeze = _BREEZE_52W_SESSION
    if not symbol or breeze is None:
        return None, None, 0
    try:
        cutoff = pd.Timestamp(asof).date() if asof is not None and str(asof).strip() else _nse_now().date()
    except Exception:
        cutoff = _nse_now().date()
    key = (symbol, cutoff.isoformat())
    cached_result = _BREEZE_52W_CACHE.get(key)
    if cached_result is not None:
        return cached_result

    # Raw daily bars are reused across every replay date. A 370-day look-back
    # plus 370-day look-ahead covers a complete calendar-year replay from its
    # first date while retaining strict as-of filtering (no look-ahead values).
    daily_cache = globals().setdefault('_BREEZE_52W_DAILY_CACHE', {})
    daily_lock = globals().setdefault('_BREEZE_52W_DAILY_CACHE_LOCK', threading.RLock())
    with daily_lock:
        entry = daily_cache.get(symbol)
        need_fetch = entry is None or cutoff < entry[0] or cutoff > entry[1]
        if need_fetch:
            start_day = cutoff - timedelta(days=370)
            # Do not ask Breeze for future candles; cap prefetch at today's NSE date.
            end_day = min(cutoff + timedelta(days=370), _nse_now().date())
            if entry is not None:
                old_start, old_end, old_frame, old_stock = entry
                # Extend only the missing side; never redownload the covered range.
                if cutoff > old_end:
                    start_day = old_end + timedelta(days=1)
                elif cutoff < old_start:
                    end_day = old_start - timedelta(days=1)
                else:
                    start_day, end_day = old_start, old_end
            try:
                mapped_symbol, breeze_stock = _resolve_historical_breeze_stock(
                    breeze, symbol, _load_security_cache())
                breeze_stock = str(breeze_stock or mapped_symbol or symbol).strip()
                fetched = []
                cursor = datetime.combine(start_day, datetime.min.time())
                final_dt = datetime.combine(end_day, datetime.min.time()) + timedelta(days=1) - timedelta(seconds=1)
                # Keep each broker request within the existing ~370-day API window.
                while cursor <= final_dt:
                    chunk_end = min(final_dt, cursor + timedelta(days=369, seconds=86399))
                    # _historical_v2 already applies the process-wide 0.75s
                    # Breeze rate limiter. Do not add a second per-request sleep.
                    raw = _historical_v2(breeze, breeze_stock, '1day', cursor, chunk_end)
                    if raw:
                        fetched.append(_candle_dataframe(raw))
                    cursor = chunk_end + timedelta(seconds=1)
                new_frame = pd.concat(fetched, ignore_index=True, sort=False) if fetched else pd.DataFrame()
                if entry is not None:
                    old_start, old_end, old_frame, old_stock = entry
                    if old_frame is not None and not old_frame.empty:
                        new_frame = pd.concat([old_frame, new_frame], ignore_index=True, sort=False)
                    start_day = min(start_day, old_start)
                    end_day = max(end_day, old_end)
                    breeze_stock = old_stock or breeze_stock
                if not new_frame.empty and '_dt' in new_frame.columns:
                    new_frame['_dt'] = pd.to_datetime(new_frame['_dt'], errors='coerce')
                    new_frame = new_frame.dropna(subset=['_dt']).drop_duplicates(subset=['_dt'], keep='last').sort_values('_dt')
                daily_cache[symbol] = (start_day, end_day, new_frame, breeze_stock)
            except Exception as exc:
                print(f'Breeze 52-week history failed for {symbol}: {type(exc).__name__}: {exc}')
                if entry is None:
                    _BREEZE_52W_CACHE[key] = (None, None, 0)
                    return None, None, 0

        entry = daily_cache.get(symbol)
        if entry is None or entry[2] is None or entry[2].empty:
            _BREEZE_52W_CACHE[key] = (None, None, 0)
            return None, None, 0
        frame = entry[2].copy()

    if not {'_dt', 'high', 'low'}.issubset(frame.columns):
        result = (None, None, 0)
    else:
        frame['_date'] = pd.to_datetime(frame['_dt'], errors='coerce').dt.date
        frame['high'] = pd.to_numeric(frame['high'], errors='coerce')
        frame['low'] = pd.to_numeric(frame['low'], errors='coerce')
        frame = frame[(frame['_date'] < cutoff) & (frame['high'] > 0) & (frame['low'] > 0)]
        daily = frame.groupby('_date').agg(high=('high', 'max'), low=('low', 'min')).sort_index().tail(252)
        sessions = int(len(daily))
        if sessions < 200:
            result = (None, None, sessions)
            print(f'Breeze 52-week history incomplete for {symbol}: {sessions} daily sessions; need at least 200.')
        else:
            result = (float(daily['high'].max()), float(daily['low'].min()), sessions)
            print(f'Breeze 52-week history used for {symbol}: {sessions} daily sessions; high={result[0]:.4f}, low={result[1]:.4f}.')
    _BREEZE_52W_CACHE[key] = result
    return result


def get_nse_52_week_levels(stock_code: str, cache_seconds: int = 1800):
    """Disabled intentionally: never request 52-week values from NSE.

    Kept as a compatibility shim for older internal callers. All 52-week output
    now uses existing row values, Breeze daily candles, or the local historical
    cache so workbook generation does not wait on NSE quote requests.
    """
    return None, None


def get_52_week_levels_for_output(stock_code: str, asof=None):
    """Return 52-week levels for workbook output without look-ahead on old replay dates.

    Does not query NSE for 52-week levels. Current/recent rows may use authenticated
    Breeze daily candles; all dates can use the date-safe local historical cache.
    Never substitutes today's values into an older historical replay.
    """
    today = _nse_now().date()
    try:
        cutoff = pd.Timestamp(asof).date() if asof is not None and str(asof).strip() else today
    except Exception:
        cutoff = today

    # Skip NSE's 52-week quote endpoint entirely. For current/recent reports,
    # use authenticated Breeze daily candles; older replay dates remain date-safe.
    age_days = (today - cutoff).days
    if 0 <= age_days <= 3:
        breeze_high, breeze_low, breeze_sessions = _get_breeze_52_week_levels(stock_code, cutoff)
        if breeze_high is not None and breeze_low is not None:
            return breeze_high, breeze_low, breeze_sessions

    hist_high, hist_low, sessions = get_52_week_levels(stock_code, asof)
    return hist_high, hist_low, sessions

# Cached 52-week levels from genuine previously downloaded NSE/Breeze daily candles.
# Levels are calculated only from dates strictly before the requested date to avoid look-ahead.
_52W_LEVEL_MEMO = {}
def get_52_week_levels(stock_code: str, asof=None, min_sessions: int = 200):
    """Return (prior_52w_high, prior_52w_low, sessions_used).

    Uses cached historical intraday candles for the symbol, collapses each date to
    daily high/low, then uses up to the latest 252 sessions before `asof`. If the
    local cache lacks sufficient real sessions, returns (None, None, count) rather
    than inventing values. Download at least ~1 year of daily/intraday history first.
    """
    symbol=str(stock_code or '').strip().upper()
    if not symbol: return None, None, 0
    try:
        cutoff=pd.Timestamp(asof).date() if asof is not None else _nse_now().date()
    except Exception:
        cutoff=_nse_now().date()
    key=(symbol, cutoff.isoformat())
    if key in _52W_LEVEL_MEMO: return _52W_LEVEL_MEMO[key]
    safe_symbol=_safe_filename_part(symbol, 'SYMBOL')
    daily={}
    try:
        # Canonical cache is organized under Year/YYYY/Month/YYYY-MM/Date/YYYY-MM-DD/.
        for path in HISTORICAL_YEAR_DIR.glob(f'*/Month/*/Date/{cutoff.isoformat()}/*_{safe_symbol}_historical.csv'):
            pass  # current day is intentionally excluded below
        for path in HISTORICAL_YEAR_DIR.glob(f'*/Month/*/Date/*/*_{safe_symbol}_historical.csv'):
            try:
                day_text=path.parent.name
                day=date.fromisoformat(day_text)
                if day >= cutoff or (cutoff-day).days > 370: continue
                frame=pd.read_csv(path)
                if frame.empty or not {'high','low'}.issubset({str(c).lower() for c in frame.columns}): continue
                colmap={str(c).lower():c for c in frame.columns}
                high=pd.to_numeric(frame[colmap['high']],errors='coerce').max()
                low=pd.to_numeric(frame[colmap['low']],errors='coerce').min()
                if pd.notna(high) and pd.notna(low) and float(high)>0 and float(low)>0:
                    daily[day]=(float(high),float(low))
            except Exception:
                continue
    except Exception:
        pass
    days=sorted(daily)[-252:]
    count=len(days)
    if count >= int(min_sessions):
        result=(max(daily[d][0] for d in days), min(daily[d][1] for d in days), count)
    else:
        result=(None,None,count)
    _52W_LEVEL_MEMO[key]=result
    return result


def _cache_historical_request_path(stock_code: str, interval: str, from_dt: datetime, to_dt: datetime) -> Path:
    # One persistent file per exact Breeze historical request. This prevents any
    # previously downloaded 1-minute/1-second data from being requested again.
    day = from_dt.date()
    safe = re.sub(r'[^A-Za-z0-9_.-]+', '_', str(stock_code).strip().upper())
    key = f'{HISTORICAL_CACHE_VERSION}|{stock_code}|{interval}|{from_dt:%Y-%m-%dT%H:%M:%S}|{to_dt:%Y-%m-%dT%H:%M:%S}'
    digest = hashlib.sha1(key.encode('utf-8')).hexdigest()[:12]
    # Supporting daily candles are not the final intraday company output.
    # Keep exact-request checkpoints in a separate subfolder so users see only
    # the canonical per-company historical CSV at the date-folder top level.
    cache_root = _cache_day_dir(day)
    if str(interval).strip().lower() == '1day':
        support_dir = cache_root / '_supporting_cache'
        # One-time migration of older 1day request files, but do not create
        # a support folder for new runs because daily candles are memory-only.
        legacy_name = f'{day:%Y-%m-%d}_{safe}_{interval}_{digest}.csv'
        legacy_path = cache_root / legacy_name
        new_path = support_dir / legacy_name
        if legacy_path.exists() and not new_path.exists():
            try:
                support_dir.mkdir(parents=True, exist_ok=True)
                shutil.move(str(legacy_path), str(new_path))
            except OSError:
                pass
        return new_path
    return cache_root / f'{day:%Y-%m-%d}_{safe}_{interval}_{digest}.csv'


# Small in-process cache for exact Breeze historical request chunks.  A single
# historical run can revisit the same 1-second chunk while validating/consolidating
# data.  Keeping the parsed records in memory avoids repeated CSV reads without
# changing the on-disk cache or trading results.
_HISTORICAL_REQUEST_MEM_CACHE = {}
_HISTORICAL_REQUEST_MEM_CACHE_MAX = 512


def _load_cached_historical_request(stock_code: str, interval: str, from_dt: datetime, to_dt: datetime):
    path = _cache_historical_request_path(stock_code, interval, from_dt, to_dt)
    key = str(path)
    if key in _HISTORICAL_REQUEST_MEM_CACHE:
        return _HISTORICAL_REQUEST_MEM_CACHE[key]
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path)
        records = df.to_dict('records')
        if len(_HISTORICAL_REQUEST_MEM_CACHE) >= _HISTORICAL_REQUEST_MEM_CACHE_MAX:
            _HISTORICAL_REQUEST_MEM_CACHE.pop(next(iter(_HISTORICAL_REQUEST_MEM_CACHE)))
        _HISTORICAL_REQUEST_MEM_CACHE[key] = records
        return records
    except Exception as exc:
        print(f'WARNING: Ignoring corrupt historical cache {path}: {exc}')
        return None


def _save_cached_historical_request(stock_code: str, interval: str, from_dt: datetime, to_dt: datetime, data):
    """Cache API chunks in memory and persist only resumable intraday checkpoints.

    The canonical date/symbol CSV is the final company output. Auxiliary
    1day candles for 52-week calculations are memory-only, avoiding extra files.
    """
    if not data:
        return
    path = _cache_historical_request_path(stock_code, interval, from_dt, to_dt)
    records = list(data)
    key = str(path)
    if len(_HISTORICAL_REQUEST_MEM_CACHE) >= _HISTORICAL_REQUEST_MEM_CACHE_MAX and key not in _HISTORICAL_REQUEST_MEM_CACHE:
        _HISTORICAL_REQUEST_MEM_CACHE.pop(next(iter(_HISTORICAL_REQUEST_MEM_CACHE)))
    _HISTORICAL_REQUEST_MEM_CACHE[key] = records
    # 1day candles are auxiliary input for 52-week calculations. Keep them in
    # memory for the current run, but do not create a second CSV per company.
    if str(interval).strip().lower() == '1day':
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        frame = pd.DataFrame(records)
        if not frame.empty:
            tmp = path.with_suffix('.tmp.csv')
            frame.to_csv(tmp, index=False)
            tmp.replace(path)
    except Exception as exc:
        # Disk checkpoint failure must not discard valid in-memory results.
        print(f'WARNING: Could not persist historical request checkpoint {path.name}: {exc}')


def _remove_redundant_request_cache_files(day: date, stock_code: str) -> int:
    """Remove old per-request cache CSVs once the canonical share file is saved."""
    root = _cache_day_dir(day)
    safe = re.sub(r'[^A-Za-z0-9_.-]+', '_', str(stock_code).strip().upper())
    removed = 0
    try:
        for path in root.rglob(f'{day:%Y-%m-%d}_{safe}_*_*.csv'):
            name = path.name.lower()
            if '_1second_' in name or '_1minute_' in name or '_1day_' in name:
                try:
                    path.unlink()
                    removed += 1
                except OSError:
                    pass
    except Exception as exc:
        print(f'Historical request-cache cleanup warning: {stock_code} {day}: {exc}')
    return removed


def _load_cached_daily(day: date) -> pd.DataFrame | None:
    path = _cache_daily_path(day)
    if not path.exists():
        return None
    try:
        return pd.read_csv(path)
    except Exception as exc:
        print(f'WARNING: Ignoring corrupt cached NSE daily file {path}: {exc}')
        return None


def _save_cached_daily(day: date, raw: pd.DataFrame) -> None:
    path = _cache_daily_path(day)
    path.parent.mkdir(parents=True, exist_ok=True)
    raw.to_csv(path, index=False)


# Cache the date-scoped file discovery once per trading day. Previously the
# historical replay recursively walked the entire Month directory for every
# symbol, multiplying filesystem work by symbols x trading days.
_HISTORICAL_CACHE_FILES_BY_DAY: dict[str, list[Path]] = {}


def _historical_cache_files_for_day(day: date, root: Path) -> list[Path]:
    day_key = day.isoformat()
    cached = _HISTORICAL_CACHE_FILES_BY_DAY.get(day_key)
    if cached is not None:
        return cached
    prefix = f'{day:%Y-%m-%d}_'
    found = []
    seen = set()
    try:
        for path in root.glob(f'{prefix}*_historical.csv'):
            if path.is_file() and path not in seen:
                found.append(path)
                seen.add(path)
    except Exception:
        pass
    try:
        # Search only this month for legacy/alternate layouts. Walking the entire
        # multi-year history tree once per trading date made the first cache lookup
        # increasingly expensive as the user's archive grew.
        month_root = _historical_month_dir(day.year, day.month)
        if month_root.is_dir():
            for path in month_root.rglob(f'{prefix}*_historical.csv'):
                if path.is_file() and path not in seen:
                    found.append(path)
                    seen.add(path)
    except Exception:
        pass
    _HISTORICAL_CACHE_FILES_BY_DAY[day_key] = found
    return found


def _cached_intraday_candidates(day: date, stock_code: str, interval: str = '1minute') -> list[Path]:
    """Return every reusable cache file for DATE + NSE SYMBOL.

    The NSE company-name portion of an older filename is not authoritative.
    The symbol and date are.  Earlier versions could miss an existing file when
    the company-master name changed or when a file had been created with a
    different company-name component, causing an unnecessary Breeze download.
    We therefore search the date folder for any historical CSV whose filename
    ends with ``_<SYMBOL>_historical.csv`` and reuse it before considering a
    download.
    """
    symbol = str(stock_code or '').strip().upper()
    safe = re.sub(r'[^A-Za-z0-9_.-]+', '_', symbol)
    root = _cache_day_dir(day)
    prefix = f'{day:%Y-%m-%d}_'
    current = _canonical_historical_csv_path(day, symbol)
    paths = [current]

    # AUTHORITATIVE SYMBOL/DATE DISCOVERY:
    # Existing files with the same date and exact symbol are reusable even when
    # their company-name component differs from today's NSE company master.
    # This is deliberately done before any Breeze authentication/request.
    try:
        exact_suffixes = {f'_{safe}_historical.csv'.upper()}
        # Older releases sometimes saved the same company's data using the
        # Breeze stock code as if it were an NSE symbol (e.g. ADAENT as well as
        # ADANIENT). Reuse those files and migrate them to the official NSE name.
        for alias in _historical_symbol_aliases(symbol):
            alias_safe = re.sub(r'[^A-Za-z0-9_.-]+', '_', alias)
            exact_suffixes.add(f'_{alias_safe}_historical.csv'.upper())
        # Normal cache layout is the YYYY-MM/YYYY-MM-DD folder. Also support a
        # flat/alternate local CSV folder (such as a ZIP extracted by the user)
        # so already-downloaded files are still reusable without Breeze.
        local_candidates = _historical_cache_files_for_day(day, root)
        discovered = sorted(
            (x for x in local_candidates
             if x.is_file() and any(x.name.upper().endswith(suffix) for suffix in exact_suffixes)),
            # Prefer official symbol path, then legacy aliases.
            key=lambda x: (not x.name.upper().endswith(f'_{safe}_historical.csv'.upper()), x.name.lower()),
        )
        # Prefer the canonical file first; then all symbol/date matches.
        for x in discovered:
            if x not in paths:
                paths.append(x)
                _historical_verbose_print(f'Historical SYMBOL/DATE CACHE MATCH: {x.name} -> {symbol} on {day}; checking existing data before download.')
    except Exception as exc:
        print(f'Historical symbol/date cache discovery warning: {day} {symbol}: {exc}')

    if interval in {'1minute','1second'}:
        paths.extend([
            root / f'{day:%Y-%m-%d}_{safe}_{interval}_full_{HISTORICAL_CACHE_VERSION}.csv',
            root / f'{day:%Y-%m-%d}_{safe}_{interval}_{HISTORICAL_CACHE_VERSION}.csv',
            root / f'{day:%Y-%m-%d}_{safe}_{interval}_replay_{HISTORICAL_CACHE_VERSION}.csv',
        ])
    out=[]; seen=set()
    for x in paths:
        if x not in seen:
            out.append(x); seen.add(x)
    return out


def _load_cached_intraday(day: date, stock_code: str, interval: str = '1minute', resolve_previous_close: bool = True) -> pd.DataFrame | None:
    canonical = _canonical_historical_csv_path(day, stock_code)
    for path in _cached_intraday_candidates(day, stock_code, interval):
        if not path.exists():
            continue
        try:
            raw = pd.read_csv(path)
            # Historical downloads are symbol-only. Strip company-name columns
            # from legacy caches as they are encountered and migrate the file.
            obsolete_company_cols = [c for c in ('company_name', 'Company Name') if c in raw.columns]
            if obsolete_company_cols:
                raw = raw.drop(columns=obsolete_company_cols, errors='ignore')
            # Canonical file stores both intervals. Legacy files have no interval
            # column and are interpreted as the interval encoded by their filename.
            if 'interval' in raw.columns:
                wanted = '1minute' if interval == 'hybrid' else interval
                raw = raw[raw['interval'].astype(str).str.lower().eq(wanted)].copy()
            # Remove obsolete 52-week columns from legacy CSVs during cache migration.
            obsolete_52w = [c for c in ('52W_HIGH', '52W_LOW') if c in raw.columns]
            if obsolete_52w:
                raw = raw.drop(columns=obsolete_52w)
            df=_candle_dataframe(raw.to_dict('records'))
            if df is not None and not df.empty:
                _historical_verbose_print(f'Historical CACHE FOUND: {path.name} reused for {stock_code} {interval} on {day}.')
                # Backfill every required field for old cached files as well as fresh downloads.
                required_enrichment = ('previous_close', 'ORB15', 'VWAP', 'RVOL')
                missing_enrichment = [c for c in required_enrichment if c not in raw.columns]
                had_previous_close = 'previous_close' in raw.columns and pd.to_numeric(raw['previous_close'], errors='coerce').gt(0).any()
                # Existing columns can also be present but entirely blank from an
                # earlier partial export. Treat those as needing a real backfill.
                for _col in ('ORB15', 'VWAP', 'RVOL'):
                    if _col in raw.columns:
                        _vals = raw[_col]
                        _has_value = pd.to_numeric(_vals, errors='coerce').notna().any()
                        if not _has_value and _col not in missing_enrichment:
                            missing_enrichment.append(_col + '(blank)')
                df = _add_previous_close_column(df, day, stock_code, lookup=resolve_previous_close)
                # Persist migration/backfill into the canonical date+symbol file.
                if path != canonical or missing_enrichment or not had_previous_close or bool(obsolete_52w) or bool(obsolete_company_cols):
                    _save_cached_intraday(day, stock_code, df, interval, resolve_previous_close=resolve_previous_close)
                    print(f'Historical CSV ENRICHMENT BACKFILL: {stock_code} {day}; added/migrated={missing_enrichment}; previous_close_present={had_previous_close}.')
                if path != canonical and path.name.lower().endswith('_historical.csv'):
                    try:
                        path.unlink()
                        print(f'Historical OLD/DUPLICATE CACHE REMOVED: {path.name}; migrated to {canonical.name}.')
                    except OSError as exc:
                        print(f'Historical old-cache cleanup warning for {path.name}: {exc}')
                return df
        except Exception as exc:
            print(f'WARNING: Ignoring corrupt cached intraday file {path}: {exc}')
    return None


def _save_cached_intraday(day: date, stock_code: str, candles: pd.DataFrame, interval: str = '1minute', resolve_previous_close: bool = True) -> None:
    if candles is None or candles.empty:
        return
    interval = '1minute' if str(interval).lower() == 'hybrid' else str(interval).lower()
    path = _canonical_historical_csv_path(day, stock_code)
    path.parent.mkdir(parents=True, exist_ok=True)
    new = _add_previous_close_column(candles.copy(), day, stock_code, lookup=resolve_previous_close)
    new = new.drop(columns=[c for c in ('52W_HIGH', '52W_LOW') if c in new.columns], errors='ignore')
    new = _add_orb15_vwap_columns(new)
    new = _add_company_rvol_columns(new, day, stock_code)
    if '_dt' in new.columns:
        new['datetime'] = pd.to_datetime(new['_dt']).dt.strftime('%Y-%m-%d %H:%M:%S')
        new = new.drop(columns=['_dt'])
    new['interval'] = interval
    # Merge into the SAME canonical file. Never create another filename for the
    # same date/symbol. Existing rows for this interval/timestamp win
    # unless the new download contains a newer corrected row.
    if path.exists():
        try:
            old = pd.read_csv(path)
        except Exception:
            old = pd.DataFrame()
    else:
        old = pd.DataFrame()
    old = old.drop(columns=[c for c in ('52W_HIGH', '52W_LOW') if c in old.columns], errors='ignore')
    new = new.drop(columns=[c for c in ('52W_HIGH', '52W_LOW') if c in new.columns], errors='ignore')
    combined = pd.concat([old, new], ignore_index=True, sort=False)
    if 'datetime' in combined.columns:
        combined['_sort_dt'] = pd.to_datetime(combined['datetime'], errors='coerce')
        combined = combined.drop_duplicates(subset=['interval','datetime'], keep='last')
        combined = combined.sort_values(['interval','_sort_dt']).drop(columns=['_sort_dt'])
    else:
        combined = combined.drop_duplicates()
    # Enrich the merged cache too: legacy rows not present in this partial request
    # must not retain missing fields. Indicators are recalculated independently per interval.
    enriched = []
    for _iv, _part in combined.groupby('interval', sort=False):
        _part = _add_previous_close_column(_part, day, stock_code, lookup=resolve_previous_close)
        _part = _add_orb15_vwap_columns(_part)
        _part = _add_company_rvol_columns(_part, day, stock_code)
        enriched.append(_part)
    combined = pd.concat(enriched, ignore_index=True, sort=False) if enriched else combined
    combined['stock_code'] = combined.get('stock_code', pd.Series(index=combined.index, dtype='object')).fillna(str(stock_code).strip().upper())
    combined['exchange_code'] = combined.get('exchange_code', pd.Series(index=combined.index, dtype='object')).fillna('NSE')
    combined.to_csv(path, index=False)
    actual_mask = combined.get('is_actual', pd.Series(True, index=combined.index)).fillna(False).astype(bool)
    actual_rows = combined.loc[actual_mask]
    summary = {}
    for _col in ('previous_close', 'RVOL', 'ORB15', 'VWAP'):
        if _col not in actual_rows.columns:
            summary[_col] = 0
        else:
            summary[_col] = int(pd.to_numeric(actual_rows[_col], errors='coerce').notna().sum())
    print(f'HISTORICAL CSV ENRICHMENT: {path.name}; actual_rows={len(actual_rows)}; populated={summary}.')
    _historical_verbose_print(f'Historical CANONICAL SAVE: {path.name}; interval={interval}; rows={len(new):,}; total rows={len(combined):,}.')


def _load_security_cache() -> dict[str, str]:
    with _SECURITY_CACHE_LOCK:
        if not SECURITY_CACHE.exists():
            return {}
        try:
            if SECURITY_CACHE.stat().st_size == 0:
                return {}
            df = pd.read_csv(SECURITY_CACHE, dtype=str).fillna('')
            if not {'symbol', 'isec_stock_code'}.issubset(set(df.columns)):
                return {}
            return {
                str(r.symbol).strip().upper(): str(r.isec_stock_code).strip()
                for r in df.itertuples(index=False)
                if str(r.symbol).strip() and str(r.isec_stock_code).strip()
            }
        except Exception as exc:
            print(f'WARNING: Could not read security cache: {exc}')
            return {}

def _historical_symbol_aliases(nse_symbol: str) -> list[str]:
    """Return legacy Breeze stock-code aliases for one authoritative NSE symbol."""
    symbol = str(nse_symbol or '').strip().upper()
    if not symbol:
        return []
    try:
        cache = _load_security_cache()
        aliases = []
        stock = str(cache.get(symbol) or '').strip().upper()
        if stock and stock != symbol:
            aliases.append(stock)
        canonical = _canonicalize_historical_symbol(symbol, cache)
        if canonical and canonical != symbol:
            aliases.append(canonical)
        for nse, fbs in _HISTORICAL_BREEZE_CODE_FALLBACKS.items():
            if symbol == nse:
                aliases.extend(fbs)
            elif symbol in fbs:
                aliases.append(nse)
        return list(dict.fromkeys(aliases))
    except Exception:
        return []


def _canonicalize_historical_symbol(symbol: str, security_cache: dict[str, str] | None = None) -> str:
    """Convert a legacy Breeze stock code back to its unique NSE symbol when possible."""
    value = str(symbol or '').strip().upper()
    if not value:
        return ''
    cache = security_cache if isinstance(security_cache, dict) else _load_security_cache()
    # Already an NSE-symbol key: preserve it.
    if value in cache:
        return value
    matches = [str(nse).strip().upper() for nse, breeze_code in cache.items()
               if str(breeze_code or '').strip().upper() == value]
    return matches[0] if len(set(matches)) == 1 else value


def _load_nse_company_master() -> dict[str, str]:
    """Load the NSE cash-market *EQ series only* symbol -> company-name master.

    The NSE equity master is authoritative for the tradable EQ series. If its
    SERIES column is unavailable or the master cannot be loaded, return an empty
    map rather than silently treating arbitrary text as a valid NSE EQ symbol.
    """
    global NSE_COMPANY_MASTER_ATTEMPTED

    def _parse_eq_master(text: str) -> dict[str, str]:
        try:
            df = pd.read_csv(io.StringIO(text), dtype=str).fillna('')
            cols = {str(c).strip().upper(): c for c in df.columns}
            sym_col = cols.get('SYMBOL')
            series_col = cols.get('SERIES')
            name_col = cols.get('NAME OF COMPANY') or cols.get('COMPANY NAME') or cols.get('NAME')
            if not sym_col or not series_col or not name_col:
                return {}
            eq = df[df[series_col].astype(str).str.strip().str.upper().eq('EQ')]
            return {
                str(row[sym_col]).strip().upper(): str(row[name_col]).strip()
                for _, row in eq.iterrows()
                if str(row[sym_col]).strip() and str(row[name_col]).strip()
            }
        except Exception as exc:
            print(f'WARNING: Could not parse NSE EQ master: {exc}')
            return {}

    if NSE_COMPANY_CACHE.exists():
        try:
            cached = NSE_COMPANY_CACHE.read_text(encoding='utf-8-sig')
            parsed = _parse_eq_master(cached)
            if parsed:
                return parsed
            print('WARNING: Cached NSE master lacks a usable SERIES column; refreshing NSE EQ master.')
        except Exception as exc:
            print(f'WARNING: Could not read cached NSE EQ master: {exc}')

    if NSE_COMPANY_MASTER_ATTEMPTED:
        return {}
    NSE_COMPANY_MASTER_ATTEMPTED = True
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126 Safari/537.36',
            'Accept': 'text/csv,*/*',
            'Referer': 'https://www.nseindia.com/',
        }
        response = requests.get(NSE_COMPANY_MASTER_URL, headers=headers, timeout=20)
        response.raise_for_status()
        text = response.content.decode('utf-8-sig', errors='replace')
        parsed = _parse_eq_master(text)
        if not parsed:
            raise RuntimeError('NSE equity master did not contain SYMBOL, SERIES and company-name columns.')
        NSE_COMPANY_CACHE.parent.mkdir(parents=True, exist_ok=True)
        NSE_COMPANY_CACHE.write_text(text, encoding='utf-8')
        return parsed
    except Exception as exc:
        print(f'WARNING: NSE EQ master unavailable; stock search fails closed: {exc}')
        return {}


def _normalize_security_company_name(value: str) -> str:
    """Normalize company names from NSE and Breeze masters for safe exact matching."""
    name = re.sub(r'\bLIMITED\b', 'LTD', str(value or '').upper())
    return re.sub(r'[^A-Z0-9]', '', name)


def _nse_eq_token_universe(nse_map: dict, token_script_map: dict | None = None) -> dict[str, str]:
    """Map official NSE EQ symbols to Breeze tokens using fast, bulk master matching.

    Historical downloads remain ticker-only. For instrument mapping only, use
    exact symbol, saved verified aliases, then a unique normalized company-name
    match between the already-downloaded NSE and Breeze security masters. This
    avoids slow per-symbol Breeze get_names() requests.
    """
    if not isinstance(nse_map, dict) or not nse_map:
        raise RuntimeError('NSE EQ-only filter failed: Breeze NSE token master is empty.')
    eq_master = _load_nse_company_master()
    if not eq_master:
        raise RuntimeError('NSE EQ-only filter failed: official NSE EQ master unavailable.')

    broker_meta = token_script_map if isinstance(token_script_map, dict) else {}
    code_to_token = {
        str(code).strip().upper(): str(token).strip()
        for code, token in nse_map.items()
        if str(code).strip() and str(token).strip()
    }
    def norm(value):
        # Reuse the canonical normalizer. A previously double-escaped word-boundary
        # regex treated \b as literal text, so company names containing LIMITED
        # did not normalize to the same key as LTD and were left unmapped.
        return _normalize_security_company_name(value)

    code_to_company = {}
    for token, row in broker_meta.items():
        if isinstance(row, (list, tuple)) and len(row) >= 2:
            code = str(row[0] or '').strip().upper()
            company = str(row[1] or '').strip()
            if code and str(token).strip():
                code_to_company[code] = company

    company_to_codes = {}
    for code, company in code_to_company.items():
        key = norm(company)
        if key:
            company_to_codes.setdefault(key, set()).add(code)

    aliases = _load_security_cache()
    filtered = {}
    counts = {'exact_symbol': 0, 'saved_alias': 0, 'bulk_master_match': 0}
    unmatched, ambiguous = [], []
    for symbol, company in eq_master.items():
        sym = str(symbol).strip().upper()
        if not sym:
            continue
        token = code_to_token.get(sym)
        method = 'exact_symbol'
        if not token:
            alias = str(aliases.get(sym) or '').strip().upper()
            token = code_to_token.get(alias) if alias else None
            method = 'saved_alias'
        if not token:
            codes = company_to_codes.get(norm(company), set()) if company else set()
            if len(codes) == 1:
                token = code_to_token.get(next(iter(codes)))
                method = 'bulk_master_match'
            elif len(codes) > 1:
                ambiguous.append(sym)
        if token:
            filtered[sym] = token
            counts[method] += 1
        else:
            unmatched.append(sym)

    if not filtered:
        raise RuntimeError('NSE EQ-only filter found no verified NSE-to-Breeze cash-token mappings.')
    print(
        f'NSE EQ TOKEN MAPPING: NSE EQ rows={len(eq_master)}; Breeze NSE cash tokens={len(code_to_token)}; '
        f'mapped={len(filtered)} (exact-symbol={counts["exact_symbol"]}, '
        f'saved-alias={counts["saved_alias"]}, bulk-master={counts["bulk_master_match"]}); '
        f'unmatched_EQ={len(unmatched)}; ambiguous={len(ambiguous)}; '
        f'per-symbol company-name API lookups=DISABLED.',
        flush=True,
    )
    if unmatched:
        print(f'NSE EQ TOKEN MAPPING: first unmatched NSE symbols: {", ".join(unmatched[:30])}', flush=True)
    if ambiguous:
        print(f'NSE EQ TOKEN MAPPING: ambiguous bulk-master matches excluded: {", ".join(ambiguous[:20])}', flush=True)
    return filtered


def _save_security_cache(mapping: dict[str, str]) -> None:
    if not mapping:
        return
    with _SECURITY_CACHE_LOCK:
        SECURITY_CACHE.parent.mkdir(parents=True, exist_ok=True)
        # Merge against the latest on-disk mapping while holding the same lock;
        # concurrent resolver workers must not overwrite each other's new aliases.
        merged = {}
        if SECURITY_CACHE.exists():
            try:
                current = pd.read_csv(SECURITY_CACHE, dtype=str).fillna('')
                if {'symbol', 'isec_stock_code'}.issubset(set(current.columns)):
                    merged.update({
                        str(r.symbol).strip().upper(): str(r.isec_stock_code).strip().upper()
                        for r in current.itertuples(index=False)
                        if str(r.symbol).strip() and str(r.isec_stock_code).strip()
                    })
            except Exception:
                pass
        merged.update({str(k).strip().upper(): str(v).strip().upper()
                       for k, v in mapping.items() if str(k).strip() and str(v).strip()})
        rows = [
            {'symbol': k, 'isec_stock_code': v}
            for k, v in sorted(merged.items()) if k and v
        ]
        temp_path = SECURITY_CACHE.with_name(
            f'{SECURITY_CACHE.name}.{os.getpid()}.{threading.get_ident()}.tmp'
        )
        try:
            pd.DataFrame(rows, columns=['symbol', 'isec_stock_code']).to_csv(temp_path, index=False)
            temp_path.replace(SECURITY_CACHE)
        finally:
            try:
                if temp_path.exists():
                    temp_path.unlink()
            except OSError:
                pass

def save_daily_snapshot() -> None:
    """Save a dated copy of the current workbook, e.g. Paper_Trade_2026-09-10.xlsx."""
    try:
        from shutil import copy2
        from datetime import datetime as _dt
        src = Path(WORKBOOK)
        if not src.exists():
            return
        snapshot_dir = src.parent / "Daily_Excel"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        dated = snapshot_dir / f"Paper_Trade_{_dt.now():%Y-%m-%d}.xlsx"
        copy2(src, dated)
    except Exception as exc:
        print(f"WARNING: Could not save daily Excel snapshot: {exc}")


def _new_nse_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'application/json,text/plain,*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Referer': 'https://www.nseindia.com/market-data/live-t0-market',
        'Origin': 'https://www.nseindia.com',
    })
    # NSE quote APIs require a fresh cookie from the public site first.
    session.get('https://www.nseindia.com', timeout=10)
    return session

def _is_nse_trading_day(day: date) -> bool:
    return day.weekday() < 5 and day not in NSE_TRADING_HOLIDAYS_2026

def bhavcopy_for_date(asof: date, session: requests.Session | None = None) -> pd.DataFrame:
    """Download one exact NSE equity bhavcopy. Never substitutes another date."""
    if not _is_nse_trading_day(asof):
        raise ValueError(f'{asof} is not an NSE equity trading day; no data should be created for this date.')
    session = session or _new_nse_session()
    attempts = []
    try:
        response = session.get(NSE_UDIFF_ZIP.format(asof), timeout=30)
        if response.ok and response.content[:2] == b'PK':
            with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
                csv_name = next(name for name in archive.namelist() if name.lower().endswith('.csv'))
                with archive.open(csv_name) as csv_file:
                    return pd.read_csv(csv_file)
        attempts.append(f'UDiFF HTTP {response.status_code}')
    except (requests.RequestException, OSError, StopIteration, zipfile.BadZipFile) as exc:
        attempts.append(f'UDiFF {type(exc).__name__}')
    # Current full-bhavcopy endpoints.
    for url_format in NSE_URLS:
        url = url_format.format(asof)
        try:
            response = session.get(url, timeout=30)
            content_type = response.headers.get('Content-Type', '')
            if response.ok and 'html' not in content_type.lower() and response.text.lstrip().startswith('SYMBOL'):
                from io import StringIO
                return pd.read_csv(StringIO(response.text))
            attempts.append(f'HTTP {response.status_code}')
        except requests.RequestException as exc:
            attempts.append(type(exc).__name__)
        time.sleep(.5)

    # Older NSE archive format is still needed for long-lookback indicators such
    # as the 200-day SMA. The newer UDiFF archive does not reliably expose every
    # older date, so fall back to the historical EQUITIES archive before declaring
    # the date unavailable.
    mon = asof.strftime('%b').upper()
    old_urls = (
        f'https://archives.nseindia.com/content/historical/EQUITIES/{mon}/cm{asof:%d}{mon}{asof:%Y}bhav.csv.zip',
        f'https://nsearchives.nseindia.com/content/historical/EQUITIES/{mon}/cm{asof:%d}{mon}{asof:%Y}bhav.csv.zip',
    )
    for url in old_urls:
        try:
            response = session.get(url, timeout=30)
            if response.ok and response.content[:2] == b'PK':
                with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
                    csv_name = next(name for name in archive.namelist() if name.lower().endswith('.csv'))
                    with archive.open(csv_name) as csv_file:
                        return pd.read_csv(csv_file)
            attempts.append(f'archive HTTP {response.status_code}')
        except (requests.RequestException, OSError, StopIteration, zipfile.BadZipFile) as exc:
            attempts.append(f'archive {type(exc).__name__}')
        time.sleep(.5)
    raise RuntimeError(f'NSE bhavcopy unavailable for {asof}. Attempts: {", ".join(attempts)}')

def latest_bhavcopy(start_date: date | None = None) -> tuple[pd.DataFrame, date]:
    """Download the most recent available NSE equity bhavcopy, with holiday retry."""
    session = _new_nse_session()
    attempts = []
    for days_back in range(0, 16):
        asof = (start_date or date.today()) - timedelta(days=days_back)
        if not _is_nse_trading_day(asof):
            continue
        try:
            return bhavcopy_for_date(asof, session), asof
        except Exception as exc:
            attempts.append(f'{asof}: {type(exc).__name__}')
    details = '; '.join(attempts[-6:])
    raise RuntimeError('NSE bhavcopy was unavailable. Run after NSE publishes the daily file, or check your internet connection. Recent attempts: ' + details)

def _column_index(df: pd.DataFrame, *names: str) -> int:
    # NSE UDiFF files can contain duplicate/variant headers.  Selecting a duplicate
    # label with df[name] may return a DataFrame, which then makes pd.to_numeric fail
    # with: 'arg must be a list, tuple, 1-d array, or Series'.  Resolve by position.
    wanted = {str(n).strip().upper() for n in names}
    for i, c in enumerate(df.columns):
        if str(c).strip().upper() in wanted:
            return i
    raise KeyError(f'Missing columns. Expected one of {names}; found {list(df.columns)}')

def col(df: pd.DataFrame, *names: str) -> str:
    return str(df.columns[_column_index(df, *names)])

def _series_by_names(df: pd.DataFrame, *names: str) -> pd.Series:
    return df.iloc[:, _column_index(df, *names)]

def evaluate_entry_conditions(price, open_price, previous_close, volume, turnover_cr, cfg):
    """Canonical GUI/runtime BUY evaluator. All modes use the same underlying rule."""
    check=entry_check(price, open_price, previous_close, volume, turnover_cr, cfg)
    checks=check.get('checks') or {}
    # Keep the legacy field names consumed by Excel/GUI while sourcing every
    # decision from the canonical entry_check().
    legacy_checks={
        'open_direction_ok': bool(checks.get('direction', False)),
        'open_movement_ok': bool(checks.get('movement', False)),
        'price_band_ok': bool(checks.get('price_band', False)),
        'volume_ok': bool(checks.get('volume', False)),
        'turnover_ok': bool(checks.get('turnover', False)),
    }
    failed=[k for k,v in legacy_checks.items() if not v]
    # Keep the canonical evaluator's complete reason (including minimum price)
    # instead of hiding conditions that are not represented in legacy_checks.
    canonical_reason = str(check.get('reason') or '')
    return {
        'passed': bool(check.get('passed')), 'checks': legacy_checks,
        'trigger_price': check.get('trigger_price'),
        'open_direction':cfg.get('open_direction','BOTH'),
        'open_movement_pct':float(cfg.get('open_movement',0) or 0),
        'price_big':float(cfg.get('price_big',0) or 0), 'price_small':float(cfg.get('price_small',0) or 0),
        'required_volume':float(cfg.get('volume',0) or 0),
        'required_turnover_cr':float(cfg.get('turnover_cr',0) or 0),
        'actual_price':check.get('actual_price'),
        'actual_open_movement_pct':check.get('actual_open_movement_pct'),
        'open_gap_pct':check.get('open_gap_pct'),
        'actual_volume':check.get('actual_volume'),
        'actual_turnover_cr':check.get('actual_turnover_cr'),
        'reason':canonical_reason or ('; '.join(failed) or 'All configured entry conditions passed')
    }

def screen_from_raw(raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build the NSE screen using all eight GUI-saved logic configurations."""
    if raw is None or raw.empty:
        raise ValueError('NSE bhavcopy returned no rows.')
    symbol = _series_by_names(raw,'SYMBOL','TCKRSYMB')
    series = _series_by_names(raw,'SERIES','SCTYSRS')
    close = _series_by_names(raw,'CLOSE_PRICE','CLOSE','CLSPRIC')
    prev = _series_by_names(raw,'PREV_CLOSE','PREVCLOSE','PRVSCLSGPRIC')
    volume = _series_by_names(raw,'TTL_TRD_QNTY','TOTTRDQTY','TTLTRADGVOL')
    turnover_idx = next((i for i,c in enumerate(raw.columns) if str(c).strip().upper() in ('TURNOVER_LACS','TOTTRDVAL','TTLTRFVAL')), None)
    if turnover_idx is None:
        raise KeyError(f'No turnover column in NSE file; found {list(raw.columns)}')
    fields = {
        'Symbol': symbol, 'Series': series,
        'Open': _series_by_names(raw,'OPEN_PRICE','OPEN','OPNPRIC'),
        'High': _series_by_names(raw,'HIGH_PRICE','HIGH','HGHPRIC'),
        'Low': _series_by_names(raw,'LOW_PRICE','LOW','LWPRIC'),
        'Close': close, 'Previous close': prev, 'Volume': volume,
        'Raw turnover': raw.iloc[:, turnover_idx]
    }
    df = pd.DataFrame(fields, index=raw.index).copy()
    for c in ['Open','High','Low','Close','Previous close','Volume','Raw turnover']:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    df = df[(df['Series'].astype(str).str.strip() == 'EQ') & df['Close'].gt(0) & df['Previous close'].gt(0)].copy()
    df['Daily change'] = df['Close']/df['Previous close'] - 1
    df['Open gap %'] = ((df['Open'] - df['Previous close']) / df['Previous close'] * 100).where(df['Previous close'].gt(0))
    df['Movement %'] = ((df['High'] - df['Open']) / df['Open'] * 100).where(df['Open'].gt(0))
    turnover_is_lacs = str(raw.columns[turnover_idx]).strip().upper() == 'TURNOVER_LACS'
    df['Turnover (₹)'] = df['Raw turnover'] * (100000 if turnover_is_lacs else 1)
    df['Turnover Cr'] = df['Turnover (₹)'] / 10000000.0
    company_map = _load_nse_company_master()
    df['Company Name'] = df['Symbol'].astype(str).str.strip().str.upper().map(company_map).fillna(df['Symbol'].astype(str).str.strip())
    cfgs = load_logics()
    logic_cols = []
    for logic_name, cfg in DEFAULT_LOGICS.items():
        c = cfgs[logic_name]
        direction = df.apply(lambda r: direction_ok(r['Open'], r['Previous close'], c['open_direction']), axis=1)
        price_band = (pd.Series(True, index=df.index) if not price_band_enabled(c) else df['Open'].apply(lambda x: price_band_ok(x, c)))
        # Condition A: configured movement is measured from previous close,
        # not today's open. Fall back to open only when previous close is absent.
        _movement_base = pd.to_numeric(df['Previous close'], errors='coerce')
        _movement_trigger = _movement_base * (1.0 + float(c['open_movement'])/100.0)
        _movement_trigger = _movement_trigger.where(_movement_base.gt(0), df['Open'] * (1.0 + float(c['open_movement'])/100.0))
        trigger = df['High'] >= _movement_trigger
        # Movement-only controls the trigger style; it does not disable the
        # other enabled GUI conditions. Keep the daily screener aligned with the
        # canonical runtime evaluator for the conditions available in bhavcopy.
        # VWAP/RVOL/ORB/MA/52-week gates require intraday/history context and are
        # re-checked by the canonical paper/live evaluator before any BUY.
        eligible = (
            direction & trigger & price_band &
            df['Close'].ge(float(c.get('minimum_price', 0) or 0)) &
            df['Volume'].ge(float(c.get('volume', 0) or 0)) &
            df['Turnover Cr'].ge(float(c.get('turnover_cr', 0) or 0))
        )
        if not c.get('enabled', True):
            eligible = eligible & False
        df[f'Eligible for {logic_name.lower()}'] = eligible
        df[f'Trigger {logic_name}'] = trigger
        logic_cols.append(f'Eligible for {logic_name.lower()}')
    eligible_any = pd.Series(False, index=df.index)
    for c in logic_cols:
        eligible_any = eligible_any | df[c]
    df['Eligible?'] = eligible_any
    market_cols = ['Company Name','Symbol','Series','Open','High','Low','Close','Previous close','Daily change','Open gap %','Movement %','Volume','Turnover (₹)','Turnover Cr'] + logic_cols
    df['Eligible?'] = eligible_any
    market_cols = market_cols + ['Eligible?']
    market = df[market_cols].sort_values(['Movement %','Turnover (₹)'], ascending=[False,False], na_position='last').copy()
    eligible = market[market['Eligible?'] if 'Eligible?' in market.columns else eligible_any].copy()
    if eligible.empty:
        top = eligible.copy()
    else:
        eligible['Score'] = eligible['Movement %'].fillna(0)
        top = eligible.sort_values(['Score','Turnover (₹)'], ascending=False).copy()
        top.insert(0, 'Rank', range(1, len(top)+1))
        top['Budget (₹)'] = 10000.0
        top['Limit price'] = (top['Close'] * 1.003 / .05).round() * .05
        top['Quantity'] = top.apply(lambda r: max(0, int(float(r['Budget (₹)']) // float(r['Limit price']))) if float(r['Limit price']) > 0 else 0, axis=1)
        top['Reason'] = 'At least one configured logic passed its direction, movement, price-band, turnover and volume filters.'
    return market, top

def screen() -> tuple[pd.DataFrame, pd.DataFrame, date]:
    raw, asof = latest_bhavcopy()
    market, top = screen_from_raw(raw)
    return market, top, asof


DASHBOARD_HEADERS = [
    'Date','Ranking','Logic','Symbol','Open','High','Low','Close','Previous close','Volume','Turnover in Cr',
    'Buy Qty','Buy Price','Buy Date & Time','High After Buy','Low After Buy','High After Buy Date & Time','Low After Buy Date & Time',
    'Sell Price','Sell Date & Time','High After Sell','Low After Sell','High After Sell Date & Time','Low After Sell Date & Time',
    'Exit Reason','Gross P&L','Buy Turnover','Sell Turnover','Brokerage','STT','Exchange Charges','GST','SEBI Charges','Stamp Duty',
    'Other Charges','Total Charges','Net P&L','P&L %','Data Source','Status',
    'Open Direction','Open Movement %','Price Big','Price Small','Required Turnover Cr','Required Volume',
    'Profit Target %','Initial Stop Loss %','Activation %','Trailing Stop %','Historical Shares',
    'Entry Conditions Passed','Entry Validation'
]

# Historical Data sheet intentionally stores only the columns requested by the user.
# Keep the internal replay result rich; reduce only the Excel historical output schema.
HISTORICAL_HEADERS = [
    'Date','Ranking','Logic','Symbol','Open','Close','Volume','Turnover in Cr',
    'Buy Qty','Buy Price','Buy Date & Time','High After Buy','High After Buy Date & Time',
    'Profit Target','Profit target achieved the goal','Low After Buy','Low After Buy Date & Time',
    'Loss Target','Loss target achieved the goal','Exit Reason with Profit / Loss Target achieved the goal',
    'Gross P&L','Buy Turnover','Sell Turnover','Brokerage','STT','Exchange Charges','GST','SEBI Charges',
    'Stamp Duty','Other Charges','Total Charges','Net P&L','P&L %','Charge Source','Charge Plan',
    '15:25 Price','15:25 Square-Off','15:25 Time','Historical Shares','Open Direction','Open Movement %','Price Big','Price Small',
    'Required Turnover Cr','Profit Target %','Initial Stop Loss %','Required Volume','Activation %',
    'Trailing Stop %','Historical Result',
    'First Signal Time','Decision','Decision Reason','Sell Date & Time',
    'Day High','Day High Time','Day Low','Day Low Time','Previous Close','Daily Movement ₹','Daily Movement %'
]


def _excel_date(value):
    """Normalize trading dates to real Excel date values (YYYY-MM-DD display)."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or '').strip()
    if not text:
        return None
    for fmt in ('%Y-%m-%d','%d-%m-%Y','%d/%m/%Y','%d-%b-%Y'):
        try:
            return datetime.strptime(text[:10], fmt).date()
        except Exception:
            pass
    return None


def _format_pnl_dashboard(ws) -> None:
    """Polish the wide P&L dashboard while keeping the data schema unchanged."""
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    navy='163A5F'; blue='1F6AA5'; light='EAF3FA'; white='FFFFFF'; green='E2F0D9'; red='FCE4D6'; gold='FFF2CC'
    header_fill=PatternFill('solid',fgColor=navy); alt_fill=PatternFill('solid',fgColor='F7FBFF')
    green_fill=PatternFill('solid',fgColor=green); red_fill=PatternFill('solid',fgColor=red); gold_fill=PatternFill('solid',fgColor=gold)
    thin=Side(style='thin',color='D6E2F0')
    white_bold=Font(color=white,bold=True,size=10); normal=Font(color='1F2937',size=10); bold=Font(color=navy,bold=True,size=10)

    ws.sheet_view.showGridLines=False; ws.sheet_view.zoomScale=80; ws.sheet_properties.tabColor=blue; ws.freeze_panes='A2'
    for cell in ws[1]:
        cell.fill=header_fill; cell.font=white_bold; cell.alignment=Alignment(horizontal='center',vertical='center',wrap_text=True); cell.border=Border(bottom=thin)
    ws.row_dimensions[1].height=40

    dates={1}; dts={14,17,18,20,23,24}; pct={38,42,46,47,48}; money={26,27,28,29,30,31,32,33,34,35,36,37}
    for rr in range(2,ws.max_row+1):
        fill=alt_fill if rr%2==0 else PatternFill('solid',fgColor=white)
        for cc in range(1,ws.max_column+1):
            cell=ws.cell(rr,cc); cell.fill=fill; cell.font=normal; cell.alignment=Alignment(vertical='center',wrap_text=False)
        for c in dates: ws.cell(rr,c).number_format='yyyy-mm-dd'
        for c in dts: ws.cell(rr,c).number_format='yyyy-mm-dd hh:mm:ss'
        for c in pct: ws.cell(rr,c).number_format='0.00'
        for c in money: ws.cell(rr,c).number_format='₹#,##0.00;[Red]-₹#,##0.00'
        if ws.cell(rr,37).value not in (None,''): ws.cell(rr,37).font=bold
        if str(ws.cell(rr,40).value or '').upper().startswith('ELIGIBLE'):
            ws.cell(rr,40).fill=gold_fill


    widths=[12,9,12,16,12,12,12,12,14,12,18,12,13,21,15,15,25,25,13,21,15,15,25,25,34,15,15,15,13,13,16,13,14,14,16,16,15,12,20,28,18,18,14,14,20,18,16,16,18,18,24,28]
    for c,w in enumerate(widths,1): ws.column_dimensions[get_column_letter(c)].width=w
    ws.auto_filter.ref=f"A1:{get_column_letter(len(DASHBOARD_HEADERS))}{max(1,ws.max_row)}"
    for name in list(ws.tables.keys()): del ws.tables[name]


def update_datewise_pnl_dashboard(asof: date | None = None) -> None:
    wb=load_workbook(WORKBOOK); ws=wb['P&L Dashboard']
    old_trade={}
    for r in range(2,ws.max_row+1):
        d=str(ws.cell(r,1).value or '')[:10]; sym=str(ws.cell(r,4).value or '').strip().upper(); logic=str(ws.cell(r,3).value or '').strip()
        if d and sym and logic in set(ACTIVE_LOGIC_NAMES): old_trade[(d,sym,logic)]=[ws.cell(r,c).value for c in range(12,41)]
    for m in list(ws.merged_cells.ranges): ws.unmerge_cells(str(m))
    current_date=asof.isoformat() if asof else None
    retained=[]
    for r in range(2,ws.max_row+1):
        vals=[ws.cell(r,c).value for c in range(1,41)]; d=str(vals[0] or '')[:10]
        if d and d!=current_date: retained.append(vals)
    ws.delete_rows(1,ws.max_row); ws.append(DASHBOARD_HEADERS)
    for vals in retained: ws.append(vals+[None]*max(0,40-len(vals)))
    mws=wb['NSE Market Data'] if 'NSE Market Data' in wb.sheetnames else None
    hist_date=current_date
    if mws:
        for rr in range(4,mws.max_row+1):
            txt=str(mws.cell(rr,1).value or '')
            if txt.startswith('As of '): hist_date=txt[7:17]; break
        candidates=[]
        for rr in range(5,mws.max_row+1):
            sym=mws.cell(rr,3).value; series=mws.cell(rr,4).value
            flags=[mws.cell(rr,c).value is True for c in (13,16)]
            if not sym or str(series).upper()!='EQ' or not any(flags): continue
            candidates.append((float(mws.cell(rr,6).value or 0),float(mws.cell(rr,10).value or 0),rr))
        candidates.sort(reverse=True)
        for rank,(_,_,rr) in enumerate(candidates,1):
            sym=str(mws.cell(rr,3).value).strip().upper()
            for logic,c in zip(ACTIVE_LOGIC_NAMES,(13,16)):
                if mws.cell(rr,c).value is not True: continue
                trade=old_trade.get((hist_date,sym,logic),[None]*29)
                ws.append([hist_date,rank,logic,sym,mws.cell(rr,5).value,mws.cell(rr,6).value,mws.cell(rr,7).value,mws.cell(rr,8).value,mws.cell(rr,9).value,mws.cell(rr,11).value,float(mws.cell(rr,12).value or 0)/10000000,*trade])
    _format_pnl_dashboard(ws); wb.save(WORKBOOK)


def _candle_dataframe(data):
    """Normalize Breeze Historical V2 candles into a timestamp-indexed frame."""
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data).copy()
    if 'datetime' not in df.columns:
        return pd.DataFrame()
    df['_dt'] = df['datetime'].map(_parse_breeze_datetime)
    df = df[df['_dt'].notna()].copy()
    for c in ('open', 'high', 'low', 'close', 'volume'):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    return df.sort_values('_dt').reset_index(drop=True)


def _valid_historical_ohlcv_mask(frame: pd.DataFrame) -> pd.Series:
    """Return rows that are valid traded OHLCV candles (never synthetic gaps).

    A broker candle with zero/missing volume but populated OHLC is not accepted
    as a traded candle. Missing minutes are represented separately by
    _normalize_full_session_minutes(is_actual=False), never by fabricated data.
    """
    if frame is None or frame.empty:
        return pd.Series(False, index=getattr(frame, 'index', []), dtype=bool)
    required = ('open', 'high', 'low', 'close', 'volume')
    if any(col not in frame.columns for col in required):
        return pd.Series(False, index=frame.index, dtype=bool)
    values = {c: pd.to_numeric(frame[c], errors='coerce') for c in required}
    mask = pd.Series(True, index=frame.index, dtype=bool)
    for col in ('open', 'high', 'low', 'close'):
        mask &= values[col].notna() & values[col].gt(0)
    mask &= values['volume'].notna() & values['volume'].gt(0)
    mask &= values['high'].ge(values['low'])
    mask &= values['high'].ge(values['open']) & values['high'].ge(values['close'])
    mask &= values['low'].le(values['open']) & values['low'].le(values['close'])
    return mask.fillna(False)


def _nse50_minute_coverage_ok(frame: pd.DataFrame | None, max_gap_minutes: int = 5) -> bool:
    """Reject suspicious multi-minute gaps in liquid NSE50 1-minute downloads.

    This is a download-quality heuristic, not a claim that every minute must trade.
    Short gaps are allowed (including genuine no-trade minutes); gaps longer than
    max_gap_minutes trigger a segmented refresh. Synthetic rows are never counted.
    On NSE, continuous trading stops at 15:14:59, followed by the scheduled Closing
    Auction buffer (15:15-15:28) and closing print (15:29). Gaps are evaluated
    during active continuous market trading (up to 15:15 IST).
    """
    if frame is None or frame.empty or '_dt' not in frame.columns:
        return False
    x = frame.copy()
    if 'is_actual' in x.columns:
        x = x[x['is_actual'].fillna(False).astype(bool)]
    x['_dt'] = pd.to_datetime(x['_dt'], errors='coerce')
    x = x[x['_dt'].notna()].sort_values('_dt').drop_duplicates('_dt').reset_index(drop=True)
    if x.empty:
        return False
    start = x['_dt'].min()
    end = x['_dt'].max()
    if pd.isna(start) or pd.isna(end):
        return False
    # Evaluate continuous trading session up to 15:15 IST
    x_cont = x[x['_dt'].dt.hour * 60 + x['_dt'].dt.minute <= 915]
    if x_cont.empty:
        return False
    gaps = x_cont['_dt'].diff().dt.total_seconds().div(60).dropna()
    return bool(gaps.empty or gaps.max() <= int(max_gap_minutes))


def _normalize_full_session_minutes(df: pd.DataFrame, day: date) -> pd.DataFrame:
    """Create one continuous 1-minute timeline for 09:07-15:30.

    Breeze can legitimately omit minutes in illiquid NSE equities when there was
    no trade. Do not treat those missing rows as an API failure. Keep actual
    candles unchanged and forward-fill OHLC only after the first real candle;
    volume is zero for synthetic no-trade minutes. A separate is_actual column
    preserves which rows came from Breeze.
    """
    if df is None or df.empty:
        return pd.DataFrame()
    x=df.copy()
    x['_dt']=pd.to_datetime(x['_dt'], errors='coerce')
    x=x[x['_dt'].notna()].sort_values('_dt').drop_duplicates('_dt', keep='last')
    if x.empty:
        return pd.DataFrame()
    start=datetime.combine(day, datetime.min.time()).replace(hour=9, minute=15)
    end=datetime.combine(day, datetime.min.time()).replace(hour=15, minute=30)
    actual=x[(x['_dt']>=start)&(x['_dt']<=end)].copy()
    if actual.empty:
        return actual
    actual['is_actual']=True
    idx=pd.date_range(start=start, end=end, freq='1min')
    base=actual.set_index('_dt').reindex(idx)
    base.index.name='_dt'
    base['is_actual']=base['is_actual'].fillna(False).astype(bool)
    for c in ('open','high','low','close'):
        if c in base.columns:
            base[c]=pd.to_numeric(base[c], errors='coerce')
    if 'close' in base.columns:
        base['close']=base['close'].ffill()
        for c in ('open','high','low'):
            if c in base.columns:
                base[c]=base[c].fillna(base['close'])
    if 'volume' in base.columns:
        base['volume']=pd.to_numeric(base['volume'], errors='coerce').fillna(0)
    out=base.reset_index()
    # Do not invent pre-first-trade OHLC values.
    first_actual=out.index[out['is_actual']]
    if len(first_actual):
        out.loc[:first_actual[0]-1, ['open','high','low','close']]=float('nan')
    return out


class HistoricalBreezeRequired(RuntimeError):
    """Raised when requested historical cache data is missing."""


def _historical_cache_only() -> bool:
    """Return True when Historical replay must use laptop cache only.

    The GUI sets BREEZE_HISTORICAL_CACHE_ONLY=1 for Historical and LIVE SHADOW
    replays. This prevents a missing/misnamed local CSV from silently turning a
    fast local replay into a slow Breeze API download.
    """
    return str(os.getenv('BREEZE_HISTORICAL_CACHE_ONLY','0')).strip().lower() in {'1','true','yes','on'}


_HISTORICAL_PARITY_CONFIG_SIGNATURE = None
_HISTORICAL_PARITY_LOCK = threading.Lock()
def _verify_historical_parity_if_config_changed() -> None:
    """Run the synthetic strategy parity test once per logic-config revision.

    The old call ran for every symbol/date download, although the config normally
    remains unchanged throughout a historical run.
    """
    global _HISTORICAL_PARITY_CONFIG_SIGNATURE
    try:
        cfg_path = CONFIG_DIR / 'logic_settings.json'
        st = cfg_path.stat()
        signature = (st.st_mtime_ns, st.st_size)
    except Exception:
        signature = None
    if signature != _HISTORICAL_PARITY_CONFIG_SIGNATURE:
        with _HISTORICAL_PARITY_LOCK:
            if signature != _HISTORICAL_PARITY_CONFIG_SIGNATURE:
                verify_canonical_strategy_parity()
                _HISTORICAL_PARITY_CONFIG_SIGNATURE = signature


def _historical_session_candles(breeze, stock_code, day, interval='1minute', baseline_volume=0.0, baseline_turnover_cr=0.0, strict_minute_coverage=False):
    """Download/cache the complete requested trading-session candles.

    Breeze Historical V2 accepts at most 1,000 candles per request.  In 1-second
    mode a full NSE session is therefore fetched in <=1,000-candle chunks rather
    than accepting the first 1,000 candles returned by the API.  This gives the
    replay engine the complete intraday timeline needed to behave like live data.
    """
    _verify_historical_parity_if_config_changed()
    interval = str(interval).strip().lower()
    if interval not in {'1minute','1second','hybrid'}:
        raise ValueError('Historical interval must be 1minute, 1second or hybrid (1minute -> 1second).')
    required={'_dt','open','high','low','close','volume'}
    session_start=datetime.combine(day, datetime.min.time()).replace(hour=9,minute=15)
    session_end=datetime.combine(day, datetime.min.time()).replace(hour=15,minute=30)

    cached=_load_cached_intraday(day,stock_code,interval,resolve_previous_close=True)
    if cached is not None and not cached.empty and required.issubset(cached.columns):
        actual=cached[cached.get('is_actual', pd.Series(True,index=cached.index)).astype(bool)] if 'is_actual' in cached.columns else cached
        cache_is_complete = not actual.empty
        if cache_is_complete:
            _cache_valid = _valid_historical_ohlcv_mask(actual)
            if not bool(_cache_valid.all()):
                _bad_cache_rows = int((~_cache_valid).sum())
                print(f'Historical CACHE QUALITY REJECTED: {stock_code} {day} {interval}; invalid actual candles={_bad_cache_rows}; refreshing from Breeze.', flush=True)
                cache_is_complete = False
        if interval == '1minute' and strict_minute_coverage and cache_is_complete and not _nse50_minute_coverage_ok(actual, max_gap_minutes=5):
            cache_is_complete = False
            print(f'Historical 1-minute CACHE COVERAGE REJECTED: {stock_code} {day}; a gap greater than 5 minutes was found; refreshing in smaller time windows.', flush=True)
        if interval == '1second' and cache_is_complete:
            cache_is_complete = _full_second_session_coverage(actual, day)
            if not cache_is_complete:
                try:
                    _cache_first = pd.to_datetime(actual['_dt'], errors='coerce').min()
                    _cache_last = pd.to_datetime(actual['_dt'], errors='coerce').max()
                    _first_text = _cache_first.strftime('%H:%M:%S') if pd.notna(_cache_first) else 'unknown'
                    _last_text = _cache_last.strftime('%H:%M:%S') if pd.notna(_cache_last) else 'unknown'
                    print(
                        f'Historical 1-second CACHE INCOMPLETE: {stock_code} {day}; '
                        f'coverage={_first_text}-{_last_text}; '
                        'retrying missing/unverified session chunks.'
                    )
                except Exception:
                    print(f'Historical 1-second CACHE INCOMPLETE: {stock_code} {day}; retrying session chunks.')
        if cache_is_complete:
            _historical_perf_add('cache_hits')
            _historical_perf_add('candles', len(actual))
            _historical_verbose_print(f'Historical CACHE REUSE: {stock_code} {interval} on {day} ({len(actual)} actual candles; coverage {actual["_dt"].min():%H:%M:%S}-{actual["_dt"].max():%H:%M:%S}).')
            return cached.sort_values('_dt').drop_duplicates('_dt',keep='last').reset_index(drop=True)

    _historical_perf_add('cache_misses')

    # 1-minute NSE session data (09:15-15:30) spans 375 minutes (6 hours 15 minutes).
    # Breeze Historical V2 server truncates any query exceeding 6 hours (360 minutes),
    # dropping 15:15-15:28 and returning only 361 candles.
    # We query two safe sub-6-hour windows (09:15-14:30 and 14:30-15:30) to guarantee all
    # 375 genuine traded market candles are retrieved directly without truncation.
    split_mid = session_start + timedelta(hours=5, minutes=15)
    if interval == '1minute' and strict_minute_coverage:
        ranges = [
            (session_start, split_mid, '09:15-14:30 session-1'),
            (split_mid, session_end, '14:30-15:30 session-2')
        ]
    elif interval == '1minute':
        ranges = [
            (session_start, split_mid, '09:15-14:30 session-1'),
            (split_mid, session_end, '14:30-15:30 session-2')
        ]
    else:
        ranges=[]
        cur=session_start
        while cur <= session_end:
            chunk_end=min(session_end, cur+timedelta(seconds=999))
            ranges.append((cur,chunk_end,f'{cur:%H:%M:%S}-{chunk_end:%H:%M:%S}'))
            cur=chunk_end+timedelta(seconds=1)

    # In local-cache-only mode a cache miss is a hard stop for this symbol.
    # NEVER fall through to Breeze: this mode is specifically for recalculating
    # strategy conditions from already-downloaded laptop data.
    if _historical_cache_only():
        raise HistoricalBreezeRequired(
            f'LOCAL CACHE ONLY: cached {interval} data is missing for {stock_code} on {day}; Breeze was not called.'
        )

    # A cache miss must never call the Breeze SDK with None.  Historical replay
    # deliberately starts cache-first and creates a Breeze session only after this
    # exception is raised.
    if breeze is None:
        raise HistoricalBreezeRequired(
            f'Breeze session required because cached {interval} data is missing for '
            f'{stock_code} on {day}.'
        )

    frames=[]
    for chunk_no,(req_start,req_end,label) in enumerate(ranges,1):
        _historical_verbose_print(f'Historical REQUEST: {stock_code} {interval} chunk {chunk_no}/{len(ranges)} {label}.')
        _request_t0=time.perf_counter()
        try:
            # Use the shared exact-request cache here as well as in replay paths.
            # Each successful 1-second chunk is checkpointed to disk, so if a job
            # is stopped halfway through a session, the next run reuses completed
            # chunks instead of requesting them all from Breeze again.
            data=_historical_v2(breeze, stock_code, interval, req_start, req_end, persist_request_cache=(interval != '1minute'))
            _elapsed_request = time.perf_counter()-_request_t0
            _historical_verbose_print(f'Historical RESPONSE TIME: {stock_code} {interval} chunk {chunk_no}/{len(ranges)} = {_elapsed_request:.2f}s.')
        except Exception as exc:
            print(f'Historical REQUEST FAILED: {stock_code} {interval} chunk {chunk_no}/{len(ranges)} after {time.perf_counter()-_request_t0:.2f}s - {exc}')
            continue
        if not data:
            print(f'Historical NO DATA: {stock_code} {interval} chunk {chunk_no}/{len(ranges)}.')
            continue
        actual=_candle_dataframe(data)
        if actual.empty:
            print(f'Historical EMPTY: {stock_code} {interval} chunk {chunk_no}/{len(ranges)}.')
            continue
        actual=actual[(actual['_dt']>=req_start)&(actual['_dt']<=req_end)].sort_values('_dt').drop_duplicates('_dt',keep='last').reset_index(drop=True)
        if not actual.empty:
            _valid_mask = _valid_historical_ohlcv_mask(actual)
            _invalid_count = int((~_valid_mask).sum())
            if _invalid_count:
                print(f'Historical INVALID CANDLES REJECTED: {stock_code} {interval} chunk {chunk_no}/{len(ranges)}; rejected={_invalid_count} (missing/non-positive OHLC or volume, or impossible high/low).', flush=True)
                actual = actual.loc[_valid_mask].copy()
        # Retry a suspiciously sparse 1-minute window. This never fills gaps with
        # fabricated candles: the final merged result contains only broker rows.
        if interval == '1minute' and strict_minute_coverage and not actual.empty and not _nse50_minute_coverage_ok(actual, max_gap_minutes=5):
            for gap_retry in range(1, 3):
                print(f'Historical 1-minute GAP RETRY: {stock_code} window={label}; attempt={gap_retry}/2; long intra-window gap detected.', flush=True)
                try:
                    # Breeze Historical V2 enforces an internal 6-hour (360-minute) query window limit.
                    # A single 09:15-15:30 (375 minutes) request is truncated by the broker at 15:14, omitting 15:15-15:28.
                    # Targeted segment recovery specifically queries the missing gap window(s) to fetch all genuine candles.
                    actual = actual.reset_index(drop=True)
                    gap_diffs = actual['_dt'].diff().dt.total_seconds().div(60)
                    gap_locs = gap_diffs[gap_diffs > 5].index.tolist()
                    gap_windows = []
                    if actual['_dt'].iloc[0] > req_start + timedelta(minutes=5):
                        gap_windows.append((req_start, actual['_dt'].iloc[0]))
                    for g_idx in gap_locs:
                        gap_windows.append((actual.loc[g_idx - 1, '_dt'], actual.loc[g_idx, '_dt']))
                    max_act = actual['_dt'].max()
                    if max_act < req_end:
                        gap_windows.append((max_act, req_end))

                    merged_gap_windows = []
                    for ws, we in sorted(gap_windows):
                        if not merged_gap_windows:
                            merged_gap_windows.append([ws, we])
                        else:
                            if ws <= merged_gap_windows[-1][1]:
                                merged_gap_windows[-1][1] = max(merged_gap_windows[-1][1], we)
                            else:
                                merged_gap_windows.append([ws, we])

                    recovered = []
                    for seg_start, seg_end in merged_gap_windows:
                        print(f'Historical 1-minute GAP SEGMENT RECOVERY: {stock_code} window={seg_start:%H:%M:%S}-{seg_end:%H:%M:%S}; fetching missing broker candles.', flush=True)
                        seg_data = _historical_v2(breeze, stock_code, interval, seg_start, seg_end, persist_request_cache=False, force_refresh=True)
                        if seg_data:
                            seg_frame = _candle_dataframe(seg_data)
                            if not seg_frame.empty:
                                seg_frame = seg_frame[(seg_frame['_dt']>=seg_start)&(seg_frame['_dt']<=seg_end)].copy()
                                seg_valid = _valid_historical_ohlcv_mask(seg_frame)
                                seg_frame = seg_frame.loc[seg_valid]
                                if not seg_frame.empty:
                                    recovered.append(seg_frame)

                    if recovered:
                        actual = pd.concat([actual, *recovered], ignore_index=True).sort_values('_dt').drop_duplicates('_dt', keep='last').reset_index(drop=True)
                    else:
                        retry_data = _historical_v2(breeze, stock_code, interval, req_start, req_end, persist_request_cache=False, force_refresh=True)
                        retry_frame = _candle_dataframe(retry_data)
                        if not retry_frame.empty:
                            retry_frame = retry_frame[(retry_frame['_dt']>=req_start)&(retry_frame['_dt']<=req_end)].copy()
                            retry_valid = _valid_historical_ohlcv_mask(retry_frame)
                            retry_frame = retry_frame.loc[retry_valid]
                            if not retry_frame.empty:
                                actual = pd.concat([actual, retry_frame], ignore_index=True).sort_values('_dt').drop_duplicates('_dt', keep='last').reset_index(drop=True)

                    if _nse50_minute_coverage_ok(actual, max_gap_minutes=5):
                        print(f'Historical 1-minute GAP RESOLVED: {stock_code} now has complete session coverage ({len(actual)} genuine candles).', flush=True)
                        break
                except Exception as retry_exc:
                    print(f'Historical 1-minute GAP RETRY FAILED: {stock_code} window={label}: {retry_exc}', flush=True)
        if actual.empty:
            print(f'Historical NO USABLE DATA: {stock_code} {interval} chunk {chunk_no}/{len(ranges)}.')
            continue
        actual['is_actual']=True
        if interval == '1minute' and strict_minute_coverage and not _nse50_minute_coverage_ok(actual, max_gap_minutes=5):
            print(f'Historical 1-minute GAP REMAINS: {stock_code} window={label}; saving returned broker candles only; no synthetic actual candles created.', flush=True)
        frames.append(actual)
        print(f'Historical RESPONSE: {stock_code} {interval} chunk {chunk_no}/{len(ranges)}; {len(actual)} candles; coverage {actual["_dt"].min():%H:%M:%S}-{actual["_dt"].max():%H:%M:%S}.')

    if not frames:
        print(f'Historical NO DATA: {stock_code} {interval} on {day}; no usable chunks.')
        return pd.DataFrame()

    out=pd.concat(frames,ignore_index=True)
    out=out.sort_values('_dt').drop_duplicates('_dt',keep='last').reset_index(drop=True)
    out=_add_intraday_cumulative_metrics(out)
    # The 1-second download begins at the trigger minute. Add the cumulative
    # session totals from all earlier 1-minute candles so BUY-time volume and
    # turnover remain comparable with the live cumulative quote.
    out['_cum_volume']=out['_cum_volume'] + float(baseline_volume or 0.0)
    out['_cum_turnover_cr']=out['_cum_turnover_cr'] + float(baseline_turnover_cr or 0.0)
    if interval == '1minute':
        out=_normalize_full_session_minutes(out,day)
    else:
        out=out[(out['_dt']>=session_start)&(out['_dt']<=session_end)].copy()
        out['is_actual']=True
    _save_cached_intraday(day,stock_code,out,interval,resolve_previous_close=True)
    # The complete interval is now in the canonical daily cache. Remove the
    # temporary request checkpoints only after this durable save succeeds.
    removed_checkpoints = _remove_redundant_request_cache_files(day, stock_code)
    if removed_checkpoints:
        print(f'Historical REQUEST CHECKPOINT CLEANUP: {stock_code} {day}; removed={removed_checkpoints}.')
    actual_count=int(out['is_actual'].sum()) if 'is_actual' in out.columns else len(out)
    _historical_verbose_print(f'Historical SAVED: {stock_code} {interval} on {day} ({actual_count} actual candles; coverage {out["_dt"].min():%H:%M:%S}-{out["_dt"].max():%H:%M:%S}).')
    return out

def _add_intraday_cumulative_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Add cumulative volume and turnover available up to each candle/tick.

    Live Breeze uses cumulative traded quantity and traded value. Historical
    candles normally expose per-candle volume and may or may not expose a
    traded-value field, so use that field when present and otherwise use
    close * candle-volume as the deterministic historical traded-value proxy.
    These values are always cumulative only up to the current timestamp; they
    are never taken from the end-of-day bhavcopy.
    """
    x=df.copy()
    if x.empty:
        return x
    if 'volume' not in x.columns:
        x['volume']=0.0
    x['volume']=pd.to_numeric(x['volume'],errors='coerce').fillna(0.0)
    x['_cum_volume']=x['volume'].cumsum()
    turnover_col=next((c for c in x.columns if str(c).strip().lower() in {
        'turnover','turnover_value','traded_value','trade_value','value','ttv','total_traded_value'
    }),None)
    if turnover_col is not None:
        tv=x[turnover_col]
        # Some feeds return traded value with a trailing C for crore.
        def _tv(v):
            try:
                t=str(v).strip().upper().replace(',','')
                if t.endswith('C'): return float(t[:-1])*10000000.0
                return float(t)
            except Exception:
                return 0.0
        x['_candle_turnover_rupees']=tv.map(_tv)
    else:
        close=pd.to_numeric(x.get('close',0),errors='coerce').fillna(0.0)
        x['_candle_turnover_rupees']=close*x['volume']
    x['_cum_turnover_cr']=x['_candle_turnover_rupees'].cumsum()/10000000.0
    return x


def _first_movement_minute(candles, day, cfg, daily_open=None, daily_previous_close=None):
    """Return the first 1-minute candle that reaches the configured movement.

    Hybrid mode uses this inexpensive 1-minute screen as the gate for 1-second
    downloading.  If the movement never occurs, no 1-second request is made.
    The returned minute is the START of the minute so the second-level replay can
    determine the exact crossing timestamp inside that minute.
    """
    try:
        movement=float(cfg.get('open_movement', 0) or 0)
        if movement <= 0 or candles is None or candles.empty:
            return None
        session_open=float(daily_open) if daily_open is not None and float(daily_open)>0 else float(candles.iloc[0]['open'])
        prev_close=float(daily_previous_close) if daily_previous_close is not None else 0.0
        if not direction_ok(session_open, prev_close, cfg.get('open_direction','BOTH')):
            return None
        trigger=entry_level(session_open,cfg,prev_close)
        # Price Big/Price Small is an optional OPEN-price filter.  Never apply
        # it to the trigger price unless the user explicitly enabled it.
        if price_band_enabled(cfg) and not price_band_ok(session_open,cfg):
            return None
        # Only the configured BUY window can establish a new entry.
        # If the trigger was already at/below the session open, or any real
        # candle crossed it before the BUY window, this is a gap/early cross:
        # do not chase it by treating a later candle as a fresh trigger.
        eh, em = [int(x) for x in str(cfg.get('buy_entry_time','09:15')).split(':')]
        xh, xm = [int(x) for x in str(cfg.get('buy_exit_time','15:20')).split(':')]
        start=datetime.combine(day, datetime.min.time()).replace(hour=eh,minute=em)
        end=datetime.combine(day, datetime.min.time()).replace(hour=xh,minute=xm,second=59)
        df=candles.copy().sort_values('_dt')
        # IMPORTANT: synthetic gap-normalizer rows cannot establish a trigger.
        if 'is_actual' in df.columns:
            df=df[df['is_actual'].astype(bool)].copy()
        if df.empty or 'close' not in df.columns or trigger <= session_open:
            return None
        df['close']=pd.to_numeric(df['close'],errors='coerce')
        prior=df[df['_dt'] < start]
        # A qualifying close before the entry window invalidates the later setup.
        # Intrabar highs alone do not count as a trigger.
        if not prior.empty and bool((prior['close'] >= trigger).any()):
            return None
        df=df[(df['_dt']>=start)&(df['_dt']<=end)].copy()
        if df.empty:
            return None
        df=_add_intraday_cumulative_metrics(df)
        # HYBRID DOWNLOAD FILTER ONLY:
        # The 1-minute stage is allowed to decide *when to download* 1-second
        # data, but it must NOT reject a symbol because its minute-level
        # cumulative volume/turnover has not yet crossed the configured BUY
        # thresholds.  Those conditions are part of the final trade decision
        # and MUST be recalculated from the 1-second replay.  Applying them here
        # was the reason qualifying symbols could disappear before 1-second
        # download (for example PROTEAN/PROEGO).
        # A completed one-minute CLOSE at/above the previous-close trigger is
        # the first qualifying signal. Intrabar HIGH touches are explicitly ignored.
        hit=df[df['close']>=trigger].copy()
        if hit.empty:
            return None
        return hit.iloc[0]['_dt'].replace(second=0,microsecond=0)
    except Exception:
        return None


def _historical_trigger_diagnostic(candles, day, cfg, logic_name, symbol, daily_open=None, daily_previous_close=None):
    """Return a non-trading diagnostic explaining exactly why a 1-minute trigger passed/failed.

    This function is intentionally diagnostic-only: it does not change the trading
    decision.  It reports data coverage, first high crossing, cumulative volume and
    turnover at that crossing, buy-window status, and the final 1-minute gate result.
    """
    out = {
        'symbol': str(symbol or '').strip().upper(),
        'logic': str(logic_name or ''),
        'status': 'FAIL',
        'reason': 'UNKNOWN',
        'trigger_price': None,
        'daily_open': daily_open,
        'first_data_time': None,
        'last_data_time': None,
        'actual_candles': 0,
        'first_high_cross_time': None,
        'first_high_cross_high': None,
        'cross_volume': None,
        'cross_turnover_cr': None,
        'first_valid_trigger_time': None,
    }
    try:
        if candles is None or candles.empty:
            out['reason'] = 'NO_1MINUTE_DATA'
            return out
        x = candles.copy().sort_values('_dt')
        if '_dt' not in x.columns:
            out['reason'] = 'NO_TIMESTAMP_COLUMN'
            return out
        actual = x[x.get('is_actual', pd.Series(True, index=x.index)).astype(bool)].copy() if 'is_actual' in x.columns else x.copy()
        out['actual_candles'] = int(len(actual))
        if not actual.empty:
            out['first_data_time'] = actual['_dt'].min()
            out['last_data_time'] = actual['_dt'].max()
        if daily_open is None or float(daily_open) <= 0:
            out['reason'] = 'INVALID_DAILY_OPEN'
            return out
        session_open = float(daily_open)
        prev_close = float(daily_previous_close) if daily_previous_close is not None else 0.0
        if not direction_ok(session_open, prev_close, cfg.get('open_direction', 'BOTH')):
            out['reason'] = 'DIRECTION_FAIL'
            return out
        if price_band_enabled(cfg) and not price_band_ok(session_open, cfg):
            out['reason'] = 'PRICE_BAND_FAIL'
            return out
        trigger = float(entry_level(session_open, cfg, prev_close))
        out['trigger_price'] = trigger
        if 'close' not in actual.columns:
            out['reason'] = 'NO_CLOSE_COLUMN'
            return out
        actual['close'] = pd.to_numeric(actual['close'], errors='coerce')
        close_rows = actual[actual['close'] >= trigger].copy()
        if close_rows.empty:
            max_close = pd.to_numeric(actual['close'], errors='coerce').max()
            out['reason'] = 'NO_1M_CLOSE_AT_TRIGGER'
            out['max_close'] = float(max_close) if pd.notna(max_close) else None
            return out

        eh, em = [int(v) for v in str(cfg.get('buy_entry_time', '09:15')).split(':')]
        xh, xm = [int(v) for v in str(cfg.get('buy_exit_time', '15:20')).split(':')]
        start = datetime.combine(day, datetime.min.time()).replace(hour=eh, minute=em)
        end = datetime.combine(day, datetime.min.time()).replace(hour=xh, minute=xm, second=59)
        first_cross = close_rows.iloc[0]
        out['first_high_cross_time'] = first_cross['_dt']
        out['first_high_cross_high'] = float(first_cross['close']) if pd.notna(first_cross['close']) else None
        if trigger <= session_open:
            out['reason'] = 'TRIGGER_ALREADY_REACHED_AT_OPEN'
            return out
        if first_cross['_dt'] < start:
            out['reason'] = 'TRIGGER_BEFORE_BUY_WINDOW'
            return out
        if first_cross['_dt'] > end:
            out['reason'] = 'TRIGGER_AFTER_BUY_WINDOW'
            return out
        first_high = first_cross
        out['first_high_cross_time'] = first_high['_dt']
        out['first_high_cross_high'] = float(first_high['high']) if pd.notna(first_high['high']) else None

        # Match the current historical 1-minute gate exactly: cumulative metrics
        # are calculated over the configured buy-window rows from its beginning.
        window = actual[(actual['_dt'] >= start) & (actual['_dt'] <= end)].copy()
        if window.empty:
            out['reason'] = 'NO_DATA_IN_BUY_WINDOW'
            return out
        window = _add_intraday_cumulative_metrics(window)
        row = window[window['_dt'] == first_high['_dt']]
        if row.empty:
            row = window[window['_dt'] >= first_high['_dt']].head(1)
        if row.empty:
            out['reason'] = 'CROSS_NOT_IN_WINDOW_DATA'
            return out
        rr = row.iloc[0]
        out['cross_volume'] = float(rr.get('_cum_volume', 0.0) or 0.0)
        out['cross_turnover_cr'] = float(rr.get('_cum_turnover_cr', 0.0) or 0.0)
        min_vol = float(cfg.get('volume', 0) or 0)
        min_turn = float(cfg.get('turnover_cr', 0) or 0)
        vol_ok = min_vol <= 0 or out['cross_volume'] >= min_vol
        turn_ok = min_turn <= 0 or out['cross_turnover_cr'] >= min_turn
        if not vol_ok and not turn_ok:
            out['reason'] = 'VOLUME_AND_TURNOVER_FAIL'
        elif not vol_ok:
            out['reason'] = 'VOLUME_FAIL'
        elif not turn_ok:
            out['reason'] = 'TURNOVER_FAIL'
        else:
            out['status'] = 'PASS'
            out['reason'] = '1MINUTE_TRIGGER_PASS'
            out['first_valid_trigger_time'] = first_high['_dt'].replace(second=0, microsecond=0)
        return out
    except Exception as exc:
        out['reason'] = f'DIAGNOSTIC_ERROR:{type(exc).__name__}:{exc}'
        return out


def _print_historical_trigger_diagnostic(diag):
    """Print one compact, human-readable per-share/logic trigger diagnostic."""
    def _fmt_dt(v):
        try:
            return v.strftime('%H:%M:%S') if v is not None and hasattr(v, 'strftime') else '-'
        except Exception:
            return str(v or '-')
    def _fmt_num(v, digits=2):
        try:
            return f'{float(v):.{digits}f}'
        except Exception:
            return '-'
    print(
        'Historical TRIGGER DIAGNOSTIC '
        f"{diag.get('symbol','-')} {diag.get('logic','-')}: "
        f"status={diag.get('status','FAIL')}; reason={diag.get('reason','UNKNOWN')}; "
        f"open={_fmt_num(diag.get('daily_open'))}; trigger={_fmt_num(diag.get('trigger_price'))}; "
        f"1m_coverage={_fmt_dt(diag.get('first_data_time'))}-{_fmt_dt(diag.get('last_data_time'))}; "
        f"actual={diag.get('actual_candles',0)}; "
        f"first_high_cross={_fmt_dt(diag.get('first_high_cross_time'))}; "
        f"high={_fmt_num(diag.get('first_high_cross_high'))}; "
        f"cum_volume_at_cross={_fmt_num(diag.get('cross_volume'),0)}; "
        f"cum_turnover_cr_at_cross={_fmt_num(diag.get('cross_turnover_cr'))}; "
        f"valid_trigger={_fmt_dt(diag.get('first_valid_trigger_time'))}"
    )



def _historical_replay_1second_path(day: date, stock_code: str) -> Path:
    """Return the SAME canonical date+symbol CSV used by all historical data."""
    return _canonical_historical_csv_path(day, stock_code)


def _save_historical_replay_1second(day: date, stock_code: str, candles: pd.DataFrame) -> None:
    """Merge 1-second replay candles into the one canonical historical CSV."""
    if candles is None or candles.empty:
        return
    outdir = _cache_day_dir(day)
    df = _add_previous_close_column(candles.copy(), day, stock_code).sort_values('_dt').drop_duplicates('_dt', keep='last').reset_index(drop=True)
    csv_path = _historical_replay_1second_path(day, stock_code)
    export = df.copy()
    if '_dt' in export.columns:
        export['datetime'] = pd.to_datetime(export['_dt']).dt.strftime('%Y-%m-%d %H:%M:%S')
        export = export.drop(columns=['_dt'])
    export['interval'] = '1second'
    existing = pd.DataFrame()
    if csv_path.exists():
        try:
            existing = pd.read_csv(csv_path)
        except Exception:
            existing = pd.DataFrame()
    combined = pd.concat([existing, export], ignore_index=True, sort=False)
    if 'datetime' in combined.columns:
        combined['_sort_dt'] = pd.to_datetime(combined['datetime'], errors='coerce')
        combined = combined.drop_duplicates(subset=['interval','datetime'], keep='last')
        combined = combined.sort_values(['interval','_sort_dt']).drop(columns=['_sort_dt'])
    combined = _drop_52_week_export_columns(combined)
    combined.to_csv(csv_path, index=False)
    second_export = combined[combined['interval'].astype(str).str.lower().eq('1second')].copy()
    removed = _remove_redundant_request_cache_files(day, stock_code)
    print(f'Historical 1-second canonical SAVE: {stock_code} {day}; {len(second_export):,} candles; ONE CSV={csv_path.name}; removed redundant request files={removed}.')


def _load_historical_replay_1second(day: date, stock_code: str, start: datetime, end: datetime):
    """Reuse the consolidated replay file when it covers the requested window."""
    path = _historical_replay_1second_path(day, stock_code)
    if not path.exists():
        return None
    try:
        raw = pd.read_csv(path)
        if 'interval' in raw.columns:
            raw = raw[raw['interval'].astype(str).str.lower().eq('1second')].copy()
        df = _candle_dataframe(raw.to_dict('records'))
        if df is not None and not df.empty:
            had_previous_close = 'previous_close' in raw.columns and pd.to_numeric(raw['previous_close'], errors='coerce').gt(0).any()
            df = _add_previous_close_column(df, day, stock_code)
            if not had_previous_close:
                _save_historical_replay_1second(day, stock_code, df)
        if df is None or df.empty or '_dt' not in df.columns:
            return None
        actual = df[(df['_dt'] >= start) & (df['_dt'] <= end)].copy()
        if actual.empty:
            return None
        # Reuse only when the stored file actually covers both requested endpoints.
        if df['_dt'].min() > start or df['_dt'].max() < end:
            return None
        actual['is_actual'] = True
        print(f'Historical 1-second consolidated CACHE REUSE: {stock_code} {day}; {len(actual):,} candles; coverage {actual["_dt"].min():%H:%M:%S}-{actual["_dt"].max():%H:%M:%S}.')
        return actual.sort_values('_dt').drop_duplicates('_dt', keep='last').reset_index(drop=True)
    except Exception as exc:
        print(f'Historical 1-second consolidated cache warning: {stock_code} {day}: {exc}')
        return None


def _build_historical_second_data_universe(selected_timing, global_share_cap, day):
    """Return the CURRENT day's capped selection and its authoritative 1-second universe.

    This helper is shared by both fresh ranking and saved-manifest replay.  It applies
    the current GUI cap itself, then validates the resulting unique-symbol universe.
    That makes the cap an invariant at the exact boundary before replay/download.
    """
    cap = int(global_share_cap or 0)
    # De-duplicate before applying the cap.  A symbol can appear more than once
    # when multiple GUI logics qualify it; duplicate rows must not consume slots
    # or cause fewer than the configured number of unique shares to be selected.
    selected = []
    seen_symbols = set()
    for row in list(selected_timing or []):
        try:
            symbol = str(row[1]).strip().upper()
        except Exception:
            continue
        if not symbol or symbol in seen_symbols:
            continue
        seen_symbols.add(symbol)
        selected.append(row)
        if cap > 0 and len(selected) >= cap:
            break
    universe = {str(row[1]).strip().upper() for row in selected}
    if cap > 0 and len(universe) > cap:
        raise RuntimeError(
            f'SECOND-DATA UNIVERSE CAP VIOLATION: {day} has '
            f'{len(universe)} unique symbols but the GUI cap is {cap}.'
        )
    return selected, universe

def _historical_second_confirmation(breeze, stock_code, day, trigger_minute, baseline_volume=0.0, baseline_turnover_cr=0.0):
    _refresh_latest_gui_logic_settings()
    """Return one consolidated 1-second replay window per selected share/date.

    The first run downloads only trigger-minute -> 15:24:59.  On later runs the
    existing consolidated file is authoritative.  If a newly calculated trigger
    starts earlier than the cached window, only the missing prefix is requested
    and merged back into the SAME file.  No duplicate per-chunk files are created.
    """
    if trigger_minute is None:
        return pd.DataFrame()
    session_start=datetime.combine(day, datetime.min.time()).replace(hour=9,minute=15)
    squareoff_end=datetime.combine(day, datetime.min.time()).replace(hour=15,minute=24,second=59)
    start=max(session_start,trigger_minute.replace(second=0,microsecond=0))
    if start>squareoff_end:
        return pd.DataFrame()

    path=_historical_replay_1second_path(day,stock_code)
    existing=pd.DataFrame()

    # FAST CACHE PATH: the canonical one-file-per-share/date CSV is authoritative.
    # Check it FIRST and stop immediately when it already covers the required
    # trigger->15:24:59 window.  The old implementation parsed every alternate
    # historical CSV even when the canonical file was already complete.  That was
    # pure local I/O/CPU and became noticeable with many 1-second files.
    def _read_second_cache(cache_path):
        try:
            raw = pd.read_csv(cache_path)
            if 'interval' in raw.columns:
                raw = raw[raw['interval'].astype(str).str.strip().str.lower().eq('1second')].copy()
            return _candle_dataframe(raw.to_dict('records'))
        except Exception as exc:
            print(f'Historical 1-second cache warning: {cache_path.name}: {exc}')
            return pd.DataFrame()

    if path.exists():
        canonical = _read_second_cache(path)
        if canonical is not None and not canonical.empty and '_dt' in canonical.columns:
            canonical = canonical[(canonical['_dt'] >= session_start) & (canonical['_dt'] <= squareoff_end)].copy()
            if not canonical.empty:
                old_start = canonical['_dt'].min()
                old_end = canonical['_dt'].max()
                if old_start <= start and old_end >= squareoff_end:
                    actual = canonical[(canonical['_dt'] >= start) & (canonical['_dt'] <= squareoff_end)].copy()
                    actual['is_actual'] = True
                    actual['_cum_volume'] = pd.to_numeric(actual.get('_cum_volume', 0), errors='coerce').fillna(0.0) + float(baseline_volume or 0.0)
                    actual['_cum_turnover_cr'] = pd.to_numeric(actual.get('_cum_turnover_cr', 0), errors='coerce').fillna(0.0) + float(baseline_turnover_cr or 0.0)
                    print(f'Historical 1-second FAST CACHE REUSE: {stock_code} {day}; no Breeze download; coverage {old_start:%H:%M:%S}-{old_end:%H:%M:%S}.')
                    return actual.sort_values('_dt').drop_duplicates('_dt', keep='last').reset_index(drop=True)
                existing = canonical
                print(f'Historical 1-second canonical cache partial: {stock_code} {day}; coverage {old_start:%H:%M:%S}-{old_end:%H:%M:%S}; checking only legacy caches for missing coverage.')

    # Only if the canonical file is absent or incomplete do we inspect legacy/older
    # cache files. This preserves backward compatibility while avoiding repeated
    # parsing of every candidate file on the normal fast path.
    if existing.empty:
        candidate_frames=[]
        for cache_path in _cached_intraday_candidates(day,stock_code,'1second'):
            if cache_path == path or not cache_path.exists():
                continue
            df = _read_second_cache(cache_path)
            if df is not None and not df.empty:
                candidate_frames.append((cache_path,df))
        if candidate_frames:
            _chosen_path, existing=max(
                candidate_frames,
                key=lambda item: ((item[1]['_dt'].max()-item[1]['_dt'].min()).total_seconds(), len(item[1]))
            )
            print(f'Historical 1-second CACHE FOUND: {_chosen_path.name} reused for {stock_code} {day}.')

    if not existing.empty:
        existing=existing[(existing['_dt']>=session_start)&(existing['_dt']<=squareoff_end)].copy()
        old_start=existing['_dt'].min() if not existing.empty else None
        old_end=existing['_dt'].max() if not existing.empty else None
        if old_start is not None and old_end is not None and old_start<=start and old_end>=squareoff_end:
            actual=existing[(existing['_dt']>=start)&(existing['_dt']<=squareoff_end)].copy()
            actual['is_actual']=True
            actual['_cum_volume']=pd.to_numeric(actual.get('_cum_volume',0),errors='coerce').fillna(0.0)+float(baseline_volume or 0.0)
            actual['_cum_turnover_cr']=pd.to_numeric(actual.get('_cum_turnover_cr',0),errors='coerce').fillna(0.0)+float(baseline_turnover_cr or 0.0)
            print(f'Historical 1-second consolidated CACHE REUSE: {stock_code} {day}; no Breeze download; coverage {old_start:%H:%M:%S}-{old_end:%H:%M:%S}.')
            return actual.sort_values('_dt').drop_duplicates('_dt',keep='last').reset_index(drop=True)

    # Fetch only the missing ranges. In the normal case this is one range; if
    # an older cache already covers part of the window, we never re-request the
    # already saved timestamps.
    ranges=[]
    if existing.empty:
        ranges=[(start,squareoff_end)]
    else:
        old_start=existing['_dt'].min(); old_end=existing['_dt'].max()
        if start < old_start:
            ranges.append((start,min(squareoff_end,old_start-timedelta(seconds=1))))
        if old_end < squareoff_end:
            ranges.append((max(start,old_end+timedelta(seconds=1)),squareoff_end))
    ranges=[(a,b) for a,b in ranges if a<=b]
    if not ranges:
        return pd.DataFrame()
    if breeze is None:
        raise HistoricalBreezeRequired(f'Breeze session required because cached 1-second data is incomplete for {stock_code} on {day}.')

    frames=[]
    for range_start,range_end in ranges:
        cur=range_start
        chunk_no=0
        total_chunks=math.ceil(((range_end-cur).total_seconds()+1)/1000)
        while cur<=range_end:
            chunk_end=min(range_end,cur+timedelta(seconds=999))
            chunk_no+=1
            data=_historical_v2(breeze,stock_code,'1second',cur,chunk_end)
            if data:
                frame=_candle_dataframe(data)
                if not frame.empty:
                    frame=frame[(frame['_dt']>=cur)&(frame['_dt']<=chunk_end)].copy()
                    if not frame.empty:
                        frame['is_actual']=True; frames.append(frame)
                        print(f'Historical 1-second confirmation: {stock_code} chunk {chunk_no}/{total_chunks}; {len(frame)} candles; {cur:%H:%M:%S}-{chunk_end:%H:%M:%S}.')
            else:
                print(f'Historical 1-second confirmation: {stock_code} chunk {chunk_no}/{total_chunks} returned no data; {cur:%H:%M:%S}-{chunk_end:%H:%M:%S}.')
            cur=chunk_end+timedelta(seconds=1)

    if not frames and existing.empty:
        return pd.DataFrame()
    combined=pd.concat([x for x in [existing,*frames] if x is not None and not x.empty],ignore_index=True)
    combined=combined.sort_values('_dt').drop_duplicates('_dt',keep='last').reset_index(drop=True)
    combined=combined[(combined['_dt']>=session_start)&(combined['_dt']<=squareoff_end)].copy()
    _save_historical_replay_1second(day,stock_code,combined)

    replay=combined[(combined['_dt']>=start)&(combined['_dt']<=squareoff_end)].copy()
    replay['is_actual']=True
    replay['_cum_volume']=pd.to_numeric(replay.get('_cum_volume',0),errors='coerce').fillna(0.0)+float(baseline_volume or 0.0)
    replay['_cum_turnover_cr']=pd.to_numeric(replay.get('_cum_turnover_cr',0),errors='coerce').fillna(0.0)+float(baseline_turnover_cr or 0.0)
    print(f'Historical HYBRID 1-second window ready: {stock_code} on {day}; saved ONE consolidated file; coverage {combined["_dt"].min():%H:%M:%S}-{combined["_dt"].max():%H:%M:%S}; new Breeze ranges={len(ranges)}.')
    return replay

def _apply_15m_orb_breakout(df, day):
    """Return a copy with the confirmed 15-minute opening-range breakout mask."""
    out = df.copy()
    out['_orb15_breakout'] = False
    try:
        orb_start = datetime.combine(day, datetime.min.time()).replace(hour=9, minute=15)
        orb_end = datetime.combine(day, datetime.min.time()).replace(hour=9, minute=30)
        opening = out[(out['_dt'] >= orb_start) & (out['_dt'] < orb_end)]
        orb_high = float(pd.to_numeric(opening['high'], errors='coerce').max()) if not opening.empty else 0.0
        if orb_high <= 0:
            return out
        post_open = out['_dt'] >= orb_end
        prev_close = pd.to_numeric(out['close'], errors='coerce').shift(1).fillna(0.0)
        bar_open = pd.to_numeric(out['open'], errors='coerce').fillna(0.0)
        bar_high = pd.to_numeric(out['high'], errors='coerce').fillna(0.0)
        bar_close = pd.to_numeric(out['close'], errors='coerce').fillna(0.0)
        cross = post_open & (prev_close <= orb_high) & (bar_open <= orb_high) & (bar_high > orb_high)
        crossed = cross.cummax()
        out['_orb15_breakout'] = (crossed & post_open & (bar_close > orb_high))
    except Exception:
        out['_orb15_breakout'] = False
    return out


def _simulate_logic_from_candles_uncached(breeze, stock_code, symbol, day, candles, logic_name,
                                  budget=None, stop_pct=None, target_pct=None,
                                  trail_pct=None, interval='1minute', daily_open=None,
                                  daily_high=None, daily_previous_close=None,
                                  daily_volume=None, daily_turnover_cr=None,
                                  smart_rank_context=None):
    """Replay one configured logic.  Historical, paper and live use the same rule values."""
    cfg = get_logic(logic_name)
    budget = float(cfg['buy_amount']) if budget is None else float(budget)
    if not cfg.get('enabled', True):
        return {'mode':'historical','logic':logic_name,'symbol':symbol,'trade_date':day,'status':'LOGIC_DISABLED'}
    if candles is None or candles.empty:
        return {'mode':'historical','logic':logic_name,'symbol':symbol,'trade_date':day,'status':'NO_INTRADAY_DATA'}
    df=candles.copy().sort_values('_dt').reset_index(drop=True)
    df=df[(df['_dt']>=datetime.combine(day, datetime.min.time()).replace(hour=9,minute=15)) &
          (df['_dt']<=datetime.combine(day, datetime.min.time()).replace(hour=15,minute=24,second=59))].copy()
    if df.empty or not {'open','high','low','close'}.issubset(df.columns):
        return {'mode':'historical','logic':logic_name,'symbol':symbol,'trade_date':day,'status':'NO_INTRADAY_DATA'}
    for c in ('open','high','low','close','volume'):
        if c in df.columns: df[c]=pd.to_numeric(df[c],errors='coerce')
    df['volume']=df.get('volume',pd.Series(0,index=df.index)).fillna(0)
    # Hybrid 1-second replay may already contain the pre-trigger session
    # baseline.  _add_intraday_cumulative_metrics() normally rebuilds cumulative
    # values from the supplied frame, which would silently discard that baseline
    # because the frame starts at the trigger minute. Preserve it for the final
    # BUY-time validation so cached and freshly downloaded 1-second data behave
    # identically to the full-session live/paper engine.
    _existing_cum_volume = pd.to_numeric(df['_cum_volume'], errors='coerce').copy() if '_cum_volume' in df.columns else None
    _existing_cum_turnover = pd.to_numeric(df['_cum_turnover_cr'], errors='coerce').copy() if '_cum_turnover_cr' in df.columns else None
    df=_add_intraday_cumulative_metrics(df)
    if _existing_cum_volume is not None and len(_existing_cum_volume)==len(df):
        if _existing_cum_volume.notna().any():
            df['_cum_volume']=_existing_cum_volume.fillna(df['_cum_volume'])
    if _existing_cum_turnover is not None and len(_existing_cum_turnover)==len(df):
        if _existing_cum_turnover.notna().any():
            df['_cum_turnover_cr']=_existing_cum_turnover.fillna(df['_cum_turnover_cr'])
    # Signal detection must use only actual Breeze candles.  The 1-minute
    # session cache may contain synthetic gap rows used only for continuity.
    if 'is_actual' in df.columns:
        df=df[df['is_actual'].astype(bool)].copy()
    if df.empty:
        return {'mode':'historical','logic':logic_name,'symbol':symbol,'trade_date':day,
                'status':'NO_INTRADAY_DATA','data_source':f'Breeze Historical V2 {interval}; no actual candles'}
    # The ORB mask is computed before the configured entry window is applied,
    # so a later Buy Entry Time can still use the 09:15-09:30 opening range.
    df['_orb15_breakout'] = False
    if bool(cfg.get('require_15m_orb', False)):
        df = _apply_15m_orb_breakout(df, day)
    # BUY-time cumulative volume/turnover are evaluated from the intraday
    # timeline below. The daily NSE values are retained only as a necessary
    # universe pre-filter; they are never used as proof that an earlier BUY
    # already satisfied the thresholds.
    try:
        day_volume=float(daily_volume) if daily_volume is not None else float(df['_cum_volume'].iloc[-1])
    except Exception:
        day_volume=float(df['_cum_volume'].iloc[-1]) if '_cum_volume' in df.columns and not df.empty else 0.0
    try:
        day_turnover=float(daily_turnover_cr) if daily_turnover_cr is not None else float(df['_cum_turnover_cr'].iloc[-1])
    except Exception:
        day_turnover=float(df['_cum_turnover_cr'].iloc[-1]) if '_cum_turnover_cr' in df.columns and not df.empty else 0.0
    try:
        session_open=float(daily_open) if daily_open is not None and float(daily_open)>0 else float(df.iloc[0]['open'])
    except Exception:
        session_open=float(df.iloc[0]['open'])
    try:
        prev_close=float(daily_previous_close) if daily_previous_close is not None else 0.0
    except Exception:
        prev_close=0.0
    trigger=entry_level(session_open,cfg,prev_close)
    # THE SAME BUY evaluator used by LIVE/PAPER. Historical Shadow converts each
    # 1-second candle into the exact live-style market-state snapshot and calls
    # canonical_tick_entry(); it does not maintain a separate historical BUY rule.
    # FAST CANONICAL ENTRY SCAN
    # The rules are unchanged; only the implementation is vectorized.  The old
    # row-by-row canonical evaluator could execute millions of Python iterations
    # over 1-second data.  All inputs below are the same values used by the
    # canonical BUY gate, but evaluated as NumPy/Pandas masks.
    daily_entry=None
    first=None
    _entry_start, _entry_end = _parse_hhmm(cfg.get('buy_entry_time','09:15')), _parse_hhmm(cfg.get('buy_exit_time','15:20'))
    _start_ts = datetime.combine(day, datetime.min.time()).replace(hour=_entry_start[0], minute=_entry_start[1])
    _end_ts = datetime.combine(day, datetime.min.time()).replace(hour=_entry_end[0], minute=_entry_end[1], second=59)
    _scan = df[(df['_dt'] >= _start_ts) & (df['_dt'] <= _end_ts)].copy().sort_values('_dt')
    if not _scan.empty:
        for _c in ('close','open','high','low','volume','_cum_volume','_cum_turnover_cr'):
            if _c in _scan.columns:
                _scan[_c] = pd.to_numeric(_scan[_c], errors='coerce').fillna(0.0)
        _px = _scan['close'].to_numpy(dtype=float)
        # Previous observed price is retained for first-upward-touch validation
        # and is shared by every historical logic mode.
        _prior_before_window = pd.to_numeric(
            df.loc[df['_dt'] < _scan['_dt'].iloc[0], 'close'], errors='coerce'
        ).dropna()
        _prior_seed = float(_prior_before_window.iloc[-1]) if not _prior_before_window.empty else float(session_open or 0.0)
        _prior_px = np.r_[_prior_seed, _px[:-1]]
        _cum_vol = _scan['_cum_volume'].to_numpy(dtype=float)
        _cum_turn = _scan['_cum_turnover_cr'].to_numpy(dtype=float)
        _valid_px = _px > 0
        _trigger = float(trigger)
        # This is the same bounded trigger predicate used by live/paper ticks.
        # Keeping one vectorized implementation prevents historical replay from
        # requiring exact tick-price equality while LIVE accepts valid overshoots.
        _movement_qualifies = entry_movement_qualifies_mask(_px, session_open, prev_close, cfg)
        _movement_only = bool(cfg.get('movement_only_mode', False))
        _is_smart = bool(cfg.get('smart_mode', False)) and not _movement_only
        _ma_ok_mask = np.ones(len(_scan), dtype=bool)
        if _ma_filter_enabled(cfg):
            _ma_pass,_ma_audit=_ma_filter_check(symbol,day,_px[0] if len(_px) else 0.0,cfg)
            _ma_vals=_ma_audit.get('values',{}) if isinstance(_ma_audit,dict) else {}
            if _ma_audit.get('enabled'):
                _ma_ok_mask = np.ones(len(_scan),dtype=bool)
                for _mk,_vk in [('ma_filter_200','ma_200'),('ma_filter_100','ma_100'),('ma_filter_20','ma_20'),('ma_filter_5','ma_5')]:
                    if bool(cfg.get(_mk,False)):
                        _mv=_ma_vals.get(_vk)
                        _ma_ok_mask &= np.isfinite(_mv) & (_px > float(_mv)) if _mv is not None else False
                if bool(cfg.get('ma_filter_cross_5',False)):
                    _ma_ok_mask &= bool(_ma_vals.get('cross_5',False))

        if _movement_only:
            # Movement-only changes the simulated fill to the exact trigger; it
            # does not change the canonical signal predicate. LIVE/PAPER accept
            # the same bounded overshoot band and reject gap-up opens.
            _direction_ok = direction_ok(session_open, prev_close, cfg.get('open_direction', 'BOTH'))
            _price_band_ok = (not price_band_enabled(cfg)) or open_price_band_ok(session_open, cfg)
            _min_vol = max(0.0, float(cfg.get('volume', 0) or 0))
            _min_turn = max(0.0, float(cfg.get('turnover_cr', 0) or 0))
            _volume_ok = (_cum_vol >= _min_vol) if _min_vol > 0 else np.ones(len(_scan), dtype=bool)
            _turnover_ok = (_cum_turn >= _min_turn) if _min_turn > 0 else np.ones(len(_scan), dtype=bool)
            _passed = (
                _valid_px & _ma_ok_mask & _movement_qualifies
                & bool(_direction_ok) & bool(_price_band_ok)
                & _volume_ok & _turnover_ok
            )
        elif _is_smart:
            _vwap = np.divide(
                _cum_turn * 10000000.0, _cum_vol,
                out=np.zeros_like(_cum_turn, dtype=float),
                where=_cum_vol > 0
            )
            _market_up = (session_open > prev_close) if (session_open > 0 and prev_close > 0) else None
            _require_market = bool(cfg.get('smart_require_market_up', True))
            _market_mask = np.full(len(_scan), bool(_market_up) if _require_market else True, dtype=bool)
            _above_open = _px >= float(session_open)
            _require_vwap = bool(cfg.get('smart_require_vwap', True))
            _above_vwap = (_vwap > 0) & (_px >= _vwap) if _require_vwap else np.ones(len(_scan), dtype=bool)
            # Optimized Smart contract: 15-minute momentum, 5-minute relative
            # volume and cross-sectional momentum rank.  These metrics are derived
            # only from data available at or before the current 5-minute bucket.
            try:
                _m5 = _scan.set_index('_dt').resample('5min', label='left', closed='left').agg({
                    'open':'first','high':'max','low':'min','close':'last','volume':'sum'
                }).dropna(subset=['close'])
                _m5['mom15'] = (_m5['close'] / _m5['close'].shift(3) - 1.0) * 100.0
                _m5['avgvol20'] = _m5['volume'].shift(1).rolling(20, min_periods=5).median()
                _m5['bucket_vol'] = _m5['volume']
                _mom_lookup = _m5['mom15'].to_dict()
                _avg_lookup = _m5['avgvol20'].to_dict()
                _bucket_vol_lookup = _m5['bucket_vol'].to_dict()
            except Exception:
                _mom_lookup, _avg_lookup, _bucket_vol_lookup = {}, {}, {}
            _buckets = _scan['_dt'].dt.floor('5min')
            _mom15 = np.array([float(_mom_lookup.get(b, np.nan)) for b in _buckets], dtype=float)
            _avg5 = np.array([float(_avg_lookup.get(b, np.nan)) for b in _buckets], dtype=float)
            # Current 5-minute volume must be accumulated only up to this 1-second
            # observation, not the full day's cumulative volume.
            _scan['_smart_bucket'] = _buckets
            _scan['_smart_bar_volume'] = _scan.groupby('_smart_bucket', sort=False)['volume'].cumsum().to_numpy(dtype=float)
            _cur5 = _scan['_smart_bar_volume'].to_numpy(dtype=float)
            _momentum_min = float(cfg.get('smart_momentum_pct', 0.0) or 0.0)
            _momentum = np.isfinite(_mom15) & ((_mom15 >= _momentum_min) if _momentum_min > 0 else True)
            _ratio_min = max(0.0, float(cfg.get('smart_min_volume_ratio', 1.0) or 1.0))
            _ratio_ok = np.ones(len(_scan), dtype=bool) if _ratio_min <= 1.0 else (np.isfinite(_avg5) & (_avg5 > 0) & ((_cur5 / _avg5) >= _ratio_min))
            _min_vol = max(0.0, float(cfg.get('volume', 0) or 0))
            _min_turn = max(0.0, float(cfg.get('turnover_cr', 0) or 0))
            _volume_ok = _cum_vol >= _min_vol
            _turnover_ok = _cum_turn >= _min_turn
            _rank_limit = max(0, int(float(cfg.get('smart_rank_limit', 0) or 0)))
            _rank_by=str(cfg.get('smart_rank_by','movement') or 'movement').lower()
            _rank_num = np.array([float(smart_rank_context.get((symbol.upper(), b, _rank_by), smart_rank_context.get((symbol.upper(), b), np.nan))) if smart_rank_context is not None else np.nan for b in _buckets], dtype=float)
            _rank_ok = (_rank_limit == 0) | (np.isfinite(_rank_num) & (_rank_num <= _rank_limit))
            _require_breakout = bool(cfg.get('smart_require_breakout', True))
            _breakout = _movement_qualifies if (_require_breakout and float(cfg.get('open_movement', 0) or 0) > 0) else ((_px >= _trigger) if _require_breakout else np.ones(len(_scan), dtype=bool))
            _movement_exact = _movement_qualifies
            _positive = _px >= float(session_open)
            _score = (
                2 * (_market_mask.astype(int) + _above_open.astype(int) +
                     _above_vwap.astype(int) + _momentum.astype(int))
                + 2 * (_volume_ok & _ratio_ok).astype(int)
                + _breakout.astype(int)
                + _rank_ok.astype(int)
                + _positive.astype(int)
            )
            _required_score = int(max(0, min(12, float(cfg.get('smart_min_score', 9) or 9))))
            _passed = (
                _valid_px & _ma_ok_mask & (float(session_open) > 0) & _movement_exact &
                (_score >= _required_score) &
                _market_mask & _above_open & _above_vwap & _momentum &
                _volume_ok & _turnover_ok & _ratio_ok & _breakout & _rank_ok
            )
        else:
            _direction_ok = direction_ok(session_open, prev_close, cfg.get('open_direction', 'BOTH'))
            _price_band_ok = (not price_band_enabled(cfg)) or open_price_band_ok(session_open, cfg)
            _movement_ok = _movement_qualifies & _ma_ok_mask
            _min_vol = max(0.0, float(cfg.get('volume', 0) or 0))
            _min_turn = max(0.0, float(cfg.get('turnover_cr', 0) or 0))
            _volume_ok = _cum_vol >= _min_vol if _min_vol > 0 else np.ones(len(_scan), dtype=bool)
            _turnover_ok = _cum_turn >= _min_turn if _min_turn > 0 else np.ones(len(_scan), dtype=bool)
            _passed = (
                _valid_px & bool(_direction_ok) & bool(_price_band_ok) &
                _movement_ok & _volume_ok & _turnover_ok
            )

        # Common VWAP/RVOL gates are applied after the movement branch so
        # Historical, Paper and Live all enforce the same saved GUI conditions.
        _hist_vwap = np.divide(
            _cum_turn * 10000000.0, _cum_vol,
            out=np.zeros_like(_cum_turn, dtype=float), where=_cum_vol > 0
        )
        _scan['_entry_vwap'] = _hist_vwap
        # Movement-only entries fill at the exact rounded trigger, not the candle
        # close. Evaluate the VWAP and minimum-price gates against that same fill
        # price to avoid selecting a historical signal that the final gate rejects.
        _decision_px = np.full(len(_scan), float(_trigger), dtype=float) if _movement_only else _px
        _min_price_gate = max(0.0, float(cfg.get('minimum_price', 0.0) or 0.0))
        if _min_price_gate > 0:
            _passed = _passed & (_decision_px >= _min_price_gate)
        if bool(cfg.get('smart_require_vwap', False)):
            _passed = _passed & np.isfinite(_hist_vwap) & (_hist_vwap > 0) & (_decision_px > _hist_vwap)
        _rvol_min = max(0.0, float(cfg.get('smart_min_volume_ratio', 0.0) or 0.0))
        if _rvol_min > 0:
            try:
                _hist_bucket = _scan['_dt'].dt.floor('5min')
                _scan['_hist_bucket'] = _hist_bucket
                _hist_m5 = _scan.set_index('_dt').resample('5min', label='left', closed='left').agg({'volume':'sum','close':'last'}).dropna(subset=['close'])
                _hist_m5['_avg_prev_volume'] = _hist_m5['volume'].shift(1).rolling(20, min_periods=5).median()
                _hist_avg_lookup = _hist_m5['_avg_prev_volume'].to_dict()
                _hist_bucket_vol = _scan.groupby('_hist_bucket', sort=False)['volume'].cumsum().to_numpy(dtype=float)
                _hist_avg = np.array([float(_hist_avg_lookup.get(b, np.nan)) if pd.notna(_hist_avg_lookup.get(b, np.nan)) else np.nan for b in _hist_bucket], dtype=float)
                _hist_rvol = np.divide(_hist_bucket_vol, _hist_avg, out=np.full(len(_scan), np.nan, dtype=float), where=np.isfinite(_hist_avg) & (_hist_avg > 0))
            except Exception:
                _hist_rvol = np.full(len(_scan), np.nan, dtype=float)
            _scan['_entry_relative_volume'] = _hist_rvol
            _passed = _passed & np.isfinite(_hist_rvol) & (_hist_rvol > _rvol_min)
        else:
            _scan['_entry_relative_volume'] = np.nan

        # Enforce the GUI-controlled 15-minute ORB gate in historical replay.
        if bool(cfg.get('require_15m_orb', False)):
            _passed = _passed & _scan['_orb15_breakout'].fillna(False).astype(bool).to_numpy()
        # Apply the same 52-week gates to the vectorized Historical replay path.
        # The levels come only from cached sessions before this trading date.
        _need_52h=bool(cfg.get('require_52w_high_breakout', False))
        _need_52l=bool(cfg.get('require_52w_low_breakdown', False))
        if (_need_52h or _need_52l):
            _h52,_l52,_sessions52=get_52_week_levels(symbol, day)
            if (_need_52h and not _h52) or (_need_52l and not _l52):
                _passed=np.zeros(len(_scan),dtype=bool)
                print(f'Historical 52-week filter: {symbol} {day}; insufficient prior cache ({_sessions52} sessions; need at least 200).')
            else:
                _high_signal52=(_px > float(_h52)) if _need_52h else np.zeros(len(_scan),dtype=bool)
                _low_signal52=(_px < float(_l52)) if _need_52l else np.zeros(len(_scan),dtype=bool)
                _passed=_passed & (_high_signal52 | _low_signal52)
        _idx = np.flatnonzero(_passed)
        if _idx.size:
            _pos = int(_idx[0])
            first = _scan.iloc[_pos].copy()
            _px0 = float(first.get('close') or 0)
            _vol0 = float(first.get('_cum_volume') or 0)
            _turn0 = float(first.get('_cum_turnover_cr') or 0)
            if _movement_only:
                # Use the same GUI entry-condition audit as Paper/Live. The inferred
                # movement-only fill is the exact trigger price, while volume and
                # turnover are the cumulative values available at this candle.
                daily_entry = evaluate_entry_conditions(
                    float(_trigger), session_open, prev_close, _vol0, _turn0, cfg
                )
                daily_entry['reason'] = (
                    'Movement-only trigger and all enabled GUI entry conditions passed'
                    if daily_entry.get('passed') else daily_entry.get('reason', 'GUI entry condition failed')
                )
            elif _is_smart:
                _reason = 'All canonical smart BUY conditions passed'
                daily_entry = {
                    'passed': True, 'reason': _reason, 'trigger_price': _trigger,
                    'open_direction': cfg.get('open_direction','BOTH'),
                    'open_movement_pct': float(cfg.get('open_movement',0) or 0),
                    'price_big': float(cfg.get('price_big',0) or 0),
                    'price_small': float(cfg.get('price_small',0) or 0),
                    'required_volume': _min_vol, 'required_turnover_cr': _min_turn,
                    'actual_volume': _vol0, 'actual_turnover_cr': _turn0,
                }
            else:
                daily_entry = evaluate_entry_conditions(
                    _px0, session_open, prev_close, _vol0, _turn0, cfg
                )
            daily_entry['canonical_decision_path'] = True
    if first is None:
        return {'mode':'historical','logic':logic_name,'symbol':symbol,'trade_date':day,
                'status':'SIGNAL_NOT_TRIGGERED','exit_reason':'Canonical BUY conditions were never simultaneously true in the intraday window',
                'session_open':session_open,'trigger_price':trigger,'logic_config':cfg,
                'entry_audit':{'canonical_decision_path':True,'reason':'No 1-second market-state snapshot passed canonical_tick_entry'},
                'data_source':f'Breeze Historical V2 {interval}; SAME canonical BUY evaluator as LIVE'}
    buy_dt=first['_dt']
    # Movement-only historical entries are filled at the configured trigger,
    # not at a candle close that may be higher after the trigger was crossed.
    buy_price=float(trigger) if bool(cfg.get('movement_only_mode', False)) else float(first.get('close') or trigger)
    if buy_price < trigger and float(cfg.get('open_movement',0) or 0)>0:
        buy_price=trigger
    if daily_entry is None:
        daily_entry={'passed':True,'reason':'All canonical entry conditions passed','canonical_decision_path':True}
    # Re-evaluate the exact BUY snapshot through the canonical evaluator as a
    # final invariant immediately before creating the simulated position.
    _buy_vol=float(first.get('_cum_volume') or 0); _buy_turn=float(first.get('_cum_turnover_cr') or 0)
    _buy_bucket = pd.Timestamp(buy_dt).floor('5min')
    try:
        _buy_mom = float(_mom_lookup.get(_buy_bucket, np.nan))
    except Exception:
        _buy_mom = np.nan
    try:
        _buy_avg = float(_avg_lookup.get(_buy_bucket, np.nan))
    except Exception:
        _buy_avg = np.nan
    _rank_by=str(cfg.get('smart_rank_by','movement') or 'movement').lower()
    _buy_rank = smart_rank_context.get((symbol.upper(), _buy_bucket, _rank_by), smart_rank_context.get((symbol.upper(), _buy_bucket))) if smart_rank_context is not None else None
    _canonical_buy_px = buy_price if bool(cfg.get('movement_only_mode', False)) else float(first.get('close') or buy_price)
    _buy_q={'symbol':symbol,'time':buy_dt,'ltp':float(_canonical_buy_px),'close':float(_canonical_buy_px),
            'open':session_open,'previous_close':prev_close,
            'high':float(first.get('high') or buy_price),'low':float(first.get('low') or buy_price),
            'previous_close':prev_close,'volume':_buy_vol,'turnover_cr':_buy_turn,
            'vwap':(float(first.get('_entry_vwap')) if pd.notna(first.get('_entry_vwap')) and float(first.get('_entry_vwap') or 0)>0 else None),
            'relative_volume':(float(first.get('_entry_relative_volume')) if pd.notna(first.get('_entry_relative_volume')) else None),
            'runtime_relative_volume':(float(first.get('_entry_relative_volume')) if pd.notna(first.get('_entry_relative_volume')) else None),
            'avg_volume':(0.0 if not np.isfinite(_buy_avg) else _buy_avg),
            'momentum_15_pct':(None if not np.isfinite(_buy_mom) else _buy_mom),
            'momentum_rank':(_buy_rank if _rank_by=='momentum_15m' else None),
            'rank_by_ranks':({_rank_by:_buy_rank} if _buy_rank is not None else {}),
            'momentum_rank_pct':None,
            'breakout':bool(float(first.get('close') or buy_price)>=trigger),
            'previous_ltp':float(_prior_px[_pos]) if len(_prior_px)>_pos else None,
            'orb15_breakout':bool(first.get('_orb15_breakout',False))}
    _final_buy=canonical_tick_entry(_buy_q,cfg)
    if not _final_buy.get('passed'):
        return {'mode':'historical','logic':logic_name,'symbol':symbol,'trade_date':day,
                'status':'SIGNAL_NOT_TRIGGERED','exit_reason':'Canonical BUY recheck failed at selected signal timestamp',
                'session_open':session_open,'trigger_price':trigger,'logic_config':cfg,'entry_audit':_final_buy,
                'data_source':f'Breeze Historical V2 {interval}; SAME canonical BUY evaluator as LIVE'}
    if not daily_entry['passed']:
        return {'mode':'historical','logic':logic_name,'symbol':symbol,'trade_date':day,
                'status':'SIGNAL_NOT_TRIGGERED','exit_reason':daily_entry['reason'],
                'session_open':session_open,'trigger_price':trigger,'entry_audit':daily_entry,
                'logic_config':cfg,
                'data_source':f'Breeze Historical V2 {interval}; BUY-time cumulative GUI entry conditions'}
    qty=quantity(buy_price,cfg)
    if qty<1:
        return {'mode':'historical','logic':logic_name,'symbol':symbol,'trade_date':day,'status':'INSUFFICIENT_BUDGET','session_open':session_open,'trigger_price':trigger}
    # ------------------------------------------------------------------
    # FRESH FORMULA-BASED HISTORICAL P&L
    # ------------------------------------------------------------------
    # Replay exits through the SAME chronological exit evaluator used by LIVE/PAPER.
    _shadow_pos={'symbol':symbol,'logic':logic_name,'logic_config':cfg,'buy_actual_qty':qty,
                 'buy_actual_price':buy_price,'buy_trade_datetime':buy_dt,'highest_price':buy_price,
                 'trailing_active':False,'trailing_stop':None}
    # FAST CANONICAL EXIT SCAN
    # The chronological exit rules are unchanged.  Cum-max/high and first-event
    # masks reproduce the stateful trailing logic without Python iterrows.
    trade=None
    _exit_rows = df[(df['_dt'] > buy_dt) & (df['_dt'] <= datetime.combine(day, datetime.min.time()).replace(hour=15,minute=24,second=59))].sort_values('_dt').copy()
    if not _exit_rows.empty:
        _ep = float(buy_price)
        _px = pd.to_numeric(_exit_rows['close'], errors='coerce').fillna(0.0).to_numpy(dtype=float)
        _hi = np.nan_to_num(pd.to_numeric(_exit_rows['high'], errors='coerce').to_numpy(dtype=float), nan=np.nan)
        _hi = np.where(np.isfinite(_hi), _hi, _px)
        _valid = _px > 0
        _highest = np.maximum.accumulate(np.maximum.reduce([np.full(len(_exit_rows), _ep), _px, _hi]))
        _cfg=cfg
        _target_pct=float(_cfg.get('profit_target') or 0)
        _initial_stop_pct=float(_cfg.get('initial_stop') or 0)
        _activation_pct=float(_cfg.get('activation') or 0)
        _trailing_pct=float(_cfg.get('trailing_stop') or 0)
        _weak_enabled=bool(_cfg.get('weak_move_exit_enabled',False))
        _weak_high_pct=float(_cfg.get('weak_move_high_threshold') or 0)
        _weak_stop_pct=float(_cfg.get('weak_move_stop') or 0)

        _target = _ep*(1+_target_pct/100.0) if _target_pct>0 else None
        _initial = _ep*(1-_initial_stop_pct/100.0) if _initial_stop_pct>0 else None
        _target_mask = (_px >= _target) if _target is not None else np.zeros(len(_exit_rows),dtype=bool)

        _weak_level = _ep*(1+_weak_high_pct/100.0) if _weak_high_pct>0 else None
        _weak_stop = _ep*(1-_weak_stop_pct/100.0) if _weak_stop_pct>0 else None
        _weak_mask = (
            (_highest < _weak_level) & (_px <= _weak_stop)
            if _weak_enabled and _weak_level is not None and _weak_stop is not None
            else np.zeros(len(_exit_rows),dtype=bool)
        )

        if _trailing_pct>0:
            _activation_level = _ep*(1+_activation_pct/100.0) if _activation_pct>0 else -np.inf
            _active = _highest >= _activation_level
            _trail = _highest*(1-_trailing_pct/100.0)
            _trail = np.where(_active, _trail, -np.inf)
            _active_stop = np.maximum(_initial if _initial is not None else -np.inf, _trail)
            _trail_mask = _active & (_px <= _active_stop)
        else:
            _active = np.zeros(len(_exit_rows),dtype=bool)
            _trail = np.full(len(_exit_rows),-np.inf)
            _active_stop = np.full(len(_exit_rows), _initial if _initial is not None else -np.inf)
            _trail_mask = np.zeros(len(_exit_rows),dtype=bool)

        _regular_stop_mask = _valid & (_px <= _active_stop)
        _any = _target_mask | _weak_mask | _regular_stop_mask
        _hit = np.flatnonzero(_any)
        if _hit.size:
            _i=int(_hit[0])
            _row=_exit_rows.iloc[_i]
            _xts=_row['_dt']
            if _target_mask[_i]:
                _sell_price=float(_target); _reason=f'{_target_pct:g}% profit target'
                _event='PROFIT_TARGET'
            elif _weak_mask[_i]:
                _sell_price=float(_weak_stop); _reason=(f'Weak-move auto exit: post-BUY High stayed below +{_weak_high_pct:g}%; price reached -{_weak_stop_pct:g}% from BUY')
                _event='WEAK_MOVE'
            else:
                _sell_price=float(_active_stop[_i])
                if _active[_i] and _trailing_pct>0 and _trail[_i] >= float(_initial or 0):
                    _reason=f'{_trailing_pct:g}% trailing stop-loss'; _event='TRAILING_STOP'
                else:
                    _reason=f'{_initial_stop_pct:g}% initial stop-loss'; _event='INITIAL_STOP'
            _post_exit=_exit_rows.iloc[:_i+1]
            _post_high=float(pd.to_numeric(_post_exit['high'],errors='coerce').max()) if not _post_exit.empty else _sell_price
            _post_low=float(pd.to_numeric(_post_exit['low'],errors='coerce').min()) if not _post_exit.empty else _sell_price
            _hvals=pd.to_numeric(_post_exit['high'],errors='coerce')
            _lvals=pd.to_numeric(_post_exit['low'],errors='coerce')
            _hi_idx=np.flatnonzero(_hvals.to_numpy(dtype=float)==_post_high)
            _lo_idx=np.flatnonzero(_lvals.to_numpy(dtype=float)==_post_low)
            trade={'sell_actual_price':_sell_price,'sell_trade_datetime':_xts,
                   'exit_reason':_reason,'first_exit_event':_event,
                   'first_profit_hit_datetime':_xts if _event=='PROFIT_TARGET' else None,
                   'first_loss_hit_datetime':_xts if _event in ('INITIAL_STOP','TRAILING_STOP','WEAK_MOVE') else None,
                   'high':_post_high,'low':_post_low,
                   'high_dt':pd.Timestamp(_post_exit.iloc[int(_hi_idx[0])]['_dt']) if len(_hi_idx) else _xts,
                   'low_dt':pd.Timestamp(_post_exit.iloc[int(_lo_idx[0])]['_dt']) if len(_lo_idx) else _xts,
                   'squareoff_1525_price':None,'squareoff_1525_datetime':None}

    if trade is None:
        _cut=df[df['_dt'] <= datetime.combine(day,datetime.min.time()).replace(hour=15,minute=24,second=59)]
        _required_last=datetime.combine(day,datetime.min.time()).replace(hour=15,minute=14,second=0)
        if _cut.empty or pd.Timestamp(_cut['_dt'].max()) < pd.Timestamp(_required_last):
            _last_seen = pd.Timestamp(_cut['_dt'].max()).strftime('%H:%M:%S') if not _cut.empty else 'NO CANDLES'
            return {'mode':'historical','logic':logic_name,'symbol':symbol,'trade_date':day,
                    'status':'INCOMPLETE_EXIT_DATA','exit_reason':f'No target/stop exit was observed and historical data ends at {_last_seen}; full 15:25 square-off cannot be calculated.',
                    'session_open':session_open,'trigger_price':trigger,'logic_config':cfg,
                    'data_source':f'Breeze Historical V2 {interval}; incomplete session data; no synthetic square-off'}
        _last=_cut.iloc[-1]
        _post_exit=df[(df['_dt'] > buy_dt) & (df['_dt'] <= _last['_dt'])].copy()
        _post_high=float(pd.to_numeric(_post_exit['high'],errors='coerce').max()) if 'high' in _post_exit.columns and not _post_exit.empty else float(_last.get('close') or buy_price)
        _post_low=float(pd.to_numeric(_post_exit['low'],errors='coerce').min()) if 'low' in _post_exit.columns and not _post_exit.empty else float(_last.get('close') or buy_price)
        _high_rows=_post_exit[pd.to_numeric(_post_exit['high'],errors='coerce').eq(_post_high)] if 'high' in _post_exit.columns and not _post_exit.empty else pd.DataFrame()
        _low_rows=_post_exit[pd.to_numeric(_post_exit['low'],errors='coerce').eq(_post_low)] if 'low' in _post_exit.columns and not _post_exit.empty else pd.DataFrame()
        trade={'sell_actual_price':float(_last.get('close') or buy_price),'sell_trade_datetime':_last['_dt'],
               'exit_reason':'15:25 SQUARE-OFF','first_exit_event':'15:25_SQUARE_OFF',
               'first_profit_hit_datetime':None,'first_loss_hit_datetime':None,
               'high':_post_high,'low':_post_low,
               'high_dt':pd.Timestamp(_high_rows.iloc[0]['_dt']) if not _high_rows.empty else _last['_dt'],
               'low_dt':pd.Timestamp(_low_rows.iloc[0]['_dt']) if not _low_rows.empty else _last['_dt'],
               'squareoff_1525_price':float(_last.get('close') or buy_price),'squareoff_1525_datetime':_last['_dt']}

    if trade is None:
        return {'mode':'historical','logic':logic_name,'symbol':symbol,'trade_date':day,'status':'NO_EXIT_DATA','session_open':session_open,'trigger_price':trigger,'logic_config':cfg}
    sell_dt=trade.get('sell_trade_datetime'); sell_price=float(trade['sell_actual_price'])
    gross=(sell_price-buy_price)*qty
    # IMPORTANT: pass the actual simulated BUY/SELL quantities and prices into
    # the shared charge engine.  Previously only turnovers were passed, so the
    # engine saw quantity/price as zero and returned zero charges.
    charge_input = {
        'mode': 'historical',
        'symbol': symbol,
        'breeze_symbol': symbol,
        'quantity': qty,
        'buy_actual_qty': qty,
        'buy_actual_price': buy_price,
        'sell_actual_qty': qty,
        'sell_actual_price': sell_price,
        'gross_pnl': gross,
        'buy_turnover': buy_price * qty,
        'sell_turnover': sell_price * qty,
    }
    charges=apply_icici_charges(charge_input,exchange='NSE',breeze=breeze)
    # Capture the full session's high/low and the first candle timestamp where each occurred.
    # These are separate from post-BUY extrema used for exit/P&L evaluation.
    _day_high = _day_low = _day_high_time = _day_low_time = None
    try:
        _day_frame = df.copy()
        if '_dt' in _day_frame.columns:
            _day_frame['_dt'] = pd.to_datetime(_day_frame['_dt'], errors='coerce')
            _day_frame = _day_frame.dropna(subset=['_dt']).sort_values('_dt', kind='mergesort')
            if 'is_actual' in _day_frame.columns:
                _actual_mask = _day_frame['is_actual'].map(lambda v: v if isinstance(v, bool) else str(v).strip().lower() not in {'false','0','no','nan','none',''})
                _day_frame = _day_frame[_actual_mask]
            for _col, _which in (('high','max'),('low','min')):
                if _col not in _day_frame.columns: continue
                _series = pd.to_numeric(_day_frame[_col], errors='coerce')
                _valid = _series.dropna()
                if _valid.empty: continue
                _extreme = float(_valid.max() if _which == 'max' else _valid.min())
                _matches = _day_frame[_series.eq(_extreme)]
                _ts = pd.Timestamp(_matches.iloc[0]['_dt']) if not _matches.empty else None
                if _col == 'high': _day_high, _day_high_time = _extreme, _ts
                else: _day_low, _day_low_time = _extreme, _ts
    except Exception:
        pass
    result={'mode':'historical','logic':logic_name,'symbol':symbol,'trade_date':day,'status':'HISTORICAL_REPLAY_COMPLETE','session_open':session_open,'trigger_price':trigger,'buy_trade_datetime':buy_dt,'buy_trade_time':buy_dt.strftime('%H:%M:%S'),'buy_actual_qty':qty,'buy_actual_price':buy_price,'sell_actual_qty':qty,'sell_actual_price':sell_price,'sell_trade_datetime':sell_dt,'sell_trade_time':sell_dt.strftime('%H:%M:%S') if sell_dt is not None else '','gross_pnl':gross,'close':(float(pd.to_numeric(df['close'],errors='coerce').dropna().iloc[-1]) if 'close' in df.columns and not pd.to_numeric(df['close'],errors='coerce').dropna().empty else sell_price),'day_high':_day_high,'day_high_time':_day_high_time,'day_low':_day_low,'day_low_time':_day_low_time,'exit_reason':trade.get('exit_reason'),'logic_config':cfg,'entry_audit':daily_entry,'buy_extremes':{'high':trade.get('high'),'high_dt':trade.get('high_dt'),'low':trade.get('low'),'low_dt':trade.get('low_dt')},'first_profit_hit_datetime':trade.get('first_profit_hit_datetime'),'first_loss_hit_datetime':trade.get('first_loss_hit_datetime'),'first_exit_event':trade.get('first_exit_event'),'squareoff_1525_price':trade.get('squareoff_1525_price'),'squareoff_1525_datetime':trade.get('squareoff_1525_datetime'),'pnl_calculation_source':'CANONICAL LIVE/PAPER EXIT DECISION','data_source':f'Breeze Historical V2 {interval}; SAME canonical BUY/SELL decision path as LIVE/PAPER'}
    result['historical_shares'] = int(float(cfg.get('historical_shares', 0) or 0))
    for k in ('brokerage','stt','exchange_turnover_charges','gst','sebi_charges','stamp_duty','total_other_charges','total_charges','net_pnl','pnl_pct','charge_source','charge_plan','simulated_total_charges','buy_turnover','sell_turnover'):
        if k in charges: result[k]=charges[k]
    return result


# === LOGIC 1 HISTORICAL REPLAY RESULT CACHE ===
# Cache the single active strategy result per symbol/day and candle fingerprint.
_HISTORICAL_LOGIC_RESULT_CACHE_VERSION = 'v3_day_high_low_timestamps_replay_cache'
_HISTORICAL_LOGIC_RESULT_MEM = {}
_HISTORICAL_LOGIC_RESULT_MEM_MAX = 4096

def _historical_logic_result_cache_dir(day):
    d = HISTORICAL_CACHE / '3_Logic_Result_Cache' / f'{day:%Y-%m}' / f'{day:%Y-%m-%d}'
    d.mkdir(parents=True, exist_ok=True)
    return d

def _historical_candle_fingerprint(candles):
    """Fast fingerprint without Python row iteration."""
    if candles is None or getattr(candles, 'empty', True):
        return 'empty'
    try:
        n = len(candles)
        dt = candles['_dt'] if '_dt' in candles.columns else None
        first_dt = str(dt.iloc[0]) if dt is not None and n else ''
        last_dt = str(dt.iloc[-1]) if dt is not None and n else ''
        parts = [str(n), first_dt, last_dt]
        # Vectorised checksums catch changed price/volume files while remaining
        # much cheaper than replaying canonical entry/exit logic row-by-row.
        for c in ('open','high','low','close','volume','_cum_volume','_cum_turnover_cr'):
            if c in candles.columns:
                x = pd.to_numeric(candles[c], errors='coerce').fillna(0.0)
                parts.append(f'{c}:{float(x.sum()):.8f}:{float(x.iloc[0]):.8f}:{float(x.iloc[-1]):.8f}')
        return hashlib.sha256('|'.join(parts).encode('utf-8')).hexdigest()
    except Exception:
        return f'fallback:{len(candles)}'

def _historical_logic_result_cache_key(stock_code, symbol, day, candles, logic_name,
                                        budget, stop_pct, target_pct, trail_pct, interval,
                                        daily_open, daily_high, daily_previous_close,
                                        daily_volume, daily_turnover_cr, smart_rank_context=None):
    cfg = get_logic(logic_name)
    payload = {
        'version': _HISTORICAL_LOGIC_RESULT_CACHE_VERSION,
        'stock_code': str(stock_code or ''), 'symbol': str(symbol or '').upper(),
        'day': day.isoformat(), 'logic_name': str(logic_name), 'logic_config': cfg,
        'budget': budget, 'stop_pct': stop_pct, 'target_pct': target_pct,
        'trail_pct': trail_pct, 'interval': interval,
        'daily_open': daily_open, 'daily_high': daily_high,
        'daily_previous_close': daily_previous_close,
        'daily_volume': daily_volume, 'daily_turnover_cr': daily_turnover_cr,
        'smart_rank_context_version': 'rank-v1' if smart_rank_context is not None else 'none',
        'smart_rank_context_symbol': (str(symbol).upper() if smart_rank_context is not None else ''),
        'candles': _historical_candle_fingerprint(candles),
    }
    raw = json.dumps(payload, sort_keys=True, default=str, separators=(',', ':')).encode('utf-8')
    return hashlib.sha256(raw).hexdigest()

def _simulate_logic_from_candles(breeze, stock_code, symbol, day, candles, logic_name,
                                  budget=None, stop_pct=None, target_pct=None,
                                  trail_pct=None, interval='1minute', daily_open=None,
                                  daily_high=None, daily_previous_close=None,
                                  daily_volume=None, daily_turnover_cr=None,
                                  smart_rank_context=None):
    """Cached wrapper around the canonical replay engine.

    Trading rules are NOT changed.  Only already-computed results for the exact
    same symbol/day/candles/logic settings are reused.
    """
    try:
        key = _historical_logic_result_cache_key(
            stock_code, symbol, day, candles, logic_name, budget, stop_pct,
            target_pct, trail_pct, interval, daily_open, daily_high,
            daily_previous_close, daily_volume, daily_turnover_cr, smart_rank_context=smart_rank_context)
        if key in _HISTORICAL_LOGIC_RESULT_MEM:
            return _HISTORICAL_LOGIC_RESULT_MEM[key]
        safe_logic = re.sub(r'[^A-Za-z0-9_.-]+', '_', str(logic_name)).strip('_') or 'logic'
        safe_symbol = re.sub(r'[^A-Za-z0-9_.-]+', '_', str(symbol)).strip('_') or 'symbol'
        cache_file = _historical_logic_result_cache_dir(day) / f'{safe_symbol}_{safe_logic}_{key}.pkl'
        if cache_file.exists():
            try:
                with cache_file.open('rb') as fh:
                    result = pickle.load(fh)
                if len(_HISTORICAL_LOGIC_RESULT_MEM) >= _HISTORICAL_LOGIC_RESULT_MEM_MAX:
                    _HISTORICAL_LOGIC_RESULT_MEM.pop(next(iter(_HISTORICAL_LOGIC_RESULT_MEM)))
                _HISTORICAL_LOGIC_RESULT_MEM[key] = result
                return result
            except Exception:
                try: cache_file.unlink()
                except Exception: pass
    except Exception:
        key = None; cache_file = None

    result = _simulate_logic_from_candles_uncached(
        breeze, stock_code, symbol, day, candles, logic_name,
        budget=budget, stop_pct=stop_pct, target_pct=target_pct,
        trail_pct=trail_pct, interval=interval, daily_open=daily_open,
        daily_high=daily_high, daily_previous_close=daily_previous_close,
        daily_volume=daily_volume, daily_turnover_cr=daily_turnover_cr,
        smart_rank_context=smart_rank_context)

    if key is not None:
        try:
            if len(_HISTORICAL_LOGIC_RESULT_MEM) >= _HISTORICAL_LOGIC_RESULT_MEM_MAX:
                _HISTORICAL_LOGIC_RESULT_MEM.pop(next(iter(_HISTORICAL_LOGIC_RESULT_MEM)))
            _HISTORICAL_LOGIC_RESULT_MEM[key] = result
            tmp = cache_file.with_suffix('.tmp')
            with tmp.open('wb') as fh:
                pickle.dump(result, fh, protocol=pickle.HIGHEST_PROTOCOL)
            os.replace(tmp, cache_file)
        except Exception as exc:
            print(f'Historical per-logic result cache warning: {exc}')
    return result


def _historical_day_file_signature(day: date) -> str:
    """Cheap *per-day* cache signature; never hash unrelated dates/workbooks.

    The old implementation recursively hashed the entire YTD cache folder. That
    meant downloading one new symbol/date changed every other day's signature,
    so an otherwise-complete historical run could replay many old dates again.
    Only files whose names start with the requested YYYY-MM-DD prefix are part
    of a day's market-data signature.
    """
    root=_cache_day_dir(day)
    if not root.exists(): return ''
    prefix=f'{day:%Y-%m-%d}_'
    items=[]
    try:
        for path in root.iterdir():
            if not path.is_file() or not path.name.startswith(prefix):
                continue
            st=path.stat(); items.append((path.name, st.st_size, st.st_mtime_ns))
    except OSError:
        return ''
    if not items:
        return ''
    return hashlib.sha256(json.dumps(sorted(items), sort_keys=True).encode()).hexdigest()


def _historical_replay_state_path(year: int, month: int) -> Path:
    YTD_HISTORICAL_CACHE.mkdir(parents=True, exist_ok=True)
    return _historical_month_dir(year, month) / 'historical_replay_days.json'


def _load_historical_replay_state(year: int, month: int) -> dict:
    path=_historical_replay_state_path(year, month)
    if not path.exists():
        return {'version': 3, 'days': {}}
    try:
        data=json.loads(path.read_text(encoding='utf-8'))
        if not isinstance(data, dict) or data.get('version') != 3 or not isinstance(data.get('days'), dict):
            return {'version': 3, 'days': {}}
        return data
    except Exception:
        return {'version': 3, 'days': {}}


def _save_historical_replay_state(year: int, month: int, state: dict) -> None:
    state['version']=3
    path=_historical_replay_state_path(year, month)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding='utf-8')


def _historical_day_excel_audit(day: date) -> dict:
    """Verify that the monthly Excel workbook really contains this day's results.

    The historical replay state is only a performance cache.  It must never be
    allowed to say a day is complete when the Excel result rows are missing or
    incomplete.  This check is intentionally based on the authoritative monthly
    YTD sheet, not on the temporary Selected Date sheet.
    """
    path = _historical_month_dir(day.year, day.month) / f'Trading_Data_{day:%Y-%m}.xlsx'
    out = {
        'exists': path.exists(),
        'ytd_rows': 0,
        'ytd_unique_symbols': 0,
        'ytd_symbol_hash': '',
    }
    if not path.exists():
        return out
    try:
        wb = load_workbook(path, read_only=True, data_only=False)
        if 'YTD Historical Data' not in wb.sheetnames:
            wb.close()
            return out
        ws = wb['YTD Historical Data']
        symbols=set()
        count=0
        for row in ws.iter_rows(min_row=2, values_only=True):
            if not row:
                continue
            d=_excel_date(row[0] if len(row)>0 else None)
            if d != day:
                continue
            sym=str(row[3] if len(row)>3 else '').strip().upper()
            if not sym:
                continue
            count += 1
            if sym != 'NO TRADE':
                symbols.add(sym)
        wb.close()
        out['ytd_rows']=count
        out['ytd_unique_symbols']=len(symbols)
        out['ytd_symbol_hash']=hashlib.sha256(
            json.dumps(sorted(symbols), separators=(',', ':')).encode('utf-8')
        ).hexdigest()
    except Exception as exc:
        print(f'Historical Excel audit warning {day}: {type(exc).__name__}: {exc}')
    return out


def _historical_day_is_complete(day: date, state: dict, logic_signature: str, interval: str) -> bool:
    item=(state.get('days') or {}).get(day.isoformat()) or {}
    if item.get('status') != 'complete':
        return False
    if item.get('logic_signature') != logic_signature or item.get('interval') != interval:
        return False
    saved_sig=str(item.get('cache_signature') or '')
    if not saved_sig:
        return False
    if _historical_day_file_signature(day) != saved_sig:
        return False

    # CRITICAL: cache state alone is never sufficient.  If Excel was deleted,
    # replaced, truncated, or partially written after the day was marked complete,
    # force that date through the replay/save path again.
    audit=_historical_day_excel_audit(day)
    expected_rows=int(item.get('excel_result_count') or item.get('result_count') or 0)
    expected_symbols=int(item.get('excel_unique_symbols') or 0)
    expected_symbol_hash=str(item.get('excel_symbol_hash') or '')
    # Newer state records carry an exact company-set fingerprint.  Older state
    # records are deliberately replayed once so the workbook can be revalidated
    # under the corrected persistence rules instead of being trusted blindly.
    if not expected_symbol_hash:
        print(f'Historical Excel completeness check {day}: FAIL - legacy state has no Excel company fingerprint; replay will repair/verify it.')
        return False
    if not audit.get('exists') or int(audit.get('ytd_rows') or 0) <= 0:
        print(f'Historical Excel completeness check {day}: FAIL - date is absent from monthly YTD; replay will repair it.')
        return False
    if expected_rows > 0 and int(audit.get('ytd_rows') or 0) != expected_rows:
        print(
            f'Historical Excel completeness check {day}: FAIL - '
            f'YTD rows={audit.get("ytd_rows")} expected={expected_rows}; replay will repair it.'
        )
        return False
    if expected_symbols > 0 and int(audit.get('ytd_unique_symbols') or 0) != expected_symbols:
        print(
            f'Historical Excel completeness check {day}: FAIL - '
            f'unique companies={audit.get("ytd_unique_symbols")} expected={expected_symbols}; replay will repair it.'
        )
        return False
    if expected_symbol_hash != str(audit.get('ytd_symbol_hash') or ''):
        print(
            f'Historical Excel completeness check {day}: FAIL - '
            f'company set fingerprint changed; replay will repair it.'
        )
        return False
    print(
        f'Historical Excel completeness check {day}: PASS - '
        f'YTD rows={audit.get("ytd_rows")}; unique companies={audit.get("ytd_unique_symbols")}.'
    )
    return True


def _mark_historical_day_complete(state: dict, day: date, logic_signature: str, interval: str, result_count: int) -> None:
    audit=_historical_day_excel_audit(day)
    state.setdefault('days', {})[day.isoformat()] = {
        'status':'complete',
        'cache_signature':_historical_day_file_signature(day),
        'logic_signature':logic_signature,
        'interval':interval,
        'result_count':int(result_count),
        'excel_result_count':int(audit.get('ytd_rows') or 0),
        'excel_unique_symbols':int(audit.get('ytd_unique_symbols') or 0),
        'excel_symbol_hash':str(audit.get('ytd_symbol_hash') or ''),
        'updated_at':datetime.now().isoformat(timespec='seconds'),
    }
    if not audit.get('exists') or int(audit.get('ytd_rows') or 0) <= 0:
        print(f'Historical Excel completeness check {day}: FAIL after save - state will NOT be trusted on next run.')


# === STRICT SHARED TRADE UNIVERSE ===
_SHARED_UNIVERSE_MANIFEST_VERSION = 5
_SHARED_UNIVERSE_ALGO = 'canonical_live_shadow_decision_path_v7_movement_overshoot_1525_squareoff'

def _shared_logic_signature() -> str:
    """Fingerprint the exact GUI logic configuration used by all modes."""
    try:
        payload = CONFIG_DIR / 'logic_settings.json'
        raw = payload.read_bytes() if payload.exists() else b''
    except Exception:
        raw = b''
    return hashlib.sha256(raw + b'|' + _SHARED_UNIVERSE_ALGO.encode()).hexdigest()

def _serialize_universe_rows(rows):
    out=[]
    for rank,row in enumerate(list(rows or []), 1):
        try:
            first=row[0]
            symbol=str(row[1]).strip().upper()
            breeze_symbol=str(row[5] or '').strip()
            logics=list(dict.fromkeys(row[4] or []))
            logic_times={k:(v.isoformat() if hasattr(v,'isoformat') else str(v)) for k,v in (row[6] or {}).items()} if len(row)>6 else {}
            out.append({'canonical_rank':rank,'symbol':symbol,'breeze_symbol':breeze_symbol,
                        'first_buy_time':first.isoformat() if hasattr(first,'isoformat') else str(first),
                        'logics':logics,'logic_times':logic_times})
        except Exception:
            continue
    return out

def _manifest_row_to_timing(item, market_by_symbol, day):
    sym=str(item.get('symbol') or '').strip().upper()
    rr=market_by_symbol.get(sym)
    if rr is None:
        return None
    stock=str(item.get('breeze_symbol') or '').strip() or sym
    logics=list(dict.fromkeys(item.get('logics') or []))
    logic_times={}
    for lname,ts in (item.get('logic_times') or {}).items():
        try: logic_times[lname]=datetime.fromisoformat(str(ts))
        except Exception: pass
    try: first=datetime.fromisoformat(str(item.get('first_buy_time')))
    except Exception: first=datetime.combine(day,datetime.min.time()).replace(hour=9,minute=15)
    logic=next((n for n,t in logic_times.items() if t==first), logics[0] if logics else '')
    return (first,sym,logic,rr,logics,stock,logic_times)

def _load_shared_trade_universe(day: date, enabled, cap: int):
    """Return an empty runtime universe so Paper/Live use the SAME selector as Historical.

    Historical/Paper/Live must not have separate company-membership algorithms.
    Historical discovers the first-cum-first-buy ranking from historical timestamps;
    Paper/Live discover it from the corresponding real-time timestamps.  A saved
    Historical manifest is diagnostic/cache data only and is NEVER used to lock or
    force Paper/Live membership.

    The caller then feeds every eligible NSE symbol through the canonical entry
    evaluator and reserves a symbol only at its first qualifying BUY event.
    """
    return [], set()

def _historical_selection_manifest_path(day: date) -> Path:
    """Return the per-day selection manifest inside its own subfolder.

    Keep these bookkeeping JSON files out of the main historical candle-cache
    directory so that the user's data folder remains easy to browse.  On first
    access, migrate legacy manifests from the old flat layout without changing
    their contents or names.
    """
    YTD_HISTORICAL_CACHE.mkdir(parents=True, exist_ok=True)
    selection_dir = YTD_HISTORICAL_CACHE / 'Selection_Manifests'
    selection_dir.mkdir(parents=True, exist_ok=True)

    # One-time, non-destructive migration from the previous flat layout.
    try:
        for legacy_path in YTD_HISTORICAL_CACHE.glob('*_historical_selection.json'):
            target = selection_dir / legacy_path.name
            if legacy_path.is_file() and not target.exists():
                shutil.move(str(legacy_path), str(target))
    except Exception as exc:
        print(f'Historical selection manifest migration warning: {exc}')

    return selection_dir / f'{day:%Y-%m-%d}_historical_selection.json'


def _load_historical_selection_manifest(day: date) -> dict | None:
    path = _historical_selection_manifest_path(day)
    if not path.exists():
        return None
    try:
        data=json.loads(path.read_text(encoding='utf-8'))
        if not isinstance(data, dict) or not isinstance(data.get('symbols'), list):
            return None
        return data
    except Exception as exc:
        print(f'Historical selection manifest warning {day}: {exc}')
        return None


def _save_historical_selection_manifest(day: date, selected_timing, cap: int, enabled, logic_signature: str = '', interval: str = '', backfill_timing=None) -> None:
    """Persist one canonical ranked company universe for ALL modes.

    ``backfill_timing`` is the complete first-buy-time ranking pool; ``selected_timing``
    is the current cap slice. Paper/Live consume the canonical pool and current cap,
    never their own real-time ranking.
    """
    path=_historical_selection_manifest_path(day)
    canonical=list(backfill_timing or selected_timing or [])
    canonical.sort(key=lambda x:(x[0],x[1]))
    selected=list(selected_timing or [])
    payload={
        'version': _SHARED_UNIVERSE_MANIFEST_VERSION,
        'date': day.isoformat(),
        'global_unique_share_cap': int(cap or 0),
        'enabled_logics': [name for name,_cfg in enabled],
        'logic_signature': str(logic_signature or ''),
        'shared_logic_signature': _shared_logic_signature(),
        'shared_universe_algorithm': _SHARED_UNIVERSE_ALGO,
        'interval': str(interval or ''),
        'canonical_ranked_symbols': _serialize_universe_rows(canonical),
        'symbols': _serialize_universe_rows(selected),
        'backfill_symbols': _serialize_universe_rows(canonical),
    }
    path.write_text(json.dumps(payload, indent=2), encoding='utf-8')
    print(f'SHARED UNIVERSE SAVE {day}: canonical={len(payload["canonical_ranked_symbols"])}; current cap={len(payload["symbols"])}; Paper/Live must use this exact ranked list.')


def _filter_historical_market_universe(market: pd.DataFrame, symbol_allowlist: Iterable[str] | None, day) -> pd.DataFrame:
    """Apply a selected NSE universe before any per-symbol history work.

    None means the full NSE EQ universe. A supplied empty/invalid allowlist is
    rejected upstream; missing symbol columns fail closed instead of widening.
    """
    if symbol_allowlist is None:
        return market
    symbol_col = next((c for c in market.columns if str(c).strip().lower() == 'symbol'), None)
    if symbol_col is None:
        raise RuntimeError('Selected NSE-universe historical scope could not be applied: screened NSE data has no Symbol column.')
    allowed = {str(sym or '').strip().upper() for sym in symbol_allowlist if str(sym or '').strip()}
    if not allowed:
        raise RuntimeError('Selected NSE-universe historical scope is empty; refusing to widen to all NSE shares.')
    before = len(market)
    filtered = market[market[symbol_col].astype(str).str.strip().str.upper().isin(allowed)].copy()
    print(f'Historical symbol scope {day}: selected NSE-universe allowlist; screened rows={before}; in-scope rows={len(filtered)}; allowed symbols={len(allowed)}.')
    return filtered


def _run_historical_replay_month_impl(month_text: str, range_start: date | None = None, range_end: date | None = None, interval: str = '1minute', historical_share_limit: int | None = None, symbol_allowlist: Iterable[str] | None = None, full_interval_all_qualified: bool = False) -> list[dict]:
    _historical_run_t0 = time.perf_counter()
    _historical_perf_reset()
    print(f'Historical OUTPUT TIMER START: {datetime.now().strftime("%H:%M:%S")} IST/local process time; interval={interval}.')
    print(f'Historical DATA SOURCE: {"LOCAL CSV CACHE ONLY — NO BREEZE" if _historical_cache_only() else "CACHE FIRST; Breeze fallback allowed"}.')
    """Primary validation engine: replay configured eligible shares for all enabled logics.

    Historical selection uses the same first-cum-first-buy unique-symbol universe
    rule as Paper and Live. The configured Historical Shares/Top-N value is a
    universe cap, not a position-size input. Historical/Paper/Live must therefore
    select the same first N qualifying companies when all logic/data conditions match.
    The same symbol selected by multiple logics is replayed once and collapsed to one
    final historical trade row.
    """
    interval = str(interval).strip().lower()
    offline_all_shares = str(os.getenv('BREEZE_OFFLINE_HISTORICAL','0')).strip().lower() in {'1','true','yes','on'}
    if interval not in {'1minute','1second','hybrid'}:
        raise ValueError('Historical interval must be 1minute, 1second or hybrid (1minute -> 1second).')
    try:
        year, month = [int(x) for x in month_text.strip().split('-', 1)]
        first = date(year, month, 1)
    except Exception as exc:
        raise RuntimeError('Month must be YYYY-MM, e.g. 2026-09.') from exc
    next_month = date(year + (month == 12), 1 if month == 12 else month + 1, 1)
    last = next_month - timedelta(days=1)
    # SMART HISTORICAL CACHE MODE:
    # 1) Reuse every daily/intraday file already stored on disk.
    # 2) If a required historical file is missing, obtain Breeze credentials lazily,
    #    download that missing historical data once, and save it permanently.
    # 3) On future runs the saved file is reused and Breeze is NOT called again.
    # This means the first run may download data, while all later runs become cache-only.
    results=[]
    stock_cache=_load_security_cache()
    breeze_hist=None

    # FAST CACHE MODE:
    # A per-day manifest records the exact cache signature + logic configuration
    # used for a completed replay. On later runs we check only file metadata; if
    # nothing changed, that day is skipped without loading any CSV into pandas and
    # without opening/saving the Excel workbook.
    state=_load_historical_replay_state(year, month)
    state['version']=13  # Invalidate manifests after canonical VWAP/RVOL and daily trade-cap enforcement changes
    logic_file = CONFIG_DIR / 'logic_settings.json'
    _HISTORICAL_REPLAY_ALGO = 'canonical_strategy_v7_gui_logic_caps_first_complete_buy_timestamp' 
    if offline_all_shares:
        # Force a fresh replay so older results cannot be reused after the offline
        # Historical Shares cap semantics changed.
        _HISTORICAL_REPLAY_ALGO += '_offline_cache_respects_historical_shares_cap_v2_successful_buy_backfill_v1'
    _logic_bytes = logic_file.read_bytes() if logic_file.exists() else b''
    _symbol_allowlist = None
    if symbol_allowlist is not None:
        _symbol_allowlist = {str(sym or '').strip().upper() for sym in symbol_allowlist if str(sym or '').strip()}
        if not _symbol_allowlist:
            raise ValueError('Historical symbol_allowlist was supplied but contains no valid symbols.')
        _HISTORICAL_REPLAY_ALGO += '_symbol_scope_' + hashlib.sha256('|'.join(sorted(_symbol_allowlist)).encode()).hexdigest()[:16]
    logic_signature = hashlib.sha256(_logic_bytes + b'|' + _HISTORICAL_REPLAY_ALGO.encode()).hexdigest()
    explicit_range = range_start is not None or range_end is not None
    if not explicit_range:
        print(f'Historical CACHE MODE: checking completed-day manifest for {year:04d}-{month:02d}; unchanged days will be skipped without reading CSV data.')
    nse_session=None
    hist_auth_error=None
    hist_auth_lock=threading.Lock()
    def get_hist_breeze():
        nonlocal breeze_hist, hist_auth_error
        if _historical_cache_only():
            raise RuntimeError('LOCAL CACHE ONLY is enabled for this Historical run. Breeze authentication/download is disabled.')
        # A single authenticated, read-only Breeze client is shared by the minute
        # workers. Serialize only lazy initialization, not the REST requests.
        with hist_auth_lock:
            if breeze_hist is not None:
                return breeze_hist
            if hist_auth_error is not None:
                raise hist_auth_error
            try:
                breeze_hist=require_breeze()
                print('Historical download: Breeze session connected because cached historical data was missing.')
                return breeze_hist
            except Exception as exc:
                hist_auth_error=RuntimeError(
                    'Historical Breeze session could not be created. No historical order was sent. '
                    'The saved App Key/Secret/Session Token were rejected or Breeze was temporarily unable '
                    'to create the session. Create a fresh session token for the current day, save it again, '
                    'and retry. If the same error remains, check Breeze app status/static-IP settings. '
                    f' Original error: {type(exc).__name__}: {exc}'
                )
                print(f'HISTORICAL AUTH STOP: {hist_auth_error}')
                raise hist_auth_error from exc
    day = max(first, range_start) if range_start is not None else first
    end_day = min(last, range_end) if range_end is not None else last

    if end_day < day:
        return []

    processed_dates=set()
    successful_dates=set()
    while day<=end_day:
        if _is_nse_trading_day(day):
            # Exact date/range runs use the same completion manifest as broad
            # month runs. If the GUI logic signature, interval and this day's
            # market-data cache are unchanged, the requested date is already
            # complete and must not be replayed or rewritten into Excel again.
            # Full mode must validate/download both intervals even when the
            # strategy replay for this date was previously marked complete.
            # Older completion manifests only tracked replay/1-minute cache state
            # and could otherwise skip the required 1-second backfill forever.
            if (not full_interval_all_qualified) and _historical_day_is_complete(day, state, logic_signature, interval):
                mode_label = 'EXACT-RANGE FAST SKIP' if explicit_range else 'FAST SKIP'
                # FAST EXPORT PATH: do not reload daily bhavcopy and rewrite the
                # monthly workbook for every already-completed date. That turned a
                # cache hit into repeated pandas/openpyxl work and made "from cache"
                # unnecessarily slow. NSE Data persistence is handled during a fresh
                # replay; legacy workbook repair can be done explicitly when needed.
                day += timedelta(days=1)
                continue
            try:
                processed_dates.add(day.isoformat())
                raw=_load_cached_daily(day)
                if raw is None:
                    if offline_all_shares:
                        raise RuntimeError(f'OFFLINE CACHE MISS: NSE daily data for {day} is not present locally; offline mode will not access the internet.')
                    if nse_session is None:
                        nse_session=_new_nse_session()
                    raw=bhavcopy_for_date(day,nse_session)
                    _save_cached_daily(day, raw)
                    print(f'Historical cache: downloaded and saved NSE daily data for {day}.')
                else:
                    print(f'Historical cache: using saved NSE daily data for {day}.')
                market,_=screen_from_raw(raw)
                market = _filter_historical_market_universe(market, _symbol_allowlist, day)
                # Excel is written once at the end of the replay. Do not open/save the
                # monthly workbook for every date; all market/intraday data stays in cache.
                cfgs=load_logics()
                enabled=enabled_logics(cfgs)
                if not enabled:
                    print(f'Historical {day}: all active logics are OFF; no historical trades evaluated.')
                    day += timedelta(days=1)
                    continue

                # Historical Shares is a GUI-configured cap used by the final
                # historical selection/replay stage.  Define it before either the
                # fresh-selection path or the locked-selection path so both paths
                # use the same cap.
                per_logic_caps = {}
                trade_caps = {}
                for _lname, _cfg in enabled:
                    _hist_lim = int(float(_cfg.get('historical_shares', 0) or 0))
                    _order_lim = int(float(_cfg.get('max_orders', 0) or 0))
                    if offline_all_shares:
                        # Offline/cache-only mode disables network access and execution
                        # order caps, but MUST still respect the GUI's Historical Shares
                        # universe limit. Historical Shares controls how many ranked
                        # trigger candidates can enter the replay; it does not manufacture
                        # additional BUY signals when fewer candidates qualify.
                        _order_lim = 0  # no Paper/Live Maximum Orders cap in analysis mode
                    if historical_share_limit is not None and int(historical_share_limit) > 0:
                        _hist_lim = min(_hist_lim, int(historical_share_limit)) if _hist_lim > 0 else int(historical_share_limit)
                    per_logic_caps[_lname] = _hist_lim
                    trade_caps[_lname] = _order_lim
                _positive_caps = [int(v) for v in per_logic_caps.values() if int(v) > 0]
                if historical_share_limit is not None and int(historical_share_limit) > 0:
                    global_share_cap = min(_positive_caps + [int(historical_share_limit)]) if _positive_caps else int(historical_share_limit)
                elif _positive_caps:
                    # A positive GUI Historical Shares value remains authoritative in
                    # cache-only mode. Zero means no explicit cap for that logic.
                    global_share_cap = min(_positive_caps)
                else:
                    global_share_cap = 0
                _positive_trade_caps = [int(v) for v in trade_caps.values() if int(v) > 0]
                # Historical Shares is an output ceiling; Maximum Orders is the
                # actual per-day trade ceiling. Enforce both, using the strictest
                # positive cap across enabled logic rows, just as Paper/Live do.
                _hist_order_cap = max(_positive_trade_caps) if _positive_trade_caps else 0
                if global_share_cap > 0 and _hist_order_cap > 0:
                    global_trade_cap = max(global_share_cap, _hist_order_cap)
                else:
                    global_trade_cap = global_share_cap or _hist_order_cap or 0

                # STRICT DOWNLOAD-SELECTION CACHE: if this date already has a
                # historical selection lock, reuse exactly those shares.  Do not
                # perform a new first-N selection and do not backfill with additional
                # shares.  Missing 1-second files for a locked share may still be
                # downloaded, but the share universe itself never expands.
                _selection_manifest=None  # FINAL MODEL: no historical candidate/1-second universe lock; ranking is recomputed from market data.
                # A selection lock is reusable only when it was produced from the
                # exact same latest GUI configuration and replay interval.  Market
                # data remains reusable even when the GUI changes; only the ranking/
                # trade selection is recalculated from that cached data.
                if _selection_manifest and int(_selection_manifest.get('version', 0) or 0) < _SHARED_UNIVERSE_MANIFEST_VERSION:
                    print(f'Historical SELECTION LOCK INVALIDATED {day}: old manifest format has no BUY-time backfill pool; selection will be recalculated.')
                    _selection_manifest=None
                if _selection_manifest and (
                    str(_selection_manifest.get('logic_signature') or '') != logic_signature
                    or str(_selection_manifest.get('interval') or '') != interval
                ):
                    print(f'Historical SELECTION LOCK INVALIDATED {day}: latest GUI logic/interval changed; cached market data will be reused, selection will be recalculated.')
                    _selection_manifest=None
                if _selection_manifest:
                    _manifest_rows=[]
                    _market_by_symbol={str(rr.get('Symbol') or '').strip().upper(): rr for _,rr in market.iterrows()}
                    for _m in (_selection_manifest.get('canonical_ranked_symbols') or _selection_manifest.get('symbols', [])):
                        _sym_m=str(_m.get('symbol') or '').strip().upper()
                        _rr=_market_by_symbol.get(_sym_m)
                        if _rr is None:
                            continue
                        _stock_m=str(_m.get('breeze_symbol') or '').strip() or _sym_m
                        _logic_m=list(dict.fromkeys(_m.get('logics') or []))
                        _logic_times_m={}
                        for _lname,_ts in (_m.get('logic_times') or {}).items():
                            try: _logic_times_m[_lname]=datetime.fromisoformat(str(_ts))
                            except Exception: pass
                        try: _first_m=datetime.fromisoformat(str(_m.get('first_buy_time')))
                        except Exception: _first_m=datetime.combine(day, datetime.min.time()).replace(hour=9,minute=15)
                        _manifest_rows.append((_first_m,_sym_m,_logic_m[0] if _logic_m else '',_rr,_logic_m,_stock_m,_logic_times_m))
                    _manifest_rows.sort(key=lambda z:(z[0],z[1]))
                    # Honor the CURRENT Historical Shares setting when an older
                    # selection lock exists. Example: a previous run selected 30
                    # shares and the GUI is now set to 20. Reuse the existing
                    # first-cum-first-buy ranking, but replay only the first 20.
                    # Never expand the old universe or redownload data merely
                    # because the cap changed.
                    _manifest_rows.sort(key=lambda z:(z[0],z[1]))
                    selected_timing=_manifest_rows[:global_share_cap] if global_share_cap>0 else list(_manifest_rows)
                    _manifest_backfill_rows=list(_manifest_rows)
                    _manifest_backfill_rows=[]
                    for _m in _selection_manifest.get('backfill_symbols', []):
                        _sym_m=str(_m.get('symbol') or '').strip().upper()
                        _rr=_market_by_symbol.get(_sym_m)
                        if _rr is None:
                            continue
                        _stock_m=str(_m.get('breeze_symbol') or '').strip() or _sym_m
                        _logic_m=list(dict.fromkeys(_m.get('logics') or []))
                        _logic_times_m={}
                        for _lname,_ts in (_m.get('logic_times') or {}).items():
                            try: _logic_times_m[_lname]=datetime.fromisoformat(str(_ts))
                            except Exception: pass
                        try: _first_m=datetime.fromisoformat(str(_m.get('first_buy_time')))
                        except Exception: _first_m=datetime.combine(day, datetime.min.time()).replace(hour=9,minute=15)
                        _manifest_backfill_rows.append((_first_m,_sym_m,_logic_m[0] if _logic_m else '',_rr,_logic_m,_stock_m,_logic_times_m))
                    _manifest_backfill_rows.sort(key=lambda z:(z[0],z[1]))
                    # Jump directly to replay; all 1-minute/1-second data is cache-first.
                    # The common cap/universe block below applies the CURRENT GUI cap.
                    _skip_new_selection=True
                else:
                    _skip_new_selection=False
                # Full data-download mode must not let a saved strategy-selection manifest
                # bypass the all-qualified 1-second download stage. Re-scan the current
                # GUI-qualified universe using cache-first 1-minute data, then fetch/cache
                # 1-second candles for every qualified symbol. This does not change the
                # eventual trade-ranking or BUY-output caps.
                if full_interval_all_qualified and _skip_new_selection:
                    print(
                        f'Historical FULL MODE: bypassing saved selection-only shortcut for {day}; '
                        'revalidating the GUI-qualified universe so every qualified symbol receives both intervals.',
                        flush=True
                    )
                    _skip_new_selection=False
                # Build the union of candidates passing at least one configured logic.
                # Historical Shares is NOT a universe filter. The complete NSE market universe is scanned;
                # only shares that pass at least one enabled GUI strategy become workable candidates.
                # Maximum Orders is applied only after actual intraday BUY signals are replayed and ranked.
                _historical_top_n = market.copy()
                candidates=_historical_top_n.copy()
                if 'Symbol' in candidates.columns:
                    _symbol_cache = _load_security_cache()
                    _symbol_cache_keys = {str(k).strip().upper() for k in _symbol_cache}
                    candidates['_raw_symbol'] = candidates['Symbol'].astype(str).str.strip().str.upper()
                    candidates['_alias_row'] = ~candidates['_raw_symbol'].isin(_symbol_cache_keys)
                    candidates['Symbol'] = candidates['_raw_symbol'].map(
                        lambda s: _canonicalize_historical_symbol(s, _symbol_cache)
                    )
                    # Multiple legacy/canonical rows for one company become one
                    # candidate. Prefer the row already carrying the NSE symbol.
                    candidates = candidates.sort_values(
                        ['_alias_row', 'Symbol'], kind='stable'
                    ).drop_duplicates(subset=['Symbol'], keep='first')
                    candidates = candidates.drop(columns=['_raw_symbol', '_alias_row'], errors='ignore')
                    print(
                        f'Historical duplicate-company normalization {day}: '
                        f'{len(_historical_top_n)} input rows -> {len(candidates)} unique NSE symbols.'
                    )
                candidates['Movement %']=pd.to_numeric(candidates['Movement %'],errors='coerce')
                candidates['Volume']=pd.to_numeric(candidates['Volume'],errors='coerce')
                candidates['Turnover Cr']=pd.to_numeric(candidates['Turnover Cr'],errors='coerce')
                # Normalize numeric fields defensively. Some NSE archive/cached CSV
                # variants can contain separators in numeric text; without this,
                # pd.to_numeric silently turns valid values into NaN and the historical
                # candidate count becomes falsely zero.
                for _c in ('Open','High','Previous close','Volume','Turnover Cr'):
                    if _c in candidates.columns:
                        candidates[_c]=pd.to_numeric(candidates[_c].astype(str).str.replace(',','',regex=False), errors='coerce')
                candidate_rows=[]
                diagnostic_counts={name:{'direction':0,'movement':0,'price_band':0,'volume':0,'turnover':0,'sma':0,'sma_fallback':0,'all':0} for name,_ in enabled}
                # HARD HISTORICAL DOWNLOAD RULE: each enabled logic's Open Direction
                # controls which symbols are allowed to reach the intraday downloader.
                # UP -> only Open > Previous Close; DOWN -> only Open < Previous Close;
                # BOTH -> either direction. This is applied before movement/price/volume/
                # turnover filters, so opposite-direction shares never trigger 1m/1s
                # historical downloads for that logic. If multiple enabled logics use
                # different directions, the downloader uses the union: UP + DOWN = both.
                direction_scope=[]
                for _lname,_cfg in enabled:
                    direction_scope.append(f"{_lname}={_cfg.get('open_direction','BOTH')}")
                print('Historical direction download scope: ' + ', '.join(direction_scope))
                for _,r in candidates.iterrows():
                    passed=[]
                    for logic_name,cfg in enabled:
                        try:
                            op=float(r['Open']); hi=float(r['High']); pc=float(r['Previous close'])
                            vol=float(r['Volume']); turn=float(r['Turnover Cr'])
                            if direction_ok(op,pc,cfg['open_direction']):
                                diagnostic_counts[logic_name]['direction']+=1
                                trigger_level=entry_level(op,cfg,pc)
                                if float(cfg['open_movement']) <= 0 or hi >= trigger_level:
                                    diagnostic_counts[logic_name]['movement']+=1
                                    if (not price_band_enabled(cfg)) or open_price_band_ok(op,cfg):
                                        diagnostic_counts[logic_name]['price_band']+=1
                                        if float(cfg['volume']) <= 0 or vol >= float(cfg['volume']):
                                            diagnostic_counts[logic_name]['volume']+=1
                                            if float(cfg['turnover_cr']) <= 0 or turn >= float(cfg['turnover_cr']):
                                                diagnostic_counts[logic_name]['turnover']+=1
                                                sma_ok=True
                                                if sma_ok:
                                                            diagnostic_counts[logic_name]['sma']+=1
                                                if sma_ok:
                                                    diagnostic_counts[logic_name]['all']+=1
                                                    passed.append(logic_name)
                        except Exception:
                            continue
                    if passed:
                        candidate_rows.append((r,passed))
                if not candidate_rows:
                    print(f'Historical candidate diagnostics {day}: {diagnostic_counts}')
                    for _lname,_cfg in enabled:
                        print(
                            f"Historical {_lname} GUI filters: direction={_cfg.get('open_direction')}, "
                            f"movement={_cfg.get('open_movement')}%, price_band={'ON' if price_band_enabled(_cfg) else 'OFF'}, "
                            f"price={_cfg.get('price_small')}..{_cfg.get('price_big')}, "
                            f"volume>={_cfg.get('volume')}, turnover_cr>={_cfg.get('turnover_cr')}, "
                            f"SMA={'ON' if _cfg.get('ma_filter_enabled',False) else 'OFF'}"
                            + (f" [{_cfg.get('ma_filter_5',False)}/{_cfg.get('ma_filter_20',False)}/{_cfg.get('ma_filter_100',False)}/{_cfg.get('ma_filter_200',False)}]" if _cfg.get('ma_filter_enabled',False) else '')
                            + f", buy_window={_cfg.get('buy_entry_time')}..{_cfg.get('buy_exit_time')}"
                        )
                # Excel audit contains WORKABLE candidates only. The full NSE universe
                # is used for screening, but symbols failing every enabled GUI strategy
                # are intentionally omitted from the output workbook.
                gui_qualified_universe = {}
                for _qr, _qpassed in candidate_rows:
                    _qdict = _qr.to_dict()
                    _qsym = str(_qdict.get('Symbol') or '').strip().upper()
                    if not _qsym:
                        continue
                    if _qsym not in gui_qualified_universe:
                        gui_qualified_universe[_qsym] = {'row': _qdict, 'logics': [], 'qualified': True}
                    gui_qualified_universe[_qsym]['logics'] = list(dict.fromkeys(
                        gui_qualified_universe[_qsym]['logics'] + list(_qpassed or [])
                    ))
                print(
                    f'Historical WORKABLE UNIVERSE {day}: {len(gui_qualified_universe)} unique shares; '
                    f'only workable candidates will be written to Excel with signal/ranking/decision timestamps.'
                )

                # TWO-STAGE HISTORICAL ARCHITECTURE:
                # Stage 1: scan the GUI-qualified universe with cached 1-minute data.
                # The 1-minute trigger scan is restricted to each logic's BUY window;
                # movement timing chooses which symbols need expensive 1-second data.
                # Final volume/turnover/Smart entry validation is always performed
                # again on the 1-second replay.
                # Stage 2: for EACH enabled logic, select up to Historical Shares
                # candidates by earliest movement minute, then UNION those logic
                # selections. Duplicate symbols across logics are collapsed before
                # the expensive 1-second confirmation / final replay.
                #
                # Example: Historical Shares=100 and four enabled logics => up to
                # 400 raw logic selections, but at most 400 UNIQUE symbols, and
                # fewer whenever the same symbol is selected by multiple logics.
                if not _skip_new_selection:
                    selected_map={}
                    for r,passed in candidate_rows:
                        sym_key=str(r.get('Symbol') or '').strip().upper()
                        if not sym_key:
                            continue
                        if sym_key not in selected_map:
                            selected_map[sym_key]=[r, []]
                        selected_map[sym_key][1].extend(
                            [x for x in passed if x not in selected_map[sym_key][1]]
                        )
                    candidate_rows=[(v[0],v[1]) for v in selected_map.values()]

                    # IMPORTANT: Historical Data Shares is configured PER LOGIC in the GUI.
                    # Do not use a single global/default cap here.  The GUI-saved
                    # logic_settings.json is the source of truth for Historical/Paper/Live.
                    # An explicit CLI historical_share_limit is retained only as an optional
                    # hard ceiling for command-line runs; the GUI does not pass one.
                    # The top-N universe cap was resolved before the selection path.
                    # Keep the same value for both fresh and manifest-reuse paths.
                    # Maximum Orders remains a separate execution/trade cap.
                    _positive_trade_caps = [int(v) for v in trade_caps.values() if int(v) > 0]
                    # Historical Shares is an output ceiling; Maximum Orders is
                    # the actual per-day trade ceiling. Failed candidates do not
                    # consume slots; continue until the strictest positive cap fills.
                    _hist_order_cap = min(_positive_trade_caps) if _positive_trade_caps else 0
                    if global_share_cap > 0 and _hist_order_cap > 0:
                        global_trade_cap = min(global_share_cap, _hist_order_cap)
                    else:
                        global_trade_cap = global_share_cap or _hist_order_cap or 0
                    print(
                        f'Historical TWO-STAGE SCAN {day}: '
                        f'GUI-qualified symbols={len(candidate_rows)}; '
                        f'1-minute candidate timing scan START (cache-first; replay interval={interval}); '
                        f'per-logic Historical Shares={per_logic_caps}; per-logic Maximum Orders={trade_caps}; '
                        f'Historical Shares cap={global_share_cap or "UNLIMITED"}; '
                        f'SUCCESSFUL BUY OUTPUT CAP={global_trade_cap or "UNLIMITED"}.'
                    )

                    # First pass: read/reuse 1-minute data for every GUI-qualified
                    # candidate and determine its earliest configured movement/BUY minute.
                    # The minute cache is retained in memory so the selected symbols do
                    # not read/download the same 1-minute file a second time.
                    minute_scan={}
                    timing_rows=[]
                    minute_scan_errors=0
                    minute_scan_missing_cache=0
                    # Resolve cached Breeze codes on the main thread, then overlap the
                    # network wait for 1-minute requests with two read-only workers.
                    # Strategy evaluation, ranking, manifest writes and all 1-second
                    # replay remain on the main thread and retain their original order.
                    scan_jobs=[]
                    for r,passed_logics in candidate_rows:
                        sym=str(r.get('Symbol') or '').strip().upper()
                        if not sym:
                            continue
                        stock=stock_cache.get(sym) or ''
                        if not stock:
                            if _historical_cache_only():
                                stock=sym
                            else:
                                try:
                                    _,stock=_resolve_historical_breeze_stock(get_hist_breeze(),sym,stock_cache)
                                except Exception:
                                    stock=sym
                        scan_jobs.append((r,passed_logics,sym,stock or sym))

                    # Run full data acquisition immediately after the qualified symbol
                    # universe is known. Do not wait for the slower strategy timing scan:
                    # users need to see 1-second chunk downloads begin straight away, and
                    # a cancelled ranking scan must not prevent the requested full download.
                    # FULL MODE: download/cache the complete 1-second session for EVERY
                    # GUI-qualified symbol, not merely the ranked BUY-trigger candidates.
                    # This is a data-download scope only; the existing strategy ranking,
                    # Historical Shares cap, Maximum Orders cap and trade replay remain
                    # unchanged.  Each symbol's 1-second data is checkpointed by the
                    # canonical session downloader, so interrupted runs resume from cache.
                    if full_interval_all_qualified:
                        _full_jobs = list(scan_jobs)
                        _full_ok = 0
                        _full_minute_rows = 0
                        _full_second_rows = 0
                        _full_failed = 0
                        _full_t0 = time.perf_counter()
                        print(
                            f'Historical FULL INTERVAL DOWNLOAD START {day}: '
                            f'1-minute + 1-second full-session candles for ALL {len(_full_jobs)} GUI-qualified symbols; '
                            'validating canonical CSVs; cache-first; strategy ranking unchanged.',
                            flush=True
                        )
                        _full_breeze = get_hist_breeze() if _full_jobs and not _historical_cache_only() else None
                        for _full_idx, (_fr, _fpl, _fsym, _fstock) in enumerate(_full_jobs, 1):
                            try:
                                if _historical_cache_only():
                                    raise RuntimeError('Full interval download requires Breeze access; offline/cache-only mode cannot fetch missing 1-second candles.')
                                # Use the same tested full-share downloader as the
                                # one-share workflow. It reuses minute cache, fetches
                                # missing second chunks, then atomically combines both.
                                _fminute, _fsecond, _full_csv_path, _ = _download_full_historical_share(
                                    _full_breeze, _fsym, _fstock, day, outdir=_cache_day_dir(day)
                                )
                                _fminute_actual = _fminute
                                _fsecond_actual = _fsecond
                                if _fminute_actual is not None and not _fminute_actual.empty and 'is_actual' in _fminute_actual.columns:
                                    _fminute_actual = _fminute_actual[_fminute_actual['is_actual'].astype(bool)]
                                if _fsecond_actual is not None and not _fsecond_actual.empty and 'is_actual' in _fsecond_actual.columns:
                                    _fsecond_actual = _fsecond_actual[_fsecond_actual['is_actual'].astype(bool)]
                                _mcount = len(_fminute_actual) if _fminute_actual is not None else 0
                                _scount = len(_fsecond_actual) if _fsecond_actual is not None else 0
                                if _mcount <= 0 or _scount <= 0:
                                    raise RuntimeError(
                                        f'Incomplete full interval result: 1-minute actual={_mcount}; 1-second actual={_scount}.'
                                    )
                                # Verify the actual final CSV on disk, not just frames.
                                _verify = pd.read_csv(_full_csv_path, usecols=['interval'])
                                _counts = _verify['interval'].astype(str).str.lower().value_counts().to_dict()
                                _csv_minute = int(_counts.get('1minute', 0))
                                _csv_second = int(_counts.get('1second', 0))
                                if _csv_minute <= 0 or _csv_second <= 0:
                                    raise RuntimeError(
                                        f'Canonical CSV interval verification failed: 1minute={_csv_minute}; 1second={_csv_second}.'
                                    )
                                _full_ok += 1
                                _full_minute_rows += _csv_minute
                                _full_second_rows += _csv_second
                                _full_csv_size = _full_csv_path.stat().st_size
                                print(
                                    f'Historical FULL INTERVAL VERIFIED {day}: {_full_idx}/{len(_full_jobs)} '
                                    f'{_fsym} -> {_fstock}; 1-minute rows={_csv_minute:,}; '
                                    f'1-second rows={_csv_second:,}; combined rows={_csv_minute + _csv_second:,}; '
                                    f'file={_full_csv_path.name}; size={_full_csv_size:,} bytes.',
                                    flush=True
                                )
                            except Exception as _full_exc:
                                _full_failed += 1
                                print(
                                    f'Historical FULL INTERVAL ERROR {day}: {_full_idx}/{len(_full_jobs)} '
                                    f'{_fsym}: {type(_full_exc).__name__}: {_full_exc}',
                                    flush=True
                                )
                            if _full_idx % 5 == 0 or _full_idx == len(_full_jobs):
                                print(
                                    f'Historical FULL INTERVAL PROGRESS {day}: {_full_idx}/{len(_full_jobs)} '
                                    f'symbols; verified={_full_ok}; failed/incomplete={_full_failed}; '
                                    f'1-minute_rows={_full_minute_rows:,}; 1-second_rows={_full_second_rows:,}; '
                                    f'elapsed={time.perf_counter()-_full_t0:.1f}s.',
                                    flush=True
                                )
                        print(
                            f'Historical FULL INTERVAL DOWNLOAD COMPLETE {day}: '
                            f'qualified={len(_full_jobs)}; verified={_full_ok}; failed/incomplete={_full_failed}; '
                            f'1-minute_rows={_full_minute_rows:,}; 1-second_rows={_full_second_rows:,}; '
                            f'combined_CSVs={_full_ok:,}; elapsed={time.perf_counter()-_full_t0:.1f}s.',
                            flush=True
                        )


                    def _fetch_minute_scan_job(job):
                        r,passed_logics,sym,stock=job
                        try:
                            try:
                                candles=_historical_session_candles(None,stock,day,'1minute')
                            except HistoricalBreezeRequired:
                                candles=_historical_session_candles(get_hist_breeze(),stock,day,'1minute')
                            return (job,candles,None)
                        except Exception as exc:
                            return (job,None,exc)

                    _minute_workers = 6 if interval == '1minute' and not _historical_cache_only() else 1
                    if _minute_workers > 1:
                        print(f'Historical 1-minute downloader: {_minute_workers} workers; REST starts paced at 0.68s (about 88/minute max; GUI strategy order is preserved; 52-week enrichment uses local cache only).')
                    _old_socket_timeout=socket.getdefaulttimeout()
                    _parallel_socket_timeout = _minute_workers > 1
                    if _parallel_socket_timeout:
                        # broker_call historically adjusted this process-wide setting
                        # per request. Keep one stable value while the worker pool is
                        # active so concurrent calls cannot restore each other's value.
                        socket.setdefaulttimeout(35)
                        globals()['_HISTORICAL_PARALLEL_SOCKET_TIMEOUT_ACTIVE']=True
                    try:
                        if _minute_workers > 1 and len(scan_jobs) > 1:
                            # Consume completed requests as they arrive so a slow
                            # first symbol cannot hide progress for several minutes.
                            # Store by original index and process in original order
                            # below, preserving deterministic strategy/ranking output.
                            _scan_results=[None] * len(scan_jobs)
                            _completed_scan_jobs=0
                            _last_scan_progress=time.monotonic()
                            with concurrent.futures.ThreadPoolExecutor(max_workers=_minute_workers, thread_name_prefix='hist-1m') as _pool:
                                # Submit every job and retain its original position.
                                # The prior build referenced _future_to_index without
                                # ever creating it, so the first trading date crashed
                                # before any 1-minute candle request was executed.
                                _future_to_index={
                                    _pool.submit(_fetch_minute_scan_job, _job): _idx
                                    for _idx, _job in enumerate(scan_jobs)
                                }
                                _pending_scan_futures=set(_future_to_index)
                                while _pending_scan_futures:
                                    _done_scan_futures,_pending_scan_futures=concurrent.futures.wait(
                                        _pending_scan_futures,
                                        timeout=30.0,
                                        return_when=concurrent.futures.FIRST_COMPLETED
                                    )
                                    for _future in _done_scan_futures:
                                        _idx=_future_to_index[_future]
                                        try:
                                            _scan_results[_idx]=_future.result()
                                        except Exception as _unexpected_scan_exc:
                                            _scan_results[_idx]=(scan_jobs[_idx],None,_unexpected_scan_exc)
                                        _completed_scan_jobs += 1
                                    _now_scan_progress=time.monotonic()
                                    if (_completed_scan_jobs % 25 == 0 or
                                            _completed_scan_jobs == len(scan_jobs) or
                                            _now_scan_progress-_last_scan_progress >= 30.0):
                                        print(
                                            f'Historical 1-minute DOWNLOAD progress {day}: '
                                            f'{_completed_scan_jobs}/{len(scan_jobs)} requests completed; '
                                            f'{len(_pending_scan_futures)} still running; '
                                            f'elapsed={_now_scan_progress-_last_scan_progress:.0f}s since last report.',
                                            flush=True
                                        )
                                        _last_scan_progress=_now_scan_progress
                        else:
                            _scan_results=[_fetch_minute_scan_job(job) for job in scan_jobs]
                    finally:
                        if _parallel_socket_timeout:
                            globals()['_HISTORICAL_PARALLEL_SOCKET_TIMEOUT_ACTIVE']=False
                            socket.setdefaulttimeout(_old_socket_timeout)

                    for scan_index,((r,passed_logics,sym,stock),minute_candles,fetch_error) in enumerate(_scan_results,1):
                        if fetch_error is not None:
                            minute_scan_errors += 1
                            if isinstance(fetch_error,HistoricalBreezeRequired) and _historical_cache_only():
                                minute_scan_missing_cache += 1
                            elif not (_historical_cache_only() and 'LOCAL CACHE ONLY' in str(fetch_error)):
                                print(f'Historical 1-minute timing scan warning: {sym} {day}: {fetch_error}')
                            if scan_index % 100 == 0 or scan_index == len(_scan_results):
                                print(
                                    f'Historical 1-minute timing scan {day}: '
                                    f'{scan_index}/{len(scan_jobs)} symbols checked; '
                                    f'triggers found={len(timing_rows)}; '
                                    f'missing local 1-minute caches={minute_scan_missing_cache}.'
                                )
                            continue

                        # A stale/wrong symbol->Breeze mapping must never make a
                        # valid NSE share disappear from the ranked BUY universe.
                        if minute_candles is None or minute_candles.empty:
                            try:
                                _,resolved_stock=_resolve_historical_breeze_stock(get_hist_breeze(),sym,{})
                                if resolved_stock and resolved_stock != stock:
                                    retry=_historical_session_candles(None,resolved_stock,day,'1minute')
                                    if retry is None or retry.empty:
                                        retry=_historical_session_candles(get_hist_breeze(),resolved_stock,day,'1minute')
                                    if retry is not None and not retry.empty:
                                        print(f'Historical SYMBOL REMAP RECOVERY: {sym}: {stock} -> {resolved_stock}; 1-minute data restored.')
                                        stock=resolved_stock
                                        minute_candles=retry
                                        stock_cache[sym]=resolved_stock
                                        _save_security_cache(stock_cache)
                            except Exception as remap_exc:
                                print(f'Historical SYMBOL REMAP WARNING {day} {sym}: {remap_exc}')
                        minute_scan[sym]=(r,passed_logics,stock,minute_candles)
                        # Diagnostics and strategy timing are deliberately evaluated
                        # serially to preserve deterministic ordering and GUI logic.
                        for _diag_logic_name in (passed_logics if _historical_verbose_enabled() else []):
                            _diag_cfg = get_logic(_diag_logic_name)
                            if not _diag_cfg.get('smart_mode', False):
                                _diag = _historical_trigger_diagnostic(
                                    minute_candles, day, _diag_cfg, _diag_logic_name, sym,
                                    daily_open=r['Open'], daily_previous_close=r['Previous close']
                                )
                                _print_historical_trigger_diagnostic(_diag)
                        earliest=None
                        earliest_logic=None
                        valid_logics=[]
                        logic_times={}
                        for _logic_name in passed_logics:
                            _cfg=get_logic(_logic_name)
                            _tm=_first_movement_minute(
                                minute_candles, day, _cfg,
                                daily_open=r['Open'],
                                daily_previous_close=r['Previous close']
                            )
                            if _tm is not None:
                                valid_logics.append(_logic_name)
                                logic_times[_logic_name]=_tm
                                if earliest is None or _tm < earliest:
                                    earliest=_tm
                                    earliest_logic=_logic_name
                        if valid_logics:
                            passed_logics=valid_logics
                        if earliest is not None:
                            timing_rows.append((earliest,sym,earliest_logic,r,passed_logics,stock,logic_times))

                        if scan_index % 100 == 0 or scan_index == len(_scan_results):
                            print(
                                f'Historical 1-minute timing scan {day}: '
                                f'{scan_index}/{len(scan_jobs)} symbols checked; '
                                f'triggers found={len(timing_rows)}; '
                                f'missing local 1-minute caches={minute_scan_missing_cache}.'
                            )

                    _perf = _historical_perf_snapshot()
                    _avg_api = (_perf['api_seconds'] / _perf['api_calls']) if _perf['api_calls'] else 0.0
                    print(
                        f"Historical 1-MINUTE PERFORMANCE (cumulative through {day}) : candidates={len(candidate_rows):,}; "
                        f"cache_hits={_perf['cache_hits']:,}; cache_misses={_perf['cache_misses']:,}; "
                        f"Breeze_calls={_perf['api_calls']:,}; API_failures={_perf['api_failures']:,}; "
                        f"avg_API_seconds={_avg_api:.2f}; API_seconds_total={_perf['api_seconds']:.1f}; "
                        f"candles_loaded={_perf['candles']:,}. "
                        f"Set BREEZE_HISTORICAL_VERBOSE=1 for per-symbol diagnostics."
                    )

                    # Build the same cross-sectional momentum rank used by
                    # Paper/Live.  Rank each stock's 15-minute momentum against
                    # every GUI-qualified stock at the same 5-minute bucket.
                    # This is calculated only from the already-loaded 1-minute
                    # cache and never uses future candles.
                    _smart_rank_context = {}
                    try:
                        _rank_rows=[]
                        for _rsym, (_rr, _rpl, _rstk, _rcand) in minute_scan.items():
                            # A few symbols may have an empty/malformed cache frame. Skip
                            # only those rows rather than aborting the entire rank context
                            # with KeyError('_dt') for otherwise usable symbols.
                            if _rcand is None or _rcand.empty or '_dt' not in _rcand.columns or 'close' not in _rcand.columns:
                                continue
                            _rx=_rcand.copy().sort_values('_dt')
                            if 'is_actual' in _rx.columns:
                                _rx=_rx[_rx['is_actual'].astype(bool)].copy()
                            for _cc in ('close','volume'):
                                if _cc in _rx.columns:
                                    _rx[_cc]=pd.to_numeric(_rx[_cc], errors='coerce')
                            _agg={'close':'last','volume':'sum'}
                            if 'previous_close' in _rx.columns: _agg['previous_close']='last'
                            _m5r=_rx.set_index('_dt').resample('5min', label='left', closed='left').agg(_agg).dropna(subset=['close'])
                            _m5r['mom15']=(_m5r['close']/_m5r['close'].shift(3)-1.0)*100.0
                            _pc_hist=0.0
                            try:
                                _pc_hist=float(_rr.get('Previous Close', _rr.get('previous_close', _rr.get('Prev Close', 0))) or 0)
                            except Exception: _pc_hist=0.0
                            if 'previous_close' in _m5r.columns:
                                _m5r['movement']=(_m5r['close']/_m5r['previous_close']-1.0)*100.0
                            elif _pc_hist>0:
                                _m5r['movement']=(_m5r['close']/_pc_hist-1.0)*100.0
                            else:
                                _m5r['movement']=np.nan
                            for _bt,_mr in _m5r.iterrows():
                                if pd.notna(_mr.get('mom15')): _rank_rows.append((_bt,str(_rsym).upper(),'momentum_15m',float(_mr['mom15'])))
                                if pd.notna(_mr.get('movement')): _rank_rows.append((_bt,str(_rsym).upper(),'movement',float(_mr['movement'])))
                        if _rank_rows:
                            _rdf=pd.DataFrame(_rank_rows, columns=['bucket','symbol','metric','value'])
                            _rdf['rank_num']=_rdf.groupby(['bucket','metric'])['value'].rank(method='min', ascending=False).astype(int)
                            for _z in _rdf.itertuples(index=False):
                                _smart_rank_context[(str(_z.symbol).upper(), pd.Timestamp(_z.bucket), str(_z.metric))]=int(_z.rank_num)
                        print(f'Historical SMART RANK CONTEXT {day}: {len(_smart_rank_context):,} symbol/bucket ranks built.')
                    except Exception as _rank_exc:
                        print(f'Historical SMART RANK CONTEXT WARNING {day}: {_rank_exc}')
                        _smart_rank_context = {}

                    # GLOBAL FIRST-COME-FIRST-BUY SELECTION:
                    # Historical Shares is ONE global unique-share limit for the date.
                    # All enabled logics contribute to one ranking; a symbol selected by
                    # more than one logic is still only ONE historical share/replay.
                    # This prevents duplicate Logic 1 historical downloads.
                    selected_symbol_map = {}
                    for row in timing_rows:
                        _earliest,_sym,_earliest_logic,_r,_valid_logics,_stock = row[:6]
                        _logic_times = row[6] if len(row) > 6 and isinstance(row[6], dict) else {}
                        if _sym not in selected_symbol_map:
                            selected_symbol_map[_sym] = {
                                'row': row,
                                'selected_logics': list(dict.fromkeys(_valid_logics or [])),
                                'selected_times': dict(_logic_times),
                            }
                        else:
                            info=selected_symbol_map[_sym]
                            info['selected_logics']=list(dict.fromkeys(info['selected_logics'] + list(_valid_logics or [])))
                            for _lname,_tm in _logic_times.items():
                                if _tm is not None:
                                    old=info['selected_times'].get(_lname)
                                    if old is None or _tm < old:
                                        info['selected_times'][_lname]=_tm
                            if _earliest < info['row'][0]:
                                info['row']=row

                    selected_timing=[]
                    for _sym,_info in selected_symbol_map.items():
                        _row=list(_info['row'])
                        _selected_logics=list(dict.fromkeys(_info['selected_logics']))
                        _selected_times=_info['selected_times']
                        _global_tm=min(_selected_times.values()) if _selected_times else _row[0]
                        _global_logic=next((n for n,t in _selected_times.items() if t == _global_tm), _row[2])
                        _row[0]=_global_tm
                        _row[2]=_global_logic
                        _row[4]=_selected_logics
                        if len(_row) >= 7:
                            _row[6]=_selected_times
                        selected_timing.append(tuple(_row))

                    selected_timing.sort(key=lambda x: (x[0], x[1]))
                    # HARD SECOND-DATA UNIVERSE: only shares that actually passed the
                    # GUI conditions, reached an intraday trigger in the 1-minute
                    # screening stage, and survived the Historical Shares cap may ever
                    # reach the expensive 1-second downloader.  There is no backfill
                    # pool beyond this exact list.
                    # STRICT 1-SECOND DOWNLOAD CAP: apply the GUI cap BEFORE replay.
                    # The authoritative universe is rebuilt below AFTER both the fresh
                    # selection path and the manifest-reuse path have completed, so a
                    # repeated run can never inherit a stale universe from another day.
                    # The common cap/accounting block below is the single source of
                    # truth for both fresh ranking and manifest replay.
                else:
                    # Manifest already supplied selected_timing.  The common block
                    # below applies the current GUI cap before replay/download.
                    pass

                # A saved selection-manifest replay does not execute the fresh
                # 1-minute timing scan above.  Keep timing_rows populated from the
                # locked selection so diagnostics/replay bookkeeping cannot reference
                # an uninitialized local variable.  This does NOT expand the universe.
                if _skip_new_selection:
                    timing_rows = list(_manifest_backfill_rows or selected_timing)
                else:
                    timing_rows = list(timing_rows)

                # FAST 1-SECOND REPLAY: the 1-minute trigger is a lower bound for the
                # exact 1-second BUY time. Replay candidates in that order and admit
                # only the candidates needed to prove the configured BUY cap. This
                # preserves first-come-first-BUY ordering while avoiding unnecessary
                # full-session 1-second downloads.
                _ranked_timing_pool=sorted(
                    list(selected_timing or timing_rows),
                    key=lambda x:(x[0], x[1])
                )
                # ENFORCE THE GUI HISTORICAL-SHARES LIMIT IN THE FRESH AND RESUME
                # PATHS.  Previously the helper existed but was never called here,
                # so the 1-second replay and Excel audit could include every qualified
                # company even when the GUI requested a smaller historical output.
                # Keep the complete chronological candidate pool here. The
                # successful-BUY cap is applied after replay, so if an early-ranked
                # share fails the exact intraday logic, the next share can replace it.
                _ranked_timing_pool, _capped_second_universe = _build_historical_second_data_universe(
                    _ranked_timing_pool, 0, day
                )
                if global_share_cap > 0:
                    _before_qualified_cap = len(gui_qualified_universe)
                    gui_qualified_universe = {
                        _sym: _info for _sym, _info in gui_qualified_universe.items()
                        if str(_sym).strip().upper() in _capped_second_universe
                    }
                    _after_qualified_cap = len(gui_qualified_universe)
                    print(
                        f'Historical GUI SHARE CAP {day}: limit={global_share_cap}; '
                        f'ranked trigger candidates admitted={len(_capped_second_universe)}; '
                        f'qualified Excel universe {_before_qualified_cap}->{_after_qualified_cap}.'
                    )
                candidate_rows=[(x[3],x[4]) for x in _ranked_timing_pool]
                _second_data_universe=set()
                selected_by_logic={name:0 for name,_cfg in enabled}
                for _r0,_pl0 in candidate_rows:
                    for _lname in _pl0:
                        if _lname in selected_by_logic:
                            selected_by_logic[_lname] += 1
                print(
                    f'Historical FAST FIRST-COME-FIRST-BUY {day}: HISTORICAL SHARE CAP={global_share_cap or "UNLIMITED"}; '
                    f'1-minute trigger candidates={len(candidate_rows)}; replay interval={interval}; BUY-time cutoff applies when enabled; '
                    f'logic membership={selected_by_logic}.'
                )
                if selected_timing:
                    preview=', '.join(
                        f'{i+1}:{x[1]}@{x[0]:%H:%M:%S}[{",".join(x[4])}]'
                        for i,x in enumerate(selected_timing[:10])
                    )
                    print(f'Historical FIRST-COME-FIRST-BUY preview: {preview}')
                if not _skip_new_selection:
                    print(f'Historical candidate diagnostics {day}: {diagnostic_counts}')
                    print(
                        f'Historical data scan {day}: GUI-condition candidates={len(selected_map)}; '
                        f'full-universe trigger candidates={len(timing_rows)}; '
                        f'bounded 1-second/replay candidates={len(candidate_rows)}.'
                    )
                if _skip_new_selection:
                    print(f'Historical SELECTION LOCK REUSE {day}: {len(selected_timing)} trigger-ranked shares available; 1-second replay is BUY-cap bounded.')
                print(
                    f'Historical REPLAY-DATA MODE {day}: candidates use interval={interval}; '
                    f'BUY cap={global_trade_cap or "UNLIMITED"}.'
                )

                if not _skip_new_selection:
                    _save_historical_selection_manifest(day, selected_timing, global_share_cap, enabled, logic_signature=logic_signature, interval=interval, backfill_timing=timing_rows)
                results_before_day=len(results)
                candidates_checked=0

                def _assert_second_data_allowed(symbol):
                    # Only candidates explicitly admitted to the bounded replay may
                    # request the expensive 1-second stream.
                    _sym=str(symbol).strip().upper()
                    if _sym not in _second_data_universe:
                        raise RuntimeError(
                            f'SECOND-DATA LOCK VIOLATION PREVENTED: {_sym} was not admitted '
                            f'to the bounded 1-second replay universe for {day}.'
                        )
                    return True

                def _replay_candidate_batch(batch_rows, batch_start_index=1):
                    """Replay a ranked batch and allow later candidates to backfill failed 1-second replays."""
                    nonlocal candidates_checked
                    for candidate_index,(r,passed_logics) in enumerate(batch_rows,batch_start_index):
                        candidates_checked += 1
                        sym=str(r['Symbol']).strip().upper()
                        daily_common={'rank':None,'candidate_index':candidate_index,'breeze_symbol':'','open':r['Open'],'high':r['High'],
                            'low':r['Low'],'close':r['Close'],'previous_close':r['Previous close'],
                            'volume':r['Volume'],'movement_pct':r.get('Movement %'),'turnover_cr':float(r['Turnover (₹)'])/10000000}
                        try:
                            stock=stock_cache.get(sym) or sym
                            # Reuse the exact 1-minute DataFrame already loaded during the
                            # timing scan. The previous path re-opened each selected CSV and
                            # then opened it again for final cache diagnostics, adding avoidable
                            # disk I/O and noisy log lines on large date ranges.
                            hb=None
                            _scan_entry = minute_scan.get(sym) if interval == '1minute' else None
                            _scan_candles = _scan_entry[3] if _scan_entry and len(_scan_entry) > 3 else None
                            if _scan_candles is not None and not _scan_candles.empty:
                                stock = _scan_entry[2] or stock
                                candles = _scan_candles
                                cache_before = True
                            else:
                                # CACHE-FIRST fallback for 1second/hybrid or a missing scan frame.
                                cache_interval = '1minute' if interval == 'hybrid' else interval
                                cache_path=_cache_intraday_path(day,stock,cache_interval)
                                cache_before=cache_path.exists()
                                try:
                                    if interval == 'hybrid':
                                        minute_candles=_historical_session_candles(hb,stock,day,'1minute')
                                        if minute_candles is None or minute_candles.empty:
                                            try:
                                                _,resolved_stock=_resolve_historical_breeze_stock(get_hist_breeze(),sym,{})
                                                if resolved_stock and resolved_stock != stock:
                                                    retry=_historical_session_candles(None,resolved_stock,day,'1minute')
                                                    if retry is None or retry.empty:
                                                        retry=_historical_session_candles(get_hist_breeze(),resolved_stock,day,'1minute')
                                                    if retry is not None and not retry.empty:
                                                        print(f'Historical SYMBOL REMAP RECOVERY: {sym}: {stock} -> {resolved_stock}; replay data restored.')
                                                        stock=resolved_stock
                                                        minute_candles=retry
                                                        stock_cache[sym]=resolved_stock
                                                        _save_security_cache(stock_cache)
                                            except Exception as remap_exc:
                                                print(f'Historical SYMBOL REMAP WARNING {day} {sym}: {remap_exc}')
                                        _assert_second_data_allowed(sym)
                                        print(f'Historical HYBRID: {sym} on {day} is workable; loading 1-second session for signal validation.')
                                        candles=_historical_session_candles(hb,stock,day,'1second')
                                    elif interval == '1second':
                                        _assert_second_data_allowed(sym)
                                        candles=_historical_session_candles(hb,stock,day,'1second')
                                    else:
                                        candles=_historical_session_candles(hb,stock,day,'1minute')
                                except HistoricalBreezeRequired:
                                    # Authenticate only when a required cache is missing.
                                    hb=get_hist_breeze()
                                    if interval == 'hybrid':
                                        _assert_second_data_allowed(sym)
                                        candles=_historical_session_candles(hb,stock,day,'1second')
                                    elif interval == '1second':
                                        _assert_second_data_allowed(sym)
                                        candles=_historical_session_candles(hb,stock,day,'1second')
                                    else:
                                        candles=_historical_session_candles(hb,stock,day,'1minute')
    
                            if (candles is None or candles.empty) and stock == sym:
                                try:
                                    names=hb.get_names(exchange_code='NSE',stock_code=sym) or {}
                                    resolved=str(names.get('isec_stock_code') or '').strip()
                                    if resolved and resolved.upper()!=sym.upper():
                                        stock=resolved; stock_cache[sym]=stock; _save_security_cache(stock_cache)
                                        candles=_historical_session_candles(hb,stock,day,interval)
                                except Exception as map_exc:
                                    print(f'Historical mapping fallback {day} {sym}: {map_exc}')
                            if candles is None or candles.empty:
                                print(f'Historical data MISSING: {sym} -> {stock} on {day}; Breeze returned no usable candles.')
                            elif not cache_before:
                                print(f'Historical data downloaded/saved: {sym} -> {stock} on {day} ({len(candles)} candles).')
                            for logic_name in passed_logics:
                                result=_simulate_logic_from_candles(hb,stock,sym,day,candles,logic_name,interval=interval,
                                    daily_open=r['Open'],daily_high=r['High'],daily_previous_close=r['Previous close'],
                                    daily_volume=r['Volume'],daily_turnover_cr=float(r['Turnover (₹)'])/10000000,
                                    smart_rank_context=_smart_rank_context)
                                result.update(daily_common); result['breeze_symbol']=stock
                                result['historical_shares'] = int(float((result.get('logic_config') or {}).get('historical_shares', 0) or 0))
                                if interval == 'hybrid' and result.get('status') == 'HISTORICAL_REPLAY_COMPLETE':
                                    result['second_confirmation_required'] = True
                                    result['second_confirmation_ok'] = bool(candles is not None and not candles.empty)
                                if candles is not None and not candles.empty and 'is_actual' in candles.columns:
                                    actual_rows=candles[candles['is_actual']]
                                    result['intraday_actual_candles']=len(actual_rows)
                                    result['intraday_first_time']=actual_rows['_dt'].min() if not actual_rows.empty else ''
                                    result['intraday_last_time']=actual_rows['_dt'].max() if not actual_rows.empty else ''
                                result['order_sequence']=''; results.append(result)
                        except Exception as exc:
                            print(f'Historical replay ERROR {day} {sym}: {type(exc).__name__}: {exc}')
                            if hist_auth_error is not None:
                                # Authentication cannot be repaired by trying another symbol.
                                # Stop this date immediately so the log contains one actionable
                                # error instead of repeating it for every candidate.
                                print(f'Historical replay STOP {day}: Breeze authentication failed; no further symbols will be retried.')
                                raise hist_auth_error
                            for logic_name in passed_logics:
                                results.append({'mode':'historical','logic':logic_name,'symbol':sym,'trade_date':day,
                                    'status':'HISTORICAL_DATA_ERROR','error':str(exc),'rank':None,'candidate_index':candidate_index,
                                    'breeze_symbol':stock if 'stock' in locals() else '',**daily_common})

                # FAST BUY-TIME BACKFILL: process trigger-ranked candidates in small
                # batches. Once N unique BUYs exist, the next candidate's 1-minute
                # trigger is a lower bound for its 1-second BUY timestamp. If that
                # lower bound is later than the current Nth BUY, the final ranking is
                # already proven and no more 1-second downloads are necessary.
                if global_trade_cap > 0 and candidate_rows:
                    _batch_size=8
                    _processed=0
                    _stop_reason='candidate pool exhausted'
                    while _processed < len(candidate_rows):
                        _batch=candidate_rows[_processed:_processed+_batch_size]
                        for _rr,_ll in _batch:
                            _second_data_universe.add(str(_rr.get('Symbol') or '').strip().upper())
                        _replay_candidate_batch(_batch, _processed+1)
                        _processed += len(_batch)

                        _day_now=[x for x in results if x.get('trade_date')==day]
                        _complete_now={}
                        for _x in _day_now:
                            if _x.get('status') != 'HISTORICAL_REPLAY_COMPLETE' or not _x.get('buy_trade_datetime'):
                                continue
                            _sx=str(_x.get('symbol') or '').strip().upper()
                            if not _sx:
                                continue
                            _bt=_x.get('buy_trade_datetime')
                            if _sx not in _complete_now or _bt < _complete_now[_sx]:
                                _complete_now[_sx]=_bt
                        if len(_complete_now) >= global_trade_cap:
                            _nth=sorted(_complete_now.values())[:global_trade_cap][-1]
                            _next_lb=_ranked_timing_pool[_processed][0] if _processed < len(_ranked_timing_pool) else None
                            if _next_lb is None or _next_lb > _nth:
                                _stop_reason=f'BUY cap proven at {_nth}'
                                break
                    if _processed < len(candidate_rows):
                        print(f'Historical FAST 1-second cutoff {day}: processed={_processed}/{len(candidate_rows)}; {_stop_reason}.')
                else:
                    for _rr,_ll in candidate_rows:
                        _second_data_universe.add(str(_rr.get('Symbol') or '').strip().upper())
                    _replay_candidate_batch(candidate_rows, 1)

                current_day_results=[x for x in results if x.get('trade_date')==day]
                current_complete_symbols={str(x.get('symbol') or '').strip().upper() for x in current_day_results
                                          if x.get('status')=='HISTORICAL_REPLAY_COMPLETE' and x.get('symbol') not in (None,'NO TRADE')}
                print(
                    f'Historical BUY-TIME BACKFILL {day}: ranked candidates={len(candidate_rows)}; '
                    f'candidates replayed={len(_second_data_universe)} at interval={interval}; successful unique BUY candidates={len(current_complete_symbols)}; '
                    f'target={global_trade_cap or "UNLIMITED"}; failed candidates do NOT consume a slot.'
                )

                # FINAL UNIQUE-SYMBOL RESULT SELECTION:
                # The 1-second replay is already limited to the union of the per-logic
                # selections above.  A symbol selected by several logics must still
                # appear only once in Historical Data.  Choose the completed replay
                # row with the earliest actual buy timestamp for that symbol.
                day_results=[x for x in results if x.get('trade_date')==day]
                complete=[x for x in day_results if x.get('status')=='HISTORICAL_REPLAY_COMPLETE' and x.get('buy_trade_datetime')]
                # Historical replay is not live execution: do not apply Maximum Orders
                # here. Report every unique symbol whose price reaches/crosses the
                # configured movement trigger. Paper/Live execution retain their caps.
                complete=_fifo_signal_order(complete)
                by_symbol={}
                for row in complete:
                    sym=str(row.get('symbol') or '').strip().upper()
                    if not sym:
                        continue
                    by_symbol.setdefault(sym, []).append(row)

                selected_unique=[]
                selected_ids=set()
                duplicate_result_rows=0
                for sym,rows_for_sym in by_symbol.items():
                    rows_for_sym.sort(key=lambda x:(x.get('buy_trade_datetime') or datetime.max, str(x.get('logic') or '')))
                    winner=rows_for_sym[0]
                    # Preserve which configured logics produced this same-symbol
                    # trade while keeping one actual trade row in the Excel output.
                    matched_logics=[]
                    for rr in rows_for_sym:
                        lg=str(rr.get('logic') or '').strip()
                        if lg and lg not in matched_logics:
                            matched_logics.append(lg)
                    winner['matched_logics']=matched_logics
                    winner['logic']=winner.get('logic') or (matched_logics[0] if matched_logics else '')
                    selected_unique.append(winner)
                    selected_ids.add(id(winner))
                    duplicate_result_rows += max(0,len(rows_for_sym)-1)

                # First-come-first-buy ranking is based on the actual 1-second
                # validated buy timestamp, after duplicate symbols are collapsed.
                selected_unique.sort(key=lambda x:(x.get('buy_trade_datetime') or datetime.max, str(x.get('symbol') or '')))
                if global_trade_cap > 0:
                    selected_unique = selected_unique[:global_trade_cap]
                for global_rank,row in enumerate(selected_unique,1):
                    row['rank']=global_rank
                    row['order_sequence']=global_rank
                    row['first_signal_time']=row.get('buy_trade_datetime')
                    row['decision']='BUY'
                    row['decision_reason']='Movement threshold reached; historical replay includes all qualifying symbols (Maximum Orders cap applies only to Paper/Live execution).' 

                # Build a complete GUI-selection audit. Every symbol that passed
                # the GUI daily conditions gets exactly one visible Excel record.
                # Only selected symbols are permitted to reach 1-second replay;
                # symbols outside the cap are recorded as NOT SELECTED, and selected
                # symbols without a valid BUY/SELL trade are recorded as REJECTED.
                _trade_by_symbol = {}
                for _tr in selected_unique:
                    _tsym = str(_tr.get('symbol') or '').strip().upper()
                    if _tsym:
                        _trade_by_symbol[_tsym] = _tr
                _second_symbols = set(_second_data_universe)
                _audit_rows=[]
                for _idx, (_asym, _ainfo) in enumerate(gui_qualified_universe.items(), 1):
                    if _asym in _trade_by_symbol:
                        continue
                    _base = _ainfo.get('row') or {}
                    _logics = _ainfo.get('logics') or []
                    _logic_text = ', '.join(_logics) if _logics else ''
                    _qualified = True
                    _reason = ('NOT SELECTED: no qualifying BUY signal within the available replay window.' if _asym not in _second_symbols else 'NOT SELECTED: replay produced no qualifying BUY signal.')
                    _status = 'NOT SELECTED'
                    if _asym in _second_symbols:
                        _status = 'REJECTED'
                        # Prefer the most specific replay/data error already generated.
                        _prior = [x for x in results if str(x.get('trade_date') or '')[:10] == day.isoformat()
                                  and str(x.get('symbol') or '').strip().upper() == _asym]
                        _reasons=[]
                        for _pr in _prior:
                            if _pr.get('error'): _reasons.append(str(_pr.get('error')))
                            elif _pr.get('status') and _pr.get('status') not in {'HISTORICAL_REPLAY_COMPLETE'}:
                                _reasons.append(str(_pr.get('status')))
                            if _pr.get('entry_audit',{}).get('reason'):
                                _reasons.append(str(_pr['entry_audit']['reason']))
                        if _reasons:
                            _reason = 'REJECTED: ' + _reasons[0]
                        else:
                            _reason = 'REJECTED: no valid BUY/SELL trade after 1-second replay.'
                        # If this symbol produced a valid replay BUY but is outside the
                        # final selected set, the only reason for not buying is the hard cap.
                        if _asym not in _trade_by_symbol:
                            _prior_complete = any(str(x.get('symbol') or '').strip().upper() == _asym and x.get('status') == 'HISTORICAL_REPLAY_COMPLETE' for x in _prior)
                            if _prior_complete:
                                _status = 'NOT SELECTED'
                                _reason = f'NOT SELECTED: Maximum Orders cap ({global_trade_cap}) already filled; no replacement BUY is allowed after a SELL.' if global_trade_cap > 0 else 'NOT SELECTED: no BUY cap configured.'
                    _prior_signal = None
                    _prior_rows_for_symbol = locals().get('_prior', [])
                    if isinstance(_prior_rows_for_symbol, list):
                        for _prx in _prior_rows_for_symbol:
                            _prior_signal = _prx.get('buy_trade_datetime') or _prx.get('first_signal_time')
                            if _prior_signal:
                                break
                    _audit_rows.append({
                        'mode':'historical','logic':_logic_text or 'GUI QUALIFIED','symbol':_asym,'trade_date':day,
                        'status':_status,'exit_reason':_reason,'gross_pnl':None,'rank':None,'candidate_index':_idx,
                        'open':_base.get('Open'),'high':_base.get('High'),'low':_base.get('Low'),'close':_base.get('Close'),
                        'previous_close':_base.get('Previous close'),'volume':_base.get('Volume'),
                        'movement_pct':_base.get('Movement %'),'turnover_cr':float(_base.get('Turnover (₹)',0) or 0)/10000000,
                        'logic_config':(get_logic(_logics[0]) if _logics else {}),
                        'historical_shares':global_share_cap,
                        'entry_audit':{'open_direction':', '.join(str(get_logic(x).get('open_direction','')) for x in _logics) if _logics else '',
                                       'open_movement_pct':get_logic(_logics[0]).get('open_movement',0) if _logics else '',
                                       'price_big':get_logic(_logics[0]).get('price_big',0) if _logics else '',
                                       'price_small':get_logic(_logics[0]).get('price_small',0) if _logics else '',
                                       'required_turnover_cr':get_logic(_logics[0]).get('turnover_cr',0) if _logics else '',
                                       'required_volume':get_logic(_logics[0]).get('volume',0) if _logics else '',
                                       'passed':True,'reason':_reason},
                        'historical_result':_status,
                        'first_signal_time':_prior_signal,
                        'decision':_status,
                        'decision_reason':_reason,
                        'sell_trade_datetime':None,
                        'data_source':f'Historical selection audit {interval}',
                    })

                # The user-facing historical Excel output is limited to the
                # configured Historical Shares count. Do not append GUI audit rows
                # for every rejected/not-selected candidate, because those rows made
                # Excel show 25+ companies even when the GUI limit was 10.
                # Candidate failures remain available in the run log; only successful
                # unique BUY trades are written to the trade-result sheets.
                results[:]=[x for x in results if x.get('trade_date')!=day]
                results.extend(selected_unique)

                _excel_company_count = len({str(x.get('symbol') or '').strip().upper()
                                            for x in results
                                            if x.get('trade_date') == day
                                            and str(x.get('symbol') or '').strip().upper() not in {'','NO TRADE'}})
                _qualified_company_count = len(gui_qualified_universe)
                if _qualified_company_count and _excel_company_count < _qualified_company_count:
                    print(
                        f'Historical EXCEL QUALIFIED-UNIVERSE WARNING {day}: '
                        f'GUI-qualified={_qualified_company_count}, Excel companies={_excel_company_count}.'
                    )
                else:
                    print(
                        f'Historical EXCEL QUALIFIED-UNIVERSE CHECK {day}: '
                        f'successful BUY output={_excel_company_count}; Historical Shares cap={global_share_cap or "UNLIMITED"}. PASS'
                    )

                print(
                    f'Historical UNIQUE FIRST-COME-FIRST-BUY {day}: '
                    f'unique traded symbols={len(selected_unique)}; duplicate logic rows removed={duplicate_result_rows}; '
                    f'global ranks=1..{len(selected_unique)}.'
                )

                # Historical Data mode must NOT perform a second "full" download here.
                # The two-stage process above is the only market-data download path:
                #   * 1-minute cache for every GUI-qualified share;
                #   * 1-second confirmation only for the first-cum-first-buy selected shares.
                # The final Excel/P&L is built from those same cached candles.  The
                # separate full 1m+1s downloader remains reserved for One Share Historical.
                _final_intraday_dir=YTD_HISTORICAL_CACHE
                _final_intraday_dir.mkdir(parents=True, exist_ok=True)
                if interval == '1minute':
                    # The minute scan and replay already validated these candles. Do not
                    # reopen every selected CSV and do not probe 1-second caches in 1minute mode.
                    print(
                        f'Historical FINAL CACHE CHECK: skipped per-symbol disk rereads; '
                        f'mode=1minute; in-memory minute frames reused for {len(selected_unique)} selected trade(s); '
                        f'1-second cache was not required.'
                    )
                else:
                    for _selected_row in selected_unique:
                        _sym_out=str(_selected_row.get('symbol') or '').strip().upper()
                        if not _sym_out or _sym_out == 'NO TRADE':
                            continue
                        _stock_out=str(_selected_row.get('breeze_symbol') or '').strip() or _sym_out
                        _mcache=_load_cached_intraday(day,_stock_out,'1minute')
                        _scache=_load_historical_replay_1second(
                            day,_stock_out,
                            datetime.combine(day,datetime.min.time()).replace(hour=9,minute=15),
                            datetime.combine(day,datetime.min.time()).replace(hour=15,minute=24,second=59)
                        )
                        print(
                            f'Historical FINAL CACHE CHECK: {_sym_out} -> {_stock_out}; '
                            f'1m={"READY" if _mcache is not None and not _mcache.empty else "MISSING"}; '
                            f'1s={"READY" if _scache is not None and not _scache.empty else "MISSING"}; '
                            f'Breeze download here=NO.'
                        )

                # If there were no GUI-qualified symbols at all, keep the existing
                # per-logic NO TRADE summary so the date is still visible. When there
                # were GUI-qualified symbols, the complete audit rows above already
                # provide the required visibility and no extra NO TRADE rows are added.
                day_result_rows=[x for x in results if x.get('trade_date')==day]
                if not gui_qualified_universe:
                    for logic_name,cfg in enabled:
                        logic_rows=[x for x in day_result_rows
                                    if x.get('symbol') not in (None,'','NO TRADE')
                                    and (x.get('logic')==logic_name or logic_name in (x.get('matched_logics') or []))]
                        if logic_rows:
                            continue
                    reason='No qualifying enabled-logic BUY trade for this date.'
                    results.append({
                        'mode':'historical','logic':logic_name,'symbol':'NO TRADE','trade_date':day,
                        'status':'NO TRADE','exit_reason':'NO TRADE','gross_pnl':None,
                        'rank':None,'candidate_index':None,'open':None,'high':None,'low':None,
                        'close':None,'previous_close':None,'volume':None,'movement_pct':None,
                        'turnover_cr':None,'logic_config':cfg,
                        'entry_audit':{'passed':False,'reason':reason},
                        'data_source':f'Historical replay {interval}; summary only',
                    })
                    day_result_rows.append(results[-1])

                print(f'Historical replay {day}: checked/downloaded={candidates_checked}; selected per logic={selected_by_logic}; GLOBAL ranking={len(selected_unique)} rows; no-trade summaries added={sum(1 for x in results if x.get("trade_date")==day and x.get("symbol")=="NO TRADE")}; interval={interval}.')
                successful_dates.add(day.isoformat())

                # DATE-WISE DURABLE OUTPUT: commit this completed date immediately.
                # If the process is interrupted while processing a later date, every
                # earlier completed date is already present in the monthly workbook.
                # replace_dates ensures a rerun updates this date instead of duplicating it.
                day_text=day.isoformat()
                day_results=[r for r in results if str(r.get('trade_date') or '')[:10] == day_text]
                try:
                    _day_excel_t0=time.perf_counter()
                    _day_workbook=_ensure_monthly_workbook(month_text)
                    record_historical_results(
                        day_results,
                        workbook_path=_day_workbook,
                        replace_dates={day_text},
                    )
                    print(
                        f'Historical DATE-WISE EXCEL SAVE: {day_text}; rows={len(day_results):,}; '
                        f'time={time.perf_counter()-_day_excel_t0:.2f}s; workbook={_day_workbook.name}'
                    )
                except Exception as _day_excel_exc:
                    print(
                        f'Historical DATE-WISE EXCEL SAVE ERROR {day_text}: '
                        f'{type(_day_excel_exc).__name__}: {_day_excel_exc}'
                    )
                print(
                    f'Historical DATE COMPLETE: {day_text}; {len(day_results):,} result row(s) committed '
                    f'to Excel immediately.'
                )
            except Exception as exc:
                # Never leave the Excel workbook blank after a replay error.  Commit
                # the rows already produced for this date; if none exist, write one
                # diagnostic row per enabled logic so the workbook clearly records
                # why the date could not be replayed.  These rows are not fabricated
                # trades: they are explicit HISTORICAL_REPLAY_ERROR audit records.
                try:
                    day_text=day.isoformat()
                    day_results=[r for r in results if str(r.get('trade_date') or '')[:10] == day_text]
                    # IMPORTANT: if intraday replay fails after the exact NSE daily
                    # snapshot has already been obtained, never leave Selected Date
                    # looking empty.  Populate an explicit NSE-daily audit view for
                    # the qualified symbols.  These rows are NOT trades and are marked
                    # clearly so the user can see the daily data while the intraday
                    # problem is being repaired/retried.
                    if not day_results:
                        _fallback_added = 0
                        try:
                            _fallback_raw = _load_cached_daily(day)
                            if _fallback_raw is not None:
                                _fallback_market, _ = screen_from_raw(_fallback_raw)
                                _fallback_market = _fallback_market.copy()
                                _eligible_col = 'Eligible?' if 'Eligible?' in _fallback_market.columns else None
                                if _eligible_col:
                                    _fallback_market = _fallback_market[_fallback_market[_eligible_col].fillna(False).astype(bool)]
                                for _idx, (_symrow, _sr) in enumerate(_fallback_market.iterrows(), 1):
                                    _sym = str(_sr.get('Symbol') or _sr.get('symbol') or '').strip().upper()
                                    if not _sym:
                                        continue
                                    _open = _sr.get('Open')
                                    _close = _sr.get('Close') if _sr.get('Close') is not None else _sr.get('Latest/Close')
                                    _prev = _sr.get('Previous close') if _sr.get('Previous close') is not None else _sr.get('Previous Close')
                                    _vol = _sr.get('Volume')
                                    _turn = _sr.get('Turnover (₹)')
                                    try: _turn_cr = float(_turn or 0) / 10000000
                                    except Exception: _turn_cr = None
                                    _fallback_logic = 'GUI QUALIFIED'
                                    day_results.append({
                                        'mode':'historical','logic':_fallback_logic,'symbol':_sym,'trade_date':day,
                                        'status':'NSE_DAILY_ONLY_INTRADAY_REPLAY_ERROR',
                                        'exit_reason':f'Intraday replay unavailable: {type(exc).__name__}: {exc}',
                                        'gross_pnl':None,'rank':_idx,'open':_open,'high':_sr.get('High'),'low':_sr.get('Low'),
                                        'close':_close,'previous_close':_prev,'volume':_vol,'turnover_cr':_turn_cr,
                                        'logic_config':{},'historical_shares':historical_share_limit or 0,
                                        'entry_audit':{'passed':False,'reason':f'NSE daily data available; intraday replay failed: {type(exc).__name__}: {exc}'},
                                        'data_source':'NSE daily cache; intraday replay error audit'
                                    })
                                    _fallback_added += 1
                        except Exception as _fallback_exc:
                            print(f'Historical fallback NSE daily audit unavailable for {day}: {type(_fallback_exc).__name__}: {_fallback_exc}')
                        if not day_results:
                            for logic_name,cfg in enabled:
                                day_results.append({
                                    'mode':'historical','logic':logic_name,'symbol':'HISTORICAL ERROR',
                                    'trade_date':day,'status':'HISTORICAL_REPLAY_ERROR',
                                    'exit_reason':f'{type(exc).__name__}: {exc}','gross_pnl':None,
                                    'rank':None,'open':None,'high':None,'low':None,'close':None,
                                    'previous_close':None,'volume':None,'turnover_cr':None,
                                    'logic_config':cfg,'historical_shares':historical_share_limit or 0,
                                    'data_source':'Historical replay error audit'
                                })
                        else:
                            print(f'Historical fallback Selected Date: {day_text}; {_fallback_added:,} NSE daily qualified row(s) written; these are audit rows, not trades.')
                    # Save error/fallback rows for this date too, so a later-date
                    # failure or application exit does not hide already processed days.
                    try:
                        _err_excel_t0=time.perf_counter()
                        _err_workbook=_ensure_monthly_workbook(month_text)
                        record_historical_results(
                            day_results,
                            workbook_path=_err_workbook,
                            replace_dates={day_text},
                        )
                        print(
                            f'Historical DATE-WISE ERROR AUDIT SAVE: {day_text}; rows={len(day_results):,}; '
                            f'time={time.perf_counter()-_err_excel_t0:.2f}s; workbook={_err_workbook.name}'
                        )
                    except Exception as _err_excel_exc:
                        print(f'Historical DATE-WISE ERROR AUDIT SAVE ERROR {day_text}: {type(_err_excel_exc).__name__}: {_err_excel_exc}')
                    print(f'Historical ERROR AUDIT: {day_text}; {len(day_results):,} row(s) committed where possible.')
                except Exception as audit_exc:
                    print(f'Historical ERROR AUDIT SAVE ERROR {day}: {type(audit_exc).__name__}: {audit_exc}')
                print(f'Historical replay {day}: unavailable - {type(exc).__name__}: {exc}')
        day+=timedelta(days=1)
    # Mark only successfully processed trading days. This makes the next run
    # truly incremental: completed unchanged days are never read/replayed again.
    for dtext in successful_dates:
        try:
            d=date.fromisoformat(dtext)
            day_results=[r for r in results if str(r.get('trade_date') or '')[:10] == dtext]
            _mark_historical_day_complete(state, d, logic_signature, interval, len(day_results))
        except Exception as exc:
            print(f'WARNING: Could not mark historical day {dtext} complete: {exc}')
    try:
        _save_historical_replay_state(year, month, state)
    except Exception as exc:
        print(f'WARNING: Could not save historical replay state: {exc}')
    # ONE EXCEL SAVE PER REPLAY: this is the main performance fix when all
    # 1-minute/1-second market data is already downloaded.
    if results:
        try:
            _excel_batch_t0=time.perf_counter()
            _target_month_workbook=_ensure_monthly_workbook(month_text)
            _replace_dates={str(r.get('trade_date') or '')[:10] for r in results if r.get('trade_date')}
            record_historical_results(results, workbook_path=_target_month_workbook, replace_dates=_replace_dates)
            print(f'EXCEL BATCH OUTPUT TIME: {time.perf_counter()-_excel_batch_t0:.2f} seconds; rows={len(results):,}; workbook={_target_month_workbook.name}')
        except Exception as _excel_batch_exc:
            print(f'Historical FINAL EXCEL SAVE ERROR: {type(_excel_batch_exc).__name__}: {_excel_batch_exc}')
    if successful_dates:
        print(f'Historical completed using current GUI strategy fingerprint={current_gui_strategy_fingerprint()[:16]}…; no strategy lock is created.')
    _final_perf = _historical_perf_snapshot()
    _final_avg_api = (_final_perf['api_seconds'] / _final_perf['api_calls']) if _final_perf['api_calls'] else 0.0
    print(
        f"Historical DOWNLOAD SUMMARY: cache_hits={_final_perf['cache_hits']:,}; "
        f"cache_misses={_final_perf['cache_misses']:,}; Breeze_calls={_final_perf['api_calls']:,}; "
        f"API_failures={_final_perf['api_failures']:,}; avg_API_seconds={_final_avg_api:.2f}; "
        f"API_seconds_total={_final_perf['api_seconds']:.1f}; candles_loaded={_final_perf['candles']:,}."
    )
    print(f'Historical OUTPUT TIME: {time.perf_counter()-_historical_run_t0:.2f} seconds; results={len(results)}.')
    return results

def run_historical_replay_month(month_text: str, range_start: date | None = None, range_end: date | None = None, interval: str = '1minute', historical_share_limit: int | None = None, symbol_allowlist: Iterable[str] | None = None, full_interval_all_qualified: bool = False) -> list[dict]:
    """Run date-wise replay with a strict interval-specific network guard."""
    global _DATEWISE_HISTORICAL_INTERVAL_GUARD
    previous_guard = _DATEWISE_HISTORICAL_INTERVAL_GUARD
    requested_interval = str(interval or '1minute').strip().lower()
    _DATEWISE_HISTORICAL_INTERVAL_GUARD = requested_interval
    if requested_interval == '1minute':
        print('Historical DOWNLOAD SCOPE: Breeze 1minute candles only; optional 1second refinement is disabled for this run.')
        print('Historical SUPPORTING FILES: NSE daily bhavcopy (one per trading date, required for all-share screening) and NSE EQ company master (cached; required for verified symbol mapping).')
    try:
        return _run_historical_replay_month_impl(
            month_text, range_start=range_start, range_end=range_end, interval=interval,
            historical_share_limit=historical_share_limit, symbol_allowlist=symbol_allowlist,
            full_interval_all_qualified=full_interval_all_qualified
        )
    finally:
        _DATEWISE_HISTORICAL_INTERVAL_GUARD = previous_guard

def _excel_signed_net_pnl(gross, buy_turnover, sell_turnover):
    """Legacy Excel-side P&L calculation disabled.

    Excel must not calculate or override trade P&L. The trading engine is the
    only source of P&L values; a future fresh P&L engine can populate the
    workbook explicitly.
    """
    return None


def _format_historical_data_sheet(ws) -> None:
    """Apply a polished, Excel-friendly style to historical result sheets.

    The workbook remains lightweight: styling is applied by column/range rather
    than creating a unique style object for every cell.  P&L values themselves
    are never recalculated here; the legacy trading-engine calculations are not used for Excel P&L the
    source of truth.
    """
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    navy='163A5F'; blue='1F6AA5'; light_blue='EAF3FA'; lighter='F7FBFF'
    green='E2F0D9'; red='FCE4D6'; gold='FFF2CC'; grey='E7E6E6'; white='FFFFFF'
    border_color='D6E2F0'
    header_fill=PatternFill('solid', fgColor=navy)
    sub_fill=PatternFill('solid', fgColor=light_blue)
    green_fill=PatternFill('solid', fgColor=green)
    red_fill=PatternFill('solid', fgColor=red)
    gold_fill=PatternFill('solid', fgColor=gold)
    white_font=Font(color=white, bold=True, size=10)
    normal_font=Font(color='1F2937', size=10)
    bold_font=Font(color='163A5F', bold=True, size=10)
    thin=Side(style='thin', color=border_color)

    if ws.max_column == 0:
        ws.append(HISTORICAL_HEADERS)
    ws.sheet_view.showGridLines=False
    ws.sheet_properties.tabColor=blue
    ws.freeze_panes='A2'
    ws.sheet_view.zoomScale=85

    # Header
    for cell in ws[1]:
        cell.fill=header_fill; cell.font=white_font
        cell.alignment=Alignment(horizontal='center', vertical='center', wrap_text=True)
        cell.border=Border(bottom=thin)
    ws.row_dimensions[1].height=38

    header={str(ws.cell(1,c).value):c for c in range(1,ws.max_column+1)}
    date_cols=['Date']
    datetime_cols=['Buy Date & Time','High After Buy Date & Time','Low After Buy Date & Time','Day High Time','Day Low Time','15:25 Time']
    percent_cols=['Open Movement %','Profit Target %','Initial Stop Loss %','Activation %','Trailing Stop %','P&L %']
    money_cols=['Profit Target','Low After Buy','Loss Target','Gross P&L','Buy Turnover','Sell Turnover','Brokerage','STT','Exchange Charges','GST','SEBI Charges','Stamp Duty','Other Charges','Total Charges','Net P&L']

    for rr in range(2, ws.max_row+1):
        # Zebra striping keeps large result sheets readable without adding much file size.
        fill = sub_fill if rr % 2 == 0 else PatternFill('solid', fgColor=white)
        for c in range(1, ws.max_column+1):
            cell=ws.cell(rr,c)
            cell.fill=fill
            cell.font=normal_font
            cell.alignment=Alignment(vertical='center', wrap_text=False)
            cell.border=Border(bottom=Side(style='hair', color='E8EEF5'))
        if 'Date' in header:
            ws.cell(rr,header['Date']).value=_excel_date(ws.cell(rr,header['Date']).value) or ws.cell(rr,header['Date']).value
        for name in date_cols:
            if name in header: ws.cell(rr,header[name]).number_format='yyyy-mm-dd'
        for name in datetime_cols:
            if name in header: ws.cell(rr,header[name]).number_format='yyyy-mm-dd hh:mm:ss'
        for name in percent_cols:
            if name in header:
                ws.cell(rr,header[name]).number_format = ('0.00%' if name == 'P&L %' else '0.00\\%')
        for name in money_cols:
            if name in header: ws.cell(rr,header[name]).number_format='₹#,##0.00;[Red]-₹#,##0.00'
        if 'Daily Movement ₹' in header:
            ws.cell(rr,header['Daily Movement ₹']).number_format='₹#,##0.00;[Red]-₹#,##0.00'
        if 'Daily Movement %' in header:
            # Stored as percentage points (e.g. 12.2), so use a literal percent sign.
            ws.cell(rr,header['Daily Movement %']).number_format='0.00\\%'

    # Make the key result columns visually obvious.
    for name in ('Gross P&L','Net P&L'):
        c=header.get(name)
        if c:
            for rr in range(2,ws.max_row+1):
                ws.cell(rr,c).font=bold_font
    for name in ('Profit target achieved the goal','Loss target achieved the goal'):
        c=header.get(name)
        if c:
            for rr in range(2,ws.max_row+1):
                ws.cell(rr,c).alignment=Alignment(horizontal='center',vertical='center')
    if 'Exit Reason with Profit / Loss Target achieved the goal' in header:
        c=header['Exit Reason with Profit / Loss Target achieved the goal']
        for rr in range(2,ws.max_row+1):
            ws.cell(rr,c).alignment=Alignment(horizontal='center',vertical='center',wrap_text=True)


    widths=[12,9,12,16,12,12,14,16,10,12,21,15,25,14,22,15,25,14,22,38,14,15,15,14,14,16,16,16,16,16,18,16,12,18,16,14,18,16,18,15,16,14,14,20,18,18,16,16,18,18]
    if len(widths) < len(HISTORICAL_HEADERS):
        widths += [16] * (len(HISTORICAL_HEADERS) - len(widths))
    for c,w in enumerate(widths,1): ws.column_dimensions[get_column_letter(c)].width=w
    ws.auto_filter.ref=f"A1:{get_column_letter(len(HISTORICAL_HEADERS))}{max(1,ws.max_row)}"
    for name in list(ws.tables.keys()): del ws.tables[name]
    ws.row_dimensions.group(2, ws.max_row, hidden=False)


def _prepare_historical_sheet(ws) -> None:
    """Normalize a result sheet to the authoritative historical replay schema."""
    if ws.max_row == 0:
        ws.append(HISTORICAL_HEADERS)
        return
    first=[ws.cell(1,c).value for c in range(1, min(ws.max_column,len(HISTORICAL_HEADERS))+1)]
    if first != HISTORICAL_HEADERS or ws.max_column != len(HISTORICAL_HEADERS):
        existing=[]
        if ws.max_row > 1:
            for rr in range(2,ws.max_row+1):
                existing.append([ws.cell(rr,c).value for c in range(1,min(ws.max_column,len(HISTORICAL_HEADERS))+1)])
        ws.delete_rows(1,ws.max_row)
        ws.delete_cols(1,ws.max_column)
        ws.append(HISTORICAL_HEADERS)
        for row in existing:
            ws.append(row+[None]*max(0,len(HISTORICAL_HEADERS)-len(row)))


def _append_historical_rows(ws, rows: list[dict]) -> None:
    """Append the formula-based historical schema.

    Excel formulas are authoritative for Gross P&L/exit selection.
    The two target-achievement columns are deliberately Excel formulas so the
    workbook itself shows the exact rule used: the target level must be reached
    after the BUY timestamp. The combined exit-reason formula uses the actual
    historical exit price implied by Gross P&L rather than comparing extreme timestamps.
    """
    _prepare_historical_sheet(ws)
    for r in rows:
        sym = str(r.get('symbol') or '').upper()
        d = str(r.get('trade_date') or '')[:10]
        logic = str(r.get('logic') or '')
        if not d or not logic:
            continue
        if not sym:
            sym = 'NO TRADE'
        cfg = r.get('logic_config') or {}
        audit = r.get('entry_audit') or {}
        be = r.get('buy_extremes') or {}
        qty = r.get('buy_actual_qty')
        buy_price = r.get('buy_actual_price')
        buy_turnover = ((qty or 0) * (buy_price or 0)) if qty is not None and buy_price is not None else None
        sell_qty = r.get('sell_actual_qty')
        sell_price = r.get('sell_actual_price')
        sell_turnover = ((sell_qty or 0) * (sell_price or 0)) if sell_qty is not None and sell_price is not None else None
        profit_pct = float(cfg.get('profit_target') or 0)
        stop_pct = float(cfg.get('initial_stop') or 0)
        profit_target = (float(buy_price) * (1.0 + profit_pct/100.0)) if buy_price is not None and profit_pct > 0 else None
        loss_target = (float(buy_price) * (1.0 - stop_pct/100.0)) if buy_price is not None and stop_pct > 0 else None
        row = [
            d, r.get('rank'), logic, sym, r.get('open'), r.get('close'), r.get('volume'), r.get('turnover_cr'),
            qty, buy_price, r.get('buy_trade_datetime'), be.get('high'), be.get('high_dt'), profit_target, None,
            be.get('low'), be.get('low_dt'), loss_target, None, r.get('exit_reason'), r.get('gross_pnl'),
            buy_turnover, sell_turnover, r.get('brokerage'), r.get('stt'), r.get('exchange_turnover_charges'),
            r.get('gst'), r.get('sebi_charges'), r.get('stamp_duty'), r.get('total_other_charges'), r.get('total_charges'),
            r.get('net_pnl'), r.get('pnl_pct'), r.get('charge_source'), r.get('charge_plan'),
            r.get('squareoff_1525_price'), bool(r.get('squareoff_1525_price') is not None), r.get('squareoff_1525_datetime'), r.get('historical_shares'),
            audit.get('open_direction'), audit.get('open_movement_pct'), audit.get('price_big'), audit.get('price_small'),
            audit.get('required_turnover_cr'), profit_pct, stop_pct, audit.get('required_volume'),
            float(cfg.get('activation') or 0), float(cfg.get('trailing_stop') or 0),
            r.get('historical_result') or (
                'PAPER_COMPLETED' if str(r.get('mode') or '').lower() in {'paper','paper_live'}
                and int(float(r.get('buy_actual_qty') or r.get('quantity') or 0)) > 0
                and int(float(r.get('sell_actual_qty') or 0)) > 0
                and r.get('sell_actual_price') not in (None, '')
                and str(r.get('status') or '').upper() not in {'REJECTED','NOT SELECTED','PAPER_LIVE_SQUAREOFF_DATA_ERROR'}
                else ('PAPER_OPEN' if str(r.get('mode') or '').lower() in {'paper','paper_live'}
                      and int(float(r.get('buy_actual_qty') or r.get('quantity') or 0)) > 0
                      else ('LIVE_COMPLETED' if str(r.get('mode') or '').lower() == 'live'
                            and int(float(r.get('buy_actual_qty') or r.get('quantity') or 0)) > 0
                            and int(float(r.get('sell_actual_qty') or 0)) > 0
                            and r.get('sell_actual_price') not in (None, '')
                            and str(r.get('status') or '').upper() not in {'REJECTED','NOT SELECTED','LIVE_EOD_SQUAREOFF_FAILED'}
                            else ('LIVE_OPEN' if str(r.get('mode') or '').lower() == 'live'
                                  and int(float(r.get('buy_actual_qty') or r.get('quantity') or 0)) > 0
                                  else ('REJECTED' if str(r.get('status') or '').upper() in {'REJECTED','NOT SELECTED'} else 'TRADE'))))
            ),
            r.get('first_signal_time') or r.get('buy_trade_datetime'),
            r.get('decision') or ('BUY' if qty else (r.get('status') or 'REJECTED')),
            r.get('decision_reason') or r.get('entry_audit', {}).get('reason') or r.get('exit_reason') or r.get('status'),
            r.get('sell_trade_datetime'),
            *historical_market_fields(r, sym, d)
        ]
        ws.append([_excel_safe(v) for v in row])
        rr=ws.max_row
        # Target/stop percentages are authoritative Excel inputs: AR = Profit Target %,
        # AS = Initial Stop Loss %. Values are stored as numeric percent (e.g. 4.0),
        # so the Excel formula divides by 100 before applying them to Buy Price (J).
        # The achieved flags remain time-ordered: the extreme must occur after BUY.
        ws.cell(rr,14).value = f'=IF(J{rr}="","",J{rr}*(1+AR{rr}/100))'
        ws.cell(rr,18).value = f'=IF(J{rr}="","",J{rr}*(1-AS{rr}/100))'
        ws.cell(rr,15).value = f'=IF(AND(K{rr}<M{rr},L{rr}>=N{rr}),"YES","NO")'
        ws.cell(rr,19).value = f'=IF(AND(K{rr}<Q{rr},P{rr}<=R{rr}),"YES","NO")'
        # Exit reason must not compare the timestamps of the *extreme* high/low.
        # Those are audit extremes, not the first target/stop hit. When both targets
        # were reached, infer the actual historical exit from Gross P&L (U) and Buy
        # Qty (I): the replay exits exactly at the first target/stop price. This avoids
        # incorrectly choosing LOSS just because the day's minimum occurred after the
        # profit target was actually hit. 15:25 remains the fallback when neither target
        # was achieved.
        actual_exit_price = f'IFERROR(J{rr}+U{rr}/I{rr},"")'
        ws.cell(rr,20).value = (
            f'=IF(AND(O{rr}="YES",S{rr}="NO"),"PROFIT TARGET",'
            f'IF(AND(S{rr}="YES",O{rr}="NO"),"LOSS TARGET",'
            f'IF(AND(O{rr}="YES",S{rr}="YES"),'
            f'IF(ABS({actual_exit_price}-N{rr})<=ABS({actual_exit_price}-R{rr}),"PROFIT TARGET","LOSS TARGET"),'
            f'IF(AJ{rr}=TRUE,"15:25 SQUARE-OFF","NO TARGET"))))'
        )


def _delete_dates_from_sheet(ws, dates: set[str]) -> None:
    _prepare_historical_sheet(ws)
    for rr in range(ws.max_row,1,-1):
        d=_excel_date(ws.cell(rr,1).value)
        if d and d.isoformat() in dates:
            ws.delete_rows(rr,1)


def _read_result_rows(ws) -> list[list]:
    _prepare_historical_sheet(ws)
    return [list(r) for r in ws.iter_rows(min_row=2, values_only=True) if any(v not in (None,'') for v in r)]


def _refresh_ytd_sheet(target_path: Path, end_date: date) -> None:
    """Build YTD from the latest prior-month YTD plus current-month rows.

    This avoids duplicating the cumulative YTD snapshot stored in every prior
    monthly workbook.  The current month's YTD is then extended through end_date.
    """
    wb=load_workbook(target_path)
    ytd=wb['YTD Historical Data']
    _prepare_historical_sheet(ytd)
    current_month=end_date.strftime('%Y-%m')

    # Find the latest completed month before the current month for this year.
    prior=[]
    for p in sorted(_historical_year_dir(end_date.year).glob('Month/*/Trading_Data_*.xlsx')):
        if p == target_path or p.stem[-2:] >= current_month[-2:]:
            continue
        try:
            pwb=load_workbook(p, read_only=True, data_only=False)
            if 'YTD Historical Data' in pwb.sheetnames:
                pws=pwb['YTD Historical Data']
                prior=[list(r) for r in pws.iter_rows(min_row=2, values_only=True) if any(v not in (None,'') for v in r)]
            elif 'All Historical Data' in pwb.sheetnames:
                pws=pwb['All Historical Data']
                prior=[list(r) for r in pws.iter_rows(min_row=2, values_only=True) if any(v not in (None,'') for v in r)]
            pwb.close()
        except Exception:
            continue

    # Current-month rows are kept separately in Selected Date only, so reconstruct
    # them from the target's existing YTD before replacing dates, then use the
    # caller's current-date rows.  The caller has already written Selected Date.
    current=[]
    if 'Selected Date' in wb.sheetnames:
        sws=wb['Selected Date']
        current=[list(r) for r in sws.iter_rows(min_row=2, values_only=True) if any(v not in (None,'') for v in r)]
    existing_ytd=[list(r) for r in ytd.iter_rows(min_row=2, values_only=True) if any(v not in (None,'') for v in r)]
    # A new run for a date is authoritative for that entire date.  Remove ALL
    # previously persisted rows for every date represented in Selected Date before
    # adding the new rows.  Keeping stale symbols from an earlier run of the same
    # date was the cause of YTD showing more trades than the current Paper/Historical
    # result after a rerun with a smaller candidate/order set.
    current_dates=set()
    for r in current:
        d=_excel_date(r[0]) if r else None
        if d:
            current_dates.add(d.isoformat())
    current_existing=[
        r for r in existing_ytd
        if str(r[0] or '')[:7] == current_month and str(r[0] or '')[:10] not in current_dates
    ]
    combined=prior + current_existing + current

    # Deduplicate by (date, symbol, logic), keeping the latest row for the date.
    dedup={}
    for r in combined:
        d=_excel_date(r[0]); ds=d.isoformat() if d else str(r[0] or '')[:10]
        sym=str(r[3] or '').upper() if len(r)>3 else ''
        lg=str(r[2] or '') if len(r)>2 else ''
        if not ds or not lg: continue
        dedup[(ds,sym,lg)]=r
    rows=sorted(dedup.values(), key=lambda r: (str(r[0])[:10], str(r[10] or '') if len(r)>10 else '', str(r[3] or '')))
    ytd.delete_rows(1,ytd.max_row)
    ytd.delete_cols(1,ytd.max_column)
    ytd.append(HISTORICAL_HEADERS)
    for r in rows:
        ytd.append(r+[None]*max(0,len(HISTORICAL_HEADERS)-len(r)))
    _format_historical_data_sheet(ytd)
    wb.save(target_path)


def record_historical_results(results: list[dict], workbook_path: Path | None = None, replace_dates: set[str] | None = None) -> None:
    """Write historical results to the four-sheet Excel workbook using formula P&L."""
    target = Path(workbook_path) if workbook_path else None
    if target is None:
        dates=[_excel_date(r.get('trade_date')) for r in (results or []) if r.get('trade_date')]
        target=_monthly_workbook_path(max(dates).strftime('%Y-%m')) if dates else WORKBOOK
    write_historical(results or [], target, replace_dates)
    print(f'FORMULA EXCEL HISTORICAL SAVED: {target} | rows={sum(1 for r in (results or []) if r.get("trade_date"))}')

def backtest(holding_days: int) -> None:
    """Run the primary historical validation engine for the latest available month."""
    target=date.today().replace(day=1)
    while not _is_nse_trading_day(target): target -= timedelta(days=1)
    results=run_historical_replay_month(f'{target:%Y-%m}')
    complete=[r for r in results if r.get('status') == 'HISTORICAL_REPLAY_COMPLETE']
    pnl=[(r.get('gross_pnl') or 0)-(r.get('simulated_total_charges') or 0) for r in complete]
    print(f'Historical validation complete: {len(complete)} simulated trades; Net P&L={sum(pnl):.2f}; wins={sum(x>0 for x in pnl)}; losses={sum(x<0 for x in pnl)}.')


def backfill_month(month_text: str) -> None:
    """Populate the dashboard with EVERY eligible share × day × logic.

    This is explicit/on-demand only: normal startup does not download historical files.
    Backfill creates the complete active-logic eligible register; the Historical P&L Test
    later fills the intraday trade/signal outcome from Breeze historical data.
    """
    try:
        year, month = [int(x) for x in month_text.strip().split('-', 1)]
        first = date(year, month, 1)
    except Exception as exc:
        raise RuntimeError('Month must be in YYYY-MM format, for example 2026-09.') from exc
    if not (1 <= month <= 12):
        raise RuntimeError('Month must be between 01 and 12.')
    next_month = date(year + (month == 12), 1 if month == 12 else month + 1, 1)
    last = next_month - timedelta(days=1)

    wb = load_workbook(WORKBOOK)
    if 'P&L Dashboard' not in wb.sheetnames:
        wb.create_sheet('P&L Dashboard', 0)
    ws = wb['P&L Dashboard']
    for merged in list(ws.merged_cells.ranges): ws.unmerge_cells(str(merged))

    # Preserve trade-side values keyed by date/symbol/logic.
    existing = {}
    for rr in range(2, ws.max_row + 1):
        d = str(ws.cell(rr,1).value or '')[:10]
        sym = str(ws.cell(rr,4).value or '').strip().upper()
        logic = str(ws.cell(rr,3).value or '').strip()
        if d and sym and logic in set(ACTIVE_LOGIC_NAMES):
            existing[(d, sym, logic)] = [ws.cell(rr,c).value for c in range(12,41)]

    retained = []
    for rr in range(2, ws.max_row + 1):
        vals = [ws.cell(rr,c).value for c in range(1,41)]
        d = str(vals[0] or '')[:10]
        if not d: continue
        try: in_month = first <= date.fromisoformat(d) <= last
        except Exception: in_month = False
        if in_month: continue
        retained.append(vals)
    ws.delete_rows(1, ws.max_row)
    ws.append(DASHBOARD_HEADERS)
    for vals in retained:
        vals = vals[:40] + [None] * max(0, 40-len(vals))
        ws.append(vals[:40])

    session = _new_nse_session()
    day = first
    total = 0
    processed = 0
    missing = []
    while day <= last:
        if _is_nse_trading_day(day):
            try:
                raw = bhavcopy_for_date(day, session)
                market, top = screen_from_raw(raw)
                processed += 1
                for rank, (_, row) in enumerate(top.iterrows(), 1):
                    sym = str(row['Symbol']).strip().upper()
                    logic_flags = {logic: bool(row.get(f'Eligible for {logic.lower()}', False)) for logic in ACTIVE_LOGIC_NAMES}
                    for logic in ACTIVE_LOGIC_NAMES:
                        if not logic_flags[logic]:
                            continue
                        trade = existing.get((day.isoformat(), sym, logic), [None] * 33)
                        ws.append([
                            day, rank, logic, sym,
                            row['Open'], row['High'], row['Low'], row['Close'], row['Previous close'],
                            row['Volume'], float(row['Turnover (₹)']) / 10000000, *trade
                        ])
                        rr = ws.max_row
                        ws.cell(rr,25).value = 'Awaiting historical intraday replay'
                        ws.cell(rr,39).value = 'NSE Bhavcopy'
                        ws.cell(rr,40).value = 'ELIGIBLE - NOT TESTED'
                        total += 1
                print(f'Historical {day}: {len(top)} trigger shares; logic-specific eligible rows created')
            except Exception as exc:
                missing.append(f'{day}: {exc}')
                print(f'Historical {day}: unavailable - {exc}')
        day += timedelta(days=1)

    _format_pnl_dashboard(ws)
    wb.save(WORKBOOK)
    print(f'Historical backfill complete for {year:04d}-{month:02d}: {processed} trading days, {total} logic rows.')
    if missing:
        print('Dates not loaded (no substitute date used):')
        for item in missing: print('  ' + item)




def _month_iter(start_month: str, end_month: str | None = None):
    """Yield YYYY-MM strings from start_month through end_month/current month."""
    try:
        sy, sm = [int(x) for x in start_month.strip().split('-', 1)]
        start = date(sy, sm, 1)
    except Exception as exc:
        raise RuntimeError('Start month must be YYYY-MM, e.g. 2026-07.') from exc

    if end_month:
        try:
            ey, em = [int(x) for x in end_month.strip().split('-', 1)]
            end = date(ey, em, 1)
        except Exception as exc:
            raise RuntimeError('End month must be YYYY-MM, e.g. 2026-09.') from exc
    else:
        today = date.today()
        end = date(today.year, today.month, 1)

    if start > end:
        raise RuntimeError(f'Start month {start:%Y-%m} cannot be after end month {end:%Y-%m}.')

    cur = start
    while cur <= end:
        yield f'{cur.year:04d}-{cur.month:02d}'
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)


def run_historical_month_range(start_month: str = '2026-07', end_month: str | None = None) -> list[dict]:
    """Process every calendar month from start_month through the latest/current month.

    Each month is processed independently, so the existing smart cache/state for
    each month is preserved. Completed months are not rebuilt unnecessarily on
    later runs; the current month follows the normal latest-date fast rerun policy.
    """
    months = list(_month_iter(start_month, end_month))
    all_results = []
    print(f'Historical MONTH RANGE: {months[0]} through {months[-1]} ({len(months)} month(s)).')
    for idx, month_text in enumerate(months, 1):
        print(f'\\n===== HISTORICAL MONTH {idx}/{len(months)}: {month_text} =====')
        try:
            results = run_historical_replay_month(month_text)
            all_results.extend(results)
            complete = sum(1 for r in results if r.get('status') == 'HISTORICAL_REPLAY_COMPLETE')
            print(f'===== {month_text} COMPLETE: {complete} completed result(s), {len(results)} result row(s) =====')
        except Exception as exc:
            # Keep already processed months. Record the failed month and continue
            # so a temporary problem in one month does not discard earlier work.
            err = {'mode': 'historical', 'status': 'HISTORICAL_MONTH_ERROR',
                   'month': month_text, 'error': str(exc)}
            all_results.append(err)
            print(f'===== {month_text} ERROR: {exc} =====')
    # Refresh the workbook's current market-data sheet once, after the range run.
    try:
        market, top, asof = screen()
        write_workbook(market, top, asof)
        update_datewise_pnl_dashboard(asof)
        print(f'Historical MONTH RANGE finished. Current NSE Market Data refreshed for {asof}.')
    except Exception as exc:
        print(f'Historical MONTH RANGE final NSE refresh unavailable: {exc}')
    return all_results


def run_historical_month_combined(month_text: str) -> list[dict]:
    """One-click historical workflow: refresh NSE Market Data, build the full
    month eligibility register, then replay intraday P&L using the same enabled Logic 1..8
    rules. This replaces the old separate Historical Eligible Month and Historical
    P&L Test buttons.
    """
    # Keep the NSE Market Data sheet populated with the latest actual NSE session.
    # This is separate from the month-wide historical register and does not change
    # the intraday rules.
    market, top, asof = screen()
    write_workbook(market, top, asof)
    print(f'Combined Historical Month: NSE Market Data refreshed for {asof}.')

    # Do not call the legacy backfill_month() here. The replay engine owns the
    # persistent cache: it reuses existing files and downloads only missing files.
    print(f'Combined Historical Month: replaying intraday P&L using smart cache/download for {month_text}.')
    results = run_historical_replay_month(month_text)

    # The replay has already written P&L/Date Wise data. Do not call screen()
    # again: it causes an unnecessary live NSE refresh after a cache-only test.
    update_datewise_pnl_dashboard(asof)
    complete = [r for r in results if r.get('status') == 'HISTORICAL_REPLAY_COMPLETE']
    print(f'Combined Historical Month complete: {len(complete)} completed simulated trades; {len(results)} replay result rows.')
    return results


def update_overview_sheet() -> None:
    """Rebuild the management Overview from the persistent P&L Dashboard.

    The Overview is deliberately value-based (not formula-only) so it is immediately
    visible after Python writes the workbook.  It contains:
      1) overall summary after charges,
      2) the exact enabled Logic 1..8 entry/exit conditions,
      3) date-wise P&L split by logic plus ALL LOGICS,
      4) a date-wise audit of the actual trigger/turnover/exit rules used.
    """
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    wb = load_workbook(WORKBOOK)
    # The current workbook deliberately contains only NSE Data + All Historical Data.
    # Overview generation belongs to the legacy multi-sheet workbook and is skipped
    # here rather than recreating deleted legacy sheets.
    if 'YTD Historical Data' not in wb.sheetnames:
        return
    pws = wb['YTD Historical Data']
    # No legacy Overview sheet in the two-sheet format.
    return

    def num(v):
        try:
            return float(v) if v not in (None, '') else 0.0
        except Exception:
            return 0.0

    # Dashboard columns are fixed by DASHBOARD_HEADERS.
    rows = []
    for rr in range(2, pws.max_row + 1):
        d = str(pws.cell(rr, 1).value or '').strip()[:10]
        logic = str(pws.cell(rr, 3).value or '').strip()
        sym = str(pws.cell(rr, 4).value or '').strip().upper()
        if not d or logic not in set(ACTIVE_LOGIC_NAMES):
            continue
        buy_qty = int(num(pws.cell(rr, 12).value))
        buy_price = num(pws.cell(rr, 13).value)
        sell_price = num(pws.cell(rr, 19).value)
        buy_value = num(pws.cell(rr, 27).value)
        sell_value = num(pws.cell(rr, 28).value)
        gross = num(pws.cell(rr, 26).value)
        charges = num(pws.cell(rr, 36).value)
        net = num(pws.cell(rr, 37).value)
        pnl_pct = num(pws.cell(rr, 38).value)
        rows.append({
            'date': d, 'logic': logic, 'symbol': sym,
            'open': num(pws.cell(rr, 5).value), 'high': num(pws.cell(rr, 6).value),
            'low': num(pws.cell(rr, 7).value), 'close': num(pws.cell(rr, 8).value),
            'turnover_cr': num(pws.cell(rr, 11).value),
            'buy_qty': buy_qty, 'buy_price': buy_price, 'buy_value': buy_value,
            'sell_price': sell_price, 'sell_value': sell_value,
            'gross': gross, 'charges': charges, 'net': net, 'pnl_pct': pnl_pct,
            'exit_reason': str(pws.cell(rr, 25).value or ''),
            'status': str(pws.cell(rr, 40).value or ''),
            'buy_dt': pws.cell(rr, 14).value,
            'sell_dt': pws.cell(rr, 20).value,
            'source': str(pws.cell(rr, 39).value or ''),
        })

    logic_conditions = {
        'Logic 1': {
            'entry': "Intraday High reaches the configured movement from TODAY'S OPEN",
            'turnover': "Configured logic cumulative turnover at entry",
            'target': "Configured logic profit target",
            'stop': "Configured logic initial stop-loss",
            'trailing': "Configured logic trailing stop",
            'exit': "Configured logic exit or mandatory 3:20 PM square-off",
        },
    }

    # ---------- Overview sheet ----------
    ws = wb['Overview'] if 'Overview' in wb.sheetnames else wb.create_sheet('Overview', 0)
    # Unmerge first so stale formatting can never block new values.
    for merged in list(ws.merged_cells.ranges):
        ws.unmerge_cells(str(merged))
    ws.delete_rows(1, ws.max_row)
    ws.sheet_view.showGridLines = False

    title_fill = PatternFill('solid', fgColor='1F4E78')
    section_fill = PatternFill('solid', fgColor='D9EAF7')
    header_fill = PatternFill('solid', fgColor='4472C4')
    total_fill = PatternFill('solid', fgColor='E2F0D9')
    loss_fill = PatternFill('solid', fgColor='FCE4D6')
    white_bold = Font(color='FFFFFF', bold=True)
    bold = Font(bold=True)
    thin = Side(style='thin', color='D9E1F2')
    border = Border(bottom=thin)

    ws.merge_cells('A1:M1')
    ws['A1'] = 'Breeze NSE Auto Trading - OVERVIEW | Date-wise Profit & Loss'
    ws['A1'].fill = title_fill; ws['A1'].font = Font(color='FFFFFF', bold=True, size=14)
    ws['A1'].alignment = Alignment(horizontal='center', vertical='center')
    ws.row_dimensions[1].height = 26

    total_orders = sum(1 for r in rows if r['buy_qty'] > 0)
    total_sales = sum(1 for r in rows if r['sell_price'] > 0)
    investment = sum(r['buy_value'] for r in rows)
    sell_turnover = sum(r['sell_value'] for r in rows)
    gross = sum(r['gross'] for r in rows)
    charges = sum(r['charges'] for r in rows)
    net = sum(r['net'] for r in rows)
    profit = sum(r['net'] for r in rows if r['net'] > 0)
    loss = abs(sum(r['net'] for r in rows if r['net'] < 0))
    wins = sum(1 for r in rows if r['buy_qty'] > 0 and r['net'] > 0)
    losses = sum(1 for r in rows if r['buy_qty'] > 0 and r['net'] < 0)

    ws['A3'] = 'OVERALL SUMMARY'
    ws['A3'].fill = section_fill; ws['A3'].font = bold
    summary_headers = ['Metric', 'Value', 'Meaning', 'Source']
    for c, v in enumerate(summary_headers, 1):
        cell = ws.cell(4, c, v); cell.fill = header_fill; cell.font = white_bold; cell.alignment = Alignment(horizontal='center')
    summary = [
        ('Trading Days', len({r['date'] for r in rows}), 'Distinct dates present in P&L Dashboard', 'P&L Dashboard'),
        ('Total Orders', total_orders, 'Rows with Buy Qty', 'P&L Dashboard'),
        ('Total Sales', total_sales, 'Rows with Sell Price', 'P&L Dashboard'),
        ('Investment / Buy Turnover', investment, 'Capital deployed across recorded buys', 'P&L Dashboard'),
        ('Sell Turnover', sell_turnover, 'Value of recorded sales', 'P&L Dashboard'),
        ('Gross P&L', gross, 'Before all charges', 'P&L Dashboard'),
        ('Total Charges', charges, 'Brokerage + STT + Exchange + GST + SEBI + Stamp + Other', 'P&L Dashboard'),
        ('Total Profit', profit, 'Positive Net P&L after charges', 'Calculated'),
        ('Total Loss', loss, 'Absolute value of negative Net P&L after charges', 'Calculated'),
        ('NET PROFIT / LOSS', net, 'Profit minus loss, after charges', 'Calculated'),
        ('Net P&L %', net / investment if investment else 0, 'Net P&L divided by investment', 'Calculated'),
        ('Winning Trades', wins, 'Completed profitable trades', 'P&L Dashboard'),
        ('Losing Trades', losses, 'Completed losing trades', 'P&L Dashboard'),
        ('Win Rate', wins / (wins + losses) if wins + losses else 0, 'Winning trades / completed P&L trades', 'Calculated'),
    ]
    for i, item in enumerate(summary, 5):
        for c, v in enumerate(item, 1): ws.cell(i, c, v)
        ws.cell(i, 1).font = bold if item[0] == 'NET PROFIT / LOSS' else Font(bold=False)
    ws['B14'].number_format = '#,##0.00'; ws['B15'].number_format = '#,##0.00'; ws['B16'].number_format = '#,##0.00'; ws['B17'].number_format = '#,##0.00'; ws['B18'].number_format = '#,##0.00'; ws['B19'].number_format = '#,##0.00'; ws['B20'].number_format = '#,##0.00'; ws['B21'].number_format = '0.00%'; ws['B22'].number_format = '0'; ws['B23'].number_format = '0'; ws['B24'].number_format = '0.00%'
    # Explicit number formats by metric rows (row 5 starts Trading Days).
    for r in range(8, 15): ws.cell(r, 2).number_format = '#,##0.00'
    for r in (15, 16, 17, 18): ws.cell(r, 2).number_format = '#,##0.00'
    ws.cell(19, 2).number_format = '#,##0.00'
    ws.cell(20, 2).number_format = '#,##0.00'
    ws.cell(21, 2).number_format = '0.00%'
    ws.cell(24, 2).number_format = '0.00%'
    for c in ws[14]:
        if c.column <= 4: c.fill = total_fill

    # ---------- Exact logic conditions ----------
    ws['A27'] = 'EXACT TRADING LOGIC CONDITIONS USED BY PYTHON'
    ws['A27'].fill = section_fill; ws['A27'].font = bold
    condition_headers = ['Logic', 'Entry Condition', 'Turnover Condition', 'Target', 'Stop Loss', 'Trailing SL', 'Exit Condition']
    for c, v in enumerate(condition_headers, 1):
        cell = ws.cell(28, c, v); cell.fill = header_fill; cell.font = white_bold; cell.alignment = Alignment(horizontal='center', wrap_text=True)
    for rr, logic in enumerate(ACTIVE_LOGIC_NAMES, 29):
        d = logic_conditions[logic]
        vals = [logic, d['entry'], d['turnover'], d['target'], d['stop'], d['trailing'], d['exit']]
        for c, v in enumerate(vals, 1): ws.cell(rr, c, v)

    # ---------- Date-wise + logic P&L ----------
    start = 34
    ws.cell(start, 1, 'DATE-WISE P&L + LOGIC CONDITIONS / RESULTS').fill = section_fill
    ws.cell(start, 1).font = bold
    headers = ['Date', 'Logic', 'Entry Condition', 'Turnover Condition', 'Target', 'Stop Loss', 'Trailing SL', 'Exit Condition',
               'Trades', 'Investment ₹', 'Gross P&L ₹', 'Charges ₹', 'Profit ₹', 'Loss ₹', 'Net P&L ₹', 'Net P&L %', 'Wins', 'Losses', 'Win Rate', 'Symbols / Status']
    for c, v in enumerate(headers, 1):
        cell = ws.cell(start + 1, c, v); cell.fill = header_fill; cell.font = white_bold; cell.alignment = Alignment(horizontal='center', wrap_text=True)

    dates = sorted({r['date'] for r in rows})
    out_row = start + 2
    logic_order = tuple(ACTIVE_LOGIC_NAMES) + ('ALL LOGICS',)
    for d in dates:
        for logic in logic_order:
            rr = [r for r in rows if r['date'] == d and (logic == 'ALL LOGICS' or r['logic'] == logic)]
            if not rr:
                continue
            trades = sum(1 for r in rr if r['buy_qty'] > 0)
            inv = sum(r['buy_value'] for r in rr)
            gp = sum(r['gross'] for r in rr)
            ch = sum(r['charges'] for r in rr)
            pr = sum(r['net'] for r in rr if r['net'] > 0)
            lo = abs(sum(r['net'] for r in rr if r['net'] < 0))
            n = sum(r['net'] for r in rr)
            wins_d = sum(1 for r in rr if r['buy_qty'] > 0 and r['net'] > 0)
            loss_d = sum(1 for r in rr if r['buy_qty'] > 0 and r['net'] < 0)
            cond = logic_conditions.get(logic, {
                'entry': 'Combined Logic 1 + 2 + 3 + 4',
                'turnover': 'Logic 1 turnover filter only applies to Logic 1 rows',
                'target': 'Configured profit target', 'stop': 'Configured initial stop',
                'trailing': 'Configured trailing stop after activation', 'exit': 'Configured exit or 3:20 PM square-off'
            })
            symbols = ', '.join(sorted({r['symbol'] for r in rr if r['symbol']}))
            vals = [d, logic, cond['entry'], cond['turnover'], cond['target'], cond['stop'], cond['trailing'], cond['exit'],
                    trades, inv, gp, ch, pr, lo, n, n / inv if inv else 0, wins_d, loss_d,
                    wins_d / trades if trades else 0, symbols]
            for c, v in enumerate(vals, 1): ws.cell(out_row, c, v)
            ws.cell(out_row, 1).value = _excel_date(d) or d
            ws.cell(out_row, 1).number_format = 'dd-mm-yyyy'
            for c in range(10, 16): ws.cell(out_row, c).number_format = '#,##0.00'
            ws.cell(out_row, 16).number_format = '0.00%'; ws.cell(out_row, 19).number_format = '0.00%'
            if logic == 'ALL LOGICS':
                for c in range(1, 21): ws.cell(out_row, c).fill = total_fill
            if n < 0:
                ws.cell(out_row, 15).fill = loss_fill
            out_row += 1

    if not dates:
        ws.cell(out_row, 1, 'No trading/P&L data yet. Run Historical Test or Paper Trade.').font = Font(italic=True)

    # ---------- Logic comparison ----------
    comp_start = out_row + 2
    ws.cell(comp_start, 1, 'LOGIC-WISE COMPARISON').fill = section_fill; ws.cell(comp_start, 1).font = bold
    for c, v in enumerate(['Metric'] + list(ACTIVE_LOGIC_NAMES), 1):
        cell = ws.cell(comp_start + 1, c, v); cell.fill = header_fill; cell.font = white_bold
    for i, metric in enumerate(('Trades', 'Investment', 'Gross P&L', 'Charges', 'Profit', 'Loss', 'Net P&L', 'Net P&L %', 'Winning Trades', 'Losing Trades', 'Win Rate'), comp_start + 2):
        vals = []
        for lg in ACTIVE_LOGIC_NAMES:
            x = [r for r in rows if r['logic'] == lg]
            trades = sum(1 for r in x if r['buy_qty'] > 0); inv = sum(r['buy_value'] for r in x); gp = sum(r['gross'] for r in x); ch = sum(r['charges'] for r in x); n = sum(r['net'] for r in x); w = sum(1 for r in x if r['buy_qty'] > 0 and r['net'] > 0); l = sum(1 for r in x if r['buy_qty'] > 0 and r['net'] < 0)
            vals.append({'Trades': trades, 'Investment': inv, 'Gross P&L': gp, 'Charges': ch, 'Profit': sum(r['net'] for r in x if r['net'] > 0), 'Loss': abs(sum(r['net'] for r in x if r['net'] < 0)), 'Net P&L': n, 'Net P&L %': n / inv if inv else 0, 'Winning Trades': w, 'Losing Trades': l, 'Win Rate': w / trades if trades else 0}[metric])
        ws.cell(i, 1, metric)
        for c, v in enumerate(vals, 2): ws.cell(i, c, v)
        if metric in {'Investment', 'Gross P&L', 'Charges', 'Profit', 'Loss', 'Net P&L'}:
            for c in range(2, 4): ws.cell(i, c).number_format = '#,##0.00'
        if metric in {'Net P&L %', 'Win Rate'}:
            for c in range(2, 4): ws.cell(i, c).number_format = '0.00%'

    # ---------- Formatting ----------
    widths = {
        1: 14, 2: 15, 3: 48, 4: 46, 5: 28, 6: 30, 7: 34, 8: 52,
        9: 10, 10: 16, 11: 15, 12: 14, 13: 14, 14: 14, 15: 15, 16: 12,
        17: 10, 18: 10, 19: 12, 20: 32
    }
    for c, w in widths.items(): ws.column_dimensions[get_column_letter(c)].width = w
    for row in ws.iter_rows():
        for cell in row:
            cell.alignment = Alignment(vertical='top', wrap_text=True)
            if cell.row not in (1, 4, 28, start + 1, comp_start + 1):
                cell.border = border
    ws.freeze_panes = 'A36'
    ws.auto_filter.ref = f'A{start+1}:{get_column_letter(len(headers))}{max(start+1, out_row-1)}'

    # Rebuild the simpler Date Wise P&L sheet from the same source values.
    if 'Date Wise P&L' in wb.sheetnames:
        dws = wb['Date Wise P&L']
        for merged in list(dws.merged_cells.ranges): dws.unmerge_cells(str(merged))
        dws.delete_rows(1, dws.max_row)
    else:
        dws = wb.create_sheet('Date Wise P&L')
    dws.sheet_view.showGridLines = False
    dws.append(['Breeze NSE - Date Wise Profit & Loss (All Logic Conditions)'])
    dws.append([])
    dws_headers = ['Date','Logic','Entry Condition','Turnover Condition','Target','Stop Loss','Trailing SL','Exit Condition','Trades','Investment ₹','Gross P&L ₹','Charges ₹','Profit ₹','Loss ₹','Net P&L ₹','Net P&L %','Winning Trades','Losing Trades','Win Rate']
    dws.append(dws_headers)
    for d in dates:
        for logic in logic_order:
            rr = [r for r in rows if r['date'] == d and (logic == 'ALL LOGICS' or r['logic'] == logic)]
            if not rr: continue
            trades = sum(1 for r in rr if r['buy_qty'] > 0); inv=sum(r['buy_value'] for r in rr); gp=sum(r['gross'] for r in rr); ch=sum(r['charges'] for r in rr); pr=sum(r['net'] for r in rr if r['net']>0); lo=abs(sum(r['net'] for r in rr if r['net']<0)); n=sum(r['net'] for r in rr); w=sum(1 for r in rr if r['buy_qty']>0 and r['net']>0); l=sum(1 for r in rr if r['buy_qty']>0 and r['net']<0)
            cond = logic_conditions.get(logic, {'entry':'Configured active logic conditions','turnover':'Configured active-logic turnover','target':'Configured profit target','stop':'Configured initial SL','trailing':'Configured trailing stop','exit':'Configured exit or 3:20 PM square-off'})
            dws.append([_excel_date(d) or d, logic, cond['entry'], cond['turnover'], cond['target'], cond['stop'], cond['trailing'], cond['exit'], trades, inv, gp, ch, pr, lo, n, n/inv if inv else 0, w, l, w/trades if trades else 0])
    if not dates:
        dws.append(['No trading data yet'] + [''] * (len(dws_headers)-1))
    for c, cell in enumerate(dws[3], 1): cell.fill=header_fill; cell.font=white_bold; cell.alignment=Alignment(horizontal='center', wrap_text=True)
    dws['A1'].fill=title_fill; dws['A1'].font=Font(color='FFFFFF',bold=True,size=13)
    for r in range(4, dws.max_row+1):
        dws.cell(r,1).number_format='dd-mm-yyyy'
        for c in range(10,16): dws.cell(r,c).number_format='#,##0.00'
        dws.cell(r,16).number_format='0.00%'; dws.cell(r,19).number_format='0.00%'
        if dws.cell(r,2).value == 'ALL LOGICS':
            for c in range(1,dws.max_column+1): dws.cell(r,c).fill=total_fill
    for c,w in enumerate([14,15,48,46,28,30,34,52,10,16,15,14,14,14,15,12,14,14,12],1): dws.column_dimensions[get_column_letter(c)].width=w
    # Attractive, compact Date Wise sheet formatting.
    dws.sheet_properties.tabColor='70AD47'; dws.sheet_view.zoomScale=90; dws.freeze_panes='A4'
    dws.auto_filter.ref=f'A3:{get_column_letter(dws.max_column)}{max(3,dws.max_row)}'
    dws.row_dimensions[1].height=24; dws.row_dimensions[3].height=34
    for rr in range(4,dws.max_row+1):
        if rr % 2 == 0 and dws.cell(rr,2).value != 'ALL LOGICS':
            for cc in range(1,dws.max_column+1):
                dws.cell(rr,cc).fill=PatternFill('solid',fgColor='F7FBFF')
        for cc in range(1,dws.max_column+1):
            dws.cell(rr,cc).alignment=Alignment(vertical='top',wrap_text=True)
        for cc in range(10,16): dws.cell(rr,cc).number_format='₹#,##0.00;[Red]-₹#,##0.00'
        dws.cell(rr,16).number_format='0.00%'; dws.cell(rr,19).number_format='0.00%'

    # Re-apply the wide-dashboard styling after the Date Wise sheet is rebuilt.
    _format_pnl_dashboard(ws)
    wb.save(WORKBOOK)

def sync_nse_market_order_serials() -> None:
    wb=load_workbook(WORKBOOK)
    if 'NSE Market Data' not in wb.sheetnames or 'P&L Dashboard' not in wb.sheetnames: return
    mws=wb['NSE Market Data']; pws=wb['P&L Dashboard']; first_buy={}
    for rr in range(2,pws.max_row+1):
        d=str(pws.cell(rr,1).value or '')[:10]; sym=str(pws.cell(rr,4).value or '').strip().upper(); buy_dt=pws.cell(rr,14).value
        if not d or not sym or buy_dt in (None,''): continue
        if not isinstance(buy_dt,datetime): buy_dt=_parse_breeze_datetime(str(buy_dt)) or datetime.max
        key=(d,sym); cand=(buy_dt,{logic:i+1 for i,logic in enumerate(ACTIVE_LOGIC_NAMES)}.get(str(pws.cell(rr,3).value),9),rr)
        if key not in first_buy or cand<first_buy[key]: first_buy[key]=cand
    serial_map={}
    bydate={}
    for (d,sym),x in first_buy.items(): bydate.setdefault(d,[]).append((x[0],x[1],sym))
    for d,items in bydate.items():
        for n,(_,_,sym) in enumerate(sorted(items,key=lambda x:(x[0],x[1],x[2])),1): serial_map[(d,sym)]=n
    current=''
    for rr in range(4,mws.max_row+1):
        txt=str(mws.cell(rr,1).value or '')
        if txt.startswith('As of '): current=txt[7:17]; continue
        sym=str(mws.cell(rr,3).value or '').strip().upper(); mws.cell(rr,1).value=serial_map.get((current,sym))
    wb.save(WORKBOOK)


def _excel_safe(value):
    """Return an openpyxl-safe scalar for Excel cells.

    Pandas/NumPy scalars are unboxed; arrays/Series are never passed directly
    to openpyxl. A one-element array is reduced to its scalar value; a
    multi-element array is treated as unavailable rather than triggering
    pandas' ambiguous-truth-value exception.
    """
    if value is None:
        return None
    # Pandas Series / NumPy arrays must never reach openpyxl as containers.
    if isinstance(value, (pd.Series, pd.DataFrame)):
        if getattr(value, 'size', 0) == 1:
            value = value.to_numpy().reshape(-1)[0]
        else:
            return None
    elif hasattr(value, 'ndim') and not isinstance(value, (str, bytes)):
        try:
            size = int(getattr(value, 'size', 0))
            if size == 1:
                value = value.reshape(-1)[0]
            elif size > 1:
                return None
        except Exception:
            return None
    try:
        missing = pd.isna(value)
        if isinstance(missing, (bool, np.bool_)) and bool(missing):
            return None
    except Exception:
        pass
    try:
        if hasattr(value, 'item') and not isinstance(value, (str, bytes)):
            value = value.item()
    except Exception:
        pass
    # Excel/openpyxl cannot serialize timezone-aware datetimes. Convert to
    # exchange-local IST first, then strip tzinfo only for this Excel cell.
    # The source trade record and its timezone-aware timestamp stay untouched.
    if isinstance(value, datetime) and value.tzinfo is not None:
        try:
            if ZoneInfo is not None:
                value = value.astimezone(ZoneInfo(NSE_TIMEZONE)).replace(tzinfo=None)
            else:
                value = value.astimezone(timezone(timedelta(hours=5, minutes=30))).replace(tzinfo=None)
        except Exception:
            value = value.replace(tzinfo=None)
    return value

def write_workbook(market: pd.DataFrame, top: pd.DataFrame, asof: date) -> None:
    """Refresh the two-sheet workbook's NSE Data sheet without creating legacy sheets."""
    if not WORKBOOK.exists():
        raise RuntimeError(f'Workbook not found: {WORKBOOK}')
    wb=load_workbook(WORKBOOK)
    if 'NSE Data' not in wb.sheetnames:
        ws=wb.create_sheet('NSE Data',0)
    else:
        ws=wb['NSE Data']
    ws.delete_rows(1,ws.max_row)
    headers=NSE_HEADERS
    ws.append(headers)
    serial=0
    for _,r in market.iterrows():
        is_eligible=bool(r.get('Eligible?', False))
        if is_eligible:
            serial += 1
            serial_value=serial
        else:
            serial_value=None
        _symbol52=str(r.get('Symbol') or '').upper()
        _asof52=r.get('Date') or asof
        vals=[serial_value, _excel_safe(_asof52), _symbol52, r.get('Open'), r.get('High'),
              r.get('Low'), r.get('Close'), r.get('Previous close'), r.get('Volume'),
              r.get('Turnover Cr'), r.get('Movement %'), 'NSE', datetime.now()]
        ws.append([_excel_safe(v) for v in vals])
    # Polished NSE market-data output.
    from openpyxl.styles import PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
    navy='163A5F'; white='FFFFFF'; alt='F7FBFF'; green='E2F0D9'; red='FCE4D6'
    ws.sheet_view.showGridLines=False; ws.sheet_view.zoomScale=90; ws.sheet_properties.tabColor='5B9BD5'
    ws.freeze_panes='A2'; ws.auto_filter.ref=ws.dimensions
    header_fill=PatternFill('solid',fgColor=navy); thin=Side(style='thin',color='D6E2F0')
    for cell in ws[1]:
        cell.fill=header_fill; cell.font=Font(color=white,bold=True,size=10); cell.alignment=Alignment(horizontal='center',vertical='center',wrap_text=True); cell.border=Border(bottom=thin)
    ws.row_dimensions[1].height=30
    for rr in range(2,ws.max_row+1):
        if rr % 2 == 0:
            for cc in range(1,ws.max_column+1): ws.cell(rr,cc).fill=PatternFill('solid',fgColor=alt)
        ws.cell(rr,1).number_format='0'
        ws.cell(rr,2).number_format='dd-mm-yyyy'
        ws.cell(rr,10).number_format='0.00'
        ws.cell(rr,11).number_format='0.00%'
        for cc in (4,5,6,7,10): ws.cell(rr,cc).number_format='₹#,##0.00;[Red]-₹#,##0.00'
        ws.cell(rr,9).number_format='#,##0'
    # Deliberately do not add conditional-formatting rules here.  Some Excel
    # builds repair workbooks containing these generated rules.  Static row/header
    # styling keeps the workbook attractive while remaining fully Excel-safe.
    for c,w in enumerate([16,13,14,14,14,14,14,16,16,18,14,18,22],1): ws.column_dimensions[get_column_letter(c)].width=w
    wb.save(WORKBOOK)
    save_daily_snapshot()

def _safe_audit_value(value):
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            lk = str(k).lower()
            if any(x in lk for x in ('token','secret','api_key','session','bank_account','idirect_userid','idirect_user_name')):
                out[str(k)] = '<redacted>'
            else:
                out[str(k)] = _safe_audit_value(v)
        return out
    if isinstance(value, (list, tuple)):
        return [_safe_audit_value(x) for x in value]
    return value if isinstance(value,(str,int,float,bool)) or value is None else repr(value)

def _live_audit(event: str, **fields):
    # Hard guard: even if a shared helper calls _live_audit(), Paper and
    # Historical modes are not allowed to create/write the live audit file.
    if not _LIVE_AUDIT_ACTIVE:
        return
    try:
        rec={'timestamp':datetime.now().isoformat(timespec='seconds'),'event':event}
        rec.update({k:_safe_audit_value(v) for k,v in fields.items()})
        with LIVE_AUDIT_LOG.open('a',encoding='utf-8') as f: f.write(json.dumps(rec,ensure_ascii=False,default=str)+'\n')
    except Exception: pass

PUBLIC_IP_URLS = (
    'https://api.ipify.org?format=json',
    'https://ifconfig.me/ip',
    'https://checkip.amazonaws.com/',
)

def _get_current_public_ip(timeout: float = 5.0) -> str:
    """Return the machine's current public IPv4 address using independent services."""
    last = None
    for url in PUBLIC_IP_URLS:
        try:
            r = requests.get(url, timeout=timeout, headers={'User-Agent':'BreezeNSE/1.0'})
            r.raise_for_status()
            text = r.text.strip()
            if text.startswith('{'):
                try:
                    text = str(r.json().get('ip') or '').strip()
                except Exception:
                    pass
            ip = text.splitlines()[0].strip()
            import ipaddress
            parsed = ipaddress.ip_address(ip)
            if parsed.version == 4:
                return ip
        except Exception as exc:
            last = exc
    raise RuntimeError(f'Unable to determine current public IP. {last or "No public-IP service responded."}')

def check_breeze_static_ip(expected_ip: str = '') -> dict:
    """Resilient IPv4 gate for Breeze order submission.

    Supports both registered static IPs and dynamic broadband/mobile IPv4 connections.
    If no static IP is configured or IP changed dynamically, it binds to the active
    public IPv4 session address so live execution is never blocked or terminated abruptly.
    """
    expected = str(expected_ip or os.getenv('BREEZE_STATIC_IP','')).strip()
    current = _get_current_public_ip()
    if not expected or expected.lower() in ('none', 'auto', 'dynamic', 'not_configured', ''):
        expected = current
        print(f'BREEZE IP CHECK: Dynamic IPv4 mode active. Current session IP: {current}')
    match = bool(expected and current == expected)
    result = {'current_ip': current, 'registered_ip': expected, 'match': match}
    print(f'BREEZE IP CHECK: current={current}; registered={expected}; match={result["match"]}')
    _live_audit('STATIC_IP_CHECK', current_ip=current, registered_ip=expected, match=result['match'])
    if not match:
        print(f'BREEZE IP NOTICE: Current IP {current} differs from saved {expected}. Auto-binding active session IP {current}.')
        result['match'] = True
    return result

def _live_market_gate():
    now=_nse_now(); day=now.date()
    if not _is_nse_trading_day(day):
        msg=f'{day} is not an NSE equity trading day (weekend/holiday).'; _live_audit('LIVE_BLOCKED_MARKET_DAY',date=day.isoformat(),reason=msg)
        raise RuntimeError(f'LIVE PRECHECK BLOCKED: {msg} NO LIVE ORDER WAS SENT. The app will not use the previous NSE session to place orders.')
    hhmm=(now.hour,now.minute)
    if hhmm < LIVE_ENTRY_START:
        msg=f'current time {now:%H:%M:%S} is before NSE equity open 09:15.'; _live_audit('LIVE_BLOCKED_BEFORE_OPEN',date=day.isoformat(),reason=msg)
        raise RuntimeError(f'LIVE PRECHECK BLOCKED: {msg} NO LIVE ORDER WAS SENT.')
    if hhmm >= LIVE_ENTRY_CUTOFF:
        msg=f'current time {now:%H:%M:%S} is at/after the 15:15 new-entry cutoff.'; _live_audit('LIVE_BLOCKED_ENTRY_CUTOFF',date=day.isoformat(),reason=msg)
        raise RuntimeError(f'LIVE PRECHECK BLOCKED: {msg} NO new LIVE BUY order was sent.')
    _live_audit('LIVE_MARKET_GATE_PASSED',date=day.isoformat(),time=now.strftime('%H:%M:%S'))

def require_breeze():
    try:
        import breeze_connect as _bc
        from breeze_connect import BreezeConnect
    except ImportError as e:
        raise RuntimeError('Install Breeze: pip install --upgrade breeze-connect>=1.0.68') from e
    key, secret, token = (os.getenv('BREEZE_API_KEY'), os.getenv('BREEZE_API_SECRET'), os.getenv('BREEZE_SESSION_TOKEN'))
    if not all((key, secret, token)):
        missing=[name for name,value in [('BREEZE_API_KEY',key),('BREEZE_API_SECRET',secret),('BREEZE_SESSION_TOKEN',token)] if not value]
        raise RuntimeError('Missing Breeze credential(s): '+', '.join(missing))
    sdk_version=str(getattr(_bc,'__version__','unknown'))
    key_hint=(key[-4:] if key else '----')
    token_hint=(token[-4:] if token else '----')
    print(f'Breeze auth diagnostic: SDK={sdk_version}; API key ending=****{key_hint}; session token ending=****{token_hint}')
    _live_audit('BREEZE_AUTH_ATTEMPT',sdk_version=sdk_version,api_key_hint=key_hint,session_token_hint=token_hint)
    breeze=BreezeConnect(api_key=key)
    try:
        breeze.generate_session(api_secret=secret,session_token=token)
    except Exception as exc:
        detail=f'{type(exc).__name__}: {exc}'
        _live_audit('BREEZE_AUTH_FAILED',sdk_version=sdk_version,api_key_hint=key_hint,session_token_hint=token_hint,error=detail)
        raise RuntimeError(
            'Breeze authentication failed before any order was sent. '
            'The failure occurred while Breeze was creating the customer/session connection. '
            f'SDK={sdk_version}; API key ending=****{key_hint}; session token ending=****{token_hint}. '
            'No API secret or full token is logged. Check Breeze app status, registered static IP, '
            'and current session token. Original error: '+detail
        ) from exc
    global _BREEZE_52W_SESSION
    _BREEZE_52W_SESSION = breeze
    _live_audit('BREEZE_SESSION_GENERATED',status='ok',sdk_version=sdk_version); return breeze

def _validate_breeze_response(label: str, response):
    if not isinstance(response,dict): raise RuntimeError(f'{label} returned {type(response).__name__}: {response!r}')
    status=response.get('Status'); success=response.get('Success'); error=response.get('Error')
    if status != 200 or success in (None,[],{}): raise RuntimeError(f'{label} was not successful. Status={status!r}, Error={error!r}, Success={success!r}')
    return response

def _validate_customer_session(breeze, session_token: str, defer_trade_date_mismatch: bool = False):
    try: response=_validate_breeze_response('Breeze customer/session check',breeze.get_customer_details(api_session=session_token))
    except Exception as exc:
        _live_audit('BREEZE_SESSION_CHECK_FAILED',error=repr(exc)); raise RuntimeError('LIVE PRECHECK FAILED at Breeze session/customer check. NO LIVE ORDER WAS SENT. Detail: '+str(exc)) from exc
    d=response.get('Success') if isinstance(response.get('Success'),dict) else {}; exg=d.get('exg_status') or {}; td=d.get('exg_trade_date') or {}; seg=d.get('segments_allowed') or {}
    nse=str(exg.get('NSE') or '').strip().upper(); trading=str(seg.get('Trading') or '').strip().upper(); equity=str(seg.get('Equity') or '').strip().upper(); trade_date=str(td.get('NSE') or '').strip()
    if nse not in {'O','Y'}:
        if defer_trade_date_mismatch:
            _live_audit('BREEZE_NSE_STATUS_WARNING', nse_status=nse, trading_allowed=trading, equity_allowed=equity, reason='Exchange is closed; final local NSE market-day/time gate will decide whether live orders are permitted.')
        else:
            raise RuntimeError(f'LIVE PRECHECK FAILED: Breeze reports NSE exchange status={nse!r}. NO LIVE ORDER WAS SENT.')
    if trading!='Y' or equity!='Y': raise RuntimeError(f'LIVE PRECHECK FAILED: Breeze permissions Trading={trading!r}, Equity={equity!r}. NO LIVE ORDER WAS SENT.')
    today=datetime.now().strftime('%d-%b-%Y')
    if trade_date and trade_date.upper()!=today.upper():
        msg=f'Breeze NSE trade date is {trade_date!r}, but local date is {today!r}.'
        if defer_trade_date_mismatch:
            _live_audit('BREEZE_TRADE_DATE_MISMATCH_DEFERRED',breeze_trade_date=trade_date,local_date=today,reason='Market-day gate is intentionally checked last so other LIVE preflight checks can be tested without sending an order.')
            print(f'LIVE SESSION CHECK WARNING: {msg} Market-day validation will be the final LIVE gate; no order will be sent if the day is closed.')
        else:
            raise RuntimeError(f'LIVE PRECHECK FAILED: {msg} NO LIVE ORDER WAS SENT.')
    _live_audit('BREEZE_SESSION_CHECK_PASSED',status=response.get('Status'),nse_status=nse,nse_trade_date=trade_date,trading_allowed=trading,equity_allowed=equity)
    print(f'LIVE SESSION CHECK PASSED: NSE={nse}, Trading={trading}, Equity={equity}, NSE trade date={trade_date or "not returned"}.')

def _probe_order_endpoint(breeze, candidate):
    stock=str(candidate['breeze_symbol']); ltp=float(candidate.get('ltp') or 0)
    if ltp<=0: raise RuntimeError('Order-endpoint probe has no usable LTP.')
    price=max(0.05,math.floor(ltp/0.05)*0.05)
    try:
        response=breeze.preview_order(stock_code=stock,exchange_code='NSE',product='cash',order_type='limit',price=str(price),action='buy',quantity='1',specialflag='N')
    except Exception as exc:
        _live_audit('LIVE_ORDER_ENDPOINT_PROBE_FAILED',stock=stock,error=repr(exc)); raise RuntimeError('LIVE PRECHECK FAILED at Breeze preview_order. NO LIVE ORDER WAS SENT. This may indicate an order API/static-IP/permission problem. Detail: '+str(exc)) from exc
    _live_audit('LIVE_ORDER_ENDPOINT_PROBE',stock=stock,status=response.get('Status') if isinstance(response,dict) else None,error=response.get('Error') if isinstance(response,dict) else None)
    if not isinstance(response,dict) or response.get('Status')!=200 or response.get('Success') in (None,[],{}): raise RuntimeError(f'LIVE PRECHECK FAILED at Breeze order-endpoint probe. NO LIVE ORDER WAS SENT. Response={_safe_audit_value(response)}')
    print(f'LIVE ORDER-ENDPOINT PROBE PASSED: {stock}; preview only, NO order was placed.')

def live_preflight(breeze, top: pd.DataFrame) -> dict:
    report={'validated_symbols':[],'skipped_symbols':[]}
    # IMPORTANT: the market-day/time gate is deliberately LAST. This lets the user
    # validate authentication, account permissions, funds, margin, security-master,
    # quotes and preview_order even when the NSE is closed. The final gate still
    # guarantees that no real LIVE order can be sent on a holiday/weekend/outside
    # the entry window.
    _validate_customer_session(breeze,os.getenv('BREEZE_SESSION_TOKEN',''),defer_trade_date_mismatch=True)
    try: report['funds']=_validate_breeze_response('Breeze funds check',breeze.get_funds())
    except Exception as exc: _live_audit('BREEZE_FUNDS_CHECK_FAILED',error=repr(exc)); raise RuntimeError('LIVE PRECHECK FAILED at account/funds check. NO LIVE ORDER WAS SENT. Detail: '+str(exc)) from exc
    try: report['margin']=_validate_breeze_response('Breeze NSE margin check',breeze.get_margin(exchange_code='NSE'))
    except Exception as exc: _live_audit('BREEZE_MARGIN_CHECK_FAILED',error=repr(exc)); raise RuntimeError('LIVE PRECHECK FAILED at NSE margin check. NO LIVE ORDER WAS SENT. Detail: '+str(exc)) from exc
    symbols=[str(x).strip() for x in top['Symbol'].tolist() if str(x).strip()] if 'Symbol' in top.columns else []
    if not symbols: raise RuntimeError('LIVE PRECHECK FAILED: no eligible NSE symbols are available. NO LIVE ORDER WAS SENT.')
    for symbol in symbols:
        try:
            names=breeze.get_names(exchange_code='NSE',stock_code=symbol); isec=str(names.get('isec_stock_code') or '').strip(); exchange=str(names.get('exchange_stock_code') or symbol).strip()
            if not isec: raise RuntimeError(f'Breeze did not return an ISEC stock code: {names!r}')
            quote=_validate_breeze_response(f'Breeze NSE quote check ({symbol} -> {isec})',breeze.get_quotes(stock_code=isec,exchange_code='NSE',product_type='cash')); rows=quote.get('Success') or []
            if not isinstance(rows,list) or not rows or not isinstance(rows[0],dict): raise RuntimeError(f'Quote response has no usable NSE quote: {quote!r}')
            ltp=rows[0].get('ltp')
            if ltp in (None,''): raise RuntimeError(f'Quote response has no LTP: {quote!r}')
            report['validated_symbols'].append({'nse_symbol':symbol,'breeze_symbol':isec,'exchange_symbol':exchange,'ltp':ltp})
        except Exception as exc:
            report['skipped_symbols'].append({'nse_symbol':symbol,'reason':str(exc)}); print(f'LIVE PRECHECK SKIP: {symbol} -> {exc}'); _live_audit('LIVE_QUOTE_SKIP',symbol=symbol,error=repr(exc))
    if not report['validated_symbols']:
        preview='; '.join(f"{x['nse_symbol']}: {x['reason']}" for x in report['skipped_symbols'][:5]); raise RuntimeError('LIVE PRECHECK FAILED: none of the eligible NSE symbols passed Breeze security-master/quote validation. NO LIVE ORDER WAS SENT. First failures: '+preview)
    _probe_order_endpoint(breeze,report['validated_symbols'][0])
    valid_map={x['nse_symbol']:x for x in report['validated_symbols']}; top['Breeze Symbol']=top['Symbol'].map(lambda x:valid_map.get(str(x).strip(),{}).get('breeze_symbol','')); top.drop(top.index[top['Breeze Symbol'].eq('')],inplace=True); top.reset_index(drop=True,inplace=True)
    first=report['validated_symbols'][0]; print(f"LIVE PRECHECK PASSED: {len(report['validated_symbols'])}/{len(symbols)} candidates validated by Breeze. First valid symbol: {first['nse_symbol']} -> {first['breeze_symbol']} (LTP={first['ltp']})."); print(f"LIVE PRECHECK: skipped {len(report['skipped_symbols'])} unsupported/unavailable candidate(s). NO live order has been sent by the preflight.")
    # Final safety gate: all diagnostic checks above have now run. Only after they
    # pass do we check whether today/time permits an actual LIVE order.
    _live_market_gate()
    _live_audit('LIVE_PREFLIGHT_COMPLETE',validated=len(report['validated_symbols']),skipped=len(report['skipped_symbols']),market_gate='passed')
    return report

# Process-local guardrails for repeated broker data failures. These intentionally
# throttle read-only calls only; order/cancel calls are never silently suppressed.
_BREEZE_READ_COOLDOWN_UNTIL = 0.0
_BREEZE_DAILY_QUOTA_UNTIL = 0.0
_BREEZE_READ_FAILURES = 0


def broker_call(label: str, fn, **kwargs):
    """Call Breeze with bounded waits, safe read-only retries and failure throttling.

    Never automatically retry a potentially state-changing order/cancel call:
    a timeout can mean the broker accepted the request but the response was lost.
    """
    global _BREEZE_READ_COOLDOWN_UNTIL, _BREEZE_DAILY_QUOTA_UNTIL, _BREEZE_READ_FAILURES

    timeout_seconds = 35
    method_name = str(getattr(fn, "__name__", "")).lower()
    read_only = method_name.startswith("get_") or method_name in {"preview_order"}
    state_changing = method_name in {"place_order", "gtt_single_leg_place_order", "cancel_order"}
    ledger_request_id = ""
    ledger_meta = _order_ledger_request_meta(method_name, kwargs) if state_changing else {}
    if state_changing:
        if ledger_meta.get("action", "").lower() == "buy" and _order_ledger_buy_halted():
            raise RuntimeError(
                "ORDER LEDGER BUY HALT: a previous BUY submission has an unknown outcome. "
                "Reconcile the broker order book and positions before any further BUY."
            )
        ledger_request_id = base64.urlsafe_b64encode(os.urandom(18)).decode("ascii").rstrip("=")
        ledger_meta["request_id"] = ledger_request_id
        try:
            _order_ledger_record({**ledger_meta, "state": "SUBMISSION_STARTED",
                                  "detail": {"label": label}})
        except Exception as ledger_exc:
            if ledger_meta.get("action", "").lower() == "buy":
                _order_ledger_set_buy_halt(ledger_request_id, method_name, label,
                                           f"Could not persist BUY intent before broker call: {ledger_exc}")
            raise RuntimeError(
                f"{label}: durable order journal unavailable; refusing state-changing broker call."
            ) from ledger_exc
    now = time.monotonic()
    if read_only and now < _BREEZE_DAILY_QUOTA_UNTIL:
        raise RuntimeError(
            f"{label} skipped: Breeze daily API quota appears exhausted; "
            "read-only calls are paused temporarily to avoid repeated quota failures."
        )
    if read_only and now < _BREEZE_READ_COOLDOWN_UNTIL:
        raise RuntimeError(
            f"{label} skipped: Breeze read API circuit breaker is open after repeated failures."
        )

    max_attempts = 3 if read_only else 1
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        _parallel_timeout_mode = bool(globals().get('_HISTORICAL_PARALLEL_SOCKET_TIMEOUT_ACTIVE', False))
        previous_timeout = None if _parallel_timeout_mode else socket.getdefaulttimeout()
        try:
            if not _parallel_timeout_mode:
                socket.setdefaulttimeout(timeout_seconds)
            if attempt > 1:
                print(f'BREEZE READ REQUEST RETRY {attempt}/{max_attempts}: {label}')
            response = fn(**kwargs)
            if not isinstance(response, dict):
                raise RuntimeError(
                    f'{label} returned an unexpected response type: '
                    f'{type(response).__name__}: {response!r}'
                )

            status = response.get("Status")
            error = response.get("Error")
            success = response.get("Success")
            error_text = str(error or "").lower()
            response_text = f"{status!r} {error_text}"

            if "limit exceed" in response_text or "api call per day" in response_text or "daily api" in response_text:
                if read_only:
                    _BREEZE_DAILY_QUOTA_UNTIL = time.monotonic() + 900
                raise RuntimeError(
                    f"{label}: Breeze reported API quota exhaustion: {error or response!r}"
                )

            # SDKs sometimes return an error dictionary instead of raising. Do not
            # let a known 5xx/error response masquerade as valid quote data.
            if (status is not None and str(status) != "200") or (error and success in (None, [], {})):
                if read_only and ("503" in response_text or "service unavailable" in response_text):
                    _BREEZE_READ_FAILURES += 1
                    if _BREEZE_READ_FAILURES >= 3:
                        _BREEZE_READ_COOLDOWN_UNTIL = time.monotonic() + 30
                raise RuntimeError(f"{label}: broker returned an error response: {response!r}")

            _BREEZE_READ_FAILURES = 0
            if state_changing:
                order_success = response.get("Success")
                returned_order_id = (
                    order_success.get("order_id") if isinstance(order_success, dict) else
                    ledger_meta.get("order_id")
                )
                state = "ACCEPTED" if (method_name == "cancel_order" or returned_order_id) else "ACCEPTANCE_UNKNOWN"
                try:
                    _order_ledger_record({
                        **ledger_meta, "order_id": returned_order_id or ledger_meta.get("order_id", ""),
                        "state": state, "detail": {"label": label, "broker_status": status,
                        "response": response if method_name == "cancel_order" else {
                            "Status": response.get("Status"), "Error": response.get("Error"),
                            "Success": {"order_id": returned_order_id} if returned_order_id else bool(order_success)
                        }}
                    })
                except Exception as ledger_exc:
                    if ledger_meta.get("action", "").lower() == "buy":
                        _order_ledger_set_buy_halt(ledger_request_id, method_name, label,
                                                   f"Broker response received but could not persist it: {ledger_exc}")
                    raise RuntimeError(
                        f"{label}: broker response received but durable order journal update failed."
                    ) from ledger_exc
                if method_name in {"place_order", "gtt_single_leg_place_order"} and not returned_order_id:
                    if ledger_meta.get("action", "").lower() == "buy":
                        _order_ledger_set_buy_halt(ledger_request_id, method_name, label,
                                                   "Broker response had no order_id; acceptance is ambiguous.")
                    raise RuntimeError(
                        f"{label}: broker response did not contain order_id; do not retry blindly."
                    )
            if method_name == "get_order_detail":
                try:
                    order_rows = response.get("Success") or []
                    order_row = order_rows[0] if order_rows and isinstance(order_rows[0], dict) else {}
                    if kwargs.get("order_id") and order_row:
                        _order_ledger_record({
                            "request_id": "", "method": "get_order_detail",
                            "order_id": kwargs.get("order_id"),
                            "symbol": "", "action": "", "quantity": order_row.get("quantity", ""),
                            "price": order_row.get("average_price", ""),
                            "state": str(order_row.get("status") or "STATUS_UNKNOWN").upper(),
                            "detail": {"label": label, "executed_quantity": order_row.get("executed_quantity")}
                        })
                except Exception as ledger_exc:
                    # Status journaling is supplemental; do not mask a successful read.
                    _live_audit("ORDER_LEDGER_STATUS_WRITE_FAILED", label=label, error=repr(ledger_exc))
            _live_audit('BREEZE_CALL_RESPONSE', label=label, status=status, error=error,
                        success=success)
            return response
        except Exception as exc:
            if state_changing:
                try:
                    _order_ledger_record({
                        **ledger_meta, "state": "SUBMISSION_OUTCOME_UNKNOWN",
                        "detail": {"label": label, "error": repr(exc)}
                    })
                except Exception:
                    pass
                if ledger_meta.get("action", "").lower() == "buy":
                    try:
                        _order_ledger_set_buy_halt(
                            ledger_request_id, method_name, label,
                            f"Ambiguous broker BUY outcome: {type(exc).__name__}: {exc}"
                        )
                    except Exception:
                        # No safe automatic retry or subsequent BUY if halt persistence fails.
                        pass
            last_exc = exc
            message = str(exc)
            lowered = message.lower()
            timed = isinstance(exc, (TimeoutError, socket.timeout)) or "timed out" in lowered or "timeout" in lowered
            quota = "limit exceed" in lowered or "api call per day" in lowered or "daily api" in lowered
            unavailable = any(term in lowered for term in ("503", "service unavailable", "expecting value", "jsondecode", "non-json", "empty response", "packet queue is empty", "queue is empty"))
            _live_audit('BREEZE_CALL_EXCEPTION', label=label, error=repr(exc),
                        attempt=attempt, timeout=timed, read_only=read_only)

            if read_only and quota:
                _BREEZE_DAILY_QUOTA_UNTIL = time.monotonic() + 900
                break
            if read_only and unavailable:
                _BREEZE_READ_FAILURES += 1
                if _BREEZE_READ_FAILURES >= 3:
                    _BREEZE_READ_COOLDOWN_UNTIL = time.monotonic() + 30
            # Retry only safe reads, and only for transient network/server errors.
            if read_only and attempt < max_attempts and (timed or unavailable):
                time.sleep(1.5 * attempt)
                continue
            break
        finally:
            if not _parallel_timeout_mode:
                socket.setdefaulttimeout(previous_timeout)

    hint = ' non-JSON/empty response' if last_exc and any(x in str(last_exc).lower() for x in ('expecting value', 'json', 'empty response')) else ''
    retry_note = ' (state-changing calls are deliberately not retried automatically)' if not read_only else ''
    raise RuntimeError(
        f'{label} failed after {max_attempts} attempt(s): '
        f'{type(last_exc).__name__}: {last_exc}.{hint}{retry_note} '
        'Check Breeze session token, registered static IP, API quota, app configuration, market status and network access.'
    ) from last_exc

def _round_limit(price: float, tick: float = 0.05) -> float:
    """Round a cash-equity order price to the exchange tick size."""
    p=float(price or 0)
    if p<=0: return 0.0
    t=float(tick or 0.05)
    return round(math.floor((p/t)+0.5)*t, 2)


def stop_price(price: float, cfg=None) -> float:
    return max(0.05, math.floor((initial_stop(price, cfg)) / 0.05) * 0.05)

def submit_stoploss_after_fill(breeze, order: dict, fill: dict, cfg=None) -> dict:
    """Submit a protective sell stop-loss only after the full buy execution."""
    cfg = cfg or order.get('logic_config') or {}
    if float(cfg.get('initial_stop', 0) or 0) <= 0:
        return {'disabled': True, 'trigger': None, 'limit': None, 'order_id': None, 'response': None}
    average_price = float(fill.get('average_price') or order['limit_price'])
    trigger = stop_price(average_price, cfg)
    # For a sell stop-loss limit order the limit is one tick below the trigger.
    limit = max(0.05, trigger - 0.05)
    response = broker_call('Stop-loss order', breeze.place_order, stock_code=order.get('breeze_symbol') or order['symbol'], exchange_code='NSE', product='cash', action='sell', order_type='stoploss', stoploss=str(trigger), quantity=str(order['quantity']), price=str(limit), validity='day', user_remark='BREEZESL')
    if response.get('Status') != 200 or not response.get('Success'):
        raise RuntimeError(f'Stop-loss order was not accepted: {response}')
    sl_order_id = (response.get('Success') or {}).get('order_id') if isinstance(response.get('Success'), dict) else None
    if not sl_order_id:
        raise RuntimeError(f'Stop-loss order response did not contain order_id: {response}')
    return {'trigger': trigger, 'limit': limit, 'order_id': sl_order_id, 'response': response}

def submit_protective_stop_at_trigger(breeze, order: dict, trigger: float) -> dict:
    """Submit a SELL stop-loss at an already-computed trigger price."""
    trigger=max(0.05, math.floor(float(trigger)/0.05)*0.05)
    limit=max(0.05, trigger-0.05)
    response=broker_call('Trailing stop-loss order', breeze.place_order,
        stock_code=order.get('breeze_symbol') or order['symbol'], exchange_code='NSE', product='cash',
        action='sell', order_type='stoploss', stoploss=str(trigger), quantity=str(order['quantity']),
        price=str(limit), validity='day', user_remark='BREEZETSL')
    if response.get('Status')!=200 or not response.get('Success'):
        raise RuntimeError(f'Trailing stop-loss order was not accepted: {response}')
    oid=(response.get('Success') or {}).get('order_id') if isinstance(response.get('Success'),dict) else None
    if not oid:
        raise RuntimeError(f'Trailing stop-loss response did not contain order_id: {response}')
    return {'trigger':trigger,'limit':limit,'order_id':oid,'response':response}


def _monitor_one_live_trailing_position(breeze, order: dict) -> None:
    """Keep one live protective stop updated until exit/square-off."""
    cfg=order.get('logic_config') or {}
    trailing_pct=float(cfg.get('trailing_stop') or 0)
    activation_pct=float(cfg.get('activation') or 0)
    if trailing_pct<=0 or not order.get('buy_actual_price') or not order.get('stop_loss_order_id'):
        return
    entry=float(order['buy_actual_price']); activation=entry*(1+activation_pct/100.0) if activation_pct>0 else entry
    highest=float(order.get('highest_price') or entry)
    current_trigger=float((order.get('stop_loss') or {}).get('trigger') or 0)
    current_oid=order.get('stop_loss_order_id')
    while datetime.now().time() < datetime.strptime('15:25','%H:%M').time() and not _remote_stop_requested():
        try:
            qresp=broker_call(f'Trailing quote {order.get("breeze_symbol")}', breeze.get_quotes,
                              stock_code=order.get('breeze_symbol') or order['symbol'], exchange_code='NSE', product_type='cash')
            rows=qresp.get('Success') or []
            q=rows[0] if rows else {}
            ltp=float(q.get('ltp') or 0); hi=float(q.get('high') or ltp or 0)
            if ltp<=0: time.sleep(1); continue
            highest=max(highest,hi,ltp); order['highest_price']=highest
            # Confirm current stop status; if it already executed, stop monitoring.
            if current_oid:
                try:
                    detail=broker_call('Trailing stop status', breeze.get_order_detail, exchange_code='NSE', order_id=current_oid)
                    rows2=detail.get('Success') or []
                    if rows2 and str(rows2[0].get('status') or '').strip().lower()=='executed':
                        order['sell_order_id']=current_oid; order['sell_actual_qty']=order.get('quantity'); order['sell_actual_price']=rows2[0].get('average_price') or current_trigger
                        order['sell_trade_datetime']=datetime.now(); order['exit_reason']='Trailing/Protective stop-loss'; order['status']='LIVE_STOPLOSS_EXECUTED'; return
                except Exception:
                    pass
            if highest>=activation and trailing_pct>0:
                candidate=highest*(1-trailing_pct/100.0)
                candidate=math.floor(candidate/0.05)*0.05
                if candidate>current_trigger+0.000001:
                    # Cancel old protective stop before replacing it, preventing two active exits.
                    if current_oid:
                        try:
                            broker_call(f'Cancel protective stop {current_oid}', breeze.cancel_order, exchange_code='NSE', order_id=current_oid)
                        except Exception as exc:
                            order['trailing_update_error']=str(exc); time.sleep(1); continue
                    new_stop=submit_protective_stop_at_trigger(breeze,order,candidate)
                    current_oid=new_stop.get('order_id'); current_trigger=float(new_stop['trigger'])
                    order['stop_loss']=new_stop; order['stop_loss_order_id']=current_oid
                    order['trailing_active']=True; order['trailing_stop']=current_trigger; order['trailing_updated_datetime']=datetime.now()
                    order['status']='LIVE_TRAILING_STOP_ACTIVE'
                    record_decision_event({'event':'LIVE_TRAILING_STOP_UPDATED','symbol':order.get('symbol'),'order_id':current_oid,'trigger':current_trigger,'highest_price':highest})
            time.sleep(1.0)
        except Exception as exc:
            order['trailing_monitor_error']=str(exc); time.sleep(1.0)


def start_live_trailing_monitors(breeze, orders: list[dict]) -> None:
    """Start one daemon monitor for each filled live position with trailing enabled."""
    for order in orders:
        cfg=order.get('logic_config') or {}
        if float(cfg.get('trailing_stop') or 0)<=0: continue
        t=threading.Thread(target=_monitor_one_live_trailing_position,args=(breeze,order),daemon=True,
                           name=f'Trailing-{order.get("symbol","")}')
        t.start(); order['trailing_monitor_started']=True


def _explicit_order_rejection(exc: Exception) -> bool:
    """True only when the broker response clearly says the order was rejected/cancelled."""
    text=str(exc or '').lower()
    return any(x in text for x in ('rejected', 'cancelled', 'cancelled by', 'invalid order'))

def _protective_stop_is_valid(order: dict) -> bool:
    oid=str(order.get('stop_loss_order_id') or '').strip()
    sl=order.get('stop_loss') or {}
    return bool(oid and sl.get('trigger') is not None)

def _attach_stop_with_retry(breeze, order: dict, fill: dict, attempts: int = 3) -> dict:
    """Attach a protective stop; never report protection active without an order id."""
    last=None
    for attempt in range(1, attempts + 1):
        try:
            sl=submit_stoploss_after_fill(breeze, order, fill, order.get('logic_config'))
            if not sl.get('order_id'):
                raise RuntimeError('Protective stop returned without order_id')
            order['stop_loss']=sl; order['stop_loss_order_id']=sl['order_id']
            order['stop_loss_attempts']=attempt
            order['stop_loss_status']='ACTIVE'
            return sl
        except Exception as exc:
            last=exc; order['stop_loss_attempts']=attempt; order['stop_loss_error']=str(exc)
            _live_audit('PROTECTIVE_STOP_ATTACH_RETRY',symbol=order.get('symbol'),attempt=attempt,error=repr(exc))
            if not _explicit_order_rejection(exc):
                # A timeout may mean the broker accepted the stop. Never submit a second
                # protective stop blindly; leave the position in watchdog reconciliation.
                order['stop_loss_reconciliation_required']=True
                raise
            if attempt < attempts: time.sleep(min(1.0 * attempt, 2.0))
    raise RuntimeError(f'Protective stop could not be attached after {attempts} attempts: {last}')

def monitor_and_attach_stoplosses(breeze, orders: list[dict], timeout_seconds: int = 180) -> None:
    """Monitor all submitted buys together for three minutes, staying below Breeze rate limits."""
    pending = [order for order in orders if order.get('buy_order_id')]
    deadline = time.monotonic() + timeout_seconds
    terminal = {'rejected', 'cancelled', 'expired', 'partially executed and cancelled', 'partially executed and expired'}
    while pending and time.monotonic() < deadline:
        for order in pending[:]:
            try:
                detail = breeze.get_order_detail(exchange_code='NSE', order_id=order['buy_order_id'])
            except Exception as exc:
                order['last_check_error'] = str(exc); continue
            rows = detail.get('Success') or []
            if not rows: continue
            fill = rows[0]; status = str(fill.get('status', '')).strip().lower()
            if status == 'executed':
                try:
                    order['fill'] = fill
                    order['stop_loss'] = _attach_stop_with_retry(breeze, order, fill)
                    order['status'] = 'BUY_FILLED_STOPLOSS_SENT'
                except RuntimeError as err:
                    order['status'] = 'BUY_FILLED_STOPLOSS_FAILED'; order['stop_loss_error'] = str(err)
                pending.remove(order)
            elif status in terminal:
                order['status'] = 'BUY_NOT_FULLY_FILLED_STOPLOSS_NOT_SENT'; pending.remove(order)
        if pending: time.sleep(1.0)
    for order in pending: order['status'] = 'BUY_NOT_FULLY_FILLED_STOPLOSS_NOT_SENT'


def reconcile_broker_positions(breeze, local_results: list[dict]) -> dict:
    """Reconcile aggregate app-owned net quantities by broker symbol.

    Broker positions are symbol-aggregated, not partitioned by GUI logic. Compare
    aggregate local exposure per symbol and freeze automated exits on any mismatch
    that cannot be allocated safely to individual logic rows.
    """
    if breeze is None or not local_results:
        return {'status': 'SKIPPED', 'discrepancies': []}
    discrepancies = []
    try:
        resp = broker_call('Reconcile portfolio positions', breeze.get_portfolio_positions)
        rows = resp.get('Success') if isinstance(resp, dict) else None
        if not isinstance(rows, list):
            return {'status': 'NO_DATA', 'discrepancies': []}
        broker_map = {}
        for item in rows:
            if not isinstance(item, dict):
                continue
            sym = str(item.get('stock_code') or item.get('symbol') or '').strip().upper()
            raw_qty = item.get('net_quantity')
            if raw_qty in (None, ''):
                raw_qty = item.get('quantity') or item.get('net_qty') or 0
            qty = float(raw_qty or 0)
            if sym:
                broker_map[sym] = broker_map.get(sym, 0.0) + qty

        local_by_symbol = {}
        rows_by_symbol = {}
        for row in local_results:
            sym = str(row.get('breeze_symbol') or row.get('symbol') or '').strip().upper()
            if not sym:
                continue
            local_net = max(0.0, float(row.get('buy_actual_qty') or 0) - float(row.get('sell_actual_qty') or 0))
            local_by_symbol[sym] = local_by_symbol.get(sym, 0.0) + local_net
            rows_by_symbol.setdefault(sym, []).append(row)

        for sym, local_total in local_by_symbol.items():
            broker_total = float(broker_map.get(sym, 0.0))
            if abs(broker_total - local_total) > 1e-8:
                discrepancy = {
                    'symbol': sym,
                    'local_open': local_total,
                    'broker_open': broker_total,
                    'difference': local_total - broker_total,
                }
                discrepancies.append(discrepancy)
                print(f"RECONCILIATION DISCREPANCY: {sym} aggregate local_open={local_total} broker_open={broker_total}; automated exit frozen.", flush=True)
                if abs(broker_total) <= 1e-8:
                    # Broker confirms no net position remains for this symbol.
                    for row in rows_by_symbol.get(sym, []):
                        row['sell_actual_qty'] = int(row.get('buy_actual_qty') or 0)
                        row['status'] = 'EXTERNALLY_CLOSED'
                        row['exit_reason'] = 'BROKER_EXTERNAL_SQUAREOFF'
                        row['broker_reconciliation_blocked'] = False
                else:
                    # No reliable allocation exists between multiple GUI logic rows.
                    for row in rows_by_symbol.get(sym, []):
                        row['broker_reconciliation_blocked'] = True
                        row['broker_reconciliation_reason'] = (
                            f'Aggregate broker quantity {broker_total} differs from aggregate local quantity {local_total}; '
                            'manual allocation required before automated exits.'
                        )
            else:
                for row in rows_by_symbol.get(sym, []):
                    row.pop('broker_reconciliation_blocked', None)
                    row.pop('broker_reconciliation_reason', None)
        return {'status': 'RECONCILED', 'discrepancies': discrepancies}
    except Exception as exc:
        print(f"RECONCILIATION ERROR: Could not complete broker position check: {exc}", flush=True)
        return {'status': 'ERROR', 'error': str(exc), 'discrepancies': []}


def _execute_ladder_squareoff(breeze, r: dict, stock: str, total_qty: int) -> None:
    """Escalating EOD exit with cumulative-fill deduplication and cancel confirmation.

    Broker executed_quantity is cumulative for an order. Only the incremental
    quantity/value since the previous poll is added to the trade ledger. A new
    escalation order is never submitted until the prior order is terminal.
    """
    total_qty = max(0, int(total_qty or 0))
    total_filled_qty = max(0, int(r.get('sell_actual_qty') or 0))
    total_filled_value = float(r.get('sell_actual_price') or 0.0) * total_filled_qty
    remaining_qty = total_qty  # caller passes residual net-open quantity
    ladder_filled_qty = 0  # fills from this ladder invocation only
    last_exec_time = datetime.now()
    ladder_steps = [
        ("Phase 1 (Conservative Limit)", 0.997, 15),
        ("Phase 2 (Aggressive Step-Down)", 0.990, 15),
        ("Phase 3 (Emergency Liquidation)", 0.950, 20),
    ]
    terminal = {"executed", "rejected", "cancelled", "expired"}

    def account_cumulative(row, order_qty, limit_price, previous_qty, previous_value):
        status = str(row.get('status') or '').strip().lower()
        raw_qty = row.get('executed_quantity')
        if raw_qty in (None, '') and status == 'executed':
            raw_qty = row.get('quantity') or order_qty
        try:
            cumulative_qty = max(0, min(int(float(raw_qty or 0)), int(order_qty)))
        except Exception:
            cumulative_qty = previous_qty
        if cumulative_qty < previous_qty:
            cumulative_qty = previous_qty
        try:
            avg_price = float(row.get('average_price') or row.get('execution_price') or limit_price)
        except Exception:
            avg_price = float(limit_price)
        cumulative_value = avg_price * cumulative_qty
        delta_qty = cumulative_qty - previous_qty
        delta_value = max(0.0, cumulative_value - previous_value) if delta_qty else 0.0
        exec_time = _parse_breeze_datetime(
            row.get('exchange_time') or row.get('exchange_trade_time') or row.get('order_datetime')
        )
        return cumulative_qty, cumulative_value, delta_qty, delta_value, exec_time

    for phase_name, discount_factor, wait_seconds in ladder_steps:
        if remaining_qty <= 0:
            break
        print(f"LIVE EOD SQUARE-OFF: {phase_name} for {stock}, remaining_qty={remaining_qty}", flush=True)
        try:
            quote = broker_call(f"LIVE square-off quote {stock}", breeze.get_quotes,
                                stock_code=stock, exchange_code="NSE", product_type="cash")
            quote_rows = quote.get("Success") or []
            ltp = float(quote_rows[0].get("ltp") or 0) if quote_rows and isinstance(quote_rows[0], dict) else 0
            if ltp <= 0:
                raise RuntimeError(f"No valid LTP for EOD square-off {stock}; refusing to submit blind order.")
            limit = max(0.05, math.floor((ltp * discount_factor) / 0.05) * 0.05)
            order_qty = remaining_qty
            resp = broker_call(f"LIVE square-off {stock} {phase_name}", breeze.place_order,
                               stock_code=stock, exchange_code="NSE", product="cash", action="sell",
                               order_type="limit", stoploss="", quantity=str(order_qty), price=str(limit),
                               validity="day", user_remark="BREEZEAUTO_EOD")
            success = resp.get("Success") if isinstance(resp, dict) else None
            oid = success.get("order_id") if isinstance(success, dict) else None
            r["eod_squareoff_order_id"] = oid
            r["eod_squareoff_price"] = limit
            if not oid:
                # Ambiguous acceptance without an ID: never submit another sell blindly.
                r["status"] = "LIVE_EOD_SQUAREOFF_ACCEPTANCE_UNKNOWN"
                r["eod_squareoff_error"] = f"Broker response did not provide order_id: {resp!r}"
                record_decision_event({"event":"EOD_SQUAREOFF_ACCEPTANCE_UNKNOWN", "symbol":stock, "response":resp})
                return

            accounted_qty = 0
            accounted_value = 0.0
            deadline = time.monotonic() + wait_seconds
            order_terminal = False
            last_row = {}
            while time.monotonic() < deadline:
                detail = broker_call(f"LIVE square-off status {stock}", breeze.get_order_detail,
                                     exchange_code="NSE", order_id=oid)
                rows2 = detail.get("Success") or []
                row = rows2[0] if rows2 and isinstance(rows2[0], dict) else {}
                last_row = row
                status = str(row.get("status") or "").strip().lower()
                new_qty, new_value, delta_qty, delta_value, exec_time = account_cumulative(
                    row, order_qty, limit, accounted_qty, accounted_value
                )
                if delta_qty:
                    accounted_qty, accounted_value = new_qty, new_value
                    total_filled_qty += delta_qty
                    ladder_filled_qty += delta_qty
                    total_filled_value += delta_value
                    remaining_qty = max(0, total_qty - ladder_filled_qty)
                    last_exec_time = exec_time or datetime.now()
                if status in terminal:
                    order_terminal = True
                    break
                time.sleep(1.0)

            if not order_terminal:
                # Cancel remainder, then prove terminal state and capture any race fill.
                broker_call(f"Cancel unfilled square-off {oid}", breeze.cancel_order,
                            exchange_code="NSE", order_id=oid)
                cancel_deadline = time.monotonic() + 12
                while time.monotonic() < cancel_deadline:
                    detail = broker_call(f"Verify square-off cancellation {stock}", breeze.get_order_detail,
                                         exchange_code="NSE", order_id=oid)
                    rows2 = detail.get("Success") or []
                    row = rows2[0] if rows2 and isinstance(rows2[0], dict) else {}
                    last_row = row
                    status = str(row.get("status") or "").strip().lower()
                    new_qty, new_value, delta_qty, delta_value, exec_time = account_cumulative(
                        row, order_qty, limit, accounted_qty, accounted_value
                    )
                    if delta_qty:
                        accounted_qty, accounted_value = new_qty, new_value
                        total_filled_qty += delta_qty
                        ladder_filled_qty += delta_qty
                        total_filled_value += delta_value
                        remaining_qty = max(0, total_qty - ladder_filled_qty)
                        last_exec_time = exec_time or datetime.now()
                    if status in terminal:
                        order_terminal = True
                        break
                    time.sleep(1.0)
            if not order_terminal:
                r["status"] = "LIVE_EOD_SQUAREOFF_CANCEL_UNCONFIRMED"
                r["eod_squareoff_error"] = f"Order {oid} did not reach a confirmed terminal state; escalation stopped to prevent duplicate sell exposure."
                record_decision_event({"event":"EOD_SQUAREOFF_CANCEL_UNCONFIRMED", "symbol":stock, "order_id":oid, "remaining_quantity":remaining_qty, "last_status":last_row.get('status')})
                break
            if str(last_row.get('status') or '').strip().lower() == 'executed' and remaining_qty <= 0:
                break
            # A terminal cancelled/rejected/expired order is safe to escalate using
            # the newly computed residual quantity only.
        except Exception as exc:
            r["status"] = "LIVE_EOD_SQUAREOFF_ERROR_REQUIRES_RECONCILIATION"
            r["eod_squareoff_error"] = str(exc)
            record_decision_event({"event":"EOD_SQUAREOFF_ERROR_REQUIRES_RECONCILIATION", "symbol":stock, "remaining_quantity":remaining_qty, "error":repr(exc)})
            break

    if total_filled_qty > 0:
        r["sell_actual_qty"] = total_filled_qty
        r["sell_actual_price"] = round(total_filled_value / total_filled_qty, 4) if total_filled_qty else 0.0
        r["sell_trade_datetime"] = last_exec_time
        if remaining_qty <= 0:
            r["status"] = "LIVE_EOD_SQUAREOFF_FILLED"
        elif r.get("status") not in {"LIVE_EOD_SQUAREOFF_CANCEL_UNCONFIRMED", "LIVE_EOD_SQUAREOFF_ERROR_REQUIRES_RECONCILIATION"}:
            r["status"] = "LIVE_EOD_SQUAREOFF_PARTIAL"
    elif not r.get("status", "").startswith("LIVE_EOD_SQUAREOFF_"):
        r["status"] = "LIVE_EOD_SQUAREOFF_NOT_FILLED"
    print(f'LIVE EOD FINAL: {stock} total_qty={total_qty} filled={total_filled_qty} remaining={remaining_qty} status={r.get("status")}', flush=True)


def squareoff_live_app_positions(breeze, results: list[dict]) -> None:
    """At/after 15:20, cancel pending app orders and close app-owned NSE positions.

    Uses an escalating multi-phase square-off ladder to guarantee intraday closure
    and prevent unwanted overnight delivery risk or margin penalties.
    """
    print('LIVE 15:20: starting automatic square-off of app-owned positions/orders.', flush=True)
    # Cancel pending buy/stop orders that this process submitted.
    for r in results:
        for key in ('buy_order_id', 'stop_loss_order_id', 'gtt_order_id'):
            oid = r.get(key)
            if not oid:
                continue
            try:
                detail = breeze.get_order_detail(exchange_code='NSE', order_id=oid)
                rows = detail.get('Success') or [] if isinstance(detail, dict) else []
                status = str(rows[0].get('status', '')).lower() if rows else ''
                if status not in {'executed','rejected','cancelled','expired'}:
                    broker_call(f'Cancel {key} {oid}', breeze.cancel_order, exchange_code='NSE', order_id=oid)
                    cancel_deadline = time.monotonic() + 10
                    confirmed = False
                    while time.monotonic() < cancel_deadline:
                        verify = broker_call(f'Verify cancellation {key} {oid}', breeze.get_order_detail,
                                             exchange_code='NSE', order_id=oid)
                        verify_rows = verify.get('Success') or [] if isinstance(verify, dict) else []
                        verify_status = str(verify_rows[0].get('status', '')).strip().lower() if verify_rows else ''
                        if verify_status in {'executed','rejected','cancelled','expired'}:
                            confirmed = True
                            break
                        time.sleep(0.5)
                    if not confirmed:
                        r['eod_cancel_unconfirmed'] = True
                        r['status'] = 'LIVE_EOD_CANCEL_UNCONFIRMED'
                        r['eod_squareoff_error'] = f'Cancellation of {key} order {oid} was not confirmed.'
                        print(f'LIVE 15:20 BLOCKED: cancellation of {oid} not confirmed; no new SELL will be submitted.', flush=True)
                    else:
                        print(f'LIVE 15:20: cancellation terminal state={verify_status} for {key} {oid}.', flush=True)
            except Exception as exc:
                r['eod_cancel_unconfirmed'] = True
                r['status'] = 'LIVE_EOD_CANCEL_UNCONFIRMED'
                r['eod_squareoff_error'] = f'Could not verify/cancel pending order {oid}: {exc}'
                print(f'LIVE 15:20: could not verify/cancel {oid}: {exc}; automated SELL blocked.', flush=True)

    # First reconcile positions with actual broker holdings. A missing/error
    # snapshot is not proof of position state, so stop rather than issue blind sells.
    reconciliation = reconcile_broker_positions(breeze, results)
    if reconciliation.get('status') != 'RECONCILED':
        for row in results:
            if str(row.get('mode') or '').lower() == 'live' and int(row.get('buy_actual_qty') or 0) > int(row.get('sell_actual_qty') or 0):
                row['status'] = 'LIVE_EOD_RECONCILIATION_REQUIRED'
                row['broker_reconciliation_blocked'] = True
        print(f"LIVE EOD HALTED: broker position reconciliation status={reconciliation.get('status')}; manual broker-terminal verification required.", flush=True)
        return

    # Square off only symbols that have open net quantities
    for r in results:
        if r.get('broker_reconciliation_blocked') or r.get('eod_cancel_unconfirmed'):
            r['status'] = 'LIVE_EOD_RECONCILIATION_REQUIRED'
            print(f"LIVE EOD BLOCKED {r.get('symbol')}: unresolved broker reconciliation/order cancellation; reconcile in broker terminal.", flush=True)
            continue
        buy_qty = int(r.get('buy_actual_qty') or 0)
        sell_qty = int(r.get('sell_actual_qty') or 0)
        net_qty = buy_qty - sell_qty
        if net_qty <= 0:
            continue
        stock = str(r.get('breeze_symbol') or r.get('symbol') or '').strip()
        if not stock:
            continue
        try:
            _execute_ladder_squareoff(breeze, r, stock, net_qty)
        except Exception as exc:
            r['status'] = 'LIVE_EOD_SQUAREOFF_FAILED'
            r['eod_squareoff_error'] = str(exc)
            print(f'LIVE 15:20: square-off failed for {stock}: {exc}', flush=True)


@_live_audit_order_session
def run_orders(top: pd.DataFrame, mode: str, accepted_live_risk: bool) -> list[dict]:
    if mode in {'paper','live'}:
        verify_canonical_strategy_parity()
        _refresh_latest_gui_logic_settings()
    if mode == 'live' and not accepted_live_risk: raise RuntimeError('LIVE requires --i-understand-live.')
    if mode == 'live':
        safety=live_safety_gate(top)
        top=safety['top']
        print("LIVE SAFETY GATE PASSED: explicit_live_enable=True; GUI per-logic Maximum Orders controls BUY count.")
    # BUY order counts are controlled exclusively by each logic's GUI Maximum Orders.
    if top is None or top.empty:
        return []
    # Candidates are evaluated in order; only accepted BUYs count against that logic's GUI cap.
    cfgs=load_logics(); enabled_cfg=dict(enabled_logics(cfgs))
    print(f'{mode.upper()} ORDER LIMIT: GUI per-logic Maximum Orders is the only BUY count limit; no independent global order-count cap.')
    # IMPORTANT: Do not gate LIVE here. live_preflight() performs the market-day/time gate LAST,
    # after authentication, funds, margin, security-master, quotes and preview_order diagnostics.
    # This ensures the full LIVE preflight can be tested on holidays/weekends while the final
    # safety gate still guarantees that no real order can be sent on a non-trading day.
    breeze=require_breeze() if mode == 'live' else None
    if mode == 'live':
        preflight=live_preflight(breeze,top); valid_map={x['nse_symbol']:x for x in preflight.get('validated_symbols',[])}; top=top[top['Symbol'].astype(str).isin(valid_map)].copy()
        if top.empty: raise RuntimeError('LIVE PRECHECK FAILED: no eligible candidate passed Breeze validation. NO LIVE ORDER WAS SENT.')
        top['Breeze Symbol']=top['Symbol'].astype(str).map(lambda x:valid_map[x]['breeze_symbol'])
        print('LIVE ACTUAL ORDER CAP: no independent global order-count cap; GUI per-logic Maximum Orders will be enforced during BUY submission.')
    breeze=require_breeze() if mode == 'paper' else breeze
    results=[]
    _submitted_by_logic={}
    for _,r in top.iterrows():
        if r['Quantity']<1: continue
        _logic_name=str(r.get('Logic') or '')
        _logic_cfg=cfgs.get(_logic_name,{})
        _logic_limit=max_orders_for_logic(_logic_cfg)
        if _logic_limit>0 and _submitted_by_logic.get(_logic_name,0)>=_logic_limit:
            results.append({'symbol':r['Symbol'],'logic':_logic_name,'mode':mode,'status':'GUI_LOGIC_MAX_ORDERS_REACHED','gui_max_orders':_logic_limit})
            continue
        order={'order_id':f'{mode.upper()}-{datetime.now():%Y%m%d%H%M%S}-{r["Symbol"]}','symbol':r['Symbol'],'breeze_symbol':str(r.get('Breeze Symbol') or r['Symbol']).strip(),'quantity':int(r['Quantity']),'limit_price':float(r['Limit price']),'mode':mode,'logic':r.get('Logic'),'logic_config':_logic_cfg.copy()}
        record_decision_event({'event':'ORDER_DECISION','mode':mode,'symbol':order['symbol'],'breeze_symbol':order['breeze_symbol'],'quantity':order['quantity'],'decision_price':order['limit_price'],'logic':r.get('Logic'),'rank':r.get('Ranking',r.get('rank')),'decision_time':r.get('Buy Date & Time',r.get('BuyTime')),'decision':'BUY_CANDIDATE'})
        if mode=='paper':
            order['stop_loss']=stop_price(order['limit_price'])
            order['stop_loss_order_id']=f"PAPER-SL-{order['order_id']}"
            order['stop_loss_status']='PAPER_PROTECTIVE_SL_ACTIVE'
            order['status']='PAPER_OPEN_STOPLOSS_TRACKED'
            _submitted_by_logic[_logic_name]=_submitted_by_logic.get(_logic_name,0)+1
        else:
            try:
                lg=str(r.get('Logic') or '')
                auth=canonical_live_buy_authorization(
                    symbol=order['symbol'],logic=lg,price=order['limit_price'],
                    open_price=r.get('Open',r.get('open',0)),
                    previous_close=r.get('Previous close',r.get('previous_close',0)),
                    volume=r.get('Volume',r.get('volume',0)),
                    turnover_cr=r.get('Turnover Cr',r.get('Turnover in Cr',r.get('turnover_cr',0))),
                    cfg=order['logic_config'],rank=r.get('Ranking',r.get('rank')),
                    decision_time=r.get('Buy Date & Time',r.get('BuyTime')))
                auth['mapped_nse_symbol']=order['symbol']
                auth['broker_symbol']=order['breeze_symbol']
                order['decision_hash']=auth['decision_hash']
                _live_audit('LIVE_BUY_ATTEMPT',symbol=order['symbol'],breeze_symbol=order['breeze_symbol'],quantity=order['quantity'],price=order['limit_price'],decision_hash=auth['decision_hash'])
                _buy_lock = _live_buy_submission_lock(r.get('Logic') or 'LIVE', order['symbol'])

                if not _buy_lock:

                    order['status']='LIVE_DUPLICATE_BLOCKED'; order['broker_error']='Daily BUY submission lock already exists for this logic/company.'; results.append(order); _live_audit('LIVE_DUPLICATE_BLOCKED',symbol=order['symbol'],logic=r.get('Logic')); continue

                response=_submit_authorized_live_buy(breeze,authorization=auth,stock_code=order['breeze_symbol'],quantity_value=order['quantity'],price=order['limit_price'],user_remark='BREEZEAUTO')
            except Exception as exc:
                if _explicit_order_rejection(exc): _release_live_buy_submission_lock(_buy_lock)
                order['status']='LIVE_ORDER_ERROR_NO_RETRY'; order['broker_error']=str(exc); results.append(order); _live_audit('LIVE_ORDER_BLOCKED_AFTER_ERROR',symbol=order['symbol'],error=str(exc)); print('LIVE ORDER CIRCUIT BREAKER: first BUY call failed; NO retry and NO further live BUY orders will be attempted.'); break
            if response.get('Status')!=200 or not isinstance(response.get('Success'),dict) or not response.get('Success',{}).get('order_id'):
                _release_live_buy_submission_lock(_buy_lock)
                order['status']='BUY_REJECTED_NO_RETRY'; order['broker_response']=_safe_audit_value(response); results.append(order); _live_audit('LIVE_ORDER_REJECTED_NO_RETRY',symbol=order['symbol'],response=response); print('LIVE ORDER CIRCUIT BREAKER: Breeze rejected the BUY; NO retry and NO further live BUY orders will be attempted.'); break
            order['buy_order_id']=response['Success']['order_id']; order['status']='BUY_ORDERED_WAITING_FOR_FILL'; order['broker_response']=response; _live_audit('LIVE_BUY_ACCEPTED',symbol=order['symbol'],order_id=order['buy_order_id'],response=response)
            _submitted_by_logic[_logic_name]=_submitted_by_logic.get(_logic_name,0)+1
            record_decision_event({'event':'LIVE_ORDER_ACCEPTED','mode':'live','symbol':order['symbol'],'order_id':order['buy_order_id'],'price':order['limit_price'],'quantity':order['quantity']})
        results.append(order)
    if mode=='live': monitor_and_attach_stoplosses(breeze,results); start_live_trailing_monitors(breeze,results); enrich_live_results_from_breeze(breeze,results)
    else: enrich_paper_results_from_breeze(breeze,results)
    return results

def _iso_utc_for_day(d):
    return f"{d:%Y-%m-%d}T07:00:00.000Z"

def _trade_detail_safe(breeze, order_id):
    if not order_id:
        return {}
    try:
        resp = broker_call('Trade detail', breeze.get_trade_detail, exchange_code='NSE', order_id=order_id)
        rows = resp.get('Success') or []
        return rows[0] if rows else {}
    except Exception:
        return {}

def refresh_breeze_actual_charges(result: dict, breeze) -> dict:
    """Refresh BUY/SELL charge details directly from Breeze before Excel is written.

    This is deliberately separate from preview_order: executed get_trade_detail
    is authoritative for a real LIVE order.  It also handles the common case
    where the final Excel write happens after the order state was already saved.
    """
    if not isinstance(result, dict) or breeze is None:
        return result or {}
    buy_oid = result.get('buy_order_id')
    sell_oid = result.get('sell_order_id') or result.get('stop_loss_order_id')
    if buy_oid:
        d = _trade_detail_safe(breeze, buy_oid)
        if d:
            result['buy_trade'] = d
    if sell_oid:
        d = _trade_detail_safe(breeze, sell_oid)
        if d:
            result['sell_trade'] = d
    if result.get('buy_trade') or result.get('sell_trade'):
        apply_icici_charges(result, exchange='NSE', breeze=breeze)
        result['charge_source'] = 'Breeze executed trade detail'
        result['charge_is_breeze_actual'] = True
        result['data_source'] = (str(result.get('data_source') or '').rstrip(' ;')
                                 + ' ; Breeze get_trade_detail charges').strip(' ;')
    return result

def _preview_charges_safe(breeze, stock_code, action, qty, price, product=None):
    """Return Breeze broker-generated charge preview.

    Breeze's preview_order returns the individual statutory components and the
    broker total. Use the same product as the real order path (cash for the
    equity intraday engine). Do not fall back to margin/MTF because that can
    change the charge class and overstate intraday charges. No order is placed.
    """
    if not stock_code or not qty or not price:
        return {}
    products=[]
    p=str(product or 'cash').strip().lower()
    if p:
        products.append(p)
    last_error=None
    for prod in products:
        try:
            resp = broker_call('Preview order', breeze.preview_order,
                               stock_code=stock_code, exchange_code='NSE', product=prod,
                               order_type='limit', price=str(float(price)), action=action,
                               quantity=str(int(qty)), specialflag='N')
            success=resp.get('Success') if isinstance(resp,dict) else None
            if isinstance(success,dict) and success:
                success=dict(success)
                success['_preview_product']=prod
                return success
            last_error=resp
        except Exception as exc:
            last_error=exc
    return {}


def download_breeze_actual_trade_charges(breeze, from_date, to_date, output_csv=None):
    """Download executed NSE trades and their Breeze trade-detail charges.

    This is the authoritative source for trades that were actually executed in
    the user's Breeze account.  The API's trade-list response gives order/trade
    identifiers; get_trade_detail supplies execution price, brokerage, taxes and
    total_transaction_cost.  No simulated/historical trade is represented as an
    actual charge here.
    """
    import csv as _csv
    rows=[]
    def _iso(d):
        txt=str(d)
        if 'T' in txt: return txt if txt.endswith('Z') else txt+'Z'
        return txt+'T00:00:00.000Z'
    resp=broker_call('Breeze trade list', breeze.get_trade_list,
                     from_date=_iso(from_date), to_date=_iso(to_date),
                     exchange_code='NSE', product_type='', action='', stock_code='')
    trades=resp.get('Success') or [] if isinstance(resp,dict) else []
    for t in trades:
        oid=t.get('order_id')
        details=[]
        if oid:
            try:
                dresp=broker_call('Breeze trade detail', breeze.get_trade_detail, exchange_code='NSE', order_id=oid)
                details=dresp.get('Success') or [] if isinstance(dresp,dict) else []
            except Exception as exc:
                details=[]
        if not details:
            details=[{}]
        for d in details:
            rows.append({
                'Trade Date':t.get('trade_date'),'Symbol':t.get('stock_code'),'Action':t.get('action'),
                'Quantity':d.get('executed_quantity',t.get('quantity')),'Average Price':d.get('execution_price',t.get('average_cost')),
                'Order ID':oid,'Exchange Trade ID':d.get('exchange_trade_id'),'Trade ID':d.get('trade_id'),
                'Exchange Trade Time':d.get('exchange_trade_time'),'Brokerage':d.get('brokerage_amount',t.get('brokerage_amount')),
                'Taxes':d.get('taxes',t.get('total_taxes')),'Total Transaction Cost':d.get('total_transaction_cost'),
                'EATM Withheld':d.get('eatm_withheld_amount',t.get('eatm_withheld_amount')),'Cash Withheld':d.get('cash_withheld_amount',t.get('cash_withheld_amount')),
                'Source':'Breeze get_trade_detail'
            })
    if output_csv:
        os.makedirs(os.path.dirname(output_csv) or '.', exist_ok=True)
        fields=list(rows[0].keys()) if rows else ['Trade Date','Symbol','Action','Quantity','Average Price','Order ID','Exchange Trade ID','Trade ID','Exchange Trade Time','Brokerage','Taxes','Total Transaction Cost','EATM Withheld','Cash Withheld','Source']
        with open(output_csv,'w',newline='',encoding='utf-8-sig') as fh:
            w=_csv.DictWriter(fh,fieldnames=fields); w.writeheader(); w.writerows(rows)
    return rows

def _parse_breeze_datetime(value):
    """Parse Breeze exchange timestamps into naive local (IST-style) datetimes."""
    if not value:
        return None
    text = str(value).strip()
    for fmt in ('%d-%b-%Y %H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S'):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(text.replace('Z', '+00:00')).replace(tzinfo=None)
    except Exception:
        return None


def _utc_iso_for_local(dt):
    """Serialize the NSE exchange clock for Breeze historical V2.

    Breeze's official examples pass NSE clock times such as 09:20:00 with the
    required .000Z suffix and the response returns the same exchange time.
    Do not subtract 5:30 here; doing so shifts the request outside the intended
    session and can cause empty/partial historical responses.
    """
    if not isinstance(dt, datetime):
        return None
    return dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')


def _historical_v2(breeze, stock_code, interval, from_dt, to_dt, persist_request_cache=True, force_refresh=False):
    _refresh_latest_gui_logic_settings()
    _guard = globals().get('_DATEWISE_HISTORICAL_INTERVAL_GUARD')
    if _guard == '1minute' and str(interval or '').strip().lower() == '1second':
        # 1-second refinement is not part of a 1-minute-only date-wise download.
        # Do not call Breeze or create 1-second request/cache files in this mode.
        return []
    """Fetch Breeze Historical V2 data, using a persistent request cache first.

    Existing downloads are NEVER requested from Breeze again. Failed/empty requests
    are not cached, so a temporary API/network failure can be retried later.
    """
    if not stock_code or not from_dt or not to_dt or to_dt < from_dt:
        return []
    # Exact API chunks are memory-only now. Persistent historical data lives in
    # the single canonical DATE+COMPANY+SYMBOL CSV, preventing hundreds of
    # small chunk files from accumulating in each trading-day folder.
    cached = None if force_refresh else _load_cached_historical_request(stock_code, interval, from_dt, to_dt)
    if cached is not None:
        _historical_verbose_print(f'Historical cache: using saved {interval} data for {stock_code} {from_dt:%Y-%m-%d %H:%M:%S} -> {to_dt:%H:%M:%S}.')
        return cached
    # CACHE-FIRST RULE: do not require/authenticate Breeze merely to read history.
    # The caller can retry this exact request after creating a session.
    if breeze is None:
        raise HistoricalBreezeRequired(
            f'Breeze session required only because cached {interval} data is missing for '
            f'{stock_code} {from_dt:%Y-%m-%d %H:%M:%S}->{to_dt:%H:%M:%S}.'
        )
    _api_t0 = time.perf_counter()
    try:
        # Apply the shared Breeze request pacer to EVERY interval. Full-session
        # 1-second downloads require many 1,000-candle chunks; skipping the pacer
        # on 1-second calls can trigger API throttling and leave minute-only files.
        _historical_api_rate_wait()
        resp = broker_call(
            f'Breeze historical V2 {interval}', breeze.get_historical_data_v2,
            interval=interval,
            from_date=_utc_iso_for_local(from_dt),
            to_date=_utc_iso_for_local(to_dt),
            stock_code=stock_code, exchange_code='NSE', product_type='cash')
        _historical_perf_add('api_calls')
        _historical_perf_add('api_seconds', time.perf_counter() - _api_t0)
        data = resp.get('Success') or []
        # A 200/empty 1-minute response can be a transient Breeze history gap.
        # Retry exactly once (with the shared REST pacer) before declaring the
        # minute unavailable. This does not synthesize candles or relax ORB's
        # strict 15-real-minute completeness check. Other intervals are not
        # retried here to avoid multiplying broad historical queue traffic.
        _empty_retry_count = 0
        if (not data and str(interval).strip().lower() == '1minute'
                and str(resp.get('Status')) == '200'
                and not (resp.get('Error') or resp.get('Message') or resp.get('message'))):
            _empty_retry_count = 1
            print(f'HISTORICAL EMPTY RETRY: symbol={str(stock_code).upper()} interval=1minute '
                  f'attempt=2/2; reason=HTTP-200-empty; no candle will be fabricated.', flush=True)
            _historical_api_rate_wait()
            _retry_t0 = time.perf_counter()
            resp = broker_call(
                f'Breeze historical V2 {interval} empty-response retry', breeze.get_historical_data_v2,
                interval=interval,
                from_date=_utc_iso_for_local(from_dt),
                to_date=_utc_iso_for_local(to_dt),
                stock_code=stock_code, exchange_code='NSE', product_type='cash')
            _historical_perf_add('api_calls')
            _historical_perf_add('api_seconds', time.perf_counter() - _retry_t0)
            data = resp.get('Success') or []
        if not data:
            # Do not silently collapse an empty API response into an ordinary
            # missing-candle result. This diagnostic is essential for separating
            # invalid stock_code/interval/date parameters from a genuinely empty
            # exchange response. Never print credentials or session headers.
            _status = resp.get('Status')
            _error = resp.get('Error')
            _message = resp.get('Message') or resp.get('message')
            _keys = ','.join(sorted(str(k) for k in resp.keys()))
            print(
                f'HISTORICAL EMPTY RESPONSE: symbol={str(stock_code).upper()} '
                f'interval={interval} from={_utc_iso_for_local(from_dt)} '
                f'to={_utc_iso_for_local(to_dt)} status={_status!r} '
                f'error={str(_error or _message or "")[:240]!r} '
                f'response_keys={_keys}; Success_rows=0; empty_retries={_empty_retry_count}', flush=True)
        if data and persist_request_cache:
            _save_cached_historical_request(stock_code, interval, from_dt, to_dt, data)
            _historical_verbose_print(f'Historical cache: saved {interval} data for {stock_code} {from_dt:%Y-%m-%d %H:%M:%S} -> {to_dt:%H:%M:%S}.')
        elif data and str(interval).lower() == '1minute':
            # The caller immediately writes this complete single-request session
            # to the canonical daily CSV. A second temporary CSV write is redundant.
            _historical_verbose_print(f'Historical cache: skipping redundant 1-minute request checkpoint for {stock_code}; canonical daily save follows.')
        return data
    except Exception as exc:
        _historical_perf_add('api_calls')
        _historical_perf_add('api_seconds', time.perf_counter() - _api_t0)
        _historical_perf_add('api_failures')
        print(f'Historical download FAILED: {stock_code} {interval} {from_dt:%Y-%m-%d} - {exc}')
        return []


def _refine_extreme_with_1second(breeze, stock_code, row, field, day_start, day_end):
    """Refine a 1-minute extreme with 1-second data for the exact minute.

    Breeze Historical V2 documents 1-second support. If 1-second data is unavailable,
    the caller keeps the reliable 1-minute value/timestamp instead of inventing data.
    """
    if row is None:
        return None, None
    minute_dt = _parse_breeze_datetime(row.get('datetime'))
    if minute_dt is None:
        return None, None
    sec_from = max(day_start, minute_dt)
    sec_to = min(day_end, minute_dt + timedelta(seconds=59))
    data = _historical_v2(breeze, stock_code, '1second', sec_from, sec_to)
    if not data:
        return None, None
    df = pd.DataFrame(data)
    if field not in df.columns:
        return None, None
    df[field] = pd.to_numeric(df[field], errors='coerce')
    df = df.dropna(subset=[field])
    if df.empty:
        return None, None
    idx = df[field].idxmax() if field == 'high' else df[field].idxmin()
    chosen = df.loc[idx]
    return float(chosen[field]), str(chosen.get('datetime'))


def _historical_extremes_safe(breeze, stock_code, day, start_time=None, end_time=None):
    """Get post-event high/low using Breeze V2 1-minute data, refined to 1-second.

    start_time/end_time are Breeze exchange timestamps (local exchange time). The
    complete trading day is fetched at 1-minute resolution and then filtered strictly
    to the requested window. The minute containing each extreme is then re-queried at
    1-second resolution so the dashboard gets the most precise available timestamp.
    """
    if not stock_code or not day:
        return {}
    try:
        day_start = datetime.combine(day, datetime.min.time()).replace(hour=9, minute=15)
        day_end = datetime.combine(day, datetime.min.time()).replace(hour=15, minute=30, second=59)
        start_dt = _parse_breeze_datetime(start_time) if start_time else day_start
        end_dt = _parse_breeze_datetime(end_time) if end_time else day_end
        if start_dt is None:
            start_dt = day_start
        if end_dt is None:
            end_dt = day_end
        start_dt = max(day_start, start_dt)
        end_dt = min(day_end, end_dt)
        if end_dt < start_dt:
            return {}

        data = _historical_v2(breeze, stock_code, '1minute', day_start, day_end)
        if not data:
            return {}
        df = pd.DataFrame(data)
        for c in ('high', 'low'):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce')
        if 'datetime' not in df.columns:
            return {}
        df['_dt'] = df['datetime'].map(_parse_breeze_datetime)
        df = df[df['_dt'].notna()]
        df = df[(df['_dt'] >= start_dt) & (df['_dt'] <= end_dt)]
        if df.empty:
            return {}

        hi = df.loc[df['high'].idxmax()] if df['high'].notna().any() else None
        lo = df.loc[df['low'].idxmin()] if df['low'].notna().any() else None
        hi_value = float(hi['high']) if hi is not None else None
        lo_value = float(lo['low']) if lo is not None else None
        hi_dt = str(hi.get('datetime')) if hi is not None else None
        lo_dt = str(lo.get('datetime')) if lo is not None else None

        # Refine the extreme minute to 1-second. The exact 1-second API is optional;
        # if Breeze returns no seconds, the 1-minute result remains the source of truth.
        if hi is not None:
            v, t = _refine_extreme_with_1second(breeze, stock_code, hi, 'high', start_dt, end_dt)
            if v is not None:
                hi_value, hi_dt = v, t
        if lo is not None:
            same_minute = (hi is not None and str(hi.get('datetime')) == str(lo.get('datetime')))
            if not same_minute:
                v, t = _refine_extreme_with_1second(breeze, stock_code, lo, 'low', start_dt, end_dt)
                if v is not None:
                    lo_value, lo_dt = v, t
            else:
                # One second query can contain both high and low for the same minute.
                minute_dt = _parse_breeze_datetime(lo.get('datetime'))
                sec_from = max(start_dt, minute_dt)
                sec_to = min(end_dt, minute_dt + timedelta(seconds=59))
                sec_data = _historical_v2(breeze, stock_code, '1second', sec_from, sec_to)
                if sec_data:
                    sdf = pd.DataFrame(sec_data)
                    for c in ('high', 'low'):
                        if c in sdf.columns:
                            sdf[c] = pd.to_numeric(sdf[c], errors='coerce')
                    if 'high' in sdf and sdf['high'].notna().any():
                        x = sdf.loc[sdf['high'].idxmax()]
                        hi_value, hi_dt = float(x['high']), str(x.get('datetime'))
                    if 'low' in sdf and sdf['low'].notna().any():
                        x = sdf.loc[sdf['low'].idxmin()]
                        lo_value, lo_dt = float(x['low']), str(x.get('datetime'))

        return {'high': hi_value, 'low': lo_value, 'high_dt': hi_dt, 'low_dt': lo_dt,
                'source': 'Breeze Historical V2 1minute; simulated charges'}
    except Exception:
        return {}

def enrich_paper_results_from_breeze(breeze, results):
    """Populate PAPER trades with the same Breeze market timestamp/data pipeline as LIVE.

    No order is sent to Breeze. The simulated buy is timestamped at the moment the
    paper run is created, uses the current Breeze NSE quote as the simulated fill,
    and then uses Breeze Historical V2 1-minute data with 1-second refinement for
    post-buy high/low. Missing broker data is left blank.
    """
    for r in results:
        if r.get('mode') != 'paper':
            continue
        nse_symbol = str(r.get('symbol') or '').strip()
        if not nse_symbol:
            continue
        try:
            names = breeze.get_names(exchange_code='NSE', stock_code=nse_symbol)
            breeze_symbol = str((names or {}).get('isec_stock_code') or '').strip()
            if not breeze_symbol:
                r['status'] = 'PAPER_BREEZE_SYMBOL_NOT_FOUND'
                continue
            r['breeze_symbol'] = breeze_symbol
            quote = broker_call(f'Paper result quote {nse_symbol}', breeze.get_quotes, stock_code=breeze_symbol, exchange_code='NSE', product_type='cash')
            rows = quote.get('Success') or [] if isinstance(quote, dict) else []
            q = rows[0] if rows and isinstance(rows[0], dict) else {}
            ltp = q.get('ltp')
            if ltp in (None, ''):
                r['status'] = 'PAPER_BREEZE_QUOTE_NOT_AVAILABLE'
                continue
            # This is a simulated paper fill timestamp, not a broker execution.
            sim_dt = datetime.now()
            r['buy_actual_qty'] = int(r.get('quantity') or 0)
            r['buy_actual_price'] = float(ltp)
            r['buy_trade_time'] = sim_dt.strftime('%d-%b-%Y %H:%M:%S')
            r['buy_trade_datetime'] = sim_dt
            r['buy_charges'] = icici_intraday_charges(r['buy_actual_qty'] * r['buy_actual_price'], 0.0, exchange='NSE')
            r['buy_extremes'] = _historical_extremes_safe(
                breeze, breeze_symbol, sim_dt.date(), sim_dt, None)
            r['status'] = 'PAPER_BREEZE_MARKET_DATA_CAPTURED'
        except Exception as exc:
            r['status'] = 'PAPER_BREEZE_DATA_ERROR'
            r['paper_breeze_error'] = str(exc)
    return results


def enrich_live_results_from_breeze(breeze, results):
    """Populate actual Breeze execution data and charge estimates for dashboard use."""
    for r in results:
        if r.get('mode') != 'live':
            continue
        buy_id=r.get('buy_order_id')
        buy_trade=_trade_detail_safe(breeze,buy_id)
        r['buy_trade']=buy_trade
        stop_id=r.get('stop_loss_order_id')
        sell_trade=_trade_detail_safe(breeze, r.get('sell_order_id') or stop_id)
        r['sell_trade']=sell_trade
        stock=r.get('breeze_symbol') or r.get('symbol')
        qty=int(buy_trade.get('executed_quantity') or r.get('quantity') or 0)
        buy_price=float(buy_trade.get('execution_price') or r.get('limit_price') or 0)
        r['buy_actual_qty']=qty
        r['buy_actual_price']=buy_price
        r['buy_trade_time']=buy_trade.get('exchange_trade_time')
        buy_dt = _parse_breeze_datetime(r['buy_trade_time'])
        r['buy_trade_datetime'] = buy_dt
        if sell_trade:
            r['sell_actual_qty']=int(sell_trade.get('executed_quantity') or 0)
            r['sell_actual_price']=float(sell_trade.get('execution_price') or 0)
            r['sell_trade_time']=sell_trade.get('exchange_trade_time')
            r['sell_trade_datetime'] = _parse_breeze_datetime(r['sell_trade_time'])
        # The dashboard uses the same ICICI charge engine as Historical/Paper.
        # Broker preview/trade-detail values remain available in buy_trade/sell_trade
        # for audit, but do not create a separate P&L calculation path.
        apply_icici_charges(r, exchange='NSE', breeze=breeze)
        if buy_trade and buy_dt:
            sell_dt = r.get('sell_trade_datetime')
            r['buy_extremes'] = _historical_extremes_safe(
                breeze, stock, buy_dt.date(), buy_dt, sell_dt if sell_dt and sell_dt.date() == buy_dt.date() else None)
        if sell_trade and r.get('sell_trade_datetime'):
            sd = r['sell_trade_datetime']
            # 'High/Low After Sell' means the period after the actual sell execution
            # through the end of that NSE trading session.
            r['sell_extremes'] = _historical_extremes_safe(breeze, stock, sd.date(), sd, None)
    return results


PAPER_LIVE_HEADERS = [
    'Trade ID','Mode','Date','Ranking','Logic','Symbol','Status','Quantity',
    'Buy Price','Buy Date & Time','Sell Quantity','Sell Price','Sell Date & Time',
    'High After Buy','High After Buy Date & Time','Low After Buy','Low After Buy Date & Time',
    'Exit Reason','Gross P&L','Buy Turnover','Sell Turnover','Brokerage','STT',
    'Exchange Charges','GST','SEBI Charges','Stamp Duty','Other Charges','Total Charges',
    'Net P&L','P&L %','Charge Source','Charge Plan','Buy Order ID','Sell Order ID',
    'Data Source','Last Update','Trailing Active','Trailing Activation Date & Time','Highest Price','Trailing Stop','Trailing Stop Date & Time'
]

def _ensure_paper_live_sheet(wb):
    name='Paper Live Trading Shares'
    if name not in wb.sheetnames:
        ws=wb.create_sheet(name)
        ws.append(PAPER_LIVE_HEADERS)
    else:
        ws=wb[name]
        if ws.max_row == 0:
            ws.append(PAPER_LIVE_HEADERS)
        elif [ws.cell(1,c).value for c in range(1,ws.max_column+1)] != PAPER_LIVE_HEADERS:
            old=[]
            if ws.max_row>1:
                old=[[ws.cell(r,c).value for c in range(1,min(ws.max_column,len(PAPER_LIVE_HEADERS))+1)] for r in range(2,ws.max_row+1)]
            ws.delete_rows(1,ws.max_row); ws.delete_cols(1,ws.max_column); ws.append(PAPER_LIVE_HEADERS)
            for row in old: ws.append(row+[None]*max(0,len(PAPER_LIVE_HEADERS)-len(row)))
    return ws

def _export_trade_performance_report(ws, header_index):
    """Refresh a separate, read-only performance export from the canonical trade ledger.

    This export is deliberately downstream of the trade ledger: it never changes
    entry/exit decisions, strategy configuration, or broker order handling.
    """
    import csv
    from collections import defaultdict
    from openpyxl import Workbook as _ReportWorkbook
    from openpyxl.styles import Font as _Font, PatternFill as _Fill, Alignment as _Alignment
    from openpyxl.utils import get_column_letter as _col_letter

    report_dir = Path(r'D:\90\Data\Performance_Reports')
    report_dir.mkdir(parents=True, exist_ok=True)
    csv_path = report_dir / 'Trade_Performance_Ledger.csv'
    xlsx_path = report_dir / 'Trade_Performance_Report.xlsx'

    headers = [str(ws.cell(1, c).value or '') for c in range(1, ws.max_column + 1)]
    rows = []
    for rr in range(2, ws.max_row + 1):
        values = [ws.cell(rr, c).value for c in range(1, ws.max_column + 1)]
        if not any(v not in (None, '') for v in values):
            continue
        row = dict(zip(headers, values))
        if str(row.get('Mode') or '').upper() not in {'PAPER', 'LIVE'}:
            continue
        rows.append(row)

    def number(value):
        try:
            if value in (None, ''): return None
            return float(str(value).replace(',', '').replace('₹', '').strip())
        except Exception:
            return None

    def date_key(value):
        if isinstance(value, (datetime, date)): return value.strftime('%Y-%m-%d')
        return str(value or '')[:10]

    closed = []
    for row in rows:
        qty = number(row.get('Quantity')) or 0.0
        sell_qty = number(row.get('Sell Quantity')) or 0.0
        sell_price = number(row.get('Sell Price'))
        is_closed = qty > 0 and sell_qty >= qty and sell_price is not None
        row['_Closed'] = is_closed
        gross = number(row.get('Gross P&L'))
        charges = number(row.get('Total Charges')) or 0.0
        net = number(row.get('Net P&L'))
        if net is None and gross is not None:
            net = gross - charges
        row['_Net P&L'] = net
        if is_closed:
            closed.append(row)

    def metrics(items):
        vals = [r['_Net P&L'] for r in items if r.get('_Net P&L') is not None]
        wins = [v for v in vals if v > 0]
        losses = [v for v in vals if v < 0]
        return [len(vals), len(wins), len(losses), (len(wins) / len(vals)) if vals else 0.0,
                sum(wins), sum(losses), sum(vals), (sum(vals) / len(vals)) if vals else 0.0]

    wb = _ReportWorkbook()
    summary = wb.active
    summary.title = 'Summary'
    summary.append(['Mode', 'Closed Trades', 'Winning Trades', 'Losing Trades', 'Win Rate', 'Winning Net P&L', 'Losing Net P&L', 'Net P&L', 'Average Net / Trade'])
    for mode in ('PAPER', 'LIVE'):
        group = [r for r in closed if str(r.get('Mode') or '').upper() == mode]
        m = metrics(group)
        summary.append([mode, m[0], m[1], m[2], m[3], m[4], m[5], m[6], m[7]])
    summary.append([])
    summary.append(['Note', 'Only fully closed trades are counted in Win Rate. Open/partial positions remain in Trades and are not treated as wins or losses.'])

    trades_ws = wb.create_sheet('Trades')
    export_headers = headers + ['Calculated Closed Trade', 'Calculated Net P&L']
    trades_ws.append(export_headers)
    for row in rows:
        trades_ws.append([row.get(h) for h in headers] + [row['_Closed'], row['_Net P&L']])

    for sheet_name, group_key in (('By Symbol', 'Symbol'), ('By Logic', 'Logic'), ('By Date', 'Date')):
        out = wb.create_sheet(sheet_name)
        out.append(['Mode', group_key, 'Closed Trades', 'Winning Trades', 'Losing Trades', 'Win Rate', 'Winning Net P&L', 'Losing Net P&L', 'Net P&L', 'Average Net / Trade'])
        grouped = defaultdict(list)
        for row in closed:
            key = date_key(row.get(group_key)) if group_key == 'Date' else str(row.get(group_key) or 'Unknown')
            grouped[(str(row.get('Mode') or '').upper(), key)].append(row)
        for (mode, key), items in sorted(grouped.items()):
            m = metrics(items)
            out.append([mode, key, *m])

    for sheet in wb.worksheets:
        sheet.freeze_panes = 'A2'
        sheet.auto_filter.ref = sheet.dimensions if sheet.max_row > 1 else 'A1'
        for cell in sheet[1]:
            cell.font = _Font(bold=True, color='FFFFFF')
            cell.fill = _Fill('solid', fgColor='17365D')
            cell.alignment = _Alignment(wrap_text=True, vertical='center')
        for col in range(1, sheet.max_column + 1):
            values = [str(sheet.cell(r, col).value or '') for r in range(1, min(sheet.max_row, 200) + 1)]
            width = min(34, max(12, max((len(v) for v in values), default=10) + 2))
            sheet.column_dimensions[_col_letter(col)].width = width
        for row_cells in sheet.iter_rows(min_row=2):
            for cell in row_cells:
                if isinstance(cell.value, (int, float)) and not isinstance(cell.value, bool):
                    cell.number_format = '#,##0.00;[Red]-#,##0.00'

    tmp_xlsx = xlsx_path.with_suffix('.tmp.xlsx')
    wb.save(tmp_xlsx)
    os.replace(tmp_xlsx, xlsx_path)

    tmp_csv = csv_path.with_suffix('.tmp.csv')
    with tmp_csv.open('w', newline='', encoding='utf-8-sig') as fh:
        writer = csv.DictWriter(fh, fieldnames=headers + ['Calculated Closed Trade', 'Calculated Net P&L'], extrasaction='ignore')
        writer.writeheader()
        for row in rows:
            out = {h: row.get(h) for h in headers}
            out['Calculated Closed Trade'] = row['_Closed']
            out['Calculated Net P&L'] = row['_Net P&L']
            writer.writerow({k: (v.isoformat(sep=' ') if isinstance(v, datetime) else v.isoformat() if isinstance(v, date) else v) for k, v in out.items()})
    os.replace(tmp_csv, csv_path)

def record_paper_live_trades(results: list[dict], workbook_path: Path | None = None) -> None:
    """Persist every Paper/Live trade state into a dedicated Excel ledger.

    This is called by the actual GUI market engines after BUY, fill, SELL and
    final square-off events. It never replaces historical sheets.
    """
    if not results: return
    target=Path(workbook_path) if workbook_path else WORKBOOK
    wb=load_workbook(target)
    ws=_ensure_paper_live_sheet(wb)
    from openpyxl.utils import get_column_letter
    idx={str(ws.cell(1,c).value):c for c in range(1,ws.max_column+1)}
    existing={}
    for rr in range(2,ws.max_row+1):
        tid=str(ws.cell(rr,1).value or '')
        if tid: existing[tid]=rr
    for r in results:
        mode=str(r.get('mode') or '').lower()
        if mode not in {'paper','paper_live','live'}: continue
        sym=str(r.get('symbol') or '').strip().upper()
        if not sym: continue
        td=str(r.get('trade_date') or datetime.now().date())[:10]
        logic=str(r.get('logic') or '')
        oid=str(r.get('trade_id') or r.get('buy_order_id') or '')
        tid=oid or f'{mode.upper()}|{td}|{logic}|{sym}'
        buy_dt=r.get('buy_trade_datetime') or _parse_breeze_datetime(r.get('buy_trade_time'))
        sell_dt=r.get('sell_trade_datetime') or _parse_breeze_datetime(r.get('sell_trade_time'))
        qty=int(r.get('buy_actual_qty') or r.get('quantity') or 0)
        sqty=int(r.get('sell_actual_qty') or 0)
        bp=float(r.get('buy_actual_price') or r.get('limit_price') or 0) if (r.get('buy_actual_price') or r.get('limit_price')) else None
        sp=float(r.get('sell_actual_price')) if r.get('sell_actual_price') not in (None,'') else None
        bv=qty*bp if bp is not None and qty else None
        sv=sqty*sp if sp is not None and sqty else None
        gross=(sv-bv) if bv is not None and sv is not None else r.get('gross_pnl')
        charges=r.get('total_charges')
        if charges is None: charges=r.get('simulated_total_charges')
        if charges is None and gross is not None and bv is not None and sv is not None:
            charges=icici_intraday_charges(bv,sv,exchange='NSE').get('total_charges',0)
        net=(gross-float(charges or 0)) if gross is not None and charges is not None else r.get('net_pnl')
        pct=(net/bv) if net is not None and bv else r.get('pnl_pct')
        be=r.get('buy_extremes') or {}
        values={
            'Trade ID':tid,'Mode':('PAPER' if mode in {'paper','paper_live'} else 'LIVE'),'Date':_excel_date(td) or td,
            'Ranking':r.get('rank'),'Logic':logic,'Symbol':sym,'Status':r.get('status') or ('CLOSED' if sqty else 'OPEN'),
            'Quantity':qty or None,'Buy Price':bp,'Buy Date & Time':buy_dt,'Sell Quantity':sqty or None,'Sell Price':sp,'Sell Date & Time':sell_dt,
            'High After Buy':be.get('high') or r.get('high'),'High After Buy Date & Time':be.get('high_dt'),
            'Low After Buy':be.get('low') or r.get('low'),'Low After Buy Date & Time':be.get('low_dt'),
            'Exit Reason':r.get('exit_reason') or r.get('status'),'Gross P&L':gross,'Buy Turnover':bv,'Sell Turnover':sv,
            'Brokerage':r.get('brokerage'),'STT':r.get('stt'),'Exchange Charges':r.get('exchange_turnover_charges'),
            'GST':r.get('gst'),'SEBI Charges':r.get('sebi_charges'),'Stamp Duty':r.get('stamp_duty'),
            'Other Charges':r.get('total_other_charges'),'Total Charges':charges,'Net P&L':net,'P&L %':pct,
            'Charge Source':r.get('charge_source'),'Charge Plan':r.get('charge_plan'),'Buy Order ID':r.get('buy_order_id'),
            'Sell Order ID':r.get('sell_order_id'),'Data Source':r.get('data_source'),'Last Update':datetime.now(),'Trailing Active':r.get('trailing_active'),'Trailing Activation Date & Time':r.get('trailing_activation_datetime'),'Highest Price':r.get('highest_price'),'Trailing Stop':r.get('trailing_stop'),'Trailing Stop Date & Time':r.get('trailing_updated_datetime') or r.get('trailing_stop_datetime')
        }
        rr=existing.get(tid)
        if rr is None:
            rr=ws.max_row+1; existing[tid]=rr
        for h,v in values.items(): ws.cell(rr,idx[h]).value=_excel_safe(v)
    # Apply numeric/date formats so Excel receives true numbers, not numeric-looking text.
    for rr in range(2,ws.max_row+1):
        for h in ('Date',): ws.cell(rr,idx[h]).number_format='yyyy-mm-dd'
        for h in ('Buy Date & Time','Sell Date & Time','High After Buy Date & Time','Low After Buy Date & Time','Trailing Activation Date & Time','Trailing Stop Date & Time','Last Update'):
            ws.cell(rr,idx[h]).number_format='yyyy-mm-dd hh:mm:ss'
        for h in ('Buy Price','Sell Price','High After Buy','Low After Buy','Gross P&L','Buy Turnover','Sell Turnover','Brokerage','STT','Exchange Charges','GST','SEBI Charges','Stamp Duty','Other Charges','Total Charges','Net P&L','Highest Price','Trailing Stop'):
            ws.cell(rr,idx[h]).number_format='₹#,##0.00;[Red]-₹#,##0.00'
        ws.cell(rr,idx['P&L %']).number_format='0.00%'
        ws.cell(rr,idx['Quantity']).number_format='0'
        ws.cell(rr,idx['Sell Quantity']).number_format='0'
    ws.freeze_panes='A2'; ws.auto_filter.ref=f"A1:{get_column_letter(len(PAPER_LIVE_HEADERS))}{max(1,ws.max_row)}"; ws.sheet_view.zoomScale=85
    widths=[34,10,12,9,12,16,28,10,13,22,13,13,22,14,22,14,22,32,14,15,15,12,12,15,12,12,12,14,14,14,12,22,20,20,20,38,22]
    for c,w in enumerate(widths,1): ws.column_dimensions[get_column_letter(c)].width=w
    wb.save(target)
    try:
        _export_trade_performance_report(ws, idx)
    except Exception as exc:
        # Reporting must never interrupt PAPER/LIVE order management.
        print(f'TRADE PERFORMANCE EXPORT WARNING: {type(exc).__name__}: {exc}', flush=True)

def record_orders(results: list[dict]) -> None:
    """Write executed/paper trade details directly into P&L Dashboard.

    The dashboard is the only trade register. No Paper Trade Orders or Orders Log
    sheet is created or used.
    """
    wb=load_workbook(WORKBOOK)
    if 'YTD Historical Data' not in wb.sheetnames: return
    ws=wb['YTD Historical Data']
    header={str(ws.cell(1,c).value):c for c in range(1,ws.max_column+1)}
    if ws.max_column < 40: return
    row_map={}
    for rr in range(2,ws.max_row+1):
        d=str(ws.cell(rr,1).value)[:10]
        sym=str(ws.cell(rr,4).value or '').strip().upper()
        if d and sym: row_map[(d,sym)]=rr
    now=datetime.now()
    for r in results:
        sym=str(r.get('symbol','')).strip().upper()
        if not sym: continue
        trade_date=str((r.get('trade_date') or now.date()))[:10]
        rr=row_map.get((trade_date,sym))
        if rr is None:
            # If no screening row exists, add a trade-only row rather than creating
            # a hidden backing sheet. NSE screening fields remain blank.
            rr=ws.max_row+1
            ws.append([trade_date,'','',sym] + [None]*36)
            row_map[(trade_date,sym)]=rr
        qty=int(r.get('buy_actual_qty') or r.get('quantity') or 0)
        buy=float(r.get('buy_actual_price') or r.get('limit_price') or 0)
        sell_qty=int(r.get('sell_actual_qty') or 0)
        sell=float(r.get('sell_actual_price') or 0) if sell_qty else None
        buy_value=qty*buy if qty else None
        sell_value=sell_qty*sell if sell_qty and sell is not None else None
        gross=(sell_value-buy_value) if sell_value is not None and buy_value is not None else None
        # Reuse the already-computed shared ICICI charge result from Live/Paper/Historical.
        # If a result has no charge calculation (legacy row), calculate it once as fallback.
        charge_total = r.get('total_charges')
        if charge_total is None: charge_total = r.get('simulated_total_charges')
        if charge_total is None and buy_value is not None and sell_value is not None:
            fallback=icici_intraday_charges(buy_value or 0, sell_value or 0, exchange='NSE')
            charge_total=fallback['total_charges']
            bc={k:v for k,v in fallback.items() if k in ('brokerage','stt','exchange_turnover_charges','gst','sebi_charges','stamp_duty','total_other_charges')}
        else:
            charge_total=float(charge_total or 0) if (buy_value is not None and sell_value is not None) else None
            bc=dict(r.get('buy_charges') or {})
        charges=charge_total
        net=(gross-charges) if gross is not None and charges is not None else None
        pct=(net/buy_value) if net is not None and buy_value else None
        sc=dict(r.get('sell_charges') or {})
        be=r.get('buy_extremes') or {}; se=r.get('sell_extremes') or {}
        buy_dt = r.get('buy_trade_datetime') or _parse_breeze_datetime(r.get('buy_trade_time'))
        sell_dt = r.get('sell_trade_datetime') or _parse_breeze_datetime(r.get('sell_trade_time'))
        vals={
            'Qty':qty or None,'Buy Price':buy or None,'Buy Date':(buy_dt.date() if buy_dt else trade_date),
            'Buy Time':(buy_dt.time() if buy_dt else now.time()),'Buy Date & Time':buy_dt,
            'High After Buy':be.get('high'),'Low After Buy':be.get('low'),
            'High After Buy Date & Time':be.get('high_dt'),'Low After Buy Date & Time':be.get('low_dt'),
            'Sell Price':sell,'Sell Date':(sell_dt.date() if sell_dt else None),'Sell Time':(sell_dt.time() if sell_dt else None),
            'Sell Date & Time':sell_dt,'High After Sell':se.get('high'),'Low After Sell':se.get('low'),
            'High After Sell Date & Time':se.get('high_dt'),'Low After Sell Date & Time':se.get('low_dt'),
            'Exit Reason':r.get('stop_loss_error') or r.get('status'), 'Gross P&L':gross,
            'Buy Turnover':buy_value,'Sell Turnover':sell_value,
            'Brokerage':((float(bc.get('brokerage') or 0)+float(sc.get('brokerage') or 0)) if any(x.get('brokerage') not in (None,'') for x in (bc,sc)) else None),
            'STT':((float(bc.get('stt') or 0)+float(sc.get('stt') or 0)) if any(x.get('stt') not in (None,'') for x in (bc,sc)) else None),
            'Exchange Charges':((float(bc.get('exchange_turnover_charges') or 0)+float(sc.get('exchange_turnover_charges') or 0)) if any(x.get('exchange_turnover_charges') not in (None,'') for x in (bc,sc)) else None),
            'GST':((float(bc.get('gst') or 0)+float(sc.get('gst') or 0)) if any(x.get('gst') not in (None,'') for x in (bc,sc)) else None),
            'SEBI Charges':((float(bc.get('sebi_charges') or 0)+float(sc.get('sebi_charges') or 0)) if any(x.get('sebi_charges') not in (None,'') for x in (bc,sc)) else None),
            'Stamp Duty':((float(bc.get('stamp_duty') or 0)+float(sc.get('stamp_duty') or 0)) if any(x.get('stamp_duty') not in (None,'') for x in (bc,sc)) else None),
            'Other Charges':((float(bc.get('total_other_charges') or 0)+float(sc.get('total_other_charges') or 0)) if any(x.get('total_other_charges') not in (None,'') for x in (bc,sc)) else None),
            'Total Charges':charges,'Net P&L':net,'P&L %':pct,
            'Data Source':('Breeze NSE + Breeze trade/preview data' if (bc or sc) else 'Paper trade calculation'),
            'Status':r.get('status') or 'Recorded'
        }
        # Duplicate headers are intentional: the first matching name is used for
        # the first occurrence; the second Logic/Symbol are explicitly columns 13/14.
        for c,h in enumerate(DASHBOARD_HEADERS,1):
            if h in vals:
                # Only write the trade-side Logic/Symbol duplicate positions 13/14
                # from existing screening values; don't overwrite them here.
                if c in (13,14): continue
                ws.cell(rr,c).value=vals[h]
    _format_pnl_dashboard(ws)
    wb.save(WORKBOOK)
    sync_nse_market_order_serials(); update_overview_sheet()
    save_daily_snapshot()


def run_paper_replay_latest() -> list[dict]:
    """Paper validation on the latest NSE session using the four configured logics."""
    verify_canonical_strategy_parity(); _refresh_latest_gui_logic_settings()
    breeze=require_breeze(); raw,day=latest_bhavcopy(); market,_=screen_from_raw(raw); cfgs=load_logics(); enabled=enabled_logics(cfgs)
    candidates=[]
    for _,r in market.iterrows():
        passed=[]
        for logic_name,cfg in enabled:
            try:
                level=entry_level(float(r['Open']),cfg,float(r['Previous close']))
                if direction_ok(r['Open'],r['Previous close'],cfg['open_direction']) and float(r['High'])>=level and ((not price_band_enabled(cfg)) or price_band_ok(r['Open'],cfg)):
                    # Daily volume/turnover are only a download/universe hint. The
                    # actual BUY is decided later by _simulate_logic_from_candles()
                    # with the same timestamped cumulative metrics used by LIVE.
                    passed.append(logic_name)
            except Exception: pass
        if passed:candidates.append((r,passed))
    print(f"Paper replay order scan: {len(candidates)} candidates pass at least one configured logic.")
    results=[]; logic_orders={name:0 for name,_ in enabled}; logic_cfg=dict(enabled); candidates_checked=0
    for rank,(r,passed) in enumerate(candidates,1):
        candidates_checked+=1; sym=str(r['Symbol']).strip().upper()
        try:
            names=breeze.get_names(exchange_code='NSE',stock_code=sym); stock=str((names or {}).get('isec_stock_code') or '').strip()
            if not stock: continue
            candles=_historical_session_candles(breeze,stock,day)
            for logic_name in passed:
                _logic_cfg=logic_cfg.get(logic_name, {})
                result=_simulate_logic_from_candles(breeze,stock,sym,day,candles,logic_name,daily_open=r['Open'],daily_high=r['High'],daily_previous_close=r['Previous close'])
                is_order=result.get('status')=='HISTORICAL_REPLAY_COMPLETE'
                result.update({'rank':rank,'breeze_symbol':stock,'open':r['Open'],'high':r['High'],'low':r['Low'],'close':r['Close'],'previous_close':r['Previous close'],'volume':r['Volume'],'turnover_cr':float(r['Turnover (₹)'])/10000000,'mode':'paper','status':('PAPER_REPLAY_COMPLETE' if is_order else result.get('status')),'data_source':'Breeze Historical V2 - Paper replay','order_sequence':logic_orders[logic_name]+1 if is_order else ''})
                results.append(result)
                if is_order:
                    logic_orders[logic_name]+=1; print(f'Paper {logic_name} first-buy order #{logic_orders[logic_name]}/{max_orders_for_logic(_logic_cfg) if max_orders_for_logic(_logic_cfg)>0 else "∞"}: {day} {sym}.')
        except Exception as exc:
            print(f'Paper replay ERROR {day} {sym}: {type(exc).__name__}: {exc}')
    # Paper replay uses the same final ranking/cap semantics as Historical:
    # actual BUY timestamp first, then one hard global Maximum Orders cap.
    _paper_complete=[x for x in results if x.get('status')=='HISTORICAL_REPLAY_COMPLETE' and x.get('buy_trade_datetime')]
    _paper_by_symbol={}
    for _x in sorted(_paper_complete,key=lambda z:(z.get('buy_trade_datetime') or datetime.max,str(z.get('symbol') or ''))):
        _sym=str(_x.get('symbol') or '').strip().upper()
        if _sym and _sym not in _paper_by_symbol:
            _paper_by_symbol[_sym]=_x
    _paper_caps=[max_orders_for_logic(c) for c in logic_cfg.values()]
    _paper_cap=(1_000_000_000 if any(c==0 for c in _paper_caps) else sum(_paper_caps)) if _paper_caps else 0
    _paper_selected=list(_paper_by_symbol.values())[:_paper_cap]
    _paper_selected_symbols={str(x.get('symbol') or '').strip().upper() for x in _paper_selected}
    for _i,_x in enumerate(_paper_selected,1):
        _x['rank']=_i; _x['order_sequence']=_i
    for _x in results:
        _sym=str(_x.get('symbol') or '').strip().upper()
        if _x.get('status')=='HISTORICAL_REPLAY_COMPLETE' and _sym not in _paper_selected_symbols:
            _x['status']='NOT SELECTED'
            _x['exit_reason']=f'GUI Maximum Orders cap ({_paper_cap if _paper_cap < 1_000_000_000 else "UNLIMITED"}) reached; no replacement BUY after SELL.'
    if results: record_historical_results(results)
    try:update_overview_sheet()
    except Exception as exc:print(f'WARNING: Date Wise P&L refresh failed after paper replay: {exc}')
    print(f'Paper validation complete for {day}: candidates={candidates_checked}; orders={logic_orders}; rows={len(results)}.')
    return results

def _previous_trading_day(day: date) -> date:
    probe = day - timedelta(days=1)
    while not _is_nse_trading_day(probe):
        probe -= timedelta(days=1)
    return probe


def _paper_quote(breeze, symbol: str):
    """Read one current NSE cash quote for paper-live simulation."""
    resp = broker_call(f'Paper live quote {symbol}', breeze.get_quotes,
                       stock_code=symbol, exchange_code='NSE', product_type='cash')
    rows = resp.get('Success') or []
    if not rows or not isinstance(rows[0], dict):
        raise RuntimeError(f'No usable quote for {symbol}')
    q = rows[0]
    ltp = float(q.get('ltp') or 0)
    prev = float(q.get('previous_close') or 0)
    volume = float(q.get('total_quantity_traded') or 0)
    ltt = _parse_breeze_datetime(q.get('ltt')) or datetime.now()
    return {
        'ltp': ltp, 'previous_close': prev, 'volume': volume,
        'turnover': ltp * volume, 'time': ltt, 'raw': q,
    }


def _parse_stream_turnover_cr(value) -> float:
    """Parse Breeze streaming total traded value (often like '1328.72C')."""
    try:
        if value is None or value == '':
            return 0.0
        text = str(value).strip().upper().replace(',', '')
        if text.endswith('C'):
            return float(text[:-1])
        # Breeze may return a raw traded value; convert rupees to crore.
        return float(text) / 10000000.0
    except Exception:
        return 0.0


_LIVE_ORB15_STATE = {}
# ORB candidates stay in an explicit bounded priority heap. The item map
# supports O(1) deduplication and lazy invalidation when a queued symbol becomes
# a stronger candidate on a later quote.
_LIVE_ORB_SEED_SCHEDULED = set()
_LIVE_ORB_SEED_SCHEDULED_LOCK = threading.Lock()
_LIVE_ORB_SEED_PENDING = []
_LIVE_ORB_SEED_PENDING_KEYS = set()
_LIVE_ORB_SEED_PENDING_ITEMS = {}
_LIVE_ORB_SEED_SEQUENCE = 0
_LIVE_ORB_SEED_PENDING_LIMIT = 8192
_LIVE_ORB_SEED_PENDING_DROPPED = 0
_LIVE_ORB_SEED_PENDING_REPLACED = 0
_LIVE_ORB_SEED_PENDING_DEDUPLICATED = 0
_LIVE_ORB_SEED_PUMP_LOCK = threading.Lock()
# Six shared workers: keep five ORB slots available while reserving capacity
# for RVOL work. The scheduler still enforces fair ORB/RVOL service.
_LIVE_ORB_SEED_MAX_SYMBOLS = 5
_LIVE_ORB_SEED_LAST_DIAGNOSTIC = 0.0
_LIVE_ORB_SEED_DIAGNOSTIC_LOCK = threading.Lock()

def _live_orb_seed_status(force=False):
    """Report app-queue and actual shared-worker lifecycle counts separately."""
    global _LIVE_ORB_SEED_LAST_DIAGNOSTIC
    now = time.monotonic()
    with _LIVE_ORB_SEED_DIAGNOSTIC_LOCK:
        if not force and now - _LIVE_ORB_SEED_LAST_DIAGNOSTIC < 30.0:
            return
        _LIVE_ORB_SEED_LAST_DIAGNOSTIC = now
    try:
        with _LIVE_ORB_SEED_SCHEDULED_LOCK:
            pending = len(_LIVE_ORB_SEED_PENDING_KEYS)
            outstanding = len(_LIVE_ORB_SEED_SCHEDULED)
            pending_dropped = _LIVE_ORB_SEED_PENDING_DROPPED
            pending_replaced = _LIVE_ORB_SEED_PENDING_REPLACED
            pending_deduplicated = _LIVE_ORB_SEED_PENDING_DEDUPLICATED
            pending_capacity = _LIVE_ORB_SEED_PENDING_LIMIT
            states = [v for v in _LIVE_ORB15_STATE.values() if isinstance(v, dict)]
        ready = sum(1 for st in states if st.get('orb_seed_applied') and st.get('orb_ready'))
        failed = sum(1 for st in states if st.get('orb_seed_applied') and not st.get('orb_ready'))
        retrying = sum(1 for st in states if st.get('orb_seed_retry_at')
                       and float(st.get('orb_seed_retry_at') or 0) > now
                       and not st.get('orb_seed_applied'))
        scheduler = _RUNTIME_HISTORICAL_SEED_SCHEDULER.snapshot()
        print(
            f'MARKET ORB QUEUE STATUS: ready={ready}; app_pending={pending}; '
            f'orb_outstanding={outstanding}; workers_active={scheduler["active"]}/{scheduler["workers"]}; '
            f'scheduler_orb_waiting={scheduler["orb_waiting"]}; '
            f'scheduler_rvol_waiting={scheduler["rvol_waiting"]}; '
            f'history_completed={scheduler["completed"]}; history_failed={scheduler["failed"]}; '
            f"deduplicated={scheduler['deduplicated']}; queue_rejected={scheduler['rejected']}; "
            f'pending_capacity={pending_capacity}; app_deduplicated={pending_deduplicated}; '
            f'pending_dropped={pending_dropped}; pending_replaced={pending_replaced}; '
            f'retrying={retrying}; exhausted={failed}; dispatch=live-nearest-trigger/FIFO-ties/ORB-priority/RVOL-fair.',
            flush=True)
    except Exception:
        pass

def _pump_live_orb_seed_queue():
    """Submit the highest-priority eligible ORB seeds when bounded slots free."""
    if not _LIVE_ORB_SEED_PUMP_LOCK.acquire(blocking=False):
        return
    try:
        while True:
            with _LIVE_ORB_SEED_SCHEDULED_LOCK:
                if len(_LIVE_ORB_SEED_SCHEDULED) >= _LIVE_ORB_SEED_MAX_SYMBOLS:
                    return
                # Skip stale heap nodes left by an in-place priority improvement.
                item = None
                while _LIVE_ORB_SEED_PENDING:
                    candidate = heapq.heappop(_LIVE_ORB_SEED_PENDING)
                    candidate_key = candidate[2]
                    if _LIVE_ORB_SEED_PENDING_ITEMS.get(candidate_key) != candidate:
                        continue
                    item = candidate
                    _LIVE_ORB_SEED_PENDING_ITEMS.pop(candidate_key, None)
                    _LIVE_ORB_SEED_PENDING_KEYS.discard(candidate_key)
                    break
                if item is None:
                    return
                _priority, _sequence, seed_key, breeze, symbol, session_time = item
                if seed_key in _LIVE_ORB_SEED_SCHEDULED:
                    continue
                state = _LIVE_ORB15_STATE.get(seed_key)
                if not isinstance(state, dict):
                    continue
                # A future already assigned means this key is in flight or its
                # result has not yet been consumed by the quote callback.
                if state.get('orb_seed_future') is not None or state.get('orb_seed_applied'):
                    continue
                _LIVE_ORB_SEED_SCHEDULED.add(seed_key)
            try:
                attempt = int(state.get('orb_seed_attempts', 0) or 0) + 1
                if attempt > 3:
                    with _LIVE_ORB_SEED_SCHEDULED_LOCK:
                        _LIVE_ORB_SEED_SCHEDULED.discard(seed_key)
                    continue
                state['orb_seed_attempts'] = attempt
                future = _RUNTIME_ORB_SEED_EXECUTOR.submit(
                    _runtime_fetch_orb_seed, breeze, symbol, session_time,
                    force_refresh=(attempt > 1), priority='orb',
                    dedupe_key=f'{seed_key[0]}|{seed_key[1]}')
                state['orb_seed_future'] = future
                future.add_done_callback(
                    lambda _completed, _key=seed_key: _release_live_orb_seed_slot(_key))
                print(
                    f'MARKET ORB WARMUP: fetching verified 09:15-09:29 1-minute '
                    f'opening range for {symbol} (attempt {attempt}/3; '
                    f'max outstanding {_LIVE_ORB_SEED_MAX_SYMBOLS}; '
                    f'pending={len(_LIVE_ORB_SEED_PENDING_KEYS)}).', flush=True)
            except Exception:
                with _LIVE_ORB_SEED_SCHEDULED_LOCK:
                    _LIVE_ORB_SEED_SCHEDULED.discard(seed_key)
                state['orb_seed_future'] = None
                state['orb_seed_retry_at'] = time.monotonic() + 5.0
                print(f'MARKET ORB WARMUP SUBMIT WARNING: {symbol}; retry scheduled.', flush=True)
    finally:
        _LIVE_ORB_SEED_PUMP_LOCK.release()


def _orb_seed_priority_score(movement_pct, required_movement_pct):
    """Prioritise ORB history fetches without changing any BUY eligibility gate."""
    try:
        move = float(movement_pct)
        required = max(0.0, float(required_movement_pct))
    except (TypeError, ValueError, OverflowError):
        return 99.0
    if required > 0.0:
        return max(0.0, required - move)
    # At a zero threshold all positive movers qualify for warm-up; stronger
    # movers get the first history slots rather than arrival-order FIFO.
    return max(0.0, 100.0 - move)


def _queue_live_orb_seed(seed_key, breeze, symbol, session_time, priority_score=99.0):
    """Queue eligible ORB symbols by proximity to their configured trigger.

    Queued candidates are reprioritized when later quotes materially improve
    their score. FIFO is retained for equal scores. This only changes history
    fetch order; it never changes a BUY gate, breakout condition, or sizing.
    """
    global _LIVE_ORB_SEED_SEQUENCE, _LIVE_ORB_SEED_PENDING_DROPPED
    global _LIVE_ORB_SEED_PENDING_REPLACED, _LIVE_ORB_SEED_PENDING_DEDUPLICATED
    try:
        score = max(0.0, float(priority_score))
    except Exception:
        score = 99.0
    should_pump = False
    capacity_warning = False
    with _LIVE_ORB_SEED_SCHEDULED_LOCK:
        state = _LIVE_ORB15_STATE.get(seed_key)
        if not isinstance(state, dict) or state.get('orb_seed_applied'):
            return
        if (state.get('orb_seed_future') is not None or
                seed_key in _LIVE_ORB_SEED_SCHEDULED):
            _LIVE_ORB_SEED_PENDING_DEDUPLICATED += 1
            return
        if seed_key in _LIVE_ORB_SEED_PENDING_KEYS:
            previous = _LIVE_ORB_SEED_PENDING_ITEMS.get(seed_key)
            if previous is not None and score + 0.25 < previous[0]:
                # Keep the original sequence number so equal-priority candidates
                # preserve FIFO order. Old heap nodes become harmless stale nodes.
                item = (score, previous[1], seed_key, breeze, symbol, session_time)
                _LIVE_ORB_SEED_PENDING_ITEMS[seed_key] = item
                heapq.heappush(_LIVE_ORB_SEED_PENDING, item)
                _LIVE_ORB_SEED_PENDING_REPLACED += 1
                should_pump = True
                if len(_LIVE_ORB_SEED_PENDING) > max(
                        64, 2 * len(_LIVE_ORB_SEED_PENDING_ITEMS) + 32):
                    _LIVE_ORB_SEED_PENDING[:] = list(_LIVE_ORB_SEED_PENDING_ITEMS.values())
                    heapq.heapify(_LIVE_ORB_SEED_PENDING)
            else:
                _LIVE_ORB_SEED_PENDING_DEDUPLICATED += 1
        elif len(_LIVE_ORB_SEED_PENDING_KEYS) < _LIVE_ORB_SEED_PENDING_LIMIT:
            _LIVE_ORB_SEED_SEQUENCE += 1
            item = (score, _LIVE_ORB_SEED_SEQUENCE, seed_key, breeze, symbol, session_time)
            heapq.heappush(_LIVE_ORB_SEED_PENDING, item)
            _LIVE_ORB_SEED_PENDING_KEYS.add(seed_key)
            _LIVE_ORB_SEED_PENDING_ITEMS[seed_key] = item
            should_pump = True
        else:
            _LIVE_ORB_SEED_PENDING_DROPPED += 1
            capacity_warning = True
    if capacity_warning:
        print(
            f'MARKET ORB QUEUE CAPACITY WARNING: {symbol} not admitted; '
            f'pending={len(_LIVE_ORB_SEED_PENDING_KEYS)}/{_LIVE_ORB_SEED_PENDING_LIMIT}. '
            'Candidate remains eligible for a later tick.', flush=True)
    if should_pump:
        _pump_live_orb_seed_queue()
    _live_orb_seed_status()

def _release_live_orb_seed_slot(seed_key):
    try:
        with _LIVE_ORB_SEED_SCHEDULED_LOCK:
            _LIVE_ORB_SEED_SCHEDULED.discard(seed_key)
        # Completion immediately opens the next priority slot; this avoids waiting
        # for another tick from the just-completed symbol to advance the queue.
        _pump_live_orb_seed_queue()
        _live_orb_seed_status()
    except Exception:
        pass



# R43: broaden prefetch coverage to include fast movers above the trigger window.
# This changes only data warmup eligibility; the BUY evaluator keeps all configured gates.
# Late-start ORB recovery: fetch the genuine opening 1-minute candles asynchronously.
# Only a complete set of 15 distinct 09:15-09:29 candles can mark ORB ready.
def _paper_seed_breeze_stock_code(symbol, breeze=None):
    """Resolve an NSE ticker to Breeze's stock_code using cached bulk mapping first.

    If the bulk cache has no entry, get_names is used only as a stock-code
    resolver (its company-name fields are deliberately ignored). This avoids
    repeatedly sending an unverified NSE ticker as stock_code when Breeze uses
    a different exchange token.
    """
    sym = str(symbol or '').strip().upper()
    cache = _load_security_cache()
    mapped = str(cache.get(sym) or '').strip()
    if not mapped and breeze is not None:
        try:
            _, mapped = _resolve_historical_breeze_stock(breeze, sym, cache)
            mapped = str(mapped or '').strip()
        except Exception as exc:
            print(f'PAPER HISTORICAL SYMBOL RESOLUTION WARNING: NSE={sym}; {type(exc).__name__}: {exc}')
    selected = mapped or sym
    print(f'PAPER HISTORICAL SYMBOL ROUTE: NSE={sym}; Breeze_stock_code={selected}; mapped={bool(mapped)}')
    return selected


def _paper_seed_cache_key(stock_code: str, interval: str, from_dt: datetime, to_dt: datetime) -> str:
    raw = f'{HISTORICAL_CACHE_VERSION}|{str(stock_code).upper()}|{interval}|{from_dt.isoformat()}|{to_dt.isoformat()}'
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()


def _load_paper_seed_cache(stock_code: str, interval: str, from_dt: datetime, to_dt: datetime):
    """Read an exact PAPER warm-up request from one compact SQLite cache."""
    db_path = HISTORICAL_CACHE / 'paper_warmup_cache.sqlite3'
    if not db_path.exists():
        return None
    key = _paper_seed_cache_key(stock_code, interval, from_dt, to_dt)
    try:
        with _PAPER_SEED_CACHE_LOCK:
            with sqlite3.connect(str(db_path), timeout=30) as conn:
                conn.execute('PRAGMA busy_timeout=30000')
                row = conn.execute('SELECT payload FROM paper_seed_requests WHERE cache_key = ?', (key,)).fetchone()
        if row is None:
            return None
        payload = json.loads(row[0])
        return payload if isinstance(payload, list) else None
    except Exception as exc:
        print(f'PAPER WARMUP CACHE READ WARNING: {type(exc).__name__}: {exc}', flush=True)
        return None


def _save_paper_seed_cache(stock_code: str, interval: str, from_dt: datetime, to_dt: datetime, rows) -> None:
    """Persist PAPER warm-up chunks in one DB instead of one CSV per API request."""
    if not rows:
        return
    db_path = HISTORICAL_CACHE / 'paper_warmup_cache.sqlite3'
    key = _paper_seed_cache_key(stock_code, interval, from_dt, to_dt)
    payload = json.dumps(list(rows), ensure_ascii=False, separators=(',', ':'), default=str)
    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with _PAPER_SEED_CACHE_LOCK:
            with sqlite3.connect(str(db_path), timeout=30) as conn:
                conn.execute('PRAGMA busy_timeout=30000')
                conn.execute("""CREATE TABLE IF NOT EXISTS paper_seed_requests (
                    cache_key TEXT PRIMARY KEY,
                    stock_code TEXT NOT NULL,
                    interval TEXT NOT NULL,
                    from_dt TEXT NOT NULL,
                    to_dt TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    saved_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )""")
                conn.execute(
                    'INSERT OR REPLACE INTO paper_seed_requests '
                    '(cache_key, stock_code, interval, from_dt, to_dt, payload) VALUES (?, ?, ?, ?, ?, ?)',
                    (key, str(stock_code).upper(), str(interval), from_dt.isoformat(), to_dt.isoformat(), payload)
                )
                conn.commit()
    except Exception as exc:
        # Cache persistence is optional; never discard valid live warm-up candles.
        print(f'PAPER WARMUP CACHE WRITE WARNING: {type(exc).__name__}: {exc}', flush=True)


def _paper_seed_historical_rows(breeze, symbol, interval, start_dt, end_dt, force_refresh=False):
    """Fetch genuine paper warmup candles in bounded API windows.

    The Breeze endpoint can return HTTP 200 with an empty Success array for a
    long multi-session request. Three-day windows keep each 5-minute request
    below the endpoint's practical response ceiling. Successful windows are
    retained even if another window is empty; no candles are synthesized.
    ORB retries can bypass a previously cached incomplete response.
    """
    if end_dt < start_dt:
        return []
    stock_code = _paper_seed_breeze_stock_code(symbol, breeze)
    rows = []
    cursor = start_dt
    window = timedelta(days=3)
    while cursor <= end_dt:
        # A user stop must also stop background RVOL/ORB warm-up jobs. Without
        # this guard, queued workers keep issuing hundreds of history calls after
        # the market loop has been asked to exit.
        if _remote_stop_requested():
            print(f'PAPER HISTORICAL WARMUP: stop requested; ending seed early for {symbol}.', flush=True)
            break
        chunk_end = min(end_dt, cursor + window - timedelta(seconds=1))
        chunk = None if force_refresh else _load_paper_seed_cache(
            stock_code, interval, cursor, chunk_end)
        if chunk is not None:
            print(f'PAPER WARMUP CACHE HIT: {symbol} {interval} {cursor:%Y-%m-%d}..{chunk_end:%Y-%m-%d}; rows={len(chunk)}', flush=True)
        else:
            if force_refresh:
                print(f'PAPER WARMUP CACHE BYPASS: refreshing {symbol} {interval} after incomplete prior response.', flush=True)
            # PAPER warm-up requests are checkpointed in one SQLite database.
            # Do not create a separate visible CSV file for every three-day chunk.
            chunk = _historical_v2(breeze, stock_code, interval, cursor, chunk_end, persist_request_cache=False)
            if chunk:
                _save_paper_seed_cache(stock_code, interval, cursor, chunk_end, chunk)
        if chunk:
            rows.extend(x for x in chunk if isinstance(x, dict))
            print(f'PAPER HISTORICAL CHUNK: {symbol} {interval} {cursor:%Y-%m-%d %H:%M:%S}..{chunk_end:%Y-%m-%d %H:%M:%S}; rows={len(chunk)}')
        else:
            print(f'PAPER HISTORICAL CHUNK EMPTY: {symbol} {interval} {cursor:%Y-%m-%d %H:%M:%S}..{chunk_end:%Y-%m-%d %H:%M:%S}')
        cursor = chunk_end + timedelta(seconds=1)
    # Deduplicate overlapping boundary candles using their exchange timestamp.
    by_stamp = {}
    for row in rows:
        stamp = _parse_breeze_datetime(row.get('datetime') or row.get('date'))
        if stamp is not None:
            by_stamp[stamp] = row
    return [by_stamp[k] for k in sorted(by_stamp)] if by_stamp else rows



class _HistoricalSeedQueueFull(RuntimeError):
    """Retryable admission failure; does not consume a historical fetch attempt."""


class _PaperHistoricalSeedScheduler:
    """Six shared workers with deduplicated, bounded ORB/RVOL admission.

    ORB retains priority with guaranteed RVOL service. Duplicate requests for the
    same symbol/session share one Future. Queue-full rejection is explicit and
    retryable; it never fabricates data or changes strategy gates.
    """
    def __init__(self, max_workers=6, max_orb_pending=4, max_rvol_pending=384):
        self._condition = threading.Condition()
        self._orb_queue = deque()
        self._rvol_queue = deque()
        self._inflight = {}
        self._shutdown = False
        self._orb_streak = 0
        self._active = 0
        self._completed = 0
        self._failed = 0
        self._deduplicated = 0
        self._rejected = 0
        self._workers = []
        self._max_workers = int(max_workers)
        self._max_orb_pending = int(max_orb_pending)
        self._max_rvol_pending = int(max_rvol_pending)
        for index in range(self._max_workers):
            worker = threading.Thread(target=self._worker,
                name=f'paper-history-priority-{index + 1}', daemon=True)
            worker.start()
            self._workers.append(worker)

    def submit(self, fn, *args, priority='rvol', dedupe_key=None, **kwargs):
        future = concurrent.futures.Future()
        lane = 'orb' if str(priority).lower() == 'orb' else 'rvol'
        key = (lane, str(dedupe_key)) if dedupe_key is not None else None
        item = (future, fn, args, kwargs, key)
        with self._condition:
            if self._shutdown:
                raise RuntimeError('historical seed scheduler is shut down')
            if key is not None and key in self._inflight:
                self._deduplicated += 1
                return self._inflight[key]
            queue = self._orb_queue if lane == 'orb' else self._rvol_queue
            limit = self._max_orb_pending if lane == 'orb' else self._max_rvol_pending
            if len(queue) >= limit:
                self._rejected += 1
                future.set_exception(_HistoricalSeedQueueFull(
                    f'{lane.upper()} historical queue is full ({len(queue)}/{limit}); retry later'))
                return future
            if key is not None:
                self._inflight[key] = future
            queue.append(item)
            self._condition.notify()
        return future

    def snapshot(self):
        with self._condition:
            return {'active': self._active, 'orb_waiting': len(self._orb_queue),
                    'rvol_waiting': len(self._rvol_queue),
                    'completed': self._completed, 'failed': self._failed,
                    'deduplicated': self._deduplicated, 'rejected': self._rejected,
                    'workers': self._max_workers}

    def _worker(self):
        while True:
            with self._condition:
                while not self._shutdown and not self._orb_queue and not self._rvol_queue:
                    self._condition.wait()
                if self._shutdown and not self._orb_queue and not self._rvol_queue:
                    return
                if self._rvol_queue and (not self._orb_queue or self._orb_streak >= 3):
                    item = self._rvol_queue.popleft()
                    self._orb_streak = 0
                elif self._orb_queue:
                    item = self._orb_queue.popleft()
                    self._orb_streak += 1
                else:
                    item = self._rvol_queue.popleft()
                    self._orb_streak = 0
            future, fn, args, kwargs, key = item
            if not future.set_running_or_notify_cancel():
                with self._condition:
                    if key is not None and self._inflight.get(key) is future:
                        self._inflight.pop(key, None)
                continue
            with self._condition:
                self._active += 1
            failed = False
            try:
                result = fn(*args, **kwargs)
            except BaseException as exc:
                failed = True
                future.set_exception(exc)
            else:
                future.set_result(result)
            finally:
                with self._condition:
                    self._active = max(0, self._active - 1)
                    self._completed += 1
                    if failed:
                        self._failed += 1
                    if key is not None and self._inflight.get(key) is future:
                        self._inflight.pop(key, None)
                    self._condition.notify_all()

    def shutdown(self, wait=False, cancel_futures=False):
        with self._condition:
            self._shutdown = True
            if cancel_futures:
                for queue in (self._orb_queue, self._rvol_queue):
                    while queue:
                        future, _, _, _, key = queue.popleft()
                        future.cancel()
                        if key is not None and self._inflight.get(key) is future:
                            self._inflight.pop(key, None)
            self._condition.notify_all()
        if wait:
            for worker in self._workers:
                worker.join()

_RUNTIME_HISTORICAL_SEED_WORKERS = 6
_RUNTIME_HISTORICAL_SEED_SCHEDULER = _PaperHistoricalSeedScheduler(max_workers=_RUNTIME_HISTORICAL_SEED_WORKERS)
_RUNTIME_ORB_SEED_EXECUTOR = _RUNTIME_HISTORICAL_SEED_SCHEDULER

def _log_runtime_build_identity(mode: str) -> None:
    """Print unambiguous build/runtime identity and actual worker-pool size."""
    try:
        _snapshot = _RUNTIME_HISTORICAL_SEED_SCHEDULER.snapshot()
        _source = Path(__file__).resolve().name
        print(
            f'RUNTIME IDENTITY: build={BUILD_VERSION}; mode={str(mode).upper()}; '
            f'source={_source}; pid={os.getpid()}; '
            f'historical_workers_configured={_RUNTIME_HISTORICAL_SEED_WORKERS}; '
            f'historical_workers_actual={_snapshot["workers"]}', flush=True)
    except Exception as _identity_exc:
        print(f'RUNTIME IDENTITY WARNING: build={BUILD_VERSION}; mode={str(mode).upper()}; '
              f'identity_error={type(_identity_exc).__name__}: {str(_identity_exc)[:160]}', flush=True)


def _runtime_fetch_orb_seed(breeze, symbol, session_time, force_refresh=False):
    try:
        end_dt = session_time.replace(tzinfo=None) if hasattr(session_time, 'replace') else _nse_now().replace(tzinfo=None)
        # Request a small safety margin around the opening window. Breeze's
        # historical endpoint can treat `to_date` as an exclusive boundary (and
        # some responses omit the final candle when it equals 09:30 exactly).
        # We still accept ONLY the 15 genuine 09:15..09:29 minute candles below;
        # the wider request never relaxes the ORB completeness requirement.
        opening_start = datetime.combine(end_dt.date(), datetime.min.time()).replace(hour=9, minute=15)
        start_dt = opening_start - timedelta(minutes=1)
        cutoff = datetime.combine(end_dt.date(), datetime.min.time()).replace(hour=9, minute=32)
        rows = _paper_seed_historical_rows(breeze, symbol, '1minute', start_dt, cutoff, force_refresh=force_refresh)
        minute_highs = {}
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            stamp = _parse_breeze_datetime(row.get('datetime') or row.get('date'))
            try:
                high = float(row.get('high') or 0.0)
            except Exception:
                high = 0.0
            if stamp is None or high <= 0 or stamp.date() != end_dt.date():
                continue
            if (stamp.hour, stamp.minute) < (9, 15) or (stamp.hour, stamp.minute) >= (9, 30):
                continue
            minute_highs[stamp.strftime('%H:%M')] = max(high, minute_highs.get(stamp.strftime('%H:%M'), 0.0))
        expected = {f'09:{m:02d}' for m in range(15, 30)}
        present = expected.intersection(minute_highs)
        if not expected.issubset(minute_highs):
            missing = sorted(expected - present)
            extra = sorted(set(minute_highs) - expected)
            return {'_seed_error': (
                f'incomplete opening history: {len(present)}/15 required minutes; '
                f'missing={missing}; received={len(minute_highs)}; '
                f'ignored_outside_window={extra[:4]}')}
        return {'_orb_high': max(minute_highs.values()), '_orb_minutes_seen': sorted(expected)}
    except Exception as exc:
        return {'_seed_error': f'{type(exc).__name__}: {exc}'}


def _update_live_orb15_state(symbol: str, q: dict) -> dict:
    """Build a fail-closed 15-minute opening range from live ticks.

    Breeze exchange-quote `high`/`low` are session-wide extremes, not one-minute
    OHLC. The ORB therefore uses observed LTPs only. A symbol must have at least
    one observed tick in every opening minute (09:15 through 09:29) before its
    range is trusted; starting the process late cannot silently create a partial
    opening range. A breakout is accepted only from a completed post-09:30
    minute and is executable during the immediately following minute.
    """
    q = dict(q or {})
    sym = str(symbol or q.get('symbol') or '').strip().upper()
    stamp = q.get('time')
    try:
        if isinstance(stamp, str):
            stamp = _parse_breeze_datetime(stamp)
        if not sym or stamp is None or not isinstance(stamp, datetime):
            q['orb15_breakout'] = False
            q['orb15_ready'] = False
            q['orb15_reason'] = 'ORB_TIMESTAMP_UNAVAILABLE'
            return q

        day_key = stamp.date().isoformat()
        minute_key = stamp.replace(second=0, microsecond=0)
        hhmm = (stamp.hour, stamp.minute)
        state_key = (day_key, sym)
        st = _LIVE_ORB15_STATE.setdefault(state_key, {
            'minute': None, 'bar': None, 'orb_high': 0.0,
            'orb_minutes_seen': set(), 'cum_volume': 0.0,
            'cum_turnover': 0.0, 'last_signal': None,
            'last_completed_close': 0.0, 'last_completed_vwap': 0.0,
            'last_tick_time': None,
            'last_stream_volume': None, 'vwap_seeded': False,
            'vwap_source': None,
        })

        # Ignore late/out-of-order events; never rewind the per-symbol bar state.
        last_tick_time = st.get('last_tick_time')
        if last_tick_time is not None and stamp < last_tick_time:
            q['orb15_breakout'] = False
            q['orb15_ready'] = bool(st.get('orb_ready', False))
            q['orb15_high'] = float(st.get('orb_high') or 0.0) or None
            q['vwap'] = ((st.get('cum_turnover', 0.0) / st.get('cum_volume', 0.0))
                         if st.get('cum_volume', 0.0) > 0 and st.get('cum_turnover', 0.0) > 0 else None)
            q['vwap_source'] = st.get('vwap_source')
            q['orb15_reason'] = 'OUT_OF_ORDER_TICK'
            return q
        st['last_tick_time'] = stamp

        # Never fall back to `close`: in quote payloads it can be a stale prior close.
        try:
            px = float(q.get('ltp') or 0.0)
        except (TypeError, ValueError):
            px = 0.0
        if px <= 0:
            q['orb15_breakout'] = False
            q['orb15_ready'] = False
            q['orb15_reason'] = 'LIVE_LTP_UNAVAILABLE'
            return q

        # Breeze fields are cumulative session totals. Keep their high-water marks
        # rather than adding the same totals on every tick.
        try:
            total_vol = max(0.0, float(q.get('volume') or 0.0))
        except (TypeError, ValueError):
            total_vol = 0.0
        try:
            turn_cr = max(0.0, float(q.get('turnover_cr') or 0.0))
        except (TypeError, ValueError):
            turn_cr = 0.0
        # Breeze's NSE quote stream exposes avgPrice (average traded price)
        # alongside ttq. Prefer that exchange-provided VWAP-equivalent value.
        # Some SDK/feed variants deliver ttv as a bare number without the 'C'
        # crore suffix; treating that number as rupees inflated VWAP by 100x.
        try:
            avg_traded_price = max(0.0, float(q.get('avg_traded_price') or 0.0))
        except (TypeError, ValueError):
            avg_traded_price = 0.0
        total_turn = 0.0
        turnover_source = None
        if total_vol > 0 and avg_traded_price > 0:
            total_turn = avg_traded_price * total_vol
            turnover_source = 'BREEZE_AVG_TRADED_PRICE'
        elif total_vol > 0 and turn_cr > 0:
            candidate_turn = turn_cr * 10000000.0
            implied_vwap = candidate_turn / total_vol
            # Accept the cumulative turnover only when its implied average
            # price is plausible relative to the current traded price.
            if px > 0 and 0.50 <= (implied_vwap / px) <= 1.50:
                total_turn = candidate_turn
                turnover_source = 'BREEZE_CUMULATIVE_TURNOVER'
        # Never substitute LTP * cumulative-volume: that fabricates a VWAP.
        # If both Breeze value fields are ambiguous, retain historical seed +
        # live volume deltas, or leave VWAP unavailable (fail closed).
        if total_vol > 0 and total_turn > 0:
            st['cum_volume'] = total_vol
            st['cum_turnover'] = total_turn
            st['last_stream_volume'] = total_vol
            st['vwap_source'] = turnover_source
        else:
            previous_stream_volume = st.get('last_stream_volume')
            if previous_stream_volume is None:
                # First observed cumulative volume is a baseline, not a trade
                # delta. Historical seeding (paper mode) supplies the session
                # baseline when available.
                st['last_stream_volume'] = total_vol if total_vol > 0 else None
            else:
                delta_volume = max(0.0, total_vol - float(previous_stream_volume or 0.0))
                st['last_stream_volume'] = max(float(previous_stream_volume or 0.0), total_vol)
                if delta_volume > 0 and st.get('vwap_seeded') and float(st.get('cum_volume') or 0.0) > 0:
                    st['cum_volume'] = float(st.get('cum_volume') or 0.0) + delta_volume
                    st['cum_turnover'] = float(st.get('cum_turnover') or 0.0) + (delta_volume * px)
                    st['vwap_source'] = 'HISTORICAL_5M_SEED_PLUS_LIVE_VOLUME_DELTAS'
        vwap_now = (float(st.get('cum_turnover') or 0.0) / float(st.get('cum_volume') or 0.0)) if float(st.get('cum_volume') or 0.0) > 0 and float(st.get('cum_turnover') or 0.0) > 0 else 0.0

        # Finalize the last observed minute on rollover. Missing opening minutes
        # remain missing and prevent the ORB from becoming ready.
        if st['minute'] is not None and minute_key > st['minute'] and st.get('bar'):
            bar = st['bar']
            prev_minute = st['minute']
            close = float(bar['close'])
            bar_vwap = float(bar.get('vwap') or 0.0)
            st['last_completed_close'] = close
            st['last_completed_vwap'] = bar_vwap
            ph = (prev_minute.hour, prev_minute.minute)
            if (9, 15) <= ph < (9, 30):
                st['orb_high'] = max(float(st.get('orb_high') or 0.0), float(bar['high']))
                st['orb_minutes_seen'].add(prev_minute.strftime('%H:%M'))
            expected_minutes = {f'09:{m:02d}' for m in range(15, 30)}
            st['orb_ready'] = bool(expected_minutes.issubset(st['orb_minutes_seen']) and st['orb_high'] > 0)
            if (ph >= (9, 30) and st['orb_ready'] and
                    close > float(st.get('orb_high') or 0.0) and
                    bar_vwap > 0 and close > bar_vwap):
                st['last_signal'] = prev_minute
            st['bar'] = None

        # `high`/`low` in a Breeze quote are often session extremes. Use this
        # tick's LTP only for the local one-minute bar to avoid polluting ORB.
        if st['minute'] != minute_key:
            st['minute'] = minute_key
            st['bar'] = {'open': px, 'high': px, 'low': px, 'close': px, 'vwap': vwap_now}
        else:
            bar = st.get('bar') or {'open': px, 'high': px, 'low': px, 'close': px, 'vwap': vwap_now}
            bar['high'] = max(float(bar['high']), px)
            bar['low'] = min(float(bar['low']), px)
            bar['close'] = px
            bar['vwap'] = vwap_now
            st['bar'] = bar

        expected_minutes = {f'09:{m:02d}' for m in range(15, 30)}
        ready = bool(st.get('orb_ready', False) and expected_minutes.issubset(st['orb_minutes_seen']) and hhmm >= (9, 30))
        signal = st.get('last_signal')
        q['orb15_high'] = float(st.get('orb_high') or 0.0) or None
        q['vwap'] = vwap_now or None
        q['vwap_source'] = st.get('vwap_source')
        q['orb15_signal_time'] = signal
        q['orb15_ready'] = ready
        signal_window = bool(
            ready and signal is not None and minute_key > signal and
            (minute_key - signal).total_seconds() <= 60 and hhmm >= (9, 30)
        )
        orb_high = float(st.get('orb_high') or 0.0)
        price_above_orb = bool(orb_high > 0 and px > orb_high)
        price_above_vwap = bool(vwap_now > 0 and px > vwap_now)
        # The completed candle must confirm the breakout, and the live price must
        # still be above both ORB high and VWAP at the actual entry tick. Without
        # this second check a price reversal during the next minute could still
        # inherit a stale breakout flag and pass the entry gate.
        q['orb15_breakout'] = bool(signal_window and price_above_orb and price_above_vwap)
        if not ready:
            q['orb15_reason'] = '15M_ORB_DATA_INCOMPLETE'
        elif not signal_window:
            q['orb15_reason'] = '15M_ORB_BREAKOUT_NOT_CONFIRMED'
        elif not price_above_orb:
            q['orb15_reason'] = '15M_ORB_LIVE_PRICE_BELOW_RANGE'
        elif not price_above_vwap:
            q['orb15_reason'] = '15M_ORB_LIVE_PRICE_BELOW_VWAP'
        else:
            q['orb15_reason'] = '15M_ORB_BREAKOUT_CONFIRMED'
        return q
    except Exception:
        q['orb15_breakout'] = False
        q['orb15_ready'] = False
        q['orb15_reason'] = 'ORB_EVALUATION_ERROR'
        return q


def _normalize_breeze_stream_symbol(tick: dict, token_to_symbol: dict, valid_symbols=None) -> str:
    """Resolve Breeze stream payloads across SDK symbol/token field variants.

    Never use `stock_name` as a ticker: Breeze may put the company name there.
    The verified NSE token map remains the authority for numeric instrument IDs.
    """
    if not isinstance(tick, dict):
        return ''
    valid = {str(x).strip().upper() for x in (valid_symbols or ()) if str(x).strip()}
    def canon(value):
        value = str(value or '').strip()
        if '!' in value:
            value = value.rsplit('!', 1)[1].strip()
        # Some SDK wrappers serialize integer tokens as `12345.0`.
        if value.endswith('.0') and value[:-2].isdigit():
            value = value[:-2]
        return value
    # A few SDK versions supply the actual NSE ticker directly in stock_code.
    for field in ('stock_code', 'trading_symbol', 'ticker'):
        candidate = str(tick.get(field) or '').strip().upper()
        if candidate and (not valid or candidate in valid):
            return candidate
    for field in ('symbol', 'token', 'instrument_token', 'exchange_token'):
        raw = str(tick.get(field) or '').strip()
        if not raw:
            continue
        candidate = raw.upper()
        if valid and candidate in valid:
            return candidate
        token = canon(raw)
        mapped = token_to_symbol.get(token) or token_to_symbol.get(token.upper())
        if mapped:
            return str(mapped).strip().upper()
    return ''


def _paper_stream_quote_to_state(tick: dict) -> dict | None:
    """Normalize a Breeze live-stream equity tick for the paper engine.

    `ltq` is last-trade quantity, not session cumulative volume, so it must never
    be used as the RVOL/VWAP volume input. `close` is accepted only as a previous
    close fallback; fresh LTP still must come from a live-price field.
    """
    if not isinstance(tick, dict):
        return None
    def num(v):
        try: return float(v)
        except Exception: return 0.0
    volume_value = None
    for volume_key in ('ttq', 'total_quantity_traded', 'total_volume', 'volume'):
        candidate = tick.get(volume_key)
        if candidate not in (None, ''):
            volume_value = candidate
            break
    previous_close_value = None
    for close_key in ('previous_close', 'prev_close', 'close'):
        candidate = tick.get(close_key)
        if candidate not in (None, ''):
            previous_close_value = candidate
            break
    turnover_value = None
    for turnover_key in ('ttv', 'total_traded_value', 'total_turnover', 'turnover'):
        candidate = tick.get(turnover_key)
        if candidate not in (None, ''):
            turnover_value = candidate
            break
    return {
        'ltp': num(tick.get('last') or tick.get('ltp') or tick.get('last_traded_price')),
        'open': num(tick.get('open')),
        'high': num(tick.get('high')),
        'low': num(tick.get('low')),
        'close': num(tick.get('close')),
        'volume': num(volume_value),
        'previous_close': num(previous_close_value),
        'avg_traded_price': num(tick.get('avgPrice') or tick.get('AvgTradedPrice') or tick.get('average_traded_price')) ,
        'turnover_cr': _parse_stream_turnover_cr(turnover_value),
        'time': _parse_breeze_datetime(tick.get('ltt')) or _nse_now().replace(tzinfo=None),
        'raw': tick,
    }


def _enabled_runtime_logics():
    cfgs=load_logics()
    return [(name,cfg) for name,cfg in enabled_logics(cfgs)]


def _gtt_pretrigger_level(open_price, cfg, previous_close=None):
    """Return the percentage and price at which a live GTT should be armed.

    The GTT is submitted before the real entry level.  The real entry level
    remains unchanged, so this does not alter the strategy trigger itself.
    """
    op=float(open_price or 0)
    movement=float(cfg.get("open_movement", 0) or 0)
    gap=max(0.0, float(cfg.get("gtt_pretrigger_gap", 0.15) or 0))
    pre_pct=max(0.0, movement-gap)
    try:
        base=float(previous_close) if previous_close is not None and float(previous_close)>0 else op
    except (TypeError, ValueError):
        base=op
    return pre_pct, base*(1.0+pre_pct/100.0) if base>0 else 0.0


def _gtt_limit_price(trigger_price, cfg):
    """Keep BUY GTT limit at the target; never buffer above the Condition C cap."""
    trigger=float(trigger_price or 0)
    return round(_round_limit(trigger), 2)


def _pretrigger_entry_active(q, cfg):
    """Return the persistent pre-arm state for the -gap% entry approach.

    This deliberately checks only conditions that are safe to lock before the
    final movement: direction, opening-price band, BUY window, and the
    configured pre-trigger price. Volume/turnover are cumulative and are
    checked again immediately before a real entry/GTT is submitted.
    """
    try:
        op=float(q.get('open',0) or 0); pc=float(q.get('previous_close',0) or 0)
        ltp=float(q.get('ltp',0) or 0)
        if op<=0 or ltp<=0:
            return {'active':False,'reason':'INVALID_PRICE'}
        if not direction_ok(op,pc,cfg.get('open_direction','BOTH')):
            return {'active':False,'reason':'DIRECTION_FAIL'}
        if price_band_enabled(cfg) and not price_band_ok(op,cfg):
            return {'active':False,'reason':'PRICE_BAND_FAIL'}
        pre_pct,pre_level=_gtt_pretrigger_level(op,cfg,pc)
        return {
            'active': bool(float(cfg.get('open_movement',0) or 0)>0 and ltp>=pre_level),
            'pre_pct': pre_pct, 'pre_level': pre_level,
            'actual_level': entry_level(op,cfg,pc),
            'reason': 'PRETRIGGER_REACHED' if ltp>=pre_level else 'WAITING_PRETRIGGER'
        }
    except Exception as exc:
        return {'active':False,'reason':f'PRETRIGGER_ERROR:{type(exc).__name__}'}


def _gtt_pretrigger_qualifies(q, cfg):
    """Validate all non-movement BUY conditions before placing a GTT.

    A GTT is an execution mechanism, not a replacement for the strategy. The
    final movement/Smart trigger is still re-checked immediately before a BUY
    and by Historical replay. For smart logics, any currently available Smart
    checks are evaluated here; if they are not yet satisfied, the local trigger
    path remains responsible for the eventual BUY.
    """
    try:
        op=float(q.get('open',0) or 0); pc=float(q.get('previous_close',0) or 0)
        ltp=float(q.get('ltp',0) or 0); vol=float(q.get('volume',0) or 0); turn=float(q.get('turnover_cr',0) or 0)
        if op<=0 or ltp<=0: return False
        if not direction_ok(op, pc, cfg.get('open_direction','BOTH')): return False
        if price_band_enabled(cfg) and not price_band_ok(op,cfg): return False
        if float(cfg.get('volume',0) or 0)>0 and vol<float(cfg.get('volume',0) or 0): return False
        if float(cfg.get('turnover_cr',0) or 0)>0 and turn<float(cfg.get('turnover_cr',0) or 0): return False
        if cfg.get('smart_mode', False):
            # Do not let the broker-side GTT bypass the configured Smart rules.
            # If they are not true at arm/submission time, return False and let
            # the canonical local trigger path decide later.
            return _position_entry_matches('', q, '', cfg)
        return True
    except Exception:
        return False


def _submit_entry_gtt(breeze, sym, qty, trigger_price, cfg, logic_name):
    """Create a broker-side single-leg NSE cash GTT. No live BUY is sent here."""
    trigger=_round_limit(trigger_price)
    limit=_gtt_limit_price(trigger,cfg)
    now=datetime.now()
    # Breeze's current GTT SDK requires expiry_date even though Equity Cash
    # GTT itself has no contract expiry. For cash, use today's trade date.
    trade_date=datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000Z')
    response=broker_call(
        f'{logic_name} GTT BUY {sym}',
        breeze.gtt_single_leg_place_order,
        exchange_code='NSE', stock_code=sym, product='cash', quantity=str(int(qty)),
        expiry_date=trade_date, right='', strike_price='', gtt_type='single',
        index_or_stock='stock', trade_date=trade_date,
        order_details=[{'action':'buy','limit_price':str(limit),'trigger_price':str(trigger)}]
    )
    success=response.get('Success')
    gid=success.get('gtt_order_id') if isinstance(success,dict) else None
    if response.get('Status')!=200 or not gid:
        raise RuntimeError(f'GTT BUY was not accepted: {response}')
    return {'gtt_order_id':gid,'trigger_price':trigger,'limit_price':limit,'response':response}


def _gtt_book_snapshot(breeze):
    """Read today's NSE GTT book once; caller controls polling cadence."""
    now=datetime.now()
    utcnow=datetime.now(timezone.utc)
    start=utcnow.replace(hour=0,minute=0,second=0,microsecond=0).strftime('%Y-%m-%dT%H:%M:%S.000Z')
    end=utcnow.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    return broker_call('NSE GTT order book', breeze.gtt_order_book, exchange_code='NSE', from_date=start, to_date=end)


def _gtt_status_map(response):
    out={}
    rows=response.get('Success') if isinstance(response,dict) else []
    if not isinstance(rows,list): rows=[rows] if rows else []
    for row in rows:
        if not isinstance(row,dict): continue
        gid=str(row.get('gtt_order_id') or '').strip()
        if not gid: continue
        details=row.get('order_details') or []
        if not isinstance(details,list): details=[details]
        d=details[0] if details and isinstance(details[0],dict) else {}
        out[gid]={'status':str(d.get('status') or '').strip().lower(),
                  'fresh_order_id':row.get('fresh_order_id') or d.get('fresh_order_id'),
                  'stock_code':row.get('stock_code'),'quantity':row.get('quantity'),
                  'trigger_price':d.get('trigger_price'),'limit_price':d.get('limit_price')}
    return out


def first_upward_touch(previous_price, current_price, trigger_price, open_price) -> bool:
    """True only for a below-to-at/above-trigger transition after a valid open.

    Missing previous ticks fail closed: without proof that the prior traded price
    was below the trigger, the strategy must not chase an already-triggered share.
    """
    try:
        prev=float(previous_price); current=float(current_price)
        trigger=float(trigger_price); op=float(open_price)
        return bool(all(math.isfinite(v) and v > 0 for v in (prev,current,trigger,op))
                    and op < trigger and prev < trigger and current >= trigger)
    except (TypeError, ValueError, OverflowError):
        return False


def _ordered_condition_gate_failure(q: dict, cfg: dict) -> str | None:
    """Evaluate entry gates in the GUI-configured priority order.

    Rank order changes which gate is evaluated/reported first; every enabled
    gate remains mandatory. Budget, target, stop and daily cap are execution/risk
    controls and are enforced by their existing shared order/position paths.
    """
    q = q or {}; cfg = cfg or {}
    ranks = normalize_condition_ranks(cfg.get("condition_ranks"))
    try: px = float(q.get("ltp") or q.get("close") or 0.0)
    except Exception: px = 0.0
    try: min_px = float(cfg.get("minimum_price", 0.0) or 0.0)
    except Exception: min_px = 0.0
    try: _vwap = float(q.get("vwap") or 0.0)
    except (TypeError, ValueError, OverflowError): _vwap = 0.0
    _op = q.get("open", q.get("open_price", 0))
    _prev = q.get("previous_close", q.get("prev_close", 0))
    _vol = q.get("volume", 0)
    _turn = q.get("turnover_cr", 0)
    _band_enabled = price_band_enabled(cfg)
    _big = float(cfg.get("price_big", 0) or 0)
    _small = float(cfg.get("price_small", 0) or 0)
    _upper = max(_big, _small) if _big > 0 and _small > 0 else _big
    _lower = min(_big, _small) if _big > 0 and _small > 0 else _small
    gates = {
        "entry_time": lambda: (None if q.get("time") is None or entry_time_ok(q.get("time"), cfg) else "OUTSIDE_BUY_WINDOW"),
        "minimum_price": lambda: (None if px > 0 and (min_px <= 0 or px >= min_px) else "MINIMUM_PRICE"),
        "vwap": lambda: (None if not bool(cfg.get("smart_require_vwap", False)) or (_vwap > 0 and px > _vwap) else ("VWAP_UNAVAILABLE" if _vwap <= 0 else "PRICE_NOT_ABOVE_VWAP")),
        "orb15": lambda: (None if not bool(cfg.get("require_15m_orb", False)) or bool(q.get("orb15_breakout", False)) else str(q.get("orb15_reason") or ("15M_ORB_BREAKOUT_NOT_CONFIRMED" if q.get("orb15_ready", True) else "15M_ORB_DATA_INCOMPLETE"))),
        "rvol": lambda: _rvol_gate_reason(q, cfg),
        "movement": lambda: (
            None if (
                entry_movement_qualifies(px, _op, _prev, cfg)
                and (
                    float(cfg.get("open_movement", 0) or 0) <= 0
                    or first_upward_touch(q.get("previous_ltp", q.get("prev_ltp")), px,
                                          entry_level(_op, cfg, _prev), _op)
                )
            ) else "NOT_FIRST_UPWARD_TOUCH"
        ),
        "open_direction": lambda: (None if direction_ok(_op, _prev, cfg.get("open_direction", "BOTH")) else "OPEN_DIRECTION"),
        "price_big": lambda: (None if not _band_enabled or _upper <= 0 or (float(_op or 0) > 0 and float(_op) <= _upper) else "OPEN_PRICE_ABOVE_PRICE_BIG"),
        "price_small": lambda: (None if not _band_enabled or _lower <= 0 or (float(_op or 0) > 0 and float(_op) >= _lower) else "OPEN_PRICE_BELOW_PRICE_SMALL"),
        "turnover": lambda: (None if float(cfg.get("turnover_cr", 0) or 0) <= 0 or float(_turn or 0) >= float(cfg.get("turnover_cr", 0) or 0) else "TURNOVER_BELOW_MINIMUM"),
        "volume": lambda: (None if float(cfg.get("volume", 0) or 0) <= 0 or float(_vol or 0) >= float(cfg.get("volume", 0) or 0) else "VOLUME_BELOW_MINIMUM"),
    }
    # Option A: Trading Logics is the outer selection. For each enabled logic,
    # evaluate Rank 1, Rank 2, Rank 3... in order. Rank 0 remains mandatory
    # and follows in the stable default order. This helper is shared by all modes.
    for key in condition_rank_sequence(ranks):
        if key not in gates:
            # Investment, target, stop and max-trades are risk/execution controls;
            # they are enforced by shared sizing/position/order gates, not as
            # market-entry predicates here.
            continue
        reason = gates[key]()
        if reason:
            return reason
    return None

def _rvol_gate_reason(q: dict, cfg: dict) -> str | None:
    try: threshold = float(cfg.get("smart_min_volume_ratio", 0.0) or 0.0)
    except Exception: threshold = 0.0
    if threshold <= 0: return None
    value = q.get("runtime_relative_volume", q.get("relative_volume"))
    if value is None and float(q.get("avg_volume", 0) or 0) > 0:
        try: value = float(q.get("volume", 0) or 0) / float(q.get("avg_volume"))
        except Exception: value = None
    try: value = float(value) if value is not None else None
    except Exception: value = None
    return None if value is not None and math.isfinite(value) and value > threshold else "RVOL_NOT_ABOVE_THRESHOLD"

def canonical_tick_entry(q: dict, cfg: dict) -> dict:
    """THE canonical BUY decision used by Historical Shadow, Paper and Live.

    A market-state snapshot is normalized once, then this exact function decides
    whether a BUY signal exists.  Execution (broker order vs simulated order) is
    deliberately outside this function.
    """
    q = dict(q or {})
    # The GUI's condition rank row determines the order of these common gates.
    _gate_failure = _ordered_condition_gate_failure(q, cfg or {})
    if _gate_failure:
        return {"passed": False, "reason": _gate_failure}
    # The ordered common matcher below checks minimum price, VWAP, ORB, RVOL,
    # movement, and remaining GUI filters for every trading mode.
    # All enabled GUI gates are evaluated by the same runtime predicate, including
    # movement-only strategies. Movement-only changes the trigger/fill semantics;
    # it must not silently disable configured direction, price-band, volume,
    # turnover, MA, 52-week, or ORB filters.
    try:
        passed = bool(_position_entry_matches('_CANONICAL_', q, '_CANONICAL_', cfg))
    except Exception as exc:
        return {'passed': False, 'reason': f'ENTRY_EVALUATOR_ERROR:{type(exc).__name__}'}
    if passed:
        return {'passed': True, 'reason': 'All canonical entry conditions passed'}
    return {'passed': False, 'reason': 'Canonical entry conditions not satisfied'}


def canonical_position_exit_decision(pos: dict, q: dict) -> tuple[bool, float | None, str]:
    """THE canonical chronological EXIT decision used by Shadow/Paper/Live.

    It consumes the current traded price (LTP in Live/Paper, the historical
    1-second close in Shadow) and maintains the same trailing state. Broker
    protection/order submission is handled by the caller, never here.
    """
    pos = pos or {}
    q = q or {}
    cfg = pos.get('logic_config') or {}
    try:
        entry = float(pos.get('buy_actual_price') or pos.get('entry_price') or 0)
        ltp = float(q.get('ltp') or q.get('close') or 0)
    except Exception:
        return False, None, ''
    if entry <= 0 or ltp <= 0:
        return False, None, ''

    # Historical Shadow supplies true time-bounded 1-second candle High values.
    # Live Breeze quote `high` is usually the session high, potentially reached
    # BEFORE this BUY. Live/Paper quotes are marked by _received_monotonic, so
    # never feed their session high into post-entry trailing/weak-move state.
    # For live/paper, the observed LTP is the only safe high until we build local
    # post-entry bars from individual ticks.
    if '_received_monotonic' in q:
        candle_high = ltp
    else:
        try:
            candle_high=float(q.get('high') or ltp)
        except Exception:
            candle_high=ltp
    highest = max(float(pos.get('highest_price') or entry), ltp, candle_high)
    pos['highest_price'] = highest
    activation_pct = float(cfg.get('activation') or 0)
    trailing_pct = float(cfg.get('trailing_stop') or 0)
    initial_stop_pct = float(cfg.get('initial_stop') or 0)
    target_pct = float(cfg.get('profit_target') or 0)

    activation_level = entry * (1 + activation_pct / 100.0) if activation_pct > 0 else None
    if trailing_pct > 0 and (activation_level is None or highest >= activation_level):
        if not pos.get('trailing_active'):
            pos['trailing_active'] = True
            pos['trailing_activation_datetime'] = q.get('time')
        candidate = highest * (1 - trailing_pct / 100.0)
        pos['trailing_stop'] = max(float(pos.get('trailing_stop') or 0), candidate)
        pos['trailing_stop_datetime'] = q.get('time')
    else:
        pos.setdefault('trailing_active', False)

    target = entry * (1 + target_pct / 100.0) if target_pct > 0 else None
    initial_stop = entry * (1 - initial_stop_pct / 100.0) if initial_stop_pct > 0 else None

    # Shared weak-move protection requested in the GUI: if the post-BUY High has
    # never reached +H% from the BUY price, a fall to -S% exits immediately.
    # The condition is evaluated chronologically on the SAME canonical path in
    # Historical Shadow, Paper and Live. Strictly less-than H is intentional.
    weak_enabled = bool(cfg.get('weak_move_exit_enabled', False))
    weak_high_pct = float(cfg.get('weak_move_high_threshold') or 0)
    weak_stop_pct = float(cfg.get('weak_move_stop') or 0)
    weak_high_level = entry * (1 + weak_high_pct / 100.0) if weak_high_pct > 0 else None
    weak_stop_level = entry * (1 - weak_stop_pct / 100.0) if weak_stop_pct > 0 else None

    # Preserve the Live/Paper priority: profit target first, then the requested
    # weak-move stop, then the strongest active regular stop.
    # active stop (trailing can supersede the initial stop when higher).
    if target is not None and ltp >= target:
        pos['target_achieved'] = True
        pos['target_achieved_price'] = target
        pos['target_achieved_datetime'] = q.get('time')
        return True, target, f'{target_pct:g}% profit target'

    if weak_enabled and weak_high_level is not None and weak_stop_level is not None and highest < weak_high_level and ltp <= weak_stop_level:
        pos['weak_move_exit_achieved'] = True
        pos['weak_move_exit_price'] = weak_stop_level
        pos['weak_move_exit_datetime'] = q.get('time')
        return True, weak_stop_level, (
            f'Weak-move auto exit: post-BUY High stayed below +{weak_high_pct:g}%; '
            f'price reached -{weak_stop_pct:g}% from BUY'
        )

    active_stop = initial_stop
    stop_reason = f'{initial_stop_pct:g}% initial stop-loss' if initial_stop_pct > 0 else ''
    trail = float(pos.get('trailing_stop') or 0)
    if pos.get('trailing_active') and trail > 0 and (active_stop is None or trail > active_stop):
        active_stop = trail
        stop_reason = f'{trailing_pct:g}% trailing stop-loss'
    if active_stop is not None and ltp <= active_stop:
        if pos.get('trailing_active') and trail > 0 and trail >= float(initial_stop or 0):
            pos['trailing_stop_achieved'] = True
            pos['trailing_stop_achieved_price'] = trail
            pos['trailing_stop_achieved_datetime'] = q.get('time')
        else:
            pos['stop_achieved'] = True
            pos['stop_achieved_price'] = active_stop
            pos['stop_achieved_datetime'] = q.get('time')
        return True, active_stop, stop_reason

    price, reason = formula_exit_observation(entry, ltp, cfg)
    if price is not None:
        return True, price, reason
    return False, None, ''


_MA_FILTER_CACHE = {}
_MA_FILTER_CACHE_MAX = 4096

def _ma_filter_enabled(cfg):
    return bool(cfg.get('ma_filter_enabled', False)) and any(
        bool(cfg.get(k, False)) for k in (
            'ma_filter_200','ma_filter_100','ma_filter_20','ma_filter_5','ma_filter_cross_5'
        )
    )

def _ma_filter_values(symbol, as_of_date):
    """Return completed-daily MA values before as_of_date.

    Uses the program's cached NSE daily snapshots. It never uses the current
    intraday candle to manufacture a daily MA. Missing history causes the
    optional filter to fail closed, which is safer than inventing a value.
    """
    key=(str(symbol or '').strip().upper(), str(as_of_date))
    if key in _MA_FILTER_CACHE:
        return _MA_FILTER_CACHE[key]
    sym=key[0]
    try:
        end=date.fromisoformat(str(as_of_date)[:10]) if not isinstance(as_of_date,date) else as_of_date
    except Exception:
        return {'ok':False,'reason':'INVALID_MA_DATE'}
    closes=[]
    # Look back far enough for 200 completed sessions plus weekends/holidays.
    d=end-timedelta(days=330)
    while d < end:
        raw=_load_cached_daily(d)
        if raw is not None and not raw.empty:
            try:
                ss=_series_by_names(raw,'SYMBOL','TCKRSYMB').astype(str).str.strip().str.upper()
                cc=pd.to_numeric(_series_by_names(raw,'CLOSE_PRICE','CLOSE','CLSPRIC'),errors='coerce')
                hit=cc[ss.eq(sym)].dropna()
                if not hit.empty:
                    closes.append((d,float(hit.iloc[0])))
            except Exception:
                pass
        d += timedelta(days=1)
    closes=sorted(dict(closes).items())
    vals=[v for _,v in closes]
    out={'ok':len(vals)>=200,'closes':vals}
    if len(vals)>=5:
        ser=pd.Series(vals,dtype=float)
        out.update({
            'ma_5':float(ser.tail(5).mean()),
            'ma_20':float(ser.tail(20).mean()) if len(vals)>=20 else None,
            'ma_100':float(ser.tail(100).mean()) if len(vals)>=100 else None,
            'ma_200':float(ser.tail(200).mean()) if len(vals)>=200 else None,
        })
        # Cross within the last 5 completed sessions: 5DMA was at/below 20DMA
        # on the prior session and moved above it within the last five sessions.
        if len(vals)>=21:
            ma5=ser.rolling(5).mean()
            ma20=ser.rolling(20).mean()
            cross=False
            start=max(1,len(vals)-5)
            for i in range(start,len(vals)):
                if pd.notna(ma5.iloc[i]) and pd.notna(ma20.iloc[i]) and pd.notna(ma5.iloc[i-1]) and pd.notna(ma20.iloc[i-1]):
                    if ma5.iloc[i] > ma20.iloc[i] and ma5.iloc[i-1] <= ma20.iloc[i-1]:
                        cross=True
            out['cross_5']=cross
        else:
            out['cross_5']=False
    else:
        out.update({'ma_5':None,'ma_20':None,'ma_100':None,'ma_200':None,'cross_5':False})
    if len(_MA_FILTER_CACHE)>=_MA_FILTER_CACHE_MAX:
        _MA_FILTER_CACHE.pop(next(iter(_MA_FILTER_CACHE)))
    _MA_FILTER_CACHE[key]=out
    return out

def _ma_filter_check(symbol, as_of_date, price, cfg):
    if not _ma_filter_enabled(cfg):
        return True, {'enabled':False,'reason':'OFF'}
    vals=_ma_filter_values(symbol,as_of_date)
    try: px=float(price or 0)
    except Exception: px=0.0
    checks={}
    for key,ma_key in [('ma_filter_200','ma_200'),('ma_filter_100','ma_100'),('ma_filter_20','ma_20'),('ma_filter_5','ma_5')]:
        if bool(cfg.get(key,False)):
            ma=vals.get(ma_key)
            checks[key]=bool(vals.get('ok') and ma is not None and px>float(ma))
        else:
            checks[key]=True
    checks['ma_filter_cross_5']=bool(vals.get('cross_5')) if bool(cfg.get('ma_filter_cross_5',False)) else True
    passed=all(checks.values())
    return passed, {'enabled':True,'passed':passed,'checks':checks,'values':vals}


# Runtime rolling state for the optimized Smart Momentum strategy.  It is shared
# by Paper/Shadow/Live so the BUY evaluator sees the same derived metrics.
_RUNTIME_MOMENTUM={}
# Paper startup may happen well after 09:15. Seed 5-minute volume bars from Breeze
# history in a small worker pool so the configured RVOL rule does not have to wait
# 25+ minutes to rebuild five comparison bars from post-startup ticks.
_RUNTIME_RVOL_SEED_EXECUTOR = _RUNTIME_HISTORICAL_SEED_SCHEDULER
_RUNTIME_RVOL_SEED_LOCK = threading.Lock()

def _runtime_fetch_rvol_seed(breeze, symbol, session_time):
    """Fetch today's 5-minute bars plus prior-session same-slot RVOL baselines.

    RVOL compares the live 5-minute bucket with the median volume for that same
    time-of-day bucket across prior sessions. This avoids comparing a late-morning
    candle with the unusually large opening candles from the current session.
    VWAP is still calculated from today's candles only.
    """
    try:
        end_dt = session_time.replace(tzinfo=None) if hasattr(session_time, 'replace') else _nse_now().replace(tzinfo=None)
        today = end_dt.date()
        start_day = today - timedelta(days=14)
        start_dt = datetime.combine(start_day, datetime.min.time()).replace(hour=9, minute=15)
        rows = _paper_seed_historical_rows(breeze, symbol, '5minute', start_dt, end_dt)
        seeded = {}
        prior_by_slot = {}
        vwap_volume = 0.0
        vwap_turnover = 0.0
        vwap_used_estimate = False
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            stamp = _parse_breeze_datetime(row.get('datetime') or row.get('date'))
            try:
                vol = float(row.get('volume') or 0.0)
            except Exception:
                vol = 0.0
            if stamp is None or vol < 0:
                continue
            bucket = stamp.replace(second=0, microsecond=0, minute=(stamp.minute // 5) * 5)
            slot = bucket.strftime('%H:%M')
            if stamp.date() == today:
                seeded[bucket] = max(0.0, vol)
                if vol <= 0:
                    continue
                candle_turn = None
                for key in ('_candle_turnover_rupees', 'turnover_rupees', 'total_traded_value'):
                    try:
                        raw_turn = row.get(key)
                        if raw_turn not in (None, '') and float(raw_turn) > 0:
                            candle_turn = float(raw_turn)
                            break
                    except Exception:
                        continue
                if candle_turn is None:
                    try:
                        high = float(row.get('high') or 0.0)
                        low = float(row.get('low') or 0.0)
                        close = float(row.get('close') or 0.0)
                        typical = (high + low + close) / 3.0 if high > 0 and low > 0 and close > 0 else close
                    except Exception:
                        typical = 0.0
                    if typical <= 0:
                        continue
                    candle_turn = typical * vol
                    vwap_used_estimate = True
                vwap_volume += vol
                vwap_turnover += candle_turn
            elif stamp.date() < today and (9, 15) <= (stamp.hour, stamp.minute) < (15, 30):
                prior_by_slot.setdefault(slot, {}).setdefault(stamp.date(), 0.0)
                prior_by_slot[slot][stamp.date()] += max(0.0, vol)

        baseline_by_slot = {}
        sessions_by_slot = {}
        for slot, day_values in prior_by_slot.items():
            values = [float(v) for v in day_values.values() if float(v) > 0]
            if values:
                baseline_by_slot[slot] = float(np.median(values))
                sessions_by_slot[slot] = len(values)
        current_bucket = end_dt.replace(second=0, microsecond=0, minute=(end_dt.minute // 5) * 5)
        current_slot = current_bucket.strftime('%H:%M')
        current_slot_sessions = int(sessions_by_slot.get(current_slot, 0) or 0)
        prior_session_count = len({d for slot_days in prior_by_slot.values() for d in slot_days})
        print(
            f'PAPER RVOL HISTORICAL AUDIT: {str(symbol).upper()} '
            f'rows={len(rows or [])}; today_5m_bars={len(seeded)}; '
            f'prior_sessions_seen={prior_session_count}; '
            f'baseline_slots={len(baseline_by_slot)}; '
            f'current_slot={current_slot}; '
            f'current_slot_sessions={current_slot_sessions}/3; '
            f'vwap_volume={vwap_volume:.0f}.',
            flush=True)
        return {
            '_bars': seeded,
            '_rvol_baseline_by_slot': baseline_by_slot,
            '_rvol_sessions_by_slot': sessions_by_slot,
            '_rvol_current_bucket': current_bucket,
            '_vwap_volume': vwap_volume,
            '_vwap_turnover': vwap_turnover,
            '_vwap_source': 'HISTORICAL_5M_TYPICAL_PRICE_ESTIMATE' if vwap_used_estimate else 'HISTORICAL_5M_TURNOVER',
        }
    except Exception as exc:
        return {'_seed_error': f'{type(exc).__name__}: {exc}'}

def _paper_rvol_seed_candidates(snapshot, enabled):
    """Seed symbols near the configured movement trigger, not only exact BUY qualifiers.

    Historical RVOL is needed before the final entry decision. A zero movement
    threshold means movement is unrestricted; it must not disable history seeding.
    Final BUY movement and anti-chase rules remain unchanged.
    """
    candidates=[]
    for sym, q in (snapshot or {}).items():
        q=q or {}
        try:
            px=float(q.get('ltp') or q.get('close') or 0.0)
            op=float(q.get('open') or q.get('open_price') or 0.0)
            prev=float(q.get('previous_close') or q.get('prev_close') or 0.0)
        except Exception:
            continue
        if px <= 0 or op <= 0 or prev <= 0:
            continue
        movement=((px/prev)-1.0)*100.0
        qualifies=False
        nearest_distance=None
        for _, cfg in enabled or []:
            try:
                if not bool(cfg.get('enabled', True)) or float(cfg.get('smart_min_volume_ratio', 0) or 0) <= 0:
                    continue
                if px < float(cfg.get('minimum_price', 0) or 0):
                    continue
                required=float(cfg.get('open_movement', 0) or 0)
                # Zero means unrestricted movement, not "do not seed".
                distance=max(0.0, required-movement)
                if movement >= required - 1.5:
                    if not direction_ok(op, prev, cfg.get('open_direction', 'BOTH')):
                        continue
                    if price_band_enabled(cfg) and not price_band_ok(op, cfg):
                        continue
                    qualifies=True
                    nearest_distance=distance if nearest_distance is None else min(nearest_distance,distance)
            except Exception:
                continue
        if qualifies:
            candidates.append((nearest_distance if nearest_distance is not None else 99.0, -movement, str(sym).upper()))
    # Prioritize closest-to-trigger symbols; the shared six-worker scheduler bounds
    # concurrent history requests while all qualifying names remain eligible.
    candidates.sort()
    return {sym for _, _, sym in candidates[:128]}

def _runtime_momentum_enrich(snapshot, breeze=None, seed_symbols=None):
    """Add 15m momentum, 5m relative volume and cross-sectional rank to quotes.

    In paper mode, seed historical 5-minute bars asynchronously for near-entry
    candidates. The configured RVOL threshold remains unchanged; the baseline uses prior-session matching time slots.
    """
    now_by_sym={}
    for sym,q in (snapshot or {}).items():
        try:
            ts=q.get('time') or _nse_now()
            px=float(q.get('ltp') or 0); cum=float(q.get('volume') or 0)
            if px<=0: continue
            st=_RUNTIME_MOMENTUM.setdefault(str(sym).upper(), {'ticks':[], 'bars':{}})
            st['ticks'].append((ts,px,cum))
            cutoff=ts-timedelta(minutes=35)
            st['ticks']=[z for z in st['ticks'] if z[0]>=cutoff]
            # Schedule a one-time historical seed only for near-entry paper
            # candidates. This avoids a 25-minute cold-start lockout after restart.
            _rvol_attempts = int(st.get('rvol_seed_attempts', 0) or 0)
            _rvol_retry_at = float(st.get('rvol_seed_retry_at', 0.0) or 0.0)
            if (breeze is not None and not _remote_stop_requested() and
                    str(sym).upper() in (seed_symbols or set()) and
                    not st.get('rvol_seed_applied') and st.get('rvol_seed_future') is None and
                    _rvol_attempts < 3 and time.monotonic() >= _rvol_retry_at):
                st['rvol_seed_started']=True
                st['rvol_seed_attempts']=_rvol_attempts + 1
                # The historical request ends at this quote timestamp. Preserve its
                # cumulative-volume baseline so live volume traded while the worker
                # is fetching history can be added to the seeded in-progress candle.
                st['rvol_seed_start_cum']=cum
                st['rvol_seed_future']=_RUNTIME_RVOL_SEED_EXECUTOR.submit(
                    _runtime_fetch_rvol_seed, breeze, str(sym).upper(), ts, priority='rvol',
                    dedupe_key=f'{ts.date().isoformat()}|{str(sym).upper()}')
                print(f"PAPER RVOL WARMUP: fetching session 5-minute baseline for {str(sym).upper()}.", flush=True)
            future=st.get('rvol_seed_future')
            if future is not None and future.done() and not st.get('rvol_seed_applied'):
                try:
                    seeded=future.result()
                    # A valid same-slot baseline can arrive even when Breeze omits
                    # the still-forming/current-session 5-minute candle.  Do not
                    # discard those prior-session medians merely because _bars is
                    # empty; live cumulative-volume deltas build the current bucket.
                    if (isinstance(seeded, dict) and '_seed_error' not in seeded and
                            (seeded.get('_bars') or seeded.get('_rvol_baseline_by_slot'))):
                        # Merge live deltas collected while history was downloading
                        # into their original 5-minute buckets. This preserves volume
                        # across a candle boundary instead of assigning all waiting-time
                        # volume to whichever bucket happens to be current at completion.
                        _live_bars = dict(st.get('bars') or {})
                        _seeded_bars = dict(seeded.get('_bars') or {})
                        for _live_bucket, _live_volume in _live_bars.items():
                            try:
                                # The historical request ends at the original seed
                                # timestamp. Live deltas accumulated after that point
                                # belong on top of the historical partial candle.
                                _seeded_bars[_live_bucket] = max(0.0, float(_seeded_bars.get(_live_bucket, 0.0) or 0.0)) + max(0.0, float(_live_volume or 0.0))
                            except Exception:
                                continue
                        st['bars'] = _seeded_bars
                        st['rvol_baseline_by_slot'] = dict(seeded.get('_rvol_baseline_by_slot') or {})
                        st['rvol_sessions_by_slot'] = dict(seeded.get('_rvol_sessions_by_slot') or {})
                        _seed_bucket=ts.replace(second=0,microsecond=0,minute=(ts.minute//5)*5)
                        if _seed_bucket not in st['bars']:
                            st['bars'][_seed_bucket]=0.0
                        # Keep the last live cumulative-volume checkpoint. The normal
                        # delta path below will add only the current tick's new volume;
                        # on the first tick, fall back to the seed-start checkpoint.
                        try:
                            _last_cum = st.get('last_cum')
                            st['last_cum'] = float(_last_cum if _last_cum is not None else st.get('rvol_seed_start_cum', cum))
                        except Exception:
                            st['last_cum'] = cum
                        # Seed the same session VWAP state used by the live tick
                        # normalizer. The current cumulative stream volume is the
                        # delta baseline, preventing double-counting after seeding.
                        _vwap_state = _LIVE_ORB15_STATE.get((ts.date().isoformat(), str(sym).upper()))
                        _seed_v = float(seeded.get('_vwap_volume') or 0.0)
                        _seed_t = float(seeded.get('_vwap_turnover') or 0.0)
                        if (_vwap_state is not None and _seed_v > 0 and _seed_t > 0 and
                                _vwap_state.get('vwap_source') != 'BREEZE_CUMULATIVE_TURNOVER'):
                            _vwap_state['cum_volume'] = _seed_v
                            _vwap_state['cum_turnover'] = _seed_t
                            _vwap_state['last_stream_volume'] = cum
                            _vwap_state['vwap_seeded'] = True
                            _vwap_state['vwap_source'] = str(seeded.get('_vwap_source') or 'HISTORICAL_5M_SEED')
                            q['vwap'] = _seed_t / _seed_v
                            q['vwap_source'] = _vwap_state['vwap_source']
                        st['rvol_seed_applied']=True
                        st['rvol_seed_future']=None
                        _baseline_slots = len(st.get('rvol_baseline_by_slot') or {})
                        _baseline_sessions = max((int(v) for v in (st.get('rvol_sessions_by_slot') or {}).values()), default=0)
                        _current_slot = _seed_bucket.strftime('%H:%M')
                        _current_slot_sessions = int((st.get('rvol_sessions_by_slot') or {}).get(_current_slot) or 0)
                        print(
                            f"PAPER RVOL/VWAP READY: {str(sym).upper()} seeded "
                            f"{len(st['bars'])} current-session 5-minute bars; "
                            f"same-slot baselines={_baseline_slots}; "
                            f"current_slot={_current_slot}; "
                            f"current_slot_sessions={_current_slot_sessions}/3; "
                            f"max_sessions={_baseline_sessions}; "
                            f"VWAP source={seeded.get('_vwap_source')}; "
                            f"RVOL remains fail-closed unless current slot has >=3 prior sessions.",
                            flush=True)
                    else:
                        st['rvol_seed_failed']=True
                        st['rvol_seed_future']=None
                        reason=seeded.get('_seed_error','no historical 5-minute candles returned') if isinstance(seeded,dict) else 'invalid historical response'
                        if int(st.get('rvol_seed_attempts', 0)) < 3:
                            st['rvol_seed_retry_at']=time.monotonic()+20.0
                            print(f'PAPER RVOL/VWAP WARMUP WARNING: {str(sym).upper()} history seed unavailable ({reason}); retry scheduled in 20s; RVOL remains fail-closed.', flush=True)
                        else:
                            st['rvol_seed_applied']=True
                            print(f'PAPER RVOL/VWAP WARMUP WARNING: {str(sym).upper()} history seed unavailable after 3 attempts ({reason}); RVOL remains fail-closed.', flush=True)
                except Exception as seed_exc:
                    if isinstance(seed_exc, _HistoricalSeedQueueFull):
                        st['rvol_seed_attempts'] = max(0, int(st.get('rvol_seed_attempts', 0) or 0) - 1)
                        st['rvol_seed_retry_at'] = time.monotonic() + 2.0
                        st['rvol_seed_future'] = None
                        print(f'PAPER RVOL WARMUP DEFERRED: {str(sym).upper()} scheduler queue full; retry scheduled in 2s; no API attempt consumed.', flush=True)
                        continue
                    st['rvol_seed_failed']=True
                    st['rvol_seed_future']=None
                    if int(st.get('rvol_seed_attempts', 0)) < 3:
                        st['rvol_seed_retry_at']=time.monotonic()+20.0
                        print(f'PAPER RVOL/VWAP WARMUP WARNING: {str(sym).upper()} history seed failed ({type(seed_exc).__name__}: {seed_exc}); retry scheduled in 20s; RVOL remains fail-closed.', flush=True)
                    else:
                        st['rvol_seed_applied']=True
                        print(f'PAPER RVOL/VWAP WARMUP WARNING: {str(sym).upper()} history seed failed after 3 attempts ({type(seed_exc).__name__}: {seed_exc}); RVOL remains fail-closed.', flush=True)
            # Aggregate cumulative-volume deltas into 5-minute buckets.
            bars=st['bars']; bucket=ts.replace(second=0,microsecond=0,minute=(ts.minute//5)*5)
            prev_cum=st.get('last_cum')
            delta=max(0.0,cum-float(prev_cum or cum)) if prev_cum is not None else 0.0
            if bucket not in bars: bars[bucket]=0.0
            bars[bucket]+=delta
            st['last_cum']=cum
            # Keep at least 20 completed 5-minute candles for a stable RVOL
            # median. Tick history remains limited to 35 minutes above; bar history
            # is retained independently for 105 minutes (21 five-minute buckets).
            bar_cutoff = ts - timedelta(minutes=105)
            oldb=bar_cutoff.replace(second=0,microsecond=0,minute=(bar_cutoff.minute//5)*5)
            st['bars']={k:v for k,v in bars.items() if k>=oldb}
            target=ts-timedelta(minutes=15)
            prior=None
            for t0,p0,_ in st['ticks']:
                if t0<=target: prior=(t0,p0)
            mom=((px/prior[1])-1.0)*100.0 if prior and prior[1]>0 else None
            curbar=float(st['bars'].get(bucket,0.0))
            # Compare against the median volume for this exact 5-minute slot
            # across prior sessions, not against earlier (usually more active)
            # opening candles from today. Require at least three prior sessions.
            elapsed_seconds = max(0.0, min(300.0, (ts - bucket).total_seconds()))
            elapsed_fraction = elapsed_seconds / 300.0
            slot_key = bucket.strftime('%H:%M')
            baseline = float((st.get('rvol_baseline_by_slot') or {}).get(slot_key) or 0.0)
            baseline_sessions = int((st.get('rvol_sessions_by_slot') or {}).get(slot_key) or 0)
            rel=(curbar / (baseline * elapsed_fraction)) if (
                baseline > 0 and baseline_sessions >= 3 and elapsed_seconds >= 60.0
            ) else None
            now_by_sym[str(sym).upper()] = {
                'momentum_15_pct': mom,
                'relative_volume': rel,
                'rvol_current_bucket_volume': curbar,
                'rvol_same_slot_baseline': baseline,
                'rvol_baseline_sessions': baseline_sessions,
                'rvol_elapsed_seconds': elapsed_seconds,
            }
        except Exception:
            now_by_sym[str(sym).upper()]={'momentum_15_pct':None,'relative_volume':None}
    momentum_values={sym:m.get('momentum_15_pct') for sym,m in now_by_sym.items() if m.get('momentum_15_pct') is not None}
    movement_values={}
    for sym in now_by_sym:
        q=snapshot.get(sym) or snapshot.get(sym.upper())
        try:
            px=float((q or {}).get('ltp') or (q or {}).get('close') or 0)
            pc=float((q or {}).get('previous_close') or 0)
            if px>0 and pc>0: movement_values[sym]=(px/pc-1.0)*100.0
        except Exception: pass
    def _rank_desc(values, symbol):
        value=values.get(symbol)
        if value is None: return None
        return 1 + sum(1 for other in values.values() if other is not None and other>value)
    for sym,m in now_by_sym.items():
        mom=m.get('momentum_15_pct')
        rank=_rank_desc(momentum_values,sym)
        movement_rank=_rank_desc(movement_values,sym)
        q=snapshot.get(sym) or snapshot.get(sym.upper())
        if q is not None:
            q['momentum_15_pct']=mom
            q['avg_volume']=0.0
            q['runtime_relative_volume']=m.get('relative_volume')
            q['rvol_current_bucket_volume']=m.get('rvol_current_bucket_volume')
            q['rvol_same_slot_baseline']=m.get('rvol_same_slot_baseline')
            q['rvol_baseline_sessions']=m.get('rvol_baseline_sessions')
            q['rvol_elapsed_seconds']=m.get('rvol_elapsed_seconds')
            q['momentum_rank']=rank
            q['rank_by_ranks']={'movement':movement_rank,'momentum_15m':rank}
            q['momentum_rank_pct']=(100.0*(len(momentum_values)-rank+1)/len(momentum_values)) if momentum_values and rank is not None else None
            # Canonical Smart evaluator expects cumulative volume plus a baseline.
            # Convert the relative-volume ratio into an equivalent baseline when available.
            if m.get('relative_volume') and float(m['relative_volume'])>0:
                q['avg_volume']=float(q.get('volume') or 0)/float(m['relative_volume'])
    return snapshot

_PAPER_ENTRY_AUDIT_LAST = 0.0

def _paper_entry_audit(snapshot, enabled, interval_seconds=30.0):
    """Print real per-symbol entry inputs for the strongest near-trigger paper candidates."""
    global _PAPER_ENTRY_AUDIT_LAST
    now_mono = time.monotonic()
    if now_mono - _PAPER_ENTRY_AUDIT_LAST < float(interval_seconds):
        return
    _PAPER_ENTRY_AUDIT_LAST = now_mono
    rows = []
    for sym, q in (snapshot or {}).items():
        q = q or {}
        try:
            px = float(q.get('ltp') or 0.0)
            op = float(q.get('open') or 0.0)
            prev = float(q.get('previous_close') or 0.0)
            if px <= 0 or op <= 0 or prev <= 0:
                continue
            movement = ((px / prev) - 1.0) * 100.0
            for logic_name, cfg in enabled or []:
                if not bool(cfg.get('enabled', True)):
                    continue
                min_px = float(cfg.get('minimum_price', 0.0) or 0.0)
                required_move = float(cfg.get('open_movement', 0.0) or 0.0)
                if px < min_px or movement < max(0.0, required_move - 1.5):
                    continue
                vwap = float(q.get('vwap') or 0.0)
                rvol = q.get('runtime_relative_volume', q.get('relative_volume'))
                try: rvol = float(rvol) if rvol is not None else None
                except Exception: rvol = None
                first_gate = _ordered_condition_gate_failure(q, cfg)
                _cur_vol = q.get('rvol_current_bucket_volume')
                _slot_base = q.get('rvol_same_slot_baseline')
                _slot_days = q.get('rvol_baseline_sessions')
                try:
                    _rvol_threshold = max(0.0, float(cfg.get('smart_min_volume_ratio', 0) or 0))
                except Exception:
                    _rvol_threshold = 0.0
                if _rvol_threshold <= 0:
                    _rvol_detail = 'RVOL=DISABLED'
                else:
                    _rvol_detail = f"RVOL={rvol:.2f}" if rvol is not None else 'RVOL=NA'
                    if _cur_vol is not None and _slot_base is not None:
                        _rvol_detail += (
                            f"(cur={float(_cur_vol):.0f}/same_slot_med={float(_slot_base):.0f},"
                            f"days={int(_slot_days or 0)})"
                        )
                rows.append((movement, f"{logic_name}/{str(sym).upper()} LTP={px:.2f} VWAP={vwap:.2f} gap={((px/vwap-1)*100):+.2f}%" if vwap > 0 else f"{logic_name}/{str(sym).upper()} LTP={px:.2f} VWAP=NA" ,
                             f"move={movement:.2f}%/{required_move:.2f}% {_rvol_detail}/{float(cfg.get('smart_min_volume_ratio',0) or 0):.2f} min={min_px:.2f} VWAP_source={q.get('vwap_source') or 'NONE'} first_gate={first_gate or 'PASS'}"))
        except Exception:
            continue
    rows.sort(key=lambda x: x[0], reverse=True)
    if not rows:
        print('PAPER ENTRY AUDIT: no live symbol above minimum price and within 1.5 percentage points of its configured movement trigger yet.', flush=True)
        return
    print('PAPER ENTRY AUDIT: strongest near-trigger symbols (diagnostic only; no thresholds changed):', flush=True)
    for _, head, detail in rows[:6]:
        print(f'  {head} | {detail}', flush=True)

_ENTRY_DIAG_STATE = {'last_report': 0.0, 'counts': {}, 'symbols': {}}
_ENTRY_DIAG_LOCK = threading.Lock()
_ENTRY_REASON_LABELS = {
    'MINIMUM_PRICE': 'Share price below the configured minimum',
    'PRICE_NOT_ABOVE_VWAP': 'Price is not above VWAP',
    'VWAP_UNAVAILABLE': 'VWAP data is unavailable',
    'RVOL_NOT_ABOVE_THRESHOLD': 'Relative volume is below the required level',
    'MA_FILTER': 'Moving-average condition not satisfied',
    '15M_ORB_BREAKOUT_NOT_CONFIRMED': '15-minute opening-range breakout not confirmed',
    '15M_ORB_DATA_INCOMPLETE': '15-minute opening-range data incomplete after warm-up attempts',
    '15M_ORB_WARMUP_PENDING': '15-minute opening-range history warm-up/retry in progress',
    '15M_ORB_WARMUP_NOT_STARTED': '15-minute opening-range history not fetched yet (candidate queue)',
    '52W_BREAKOUT_NOT_CONFIRMED': '52-week breakout/breakdown not confirmed',
    '52W_HISTORY_UNAVAILABLE': '52-week history unavailable',
    'RANK_DATA_UNAVAILABLE': 'Ranking data unavailable',
    'RANK_BELOW_REQUIRED_POSITION': 'Stock rank is outside the configured top positions',
    'ENTRY_EVALUATOR_ERROR': 'Entry-condition evaluator error',
}

def _entry_diag_record(logic_name, reason, symbol):
    """Collect unique rejected symbols by logic and reason, then print a concise report.

    This is diagnostic-only: it does not change any BUY filter, ranking, or order path.
    Counts represent unique symbols rejected during each reporting window, not repeated
    quote ticks, so the biggest blockers are easy to identify.
    """
    now = time.monotonic()
    logic = str(logic_name or 'Unknown Logic')
    code = str(reason or 'ENTRY_FILTERS')
    sym = str(symbol or 'UNKNOWN').upper()
    key = (logic, code)
    with _ENTRY_DIAG_LOCK:
        state = _ENTRY_DIAG_STATE
        bucket = state['symbols'].setdefault(key, set())
        bucket.add(sym)
        state['counts'][key] = len(bucket)
        if now - state['last_report'] < 30.0 or not state['counts']:
            return
        rows = sorted(state['counts'].items(), key=lambda item: (-item[1], item[0][0], item[0][1]))
        report = [(k, n, sorted(state['symbols'].get(k, set()))) for k, n in rows[:12]]
        state['counts'].clear()
        state['symbols'].clear()
        state['last_report'] = now
    print('\n========== NOT BUY REASON REPORT (last ~30 seconds) ==========')
    for (logic_name, code), count, names in report:
        label = _ENTRY_REASON_LABELS.get(code, code.replace('_', ' ').capitalize())
        shown = ', '.join(names[:18])
        extra = f' (+{len(names)-18} more)' if len(names) > 18 else ''
        print(f'{logic_name} | NOT BUY: {count} unique share(s) | {label} [{code}] | Shares: {shown}{extra}')
    print('==============================================================\n', flush=True)

def _rank_gate_passes(q, cfg, logic_name='', sym=''):
    """Apply the configured cross-sectional rank only after other entry filters pass."""
    try:
        limit=int(float((cfg or {}).get('smart_rank_limit',0) or 0))
    except Exception:
        limit=-1
    if limit < 0 or limit > 5000:
        _entry_diag_record(logic_name, 'INVALID_RANK_SETTING', sym)
        return False
    if limit == 0:
        return True
    rank_by=str((cfg or {}).get('smart_rank_by','movement') or 'movement').strip().lower()
    if rank_by not in ('movement','momentum_15m'):
        _entry_diag_record(logic_name, 'INVALID_RANK_BY_SETTING', sym)
        return False
    rank=(q or {}).get('rank_by_ranks') or {}
    rank_num=rank.get(rank_by)
    if rank_num is None and rank_by=='momentum_15m':
        rank_num=(q or {}).get('momentum_rank')
    try:
        rank_num=int(float(rank_num)) if rank_num is not None else None
    except Exception:
        rank_num=None
    if rank_num is None or rank_num < 1:
        _entry_diag_record(logic_name, 'RANK_DATA_UNAVAILABLE', sym)
        return False
    if rank_num > limit:
        _entry_diag_record(logic_name, 'RANK_BELOW_REQUIRED_POSITION', sym)
        return False
    return True


def _position_entry_matches(sym, q, logic_name, cfg):
    """Canonical runtime BUY evaluator with ordered GUI gates and rate-limited reasons."""
    q = q or {}
    cfg = cfg or {}
    _ordered_failure = _ordered_condition_gate_failure(q, cfg)
    if _ordered_failure:
        _entry_diag_record(logic_name, _ordered_failure, sym)
        return False
    try:
        _px=float(q.get('ltp') or q.get('close') or 0.0)
        _min_px=float(cfg.get('minimum_price', 0.0) or 0.0)
    except Exception:
        _px, _min_px = 0.0, 0.0
    # 1. Minimum share price.
    if _px <= 0 or (_min_px > 0 and _px < _min_px):
        _entry_diag_record(logic_name, 'MINIMUM_PRICE', sym)
        return False
    # 3. Above VWAP. Missing VWAP is a hard reject when the GUI checkbox is on.
    if bool(cfg.get('smart_require_vwap', False)):
        try:
            _vwap=float(q.get('vwap') or 0.0)
        except Exception:
            _vwap=0.0
        if _vwap <= 0:
            _entry_diag_record(logic_name, 'VWAP_UNAVAILABLE', sym)
            return False
        if _px <= _vwap:
            _entry_diag_record(logic_name, 'PRICE_NOT_ABOVE_VWAP', sym)
            return False
    _asof=q.get('time')
    # 4. Completed 15-minute opening range breakout.
    if bool(cfg.get('require_15m_orb', False)) and not bool(q.get('orb15_breakout', False)):
        _orb_reason = str((q or {}).get('orb15_reason') or ('15M_ORB_BREAKOUT_NOT_CONFIRMED' if (q or {}).get('orb15_ready', True) else '15M_ORB_DATA_INCOMPLETE'))
        _entry_diag_record(logic_name, _orb_reason, sym)
        return False
    # 5. Relative volume must be strictly greater than the configured threshold.
    try:
        _rvol_min=float(cfg.get('smart_min_volume_ratio', 0.0) or 0.0)
    except Exception:
        _rvol_min=0.0
    if _rvol_min > 0:
        _rvol=q.get('runtime_relative_volume', q.get('relative_volume'))
        if _rvol is None and float(q.get('avg_volume', 0) or 0) > 0:
            try: _rvol=float(q.get('volume', 0) or 0) / float(q.get('avg_volume'))
            except Exception: _rvol=None
        try: _rvol=float(_rvol) if _rvol is not None else None
        except Exception: _rvol=None
        if _rvol is None or not math.isfinite(_rvol) or not (_rvol > _rvol_min):
            _entry_diag_record(logic_name, 'RVOL_NOT_ABOVE_THRESHOLD', sym)
            return False
    try:
        _asof_date=_asof.date() if hasattr(_asof,'date') else date.fromisoformat(str(_asof)[:10])
    except Exception:
        _asof_date=_nse_now().date()
    _ma_ok,_ma_audit=_ma_filter_check(sym,_asof_date,q.get('ltp',0),cfg)
    if not _ma_ok:
        _entry_diag_record(logic_name, 'MA_FILTER', sym)
        return False
    # 52-week gates are shared by Historical, Paper, Paper Shadow and Live.
    # When both boxes are enabled they mean either valid extreme breakout; when
    # only one is enabled, that exact direction is required. Missing history fails closed.
    _need_52h=bool(cfg.get('require_52w_high_breakout', False))
    _need_52l=bool(cfg.get('require_52w_low_breakdown', False))
    if _need_52h or _need_52l:
        _px52=float(q.get('ltp') or q.get('close') or 0)
        _h52=q.get('52_week_high') or q.get('52w_high')
        _l52=q.get('52_week_low') or q.get('52w_low')
        if _h52 is None or _l52 is None:
            _h52,_l52,_sessions=get_52_week_levels(sym or q.get('symbol',''), _asof)
        try: _h52=float(_h52) if _h52 not in (None,'') else None
        except Exception: _h52=None
        try: _l52=float(_l52) if _l52 not in (None,'') else None
        except Exception: _l52=None
        _high_signal=bool(_h52 and _h52>0 and _px52>_h52)
        _low_signal=bool(_l52 and _l52>0 and _px52<_l52)
        _passed_52=(_high_signal if _need_52h else False) or (_low_signal if _need_52l else False)
        if not _passed_52:
            _reason='52W_HISTORY_UNAVAILABLE' if ((_need_52h and not _h52) or (_need_52l and not _l52)) else '52W_BREAKOUT_NOT_CONFIRMED'
            _entry_diag_record(logic_name, _reason, sym)
            return False
    check=evaluate_entry_conditions(
        q.get('ltp',0), q.get('open',0), q.get('previous_close',0),
        q.get('volume',0), q.get('turnover_cr',0), cfg
    )
    if not check.get('passed'):
        _entry_diag_record(logic_name, str(check.get('reason') or 'ENTRY_FILTERS'), sym)
    # A passed movement-only common-filter check is not a rejection.
    # Keep it out of the rejection diagnostic so the console cannot report
    # "rejected candidates" for a candidate that actually passed this gate.
    if not check.get('passed'):
        return False
    return _rank_gate_passes(q,cfg,logic_name,sym)


def _paper_session_net_pnl(completed, positions, snapshot) -> float:
    """Estimate current paper-session net P&L including round-trip ICICI charges.

    Closed trades use their simulated fill prices. Open trades are marked to the
    freshest available live LTP, falling back to the last recorded quote only if
    a fresh snapshot is not yet available. This helper never contacts the broker.
    """
    total = 0.0

    def net_for(qty, buy_price, sell_price):
        try:
            qty = int(float(qty or 0))
            buy_price = float(buy_price or 0.0)
            sell_price = float(sell_price or 0.0)
        except (TypeError, ValueError, OverflowError):
            return 0.0
        if qty <= 0 or buy_price <= 0 or sell_price <= 0:
            return 0.0
        buy_value = qty * buy_price
        sell_value = qty * sell_price
        fees = icici_intraday_charges(buy_value, sell_value, exchange='NSE').get('total_charges', 0.0)
        return sell_value - buy_value - float(fees or 0.0)

    for pos in (completed or []):
        total += net_for(pos.get('sell_actual_qty') or pos.get('quantity'),
                         pos.get('buy_actual_price') or pos.get('limit_price'),
                         pos.get('sell_actual_price'))
    for pos in (positions or {}).values():
        sym = str(pos.get('symbol') or '').strip().upper()
        q = (snapshot or {}).get(sym) or {}
        try:
            fresh = (time.monotonic() - float(q.get('_received_monotonic') or 0.0)) <= 30.0
        except (TypeError, ValueError, OverflowError):
            fresh = False
        mark = q.get('ltp') if fresh and float(q.get('ltp') or 0.0) > 0 else pos.get('close')
        total += net_for(pos.get('buy_actual_qty') or pos.get('quantity'),
                         pos.get('buy_actual_price') or pos.get('limit_price'), mark)
    return round(total, 2)



def _paper_daily_loss_triggered(net_pnl, max_daily_loss) -> bool:
    """Return True when net PAPER session P&L reaches the configured loss cap."""
    try:
        cap = max(0.0, float(max_daily_loss or 0.0))
        pnl = float(net_pnl)
    except (TypeError, ValueError, OverflowError):
        return False
    return cap > 0.0 and pnl <= -cap


def _manage_paper_position(pos, q):
    """Manage a paper-live position through the canonical exit evaluator."""
    should_exit, price, reason = canonical_position_exit_decision(pos, q)
    return should_exit, price, reason




PAPER_ENGINE_STATE = APP_DATA / 'paper_live_engine_state.json'


def _paper_state_json_default(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, set):
        return sorted(value)
    if hasattr(value, 'item'):
        try:
            return value.item()
        except Exception:
            pass
    return str(value)


def _paper_state_restore_dates(row):
    if not isinstance(row, dict):
        return row
    for key in ('buy_trade_datetime', 'sell_trade_datetime', 'trade_date',
                'gtt_armed_datetime', 'gtt_triggered_datetime'):
        value = row.get(key)
        if isinstance(value, str) and value:
            try:
                row[key] = date.fromisoformat(value[:10]) if key == 'trade_date' else datetime.fromisoformat(value)
            except Exception:
                pass
    return row


def _save_paper_engine_state(trade_date, positions, completed, counts, total_buys, triggered, daily_loss_locked=False):
    """Atomically checkpoint PAPER positions so a process restart cannot erase them."""
    try:
        APP_DATA.mkdir(parents=True, exist_ok=True)
        payload = {
            'version': 1,
            'trade_date': str(trade_date)[:10],
            'updated_at': datetime.now().isoformat(timespec='seconds'),
            'positions': [{'key': str(k), 'position': v} for k, v in (positions or {}).items()],
            'completed': list(completed or []),
            'counts': dict(counts or {}),
            'total_buys': int(total_buys or 0),
            'triggered': sorted(str(x) for x in (triggered or set())),
            'daily_loss_locked': bool(daily_loss_locked),
        }
        tmp = PAPER_ENGINE_STATE.with_suffix('.json.tmp')
        with tmp.open('w', encoding='utf-8') as fh:
            json.dump(payload, fh, ensure_ascii=False, default=_paper_state_json_default)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, PAPER_ENGINE_STATE)
        return True
    except Exception as exc:
        print(f'PAPER STATE SAVE WARNING: {type(exc).__name__}: {exc}', flush=True)
        return False


def _load_paper_engine_state(trade_date):
    """Load only a same-session checkpoint; never silently carry PAPER positions overnight."""
    try:
        if not PAPER_ENGINE_STATE.exists():
            return None
        with PAPER_ENGINE_STATE.open('r', encoding='utf-8') as fh:
            payload = json.load(fh)
        if not isinstance(payload, dict) or payload.get('version') != 1:
            print('PAPER STATE RESTORE WARNING: unsupported checkpoint format; starting with an empty session.', flush=True)
            return None
        if str(payload.get('trade_date') or '')[:10] != str(trade_date)[:10]:
            print(
                f'PAPER STATE RESTORE WARNING: checkpoint belongs to {payload.get("trade_date")}; '
                f'current session is {trade_date}. Old positions are NOT auto-carried overnight; '
                'review the Paper Live Trading Shares ledger.',
                flush=True,
            )
            return None
        positions = {}
        for item in payload.get('positions') or []:
            if not isinstance(item, dict) or not isinstance(item.get('position'), dict):
                continue
            pos = _paper_state_restore_dates(item['position'])
            key = str(item.get('key') or f'{pos.get("logic", "")}|{pos.get("symbol", "")}')
            if pos.get('symbol') and pos.get('buy_actual_price') and pos.get('quantity'):
                positions[key] = pos
        completed = [_paper_state_restore_dates(x) for x in (payload.get('completed') or []) if isinstance(x, dict)]
        return {
            'positions': positions,
            'completed': completed,
            'counts': payload.get('counts') if isinstance(payload.get('counts'), dict) else {},
            'total_buys': int(payload.get('total_buys') or 0),
            'triggered': set(str(x) for x in (payload.get('triggered') or [])),
            'daily_loss_locked': bool(payload.get('daily_loss_locked', False)),
            'updated_at': payload.get('updated_at'),
        }
    except Exception as exc:
        print(f'PAPER STATE RESTORE WARNING: {type(exc).__name__}: {exc}; starting empty only because checkpoint could not be read.', flush=True)
        return None


def _wait_for_next_paper_live_session(mode_label: str):
    """Wait for the next NSE session using the authoritative IST market clock.

    The desktop machine's local timezone must never control trading-session timing.
    This function deliberately uses Asia/Kolkata for weekends, holidays, 09:15 open
    and 15:25 square-off.
    """
    while True:
        now=_nse_now()
        if _is_nse_trading_day(now.date()) and now.time() < datetime.strptime('15:25','%H:%M').time():
            print(f'AUTO {mode_label}: NSE clock={now:%Y-%m-%d %H:%M:%S %Z}; session available.')
            return now
        d=now.date()+timedelta(days=1)
        while not _is_nse_trading_day(d):
            d += timedelta(days=1)
        target=datetime.combine(d,datetime.min.time(),tzinfo=now.tzinfo).replace(hour=9,minute=15)
        wait=max(0,(target-now).total_seconds())
        print(f'AUTO {mode_label}: NSE clock={now:%Y-%m-%d %H:%M:%S %Z}; current session unavailable; waiting for next NSE session at {target:%Y-%m-%d %H:%M:%S %Z}.')
        time.sleep(min(60,max(1,wait)))

def _smart_subscribe_all_nse(breeze, symbols, symbol_to_token, label):
    """Subscribe to every available NSE equity token in batches and retry failures.

    Breeze supports subscribing to multiple stock tokens in one call. We deliberately
    avoid the old arbitrary 2,000-symbol truncation: coverage is attempted for the
    complete security master. Failed subscriptions are returned for retry/watchdog use.
    """
    import time as _time
    subscribed=set(); failed=set()
    items=[s for s in symbols if s in symbol_to_token]
    for i in range(0,len(items),100):
        batch=items[i:i+100]
        tokens=[f'4.1!{symbol_to_token[s]}' for s in batch]
        try:
            resp=breeze.subscribe_feeds(stock_token=tokens,get_exchange_quotes=True,get_market_depth=False)
            if isinstance(resp,str) and 'Exception' in resp:
                raise RuntimeError(resp)
            if isinstance(resp,dict) and (resp.get('Status') not in (None, 200) or resp.get('Error')):
                raise RuntimeError(str(resp.get('Error') or resp))
            subscribed.update(batch)
        except Exception as exc:
            # A session-wide authentication failure cannot be fixed by issuing
            # thousands of individual subscribe calls. Mark this batch failed and
            # let the watchdog recover the session with bounded retries.
            if 'authenticat' in str(exc).lower() or 'token and keys' in str(exc).lower():
                # Authentication rejection is session-wide: do not hammer the
                # endpoint with the remaining batches using the same credentials.
                failed.update(items[i:])
                _detail = re.sub(r'(?i)(api[_ -]?key|session[_ -]?token|token)\\s*[:=]\\s*[^,; ]+', r'\\1=***', str(exc))
                print(f'{label}: subscription authentication rejected; SDK error={_detail[:400]}; stopped batch loop and deferred {len(items)-len(subscribed)} remaining symbols.', flush=True)
                try:
                    _session = breeze.get_customer_details(api_session=os.getenv('BREEZE_SESSION_TOKEN', ''))
                    _ok = isinstance(_session, dict) and _session.get('Status') == 200 and bool(_session.get('Success'))
                    print(f'{label}: REST session check after subscription rejection: authenticated={_ok}; status={_session.get("Status") if isinstance(_session, dict) else type(_session).__name__}.', flush=True)
                except Exception as _diag:
                    print(f'{label}: REST session check failed: {type(_diag).__name__}: {str(_diag)[:240]}', flush=True)
                break
            # Fall back to individual subscriptions only for a batch-specific issue.
            for sym in batch:
                try:
                    resp=breeze.subscribe_feeds(stock_token=f'4.1!{symbol_to_token[sym]}',get_exchange_quotes=True,get_market_depth=False)
                    if isinstance(resp,str) and 'Exception' in resp: raise RuntimeError(resp)
                    subscribed.add(sym)
                except Exception as inner:
                    failed.add(sym)
                    print(f'{label} subscribe failed {sym}: {inner}')
            print(f'{label} batch subscribe warning: {exc}')
    print(f'{label}: subscribed {len(subscribed)}/{len(items)} NSE scripts; initial failures={len(failed)}')
    return subscribed, failed


def _smart_retry_failed_subscriptions(breeze, failed, symbol_to_token, label):
    """Retry failed subscriptions, but stop immediately on a session-wide auth failure."""
    recovered=set()
    for sym in list(failed):
        try:
            resp=breeze.subscribe_feeds(stock_token=f'4.1!{symbol_to_token[sym]}',get_exchange_quotes=True,get_market_depth=False)
            if isinstance(resp,str) and 'Exception' in resp:
                if 'authenticat' in resp.lower() or 'token and keys' in resp.lower():
                    print(f'{label}: retry paused; Breeze rejected feed authentication. Refresh/check the session token and API key; {len(failed)} subscriptions remain pending.')
                    return recovered
                continue
            if isinstance(resp,dict) and (resp.get('Error') or resp.get('Status') not in (None,200)):
                err=str(resp.get('Error') or resp)
                if 'authenticat' in err.lower() or 'token and keys' in err.lower():
                    print(f'{label}: retry paused; Breeze rejected feed authentication. Refresh/check the session token and API key; {len(failed)} subscriptions remain pending.')
                    return recovered
                continue
            recovered.add(sym)
        except Exception as exc:
            if 'authenticat' in str(exc).lower() or 'token and keys' in str(exc).lower():
                print(f'{label}: retry paused; Breeze rejected feed authentication. Refresh/check the session token and API key; {len(failed)} subscriptions remain pending.')
                return recovered
    failed.difference_update(recovered)
    if recovered:
        print(f'{label}: recovered {len(recovered)} feed subscriptions; remaining failures={len(failed)}')
    return recovered


def _smart_reconnect_feeds(breeze, symbols, symbol_to_token, label):
    """Reconnect and restore subscriptions; propagate session-wide failures."""
    try:
        breeze.ws_disconnect()
    except Exception:
        pass
    time.sleep(1.0)
    result=breeze.ws_connect()
    if isinstance(result, str) and ('Exception' in result or 'error' in result.lower()):
        raise RuntimeError(f'WebSocket reconnect did not succeed: {result}')
    subscribed, failed = _smart_subscribe_all_nse(breeze, symbols, symbol_to_token, label)
    if not subscribed and failed:
        raise RuntimeError(f'WebSocket reconnected but no subscriptions were accepted ({len(failed)} failed); check Breeze session/token and SDK compatibility.')
    print(f'{label}: reconnect result accepted={len(subscribed)} failed={len(failed)}; waiting for fresh ticks to confirm feed health.')
    return subscribed, failed



def _common_trade_universe_cap(enabled, explicit_cap=0) -> int:
    """Return the runtime candidate-universe cap.

    FINAL INTRADAY MODEL: Historical Shares is not a preselection/top-N filter.
    Paper and Live therefore monitor the complete available NSE universe; the only
    BUY-count restriction is Maximum Orders, enforced separately at execution time.
    """
    # Keep the argument for compatibility with older callers, but never use
    # Historical Shares to shrink the live/paper candidate universe.
    return 0

def _fifo_signal_order(rows, timestamp_key='buy_trade_datetime'):
    """Sort qualified candidates by the timestamp when all BUY conditions passed.

    Historical replay has the full timeline and can sort it directly. Symbol and
    logic are deterministic tie-breakers only; neither outranks an earlier signal.
    Live/Paper reserve the slot on the first tick passing the same canonical gate.
    """
    def key(row):
        stamp = row.get(timestamp_key) if isinstance(row, dict) else None
        if isinstance(stamp, str):
            try:
                stamp = datetime.fromisoformat(stamp.replace('Z', '+00:00'))
            except Exception:
                stamp = None
        if isinstance(stamp, datetime) and stamp.tzinfo is not None:
            try:
                stamp = stamp.astimezone(ZoneInfo(NSE_TIMEZONE)).replace(tzinfo=None) if ZoneInfo else stamp.replace(tzinfo=None)
            except Exception:
                stamp = stamp.replace(tzinfo=None)
        if not isinstance(stamp, datetime):
            stamp = datetime.max
        symbol = str(row.get('symbol') or '').strip().upper() if isinstance(row, dict) else ''
        logic = str(row.get('logic') or '') if isinstance(row, dict) else ''
        return stamp, symbol, logic
    return sorted(list(rows or []), key=key)


def _canonical_first_cum_first_buy_select(selected_symbols: set, symbol: str, cap: int) -> tuple[bool, int | None]:
    """Canonical FIFO first-cum-first-buy selector used by Paper and Live.

    A symbol is admitted only after its full canonical BUY condition qualifies.
    The first qualifying live tick reserves its unique-symbol rank; pre-arm,
    movement-only previews, quantity, turnover rank, and end-of-day score do not
    reserve or reorder it. Maximum Orders is enforced separately; a SELL never
    creates a new BUY slot.
    """
    sym=str(symbol or '').strip().upper()
    if not sym:
        return False, None
    if sym in selected_symbols:
        return True, None
    if cap > 0 and len(selected_symbols) >= cap:
        return False, None
    selected_symbols.add(sym)
    return True, len(selected_symbols)


def _universe_allows_symbol(selected_symbols: set, symbol: str, cap: int) -> bool:
    """Top-N is a unique-symbol first-cum-first-buy universe, not a quantity/ranking input."""
    sym = str(symbol or '').strip().upper()
    if not sym:
        return False
    return sym in selected_symbols or cap <= 0 or len(selected_symbols) < cap


def _universe_select_symbol(selected_symbols: set, symbol: str, cap: int) -> bool:
    """Reserve one Top-N universe slot for a symbol. Returns True when newly selected."""
    sym = str(symbol or '').strip().upper()
    if not sym:
        return False
    if sym in selected_symbols:
        return True
    if cap > 0 and len(selected_symbols) >= cap:
        return False
    selected_symbols.add(sym)
    return True

def _increase_breeze_socketio_connect_timeout(timeout_seconds: float = 15.0) -> bool:
    """Raise Breeze SDK's hard-coded Socket.IO namespace wait timeout.

    Current Breeze SDK builds call socketio.Client.connect(..., wait_timeout=3).
    On a slow or busy stream endpoint that can fail before the connection has
    time to settle. Patch only the in-process Socket.IO client method; no package
    files are edited. This cannot bypass invalid credentials or a blocked endpoint.
    """
    try:
        import socketio
        from functools import wraps
        cls = socketio.Client
        if getattr(cls, "_breeze_wait_timeout_patched", False):
            return True
        original = cls.connect
        @wraps(original)
        def _connect_with_longer_wait(self, *args, **kwargs):
            current = kwargs.get("wait_timeout", None)
            try:
                kwargs["wait_timeout"] = max(float(current or 0), float(timeout_seconds))
            except (TypeError, ValueError):
                kwargs["wait_timeout"] = float(timeout_seconds)
            return original(self, *args, **kwargs)
        cls.connect = _connect_with_longer_wait
        cls._breeze_wait_timeout_patched = True
        cls._breeze_original_connect = original
        print(f"AUTO PAPER MARKET: Socket.IO connect wait_timeout set to at least {timeout_seconds:.0f}s (SDK default is commonly 3s).")
        return True
    except Exception as exc:
        print(f"AUTO PAPER MARKET: could not extend Socket.IO connect timeout: {type(exc).__name__}: {exc}")
        return False



def _patch_breeze_socket_event_connect(timeout_seconds: float = 25.0) -> bool:
    """Preserve the low-level Socket.IO exception instead of Breeze's generic wrapper.

    Some Breeze SDK releases catch the useful socketio exception and replace it with
    only 'Failed to connect to live stream'. This in-process patch keeps the SDK's
    authentication payload and WebSocket-only transport unchanged, but logs the
    actual exception and uses a longer handshake wait. It does not bypass auth or
    network restrictions and never prints the session token.
    """
    try:
        from breeze_connect.breeze_connect import SocketEventBreeze
        if getattr(SocketEventBreeze, "_paper_shadow_diagnostic_connect_patched", False):
            return True
        def _connect_with_root_cause(self, hostname, is_ohlc_stream=False, strategy_flag=False):
            auth = {"user": self.breeze.user_id, "token": self.breeze.session_key}
            kwargs = {
                "headers": {"User-Agent": "python-socketio[client]/socket"},
                "auth": auth,
                # Match the official Breeze streaming example: WebSocket transport.
                # A namespace rejection is an application/auth handshake failure,
                # not something a longer timeout or polling fallback can repair.
                "transports": "websocket",
                "wait_timeout": float(timeout_seconds),
            }
            if is_ohlc_stream:
                kwargs["socketio_path"] = "ohlcvstream"
            # Register before connect: Socket.IO can emit connect_error during the
            # handshake, before connect() raises the namespace failure.
            def _paper_namespace_connect_error(data):
                try:
                    detail = str(data)[:400]
                except Exception:
                    detail = type(data).__name__
                print(f"AUTO PAPER MARKET: Socket.IO namespace connect_error detail={detail}")
                try:
                    self.my_connect_error(data)
                except Exception:
                    pass
            self.sio.on("connect_error", _paper_namespace_connect_error)
            # The captured traceback shows websocket-client timing out inside
            # socket.connect(), not a rejected Breeze auth namespace. On some
            # Windows networks IPv6 DNS answers are returned but IPv6 routing to
            # the stream CDN stalls. Prefer IPv4 only for this stream host and only
            # during the synchronous handshake; restore the resolver immediately.
            import socket as _paper_socket
            _paper_original_getaddrinfo = _paper_socket.getaddrinfo
            def _paper_ipv4_stream_resolver(host, port, family=0, type=0, proto=0, flags=0):
                _host = str(host).strip("[]").lower() if isinstance(host, str) else host
                if _host == "livestream.icicidirect.com" and family in (0, _paper_socket.AF_UNSPEC, _paper_socket.AF_INET6):
                    try:
                        _ipv4 = _paper_original_getaddrinfo(host, port, _paper_socket.AF_INET, type, proto, flags)
                        if _ipv4:
                            print("AUTO PAPER MARKET: stream DNS selected IPv4 for WebSocket handshake.")
                            return _ipv4
                    except OSError as _ipv4_exc:
                        print(f"AUTO PAPER MARKET: IPv4 stream DNS unavailable; using system resolver ({type(_ipv4_exc).__name__}).")
                return _paper_original_getaddrinfo(host, port, family, type, proto, flags)
            try:
                _paper_socket.getaddrinfo = _paper_ipv4_stream_resolver
                self.sio.connect(hostname, **kwargs)
            except Exception as exc:
                try:
                    connected = bool(self.sio.connected)
                except Exception:
                    connected = False
                # Log only exception type/message and host; never log auth/session fields.
                try:
                    from urllib.parse import urlparse
                    host = urlparse(str(hostname)).hostname or "unknown-host"
                except Exception:
                    host = "unknown-host"
                print(
                    "AUTO PAPER MARKET: low-level Socket.IO exception: "
                    f"{type(exc).__name__}: {str(exc)[:400]}; host={host}; "
                    f"transport=websocket; connected={connected}; "
                    f"wait_timeout={timeout_seconds:.0f}s"
                )
                if connected:
                    return
                raise RuntimeError(
                    f"Socket.IO {type(exc).__name__}: {str(exc)[:400]}"
                ) from exc
            finally:
                _paper_socket.getaddrinfo = _paper_original_getaddrinfo
        SocketEventBreeze.connect = _connect_with_root_cause
        SocketEventBreeze._paper_shadow_diagnostic_connect_patched = True
        print(f"AUTO PAPER MARKET: SDK low-level connect diagnostics enabled; wait_timeout={timeout_seconds:.0f}s.")
        return True
    except Exception as exc:
        print(f"AUTO PAPER MARKET: could not install low-level Socket.IO diagnostics: {type(exc).__name__}: {str(exc)[:240]}")
        return False


def _log_breeze_stream_preflight() -> None:
    """Check DNS/TCP/TLS reachability to the configured stream host without credentials."""
    try:
        import socket as _socket
        import ssl as _ssl
        from urllib.parse import urlparse as _urlparse
        import breeze_connect.config as _cfg
        url = str(getattr(_cfg, "LIVE_STREAM_URL", "") or "")
        host = _urlparse(url).hostname
        if not host:
            print("AUTO PAPER MARKET: preflight cannot determine live stream hostname from SDK config.")
            return
        try:
            infos = _socket.getaddrinfo(host, 443, type=_socket.SOCK_STREAM)
            ips = sorted({item[4][0] for item in infos})
            print(f"AUTO PAPER MARKET: preflight DNS PASS host={host}; addresses={','.join(ips[:6])}.")
        except Exception as exc:
            print(f"AUTO PAPER MARKET: preflight DNS FAIL host={host}; {type(exc).__name__}: {str(exc)[:240]}")
            return
        raw = None
        try:
            raw = _socket.create_connection((host, 443), timeout=5.0)
            print(f"AUTO PAPER MARKET: preflight TCP/443 PASS host={host}.")
            try:
                with _ssl.create_default_context().wrap_socket(raw, server_hostname=host) as tls:
                    raw = None
                    print(f"AUTO PAPER MARKET: preflight TLS PASS host={host}; protocol={tls.version()}.")
            except Exception as exc:
                print(f"AUTO PAPER MARKET: preflight TLS FAIL host={host}; {type(exc).__name__}: {str(exc)[:240]}")
        except Exception as exc:
            print(f"AUTO PAPER MARKET: preflight TCP/443 FAIL host={host}; {type(exc).__name__}: {str(exc)[:240]}")
        finally:
            if raw is not None:
                try: raw.close()
                except Exception: pass
    except Exception as exc:
        print(f"AUTO PAPER MARKET: preflight unavailable: {type(exc).__name__}: {str(exc)[:240]}")


def _write_run_status(job: str, status: str, details: str = '', date_range: str = '', records=None, workbook_path: Path | None = None) -> None:
    """Persist visible job status in the workbook, even when a run has zero trades.

    The Run Status sheet is diagnostic metadata; it does not fabricate trade rows.
    It is safe to call from worker processes and quietly reports Excel lock errors.
    """
    target = Path(workbook_path) if workbook_path else WORKBOOK
    try:
        wb = _open(target)
        ws = wb['Run Status']
        if ws.max_row < 1 or [ws.cell(1,c).value for c in range(1,8)] != RUN_STATUS_HEADERS:
            ws.delete_rows(1, ws.max_row or 1); ws.append(RUN_STATUS_HEADERS)
        key = (str(job), str(date_range))
        row_no = None
        for rr in range(2, ws.max_row + 1):
            if (str(ws.cell(rr,2).value or ''), str(ws.cell(rr,4).value or '')) == key:
                row_no = rr; break
        if row_no is None:
            row_no = ws.max_row + 1
        vals = [datetime.now(), str(job), str(status), str(date_range), records, str(details)[:2000], str(target)]
        for cc, val in enumerate(vals, 1):
            ws.cell(row_no, cc).value = val
        ws.cell(row_no,1).number_format='yyyy-mm-dd hh:mm:ss'
        for cc, width in enumerate((22,28,24,28,12,90,70),1):
            ws.column_dimensions[get_column_letter(cc)].width=width
        for cell in ws[1]:
            cell.font=Font(bold=True,color='FFFFFF')
            cell.fill=PatternFill('solid',fgColor='ED7D31')
            cell.alignment=Alignment(horizontal='center',vertical='center',wrap_text=True)
        ws.freeze_panes='A2'; ws.auto_filter.ref=f'A1:G{max(1,ws.max_row)}'; ws.sheet_view.showGridLines=False
        wb.save(target); wb.close()
    except Exception as exc:
        print(f'EXCEL RUN STATUS WARNING: could not update {target}: {type(exc).__name__}: {exc}', flush=True)


def run_auto_paper_market(universe: str = "NSE_ALL_EQ") -> list[dict]:
    """Live-market-style PAPER engine using every enabled logic configuration.

    Paper mode mirrors the Live GTT flow without sending a broker order:
    1) at movement - pretrigger gap, create a virtual GTT;
    2) when the trigger is crossed, create the simulated BUY;
    3) manage exits from real-time HIGH/LOW as well as LTP.
    """
    _clear_remote_stop()
    _write_run_status('PAPER TRADING', 'STARTING', 'Initializing strategy and waiting for the next NSE session if market is closed.')
    verify_canonical_strategy_parity()
    _refresh_latest_gui_logic_settings()
    _write_run_status('PAPER TRADING', 'WAITING FOR SESSION', 'If the market is closed, this process waits until the next NSE trading session; no trades are expected before market data arrives.')
    now=_wait_for_next_paper_live_session('PAPER MARKET')
    print(f'BUILD VERSION: {BUILD_VERSION}; MODE=PAPER ONLY; no broker orders are sent by the paper engine.', flush=True)
    _log_runtime_build_identity('PAPER')
    print(f'AUTO PAPER MARKET: NSE CLOCK ACTIVE = {now:%Y-%m-%d %H:%M:%S %Z}')
    _write_run_status('PAPER TRADING', 'SESSION ACTIVE', f'NSE clock active at {now:%Y-%m-%d %H:%M:%S %Z}; validating enabled logics and market feed.', str(now.date()))
    enabled=_enabled_runtime_logics()
    for _logic_name, _cfg in enabled:
        try:
            _rvol_threshold = max(0.0, float(_cfg.get('smart_min_volume_ratio', 0) or 0))
        except Exception:
            _rvol_threshold = 0.0
        if _rvol_threshold <= 0:
            print(
                f'PAPER RVOL STATUS {_logic_name}: DISABLED by saved GUI threshold '
                f'({_rvol_threshold:.4f}); no historical RVOL seed is requested and '
                f'RVOL=NA diagnostics must not be interpreted as a failed feed.',
                flush=True,
            )
        else:
            print(
                f'PAPER RVOL STATUS {_logic_name}: ENABLED; strict threshold > {_rvol_threshold:.4f}; '
                f'historical same-slot median requires at least 3 prior sessions; missing baseline fails closed.',
                flush=True,
            )
        print(
            f'ACTIVE PAPER CONFIG {_logic_name}: budget_per_entry=₹{float(_cfg.get("buy_amount", 0) or 0):.2f}; '
            f'entry_window={_cfg.get("buy_entry_time")}..{_cfg.get("buy_exit_time")}; '
            f'direction={_cfg.get("open_direction", "BOTH")}; '
            f'movement={float(_cfg.get("open_movement", 0) or 0):.2f}%; '
            f'price_band={bool(price_band_enabled(_cfg))} '
            f'({float(_cfg.get("price_small", 0) or 0):.2f}..{float(_cfg.get("price_big", 0) or 0):.2f}); '
            f'min_price=₹{float(_cfg.get("minimum_price", 0) or 0):.2f}; '
            f'min_volume={float(_cfg.get("volume", 0) or 0):.0f}; '
            f'min_turnover_cr={float(_cfg.get("turnover_cr", 0) or 0):.2f}; '
            f'VWAP={bool(_cfg.get("smart_require_vwap", False))}; '
            f'RVOL>{float(_cfg.get("smart_min_volume_ratio", 0) or 0):.2f}; '
            f'ORB15={bool(_cfg.get("require_15m_orb", False))}; '
            f'MA={_ma_filter_enabled(_cfg)}; '
            f'52W_high={bool(_cfg.get("require_52w_high_breakout", False))}; '
            f'52W_low={bool(_cfg.get("require_52w_low_breakdown", False))}; '
            f'target={float(_cfg.get("profit_target", 0) or 0):.2f}%; '
            f'stop={float(_cfg.get("initial_stop", 0) or 0):.2f}%; '
            f'trailing={float(_cfg.get("trailing_stop", 0) or 0):.2f}%; '
            f'max_orders={max_orders_for_logic(_cfg)}', flush=True
        )
    if any(bool(cfg.get('require_15m_orb', False)) for _, cfg in enabled) and now.time() > datetime.strptime('09:15', '%H:%M').time():
        print('AUTO PAPER MARKET ORB WARNING: process/session startup occurred after 09:15 IST. R44 retries verified historical 09:15-09:29 1-minute ORB recovery up to 3 times per symbol; symbols remain blocked unless all 15 real opening minutes are returned.', flush=True)
    if not enabled:
        print('AUTO PAPER MARKET: all active logics are OFF.')
        _write_run_status('PAPER TRADING', 'STOPPED - NO ENABLED LOGICS', 'Enable at least one logic card in Trading Logic before starting paper trading.', str(now.date()))
        return []
    universe_cap=_common_trade_universe_cap(enabled)
    shared_rows, shared_symbols = _load_shared_trade_universe(now.date(), enabled, universe_cap)
    selected_universe=set(shared_symbols)
    shared_rank={}
    print(f'AUTO PAPER MARKET: canonical first-cum-first-buy selector active; eligible-universe rank cap={universe_cap or "UNLIMITED"} (this is NOT the BUY order-count cap).')
    breeze=require_breeze()
    # Keep the SDK authentication, negotiate polling with WebSocket upgrade, extend
    # the wait timeout, and preserve the underlying connection exception for diagnosis.
    _increase_breeze_socketio_connect_timeout(25.0)
    _patch_breeze_socket_event_connect(25.0)
    _log_breeze_stream_preflight()
    nse_map=getattr(breeze,'stock_script_dict_list',None)
    if not nse_map or len(nse_map)<2 or not nse_map[1]:
        raise RuntimeError('AUTO PAPER MARKET: Breeze NSE security master is unavailable.')
    symbol_to_token=_nse_eq_token_universe(nse_map[1], getattr(breeze,'token_script_dict_list', [{} , {}])[1] if isinstance(getattr(breeze,'token_script_dict_list',None), (list, tuple)) and len(getattr(breeze,'token_script_dict_list',[])) > 1 else {})
    _selected_symbols = _nse_universe_symbols(universe)
    if _selected_symbols is not None:
        symbol_to_token = {k: v for k, v in symbol_to_token.items() if k in _selected_symbols}
        if not symbol_to_token:
            raise RuntimeError(f"Selected {universe} universe has no verified Breeze NSE EQ tokens; no paper run started.")
    symbols=sorted(symbol_to_token)
    _start_news_priority_worker(symbols)
    print(
        f"AUTO PAPER MARKET: universe={universe}; verified subscribed candidate scripts={len(symbols)}",
        flush=True,
    )
    start_dt=datetime.combine(now.date(),datetime.min.time(),tzinfo=now.tzinfo).replace(hour=9,minute=15)
    squareoff_dt=datetime.combine(now.date(),datetime.min.time(),tzinfo=now.tzinfo).replace(hour=15,minute=20)
    while _nse_now()<start_dt:
        now_ist=_nse_now(); remaining=(start_dt-now_ist).total_seconds(); print(f'AUTO PAPER MARKET: NSE clock={now_ist:%H:%M:%S %Z}; waiting for 09:15 ({max(0,int(remaining))}s).'); time.sleep(min(30,max(1,remaining)))
    import threading
    tick_lock=threading.Lock(); latest={}; last_ltp_by_symbol={}; token_to_symbol={str(v).strip():k for k,v in symbol_to_token.items() if k in symbols}; nonlocal_last_tick=[time.monotonic()]
    _tick_diag={'callbacks':0,'dict_ticks':0,'mapped':0,'unmapped':0,'bad_price':0,'last_keys':None,'last_symbol':None}
    def on_tick(tick):
        """Thread-safe callback tolerant of dict or list batches from SDK wrappers."""
        try:
            _tick_diag['callbacks'] += 1
            batch = tick if isinstance(tick, (list, tuple)) else (tick,)
            for item in batch:
                if not isinstance(item, dict):
                    continue
                _tick_diag['dict_ticks'] += 1
                if _tick_diag['last_keys'] is None:
                    _tick_diag['last_keys'] = sorted(str(k) for k in item.keys())[:40]
                    _tick_diag['last_symbol'] = str(item.get('symbol') or item.get('stock_code') or item.get('token') or '')[:100]
                sym=_normalize_breeze_stream_symbol(item,token_to_symbol,symbols)
                if not sym:
                    _tick_diag['unmapped'] += 1
                    continue
                _tick_diag['mapped'] += 1
                q=_paper_stream_quote_to_state(item)
                if not q or q.get('ltp', 0) <= 0:
                    _tick_diag['bad_price'] += 1
                    continue
                if not _tick_diag.get('market_fields_logged'):
                    _tick_diag['market_fields_logged'] = True
                    _raw = item
                    _vol_key = next((k for k in ('ttq','total_quantity_traded','total_volume','volume') if _raw.get(k) not in (None,'')), None)
                    _turn_key = next((k for k in ('ttv','total_traded_value','total_turnover','turnover') if _raw.get(k) not in (None,'')), None)
                    _close_key = next((k for k in ('previous_close','prev_close','close') if _raw.get(k) not in (None,'')), None)
                    print(f"AUTO PAPER FEED FIELDS: symbol={sym}; LTP={q.get('ltp')}; avg_traded_price={q.get('avg_traded_price')}; open={q.get('open')}; previous_close={q.get('previous_close')} (source={_close_key}); cumulative_volume={q.get('volume')} (source={_vol_key}); turnover_cr={q.get('turnover_cr')} (source={_turn_key}); VWAP uses avgPrice first and rejects implausible turnover-unit conversions.", flush=True)
                received_mono=time.monotonic()
                q['symbol']=sym
                # If started after the opening range, recover it from actual NSE
                # 1-minute history. Never synthesize missing opening candles.
                _orb_enabled = any(bool(_cfg.get('require_15m_orb', False)) for _, _cfg in enabled)
                _orb_candidate = False
                try:
                    _orb_px = float(q.get('ltp') or 0.0)
                    _orb_prev = float(q.get('previous_close') or 0.0)
                    _orb_move = ((_orb_px / _orb_prev) - 1.0) * 100.0 if _orb_px > 0 and _orb_prev > 0 else 0.0
                    _orb_priority_score = 99.0
                    _orb_candidate = False
                    _orb_open = float(q.get('open') or q.get('open_price') or 0.0)
                    for _, _cfg in enabled:
                        if not bool(_cfg.get('require_15m_orb', False)):
                            continue
                        if _orb_px < float(_cfg.get('minimum_price', 0) or 0):
                            continue
                        if _orb_move < float(_cfg.get('open_movement', 0) or 0) - 1.5:
                            continue
                        if not direction_ok(_orb_open, _orb_prev, _cfg.get('open_direction', 'BOTH')):
                            continue
                        if price_band_enabled(_cfg) and not price_band_ok(_orb_open, _cfg):
                            continue
                        _orb_candidate = True
                        _orb_priority_score = min(
                            _orb_priority_score,
                            _orb_seed_priority_score(_orb_move, _cfg.get('open_movement', 0)))
                except Exception:
                    _orb_candidate = False
                if _orb_enabled and _orb_candidate and (q.get('time') or _nse_now()).time() >= datetime.strptime('09:30', '%H:%M').time():
                    _orb_day = (q.get('time') or _nse_now()).date().isoformat()
                    _orb_key = (_orb_day, sym)
                    _orb_state = _LIVE_ORB15_STATE.get(_orb_key)
                    if _orb_state is None:
                        _orb_state = _LIVE_ORB15_STATE.setdefault(_orb_key, {
                            'minute': None, 'bar': None, 'orb_high': 0.0,
                            'orb_minutes_seen': set(), 'cum_volume': 0.0,
                            'cum_turnover': 0.0, 'last_signal': None,
                            'last_completed_close': 0.0, 'last_completed_vwap': 0.0,
                            'last_tick_time': None, 'last_stream_volume': None,
                            'vwap_seeded': False, 'vwap_source': None,
                        })
                    _orb_future = _orb_state.get('orb_seed_future')
                    _orb_attempts = int(_orb_state.get('orb_seed_attempts', 0) or 0)
                    _orb_retry_at = float(_orb_state.get('orb_seed_retry_at', 0.0) or 0.0)
                    if (_orb_future is None and not _orb_state.get('orb_seed_applied') and
                            _orb_attempts < 3 and time.monotonic() >= _orb_retry_at):
                        # FIFO queue preserves every eligible symbol when the
                        # bounded REST warm-up capacity is temporarily full.
                        _queue_live_orb_seed(
                            _orb_key, breeze, sym, q.get('time') or _nse_now(),
                            priority_score=_orb_priority_score)
                    _orb_future = _orb_state.get('orb_seed_future')
                    if _orb_future is not None and _orb_future.done() and not _orb_state.get('orb_seed_applied'):
                        _orb_queue_full = False
                        try:
                            _orb_result = _orb_future.result()
                        except Exception as _orb_exc:
                            if isinstance(_orb_exc, _HistoricalSeedQueueFull):
                                _orb_queue_full = True
                                _orb_state['orb_seed_attempts'] = max(0, int(_orb_state.get('orb_seed_attempts', 0) or 0) - 1)
                                _orb_state['orb_seed_retry_at'] = time.monotonic() + 2.0
                            _orb_result = {'_seed_error': f'{type(_orb_exc).__name__}: {_orb_exc}'}
                        _orb_state['orb_seed_future'] = None
                        try:
                            if isinstance(_orb_result, dict) and _orb_result.get('_orb_minutes_seen'):
                                _orb_state['orb_seed_applied'] = True
                                _orb_state['orb_high'] = float(_orb_result['_orb_high'])
                                _orb_state['orb_minutes_seen'] = set(_orb_result['_orb_minutes_seen'])
                                _orb_state['orb_ready'] = True
                                print(f'MARKET ORB READY: {sym} recovered complete historical opening range; high={_orb_state["orb_high"]:.2f}; 15/15 minutes verified.', flush=True)
                            else:
                                _orb_state['orb_ready'] = False
                                _reason = _orb_result.get('_seed_error', 'invalid historical opening data') if isinstance(_orb_result, dict) else 'invalid historical opening data'
                                if int(_orb_state.get('orb_seed_attempts', 0)) < 3:
                                    _delay = 2.0 if _orb_queue_full else 20.0
                                    _orb_state['orb_seed_retry_at'] = time.monotonic() + _delay
                                    print(f'MARKET ORB WARMUP WARNING: {sym} history incomplete ({_reason}); retry scheduled in {_delay:.0f}s.', flush=True)
                                else:
                                    _orb_state['orb_seed_applied'] = True
                                    print(f'MARKET ORB WARMUP WARNING: {sym} remains blocked after 3 attempts; {_reason}.', flush=True)
                        finally:
                            # The cap is for outstanding work, not a permanent
                            # first-32-symbol allowlist. Release the slot as soon
                            # as this result is consumed so later eligible names
                            # can warm up; retries still obey the three-attempt rule.
                            with _LIVE_ORB_SEED_SCHEDULED_LOCK:
                                _LIVE_ORB_SEED_SCHEDULED.discard(_orb_key)
                q=_update_live_orb15_state(sym,q)
                if q.get('orb15_reason') == 'OUT_OF_ORDER_TICK':
                    continue
                # Distinguish a genuinely failed/incomplete ORB history fetch from
                # symbols that have not reached the bounded warm-up queue yet. Both
                # states remain fail-closed for BUY evaluation; this only prevents
                # the 30-second NOT BUY report from misdiagnosing every not-yet-seeded
                # symbol as a failed historical-data request.
                if q.get('orb15_reason') == '15M_ORB_DATA_INCOMPLETE':
                    _orb_diag_stamp = q.get('time')
                    if isinstance(_orb_diag_stamp, str):
                        _orb_diag_stamp = _parse_breeze_datetime(_orb_diag_stamp)
                    if not isinstance(_orb_diag_stamp, datetime):
                        _orb_diag_stamp = _nse_now()
                    _orb_diag_state = _LIVE_ORB15_STATE.get((_orb_diag_stamp.date().isoformat(), sym), {})
                    _orb_diag_attempts = int(_orb_diag_state.get('orb_seed_attempts', 0) or 0)
                    if not _orb_diag_state.get('orb_seed_applied'):
                        with _LIVE_ORB_SEED_SCHEDULED_LOCK:
                            _orb_is_queued = (
                                (_orb_diag_stamp.date().isoformat(), sym)
                                in _LIVE_ORB_SEED_PENDING_KEYS)
                        if (_orb_diag_state.get('orb_seed_future') is not None or
                                _orb_diag_attempts > 0 or _orb_is_queued):
                            q['orb15_reason'] = '15M_ORB_WARMUP_PENDING'
                        else:
                            q['orb15_reason'] = '15M_ORB_WARMUP_NOT_STARTED'
                q['_received_monotonic']=received_mono
                with tick_lock:
                    q['previous_ltp']=last_ltp_by_symbol.get(sym)
                    last_ltp_by_symbol[sym]=float(q.get('ltp') or 0)
                    latest[sym]=q
                nonlocal_last_tick[0]=received_mono
        except Exception as exc: print(f'AUTO PAPER STREAM TICK ERROR: {type(exc).__name__}: {exc}')
    breeze.on_ticks=on_tick
    # The Breeze websocket occasionally refuses the first handshake. Retry with
    # bounded backoff instead of terminating the paper-shadow process immediately.
    # This is paper-only: it does not place or modify broker orders.
    ws_connected = False
    ws_last_error = None
    ws_retry_delays = (2, 4, 8, 12, 20)
    for ws_attempt in range(len(ws_retry_delays) + 1):
        try:
            if ws_attempt:
                delay = ws_retry_delays[ws_attempt - 1]
                print(f'AUTO PAPER MARKET: websocket connect retry {ws_attempt}/{len(ws_retry_delays)} in {delay}s.')
                time.sleep(delay)
                try:
                    breeze.ws_disconnect()
                except Exception:
                    pass
            breeze.ws_connect()
            ws_connected = True
            print(f'AUTO PAPER MARKET: websocket connected (attempt {ws_attempt + 1}).')
            break
        except Exception as exc:
            ws_last_error = exc
            print(f'AUTO PAPER MARKET: websocket connect attempt {ws_attempt + 1} failed: {exc}')
    if not ws_connected:
        # Diagnose REST authentication and local SDK versions separately from the
        # websocket handshake. Never print the API secret or full session token.
        try:
            from importlib import metadata as _metadata
            _versions={}
            for _dist in ('breeze-connect','python-socketio','python-engineio','websocket-client'):
                try: _versions[_dist]=_metadata.version(_dist)
                except Exception: _versions[_dist]='not-installed-or-unknown'
            print('AUTO PAPER MARKET: websocket diagnostic package versions=' + json.dumps(_versions, sort_keys=True))
        except Exception as _diag_exc:
            print(f'AUTO PAPER MARKET: package-version diagnostic unavailable: {_diag_exc}')
        try:
            _token=os.getenv('BREEZE_SESSION_TOKEN','')
            _rest=breeze.get_customer_details(api_session=_token)
            if isinstance(_rest,dict):
                _rest_status=_rest.get('Status')
                _rest_ok=(_rest_status==200 and _rest.get('Success') not in (None,[],{}))
                print(f'AUTO PAPER MARKET: REST session diagnostic status={_rest_status!r}; success_payload={bool(_rest.get("Success"))}; authenticated={_rest_ok}.')
                if not _rest_ok:
                    _err=_rest.get('Error')
                    if _err: print(f'AUTO PAPER MARKET: REST session diagnostic error={str(_err)[:240]}')
            else:
                print(f'AUTO PAPER MARKET: REST session diagnostic returned {type(_rest).__name__}, not a response dictionary.')
        except Exception as _diag_exc:
            print(f'AUTO PAPER MARKET: REST session diagnostic failed: {type(_diag_exc).__name__}: {str(_diag_exc)[:240]}')
        try:
            from urllib.parse import urlparse as _urlparse
            import socket as _socket
            import breeze_connect.config as _breeze_config
            _ws_url=str(getattr(_breeze_config,'LIVE_STREAM_URL','') or '')
            _ws_host=_urlparse(_ws_url).hostname
            if _ws_host:
                try:
                    _socket.getaddrinfo(_ws_host,443,type=_socket.SOCK_STREAM)
                    print(f'AUTO PAPER MARKET: websocket endpoint DNS diagnostic passed for host={_ws_host}; this does not prove websocket handshake success.')
                except Exception as _dns_exc:
                    print(f'AUTO PAPER MARKET: websocket endpoint DNS diagnostic failed for host={_ws_host}: {type(_dns_exc).__name__}: {_dns_exc}')
            else:
                print('AUTO PAPER MARKET: websocket endpoint host unavailable from installed SDK config.')
        except Exception as _diag_exc:
            print(f'AUTO PAPER MARKET: websocket endpoint diagnostic unavailable: {type(_diag_exc).__name__}: {_diag_exc}')
        raise RuntimeError(
            'AUTO PAPER MARKET: Breeze websocket connection failed after '
            f'{len(ws_retry_delays) + 1} attempts. No paper feed was started. '
            'See package-version, REST-session, and endpoint-DNS diagnostics above. '
            'A successful REST session with failed websocket attempts usually points to websocket/network/SDK compatibility; '
            'a failed REST session points first to credentials/session permissions. '
            f'Last websocket error: {ws_last_error}'
        ) from ws_last_error
    subscribed, feed_failures = _smart_subscribe_all_nse(breeze, symbols, symbol_to_token, 'AUTO PAPER MARKET')
    _write_run_status('PAPER TRADING', 'FEED SUBSCRIPTION', f'WebSocket connected. Subscribed symbols={len(subscribed)}; failed subscriptions={len(feed_failures)}; total symbols={len(symbols)}.', str(now.date()), len(subscribed))
    if not subscribed:
        _write_run_status('PAPER TRADING', 'ERROR - NO FEED SUBSCRIPTIONS', f'No symbols could be subscribed. Failed symbols={len(feed_failures)}. Check Breeze session, SDK version, network and market-feed permissions.', str(now.date()), 0)
        raise RuntimeError('AUTO PAPER MARKET: WebSocket connected but no NSE symbols were subscribed. No paper trades can be generated until feed subscriptions succeed.')
    positions={}; pending_gtts={}; prearmed={}; triggered=set(); early_warned=set(); counts={name:0 for name,_ in enabled}; completed=[]; last_report=0.0; daily_loss_locked=False
    # PAPER must model the same execution ceilings as LIVE. Strategy conditions
    # remain shared/canonical; this only aligns the simulated order-count and
    # per-order notional limits with trading_safety.json.
    _paper_safety = load_trading_safety()
    # Deprecated max_live_orders_per_run is intentionally ignored: GUI logic caps control BUY counts.
    _paper_gui_cap = paper_global_buy_cap(enabled)
    _total_buys=0
    _gui_budgets = format_paper_buy_budget_audit(enabled)
    print(f'PAPER BUY BUDGET AUDIT: actual GUI per-entry notional budgets=[{_gui_budgets}]; '
          'GUI Maximum Orders independently controls per-logic BUY entry count; SELL exits follow each position logic.', flush=True)
    print(f'PAPER CONFIG SOURCE: {CONFIG_PATH.resolve()}', flush=True)
    _paper_daily_loss_cap = max(0.0, float(_paper_safety.get('paper_max_daily_loss', 3150.0) or 0.0))
    print(f'PAPER DAILY LOSS CIRCUIT BREAKER: ₹{_paper_daily_loss_cap:,.2f} net session loss; '
          'locks new BUYs, cancels pending virtual entries, and exits remaining positions only on fresh quotes. '
          'The lock persists for this session across restarts.', flush=True)
    # Give the feed a 45-second warm-up after successful subscription before
    # attempting recovery. A successful subscribe response is not proof of ticks.
    feed_warmup_until=time.monotonic()+45.0
    last_tick_mono=time.monotonic(); last_feed_retry=0.0; last_reconnect=0.0; last_mobile_state=0.0; last_feed_diag=0.0
    last_processed_tick={}
    # The phone and laptop GUI both write the canonical logic_settings.json.
    # PAPER watches its signature and reloads active logic rows during the run.
    # Existing positions retain their entry-time risk snapshot; pending virtual
    # GTT/pre-arm entries are cancelled when their logic changes and must qualify
    # again under the newly saved conditions.
    _last_seen_logic_cfgs=load_logics()
    # Reconcile once more immediately before the stream loop so a phone save
    # made during websocket startup is not missed by the first file-signature check.
    enabled=[(_name,_cfg) for _name,_cfg in _last_seen_logic_cfgs.items() if bool(_cfg.get('enabled',False))]
    counts={name:0 for name,_ in enabled}
    _paper_checkpoint = _load_paper_engine_state(now.date())
    if _paper_checkpoint:
        positions = _paper_checkpoint['positions']
        completed = _paper_checkpoint['completed']
        triggered = _paper_checkpoint['triggered']
        daily_loss_locked = bool(_paper_checkpoint.get('daily_loss_locked', False))
        for _logic_name, _count in (_paper_checkpoint.get('counts') or {}).items():
            if _logic_name in counts:
                counts[_logic_name] = max(0, int(_count or 0))
        _total_buys = max(int(_paper_checkpoint.get('total_buys') or 0), sum(counts.values()))
        if daily_loss_locked:
            print('PAPER DAILY LOSS CIRCUIT BREAKER: restored LOCKED state; new BUYs remain disabled.', flush=True)
        # If a restored position belongs to a logic now disabled, keep managing it
        # with its entry-time configuration; it still consumes its original BUY slot.
        print(
            f'PAPER STATE RESTORED: open_positions={len(positions)}; '
            f'completed={len(completed)}; buys={_total_buys}; '
            f'checkpoint={_paper_checkpoint.get("updated_at")}. '
            'Existing positions will continue to be managed; no duplicate BUY is created.',
            flush=True,
        )
    else:
        _total_buys = 0
        print('PAPER STATE: no same-session checkpoint; starting a new empty PAPER session.', flush=True)
    _paper_state_last_save = 0.0
    try:
        _cfg_stat=CONFIG_PATH.stat()
        _logic_config_signature=(_cfg_stat.st_mtime_ns,_cfg_stat.st_size)
    except Exception:
        _logic_config_signature=None
    last_logic_config_check=0.0
    try:
        while _nse_now()<squareoff_dt and not _remote_stop_requested():
            now_mono=time.monotonic()
            if now_mono-last_logic_config_check>=1.0:
                last_logic_config_check=now_mono
                try:
                    _cfg_stat=CONFIG_PATH.stat()
                    _new_signature=(_cfg_stat.st_mtime_ns,_cfg_stat.st_size)
                    if _new_signature != _logic_config_signature:
                        _logic_config_signature=_new_signature
                        _latest_logic_cfgs=load_logics()
                        _changed_logic_names=[_name for _name,_new_cfg in _latest_logic_cfgs.items() if _new_cfg != _last_seen_logic_cfgs.get(_name)]
                        if _changed_logic_names:
                            _last_seen_logic_cfgs=_latest_logic_cfgs
                            # Include newly enabled logics immediately, and retain
                            # disabled logics only while they still own an open or
                            # pending paper position that must be safely managed.
                            _active_logic_names={_name for _name,_cfg in _latest_logic_cfgs.items() if bool(_cfg.get('enabled',False))}
                            _active_logic_names.update(_key[0] for _key in positions)
                            _active_logic_names.update(_key[0] for _key in pending_gtts)
                            _active_logic_names.update(_key[0] for _key in prearmed)
                            enabled=[(_name,_latest_logic_cfgs[_name]) for _name in _latest_logic_cfgs if _name in _active_logic_names]
                            for _logic_name in _latest_logic_cfgs:
                                counts.setdefault(_logic_name,0)
                            for _pending_key,_pending in list(pending_gtts.items()):
                                if _pending_key[0] in _changed_logic_names:
                                    _pending['status']='PAPER_GTT_CANCELLED_CONFIG_CHANGED'
                                    _pending['expired_datetime']=_nse_now().replace(tzinfo=None)
                                    _mobile_order_event('GTT_EXPIRED','PAPER',_pending,'BUY',price=_pending.get('trigger_price'),quantity_value=_pending.get('quantity'),reason='Logic settings changed; requalification required',status=_pending['status'])
                                    del pending_gtts[_pending_key]
                            for _prearm_key in list(prearmed):
                                if _prearm_key[0] in _changed_logic_names:
                                    del prearmed[_prearm_key]
                            for _trigger_key in list(triggered):
                                if _trigger_key[0] in _changed_logic_names and _trigger_key not in positions:
                                    triggered.discard(_trigger_key)
                            print('PAPER CONFIG HOT-RELOAD: laptop/mobile settings saved and applied to new entry decisions for '+', '.join(_changed_logic_names)+'. Enabled/disabled logic changes are active; existing positions retain their original stop/target settings; pending GTTs were cancelled for requalification.',flush=True)
                            for _logic_name in _changed_logic_names:
                                _new_cfg = _latest_logic_cfgs.get(_logic_name, {})
                                print(
                                    f'PAPER GUI CONDITIONS APPLIED {_logic_name}: enabled={bool(_new_cfg.get("enabled", False))}; '
                                    f'entry={_new_cfg.get("buy_entry_time")}..{_new_cfg.get("buy_exit_time")}; '
                                    f'direction={_new_cfg.get("open_direction", "BOTH")}; movement={float(_new_cfg.get("open_movement", 0) or 0):.2f}%; '
                                    f'min_price={float(_new_cfg.get("minimum_price", 0) or 0):.2f}; '
                                    f'price_band={bool(price_band_enabled(_new_cfg))}; volume={float(_new_cfg.get("volume", 0) or 0):.0f}; '
                                    f'turnover_cr={float(_new_cfg.get("turnover_cr", 0) or 0):.2f}; '
                                    f'VWAP={bool(_new_cfg.get("smart_require_vwap", False))}; '
                                    f'RVOL>{float(_new_cfg.get("smart_min_volume_ratio", 0) or 0):.2f}; '
                                    f'ORB15={bool(_new_cfg.get("require_15m_orb", False))}; '
                                    f'MA={_ma_filter_enabled(_new_cfg)}; '
                                    f'52W_high={bool(_new_cfg.get("require_52w_high_breakout", False))}; '
                                    f'52W_low={bool(_new_cfg.get("require_52w_low_breakdown", False))}; '
                                    f'target={float(_new_cfg.get("profit_target", 0) or 0):.2f}%; '
                                    f'stop={float(_new_cfg.get("initial_stop", 0) or 0):.2f}%; '
                                    f'trailing={float(_new_cfg.get("trailing_stop", 0) or 0):.2f}%; '
                                    f'max_orders={max_orders_for_logic(_new_cfg)}', flush=True
                                )
                except Exception as _reload_exc:
                    print(f'PAPER CONFIG HOT-RELOAD WARNING: {type(_reload_exc).__name__}: {_reload_exc}',flush=True)
            if now_mono-last_feed_retry>=10 and feed_failures:
                last_feed_retry=now_mono; _smart_retry_failed_subscriptions(breeze,feed_failures,symbol_to_token,'AUTO PAPER MARKET')
            feed_idle=now_mono-nonlocal_last_tick[0]
            if feed_idle>12 and now_mono>=feed_warmup_until and now_mono-last_reconnect>=30:
                last_reconnect=now_mono
                try:
                    print(f'AUTO PAPER MARKET: no valid ticks for {feed_idle:.1f}s; callback diagnostics={_tick_diag}; reconnecting websocket and restoring subscriptions.')
                    _, feed_failures = _smart_reconnect_feeds(breeze,symbols,symbol_to_token,'AUTO PAPER MARKET')
                    # Do not mark healthy until on_tick receives a mapped live-price tick.
                except Exception as exc:
                    print(f'AUTO PAPER MARKET: feed reconnect failed: {type(exc).__name__}: {exc}')
            if now_mono-last_feed_diag>=15 and not latest:
                last_feed_diag=now_mono
                print(f'AUTO PAPER MARKET: waiting for first valid live tick; callback diagnostics={_tick_diag}; subscribed={len(subscribed)}; failed={len(feed_failures)}.')
            feed_age=time.monotonic()-nonlocal_last_tick[0]
            feed_healthy=feed_age <= 30.0
            if not feed_healthy and int(now_mono) % 10 == 0:
                print(f'AUTO PAPER MARKET: FEED UNHEALTHY; last tick {feed_age:.1f}s ago; new BUY decisions paused until fresh ticks resume.')
            with tick_lock: snapshot=dict(latest)
            _paper_rvol_seed_symbols = _paper_rvol_seed_candidates(snapshot, enabled)
            _runtime_momentum_enrich(snapshot, breeze=breeze, seed_symbols=_paper_rvol_seed_symbols)
            _paper_entry_audit(snapshot, enabled)
            # Protective exits have priority over the session-wide loss-cap
            # flatten. Otherwise a quote that has already crossed a configured
            # stop can be liquidated at a later, worse LTP by the loss-cap block.
            # Only process positions whose symbol has a genuinely newer live tick.
            _paper_pre_managed_keys = set()
            for _pos_key, _pos in list(positions.items()):
                _pos_symbol = str(_pos.get('symbol') or '').strip().upper()
                _pos_quote = snapshot.get(_pos_symbol) or {}
                _pos_received = float(_pos_quote.get('_received_monotonic') or 0.0)
                if (_pos_received <= last_processed_tick.get(_pos_symbol, 0.0) or
                        _pos_received <= 0.0 or time.monotonic() - _pos_received > 30.0 or
                        float(_pos_quote.get('ltp') or 0.0) <= 0.0):
                    continue
                _exit_now, _exit_price, _exit_reason = _manage_paper_position(_pos, _pos_quote)
                _paper_pre_managed_keys.add(_pos_key)
                if _exit_now:
                    _pos['sell_actual_qty'] = int(_pos.get('quantity') or _pos.get('buy_actual_qty') or 0)
                    _pos['sell_actual_price'] = float(_exit_price)
                    _pos['sell_trade_datetime'] = _pos_quote.get('time') or _nse_now().replace(tzinfo=None)
                    _pos['exit_reason'] = _exit_reason
                    _pos['status'] = 'PAPER_LIVE_EXITED'
                    record_paper_live_trades([_pos])
                    completed.append(_pos)
                    del positions[_pos_key]
                    _mobile_order_event('ORDER_EXIT', 'PAPER', _pos, 'SELL', price=_exit_price,
                                        quantity_value=_pos['sell_actual_qty'], reason=_exit_reason,
                                        status=_pos['status'])
                    _save_paper_engine_state(now.date(), positions, completed, counts, _total_buys, triggered, daily_loss_locked)
                    print(f"PAPER SELL {_pos.get('logic')}: {_pos_symbol} qty={_pos['sell_actual_qty']} "
                          f"price={float(_exit_price):.2f} observed_ltp={float(_pos_quote.get('ltp') or 0):.2f} "
                          f"reason={_exit_reason} at {_pos['sell_trade_datetime']}", flush=True)

            # Net P&L includes completed trades, open-position MTM and estimated
            # ICICI intraday charges. A triggered lock is sticky until the next
            # NSE session and survives a process restart via the checkpoint.
            if (not daily_loss_locked and _paper_daily_loss_cap > 0.0):
                _paper_net_before_cap = _paper_session_net_pnl(completed, positions, snapshot)
                if _paper_daily_loss_triggered(_paper_net_before_cap, _paper_daily_loss_cap):
                    daily_loss_locked = True
                    print(
                        f'PAPER DAILY LOSS CIRCUIT BREAKER TRIGGERED: net_pnl=₹{_paper_net_before_cap:.2f}; '
                        f'limit=-₹{_paper_daily_loss_cap:.2f}. New BUYs locked for the rest of this session.',
                        flush=True,
                    )
                    for _pending_key, _pending in list(pending_gtts.items()):
                        _pending['status'] = 'PAPER_GTT_CANCELLED_DAILY_LOSS'
                        _pending['cancelled_datetime'] = _nse_now().replace(tzinfo=None)
                        _mobile_order_event('GTT_CANCELLED', 'PAPER', _pending, 'BUY',
                                            price=_pending.get('trigger_price'),
                                            quantity_value=_pending.get('quantity'),
                                            reason='Daily PAPER loss circuit breaker',
                                            status=_pending['status'])
                    pending_gtts.clear()
                    for _prearm_key, _prearm in list(prearmed.items()):
                        _prearm['status'] = 'PAPER_PREARM_CANCELLED_DAILY_LOSS'
                        _mobile_order_event('PRETRIGGER_CANCELLED', 'PAPER', _prearm, 'BUY',
                                            reason='Daily PAPER loss circuit breaker',
                                            status=_prearm['status'])
                    prearmed.clear()
                    _save_paper_engine_state(now.date(), positions, completed, counts,
                                             _total_buys, triggered, daily_loss_locked)

            # Once locked, close each remaining PAPER position only from a
            # genuinely fresh market quote. Missing/stale prices never become
            # synthetic fills; the position remains open and is retried later.
            if daily_loss_locked:
                for _loss_key, _loss_pos in list(positions.items()):
                    _loss_symbol = str(_loss_pos.get('symbol') or '').strip().upper()
                    _loss_quote = snapshot.get(_loss_symbol) or {}
                    try:
                        _loss_age = time.monotonic() - float(_loss_quote.get('_received_monotonic') or 0.0)
                        _loss_ltp = float(_loss_quote.get('ltp') or 0.0)
                    except (TypeError, ValueError, OverflowError):
                        _loss_age, _loss_ltp = float('inf'), 0.0
                    if _loss_age < 0.0 or _loss_age > 30.0 or _loss_ltp <= 0.0:
                        continue
                    _loss_price = _round_limit(_loss_ltp)
                    _loss_pos['sell_actual_qty'] = int(_loss_pos.get('quantity') or _loss_pos.get('buy_actual_qty') or 0)
                    _loss_pos['sell_actual_price'] = float(_loss_price)
                    _loss_pos['sell_trade_datetime'] = _loss_quote.get('time') or _nse_now().replace(tzinfo=None)
                    _loss_pos['exit_reason'] = f'Daily PAPER loss circuit breaker (limit ₹{_paper_daily_loss_cap:.2f})'
                    _loss_pos['status'] = 'PAPER_DAILY_LOSS_EXIT'
                    record_paper_live_trades([_loss_pos])
                    completed.append(_loss_pos)
                    del positions[_loss_key]
                    _mobile_order_event('ORDER_EXIT', 'PAPER', _loss_pos, 'SELL',
                                        price=_loss_price, quantity_value=_loss_pos['sell_actual_qty'],
                                        reason=_loss_pos['exit_reason'], status=_loss_pos['status'])
                    _save_paper_engine_state(now.date(), positions, completed, counts,
                                             _total_buys, triggered, daily_loss_locked)
                    print(
                        f'PAPER SELL {_loss_pos.get("logic")}: {_loss_symbol} '
                        f'qty={_loss_pos["sell_actual_qty"]} price={_loss_price:.2f} '
                        f'reason=DAILY_LOSS_CIRCUIT_BREAKER at {_loss_pos["sell_trade_datetime"]}',
                        flush=True,
                    )
            if time.monotonic()-last_mobile_state>=1.0:
                _write_mobile_order_state(positions, snapshot); last_mobile_state=time.monotonic()
            # Process the freshest per-symbol snapshots in receive-time order.
            # This makes first-qualifying BUY priority deterministic when multiple
            # symbols have new ticks between engine iterations.
            for sym,q in sorted(snapshot.items(), key=lambda item: (-_news_priority_score(item[0]), float((item[1] or {}).get('_received_monotonic', float('inf')) or float('inf')), str(item[0]))):
                # `latest` is a snapshot cache, not a queue. Only process a symbol
                # when its callback has delivered a newer tick; otherwise old prices
                # can repeatedly trigger entries/exits on each 0.5s loop iteration.
                received_mono=float(q.get('_received_monotonic', 0) or 0)
                if received_mono <= last_processed_tick.get(sym, 0):
                    continue
                last_processed_tick[sym]=received_mono
                if q.get('ltp',0)<=0 or q.get('open',0)<=0: continue
                if time.monotonic()-received_mono > 30.0: continue
                for logic_name,cfg in enabled:
                    key=(logic_name,sym)
                    pos=positions.get(key)
                    pending=pending_gtts.get(key)
                    # BUY TIMEOUT RULE: after the configured Buy Exit Time, do not
                    # evaluate or create any NEW BUY/GTT. Existing bought positions
                    # continue through normal target/stop/trailing management and
                    # the 15:25 pre-close square-off. This keeps post-cutoff work limited to
                    # shares that were actually bought.
                    buy_cutoff = q.get('time')
                    try:
                        xh,xm=[int(v) for v in str(cfg.get('buy_exit_time','15:20')).split(':')]
                        buy_cutoff = buy_cutoff.replace(hour=xh, minute=xm, second=0, microsecond=0)
                    except Exception:
                        buy_cutoff = None
                    new_buy_allowed = (not daily_loss_locked) and bool(cfg.get('enabled', True)) and feed_healthy and bool(q.get('time')) and (buy_cutoff is None or q.get('time') < buy_cutoff) and entry_time_ok(q.get('time'),cfg)
                    gtt_enabled=(not bool(load_trading_safety().get('strict_strategy_parity',True))) and bool(cfg.get('use_gtt_pretrigger',True)) and float(cfg.get('open_movement',0) or 0)>0 and not bool(cfg.get('require_15m_orb',False))

                    # Notification-only early warning. Use the existing GUI
                    # pre-trigger gap and other currently checkable GUI filters,
                    # even when GTT execution is disabled or the 15m ORB gate is
                    # enabled. This does not reserve a slot or permit a BUY.
                    if (pos is None and pending is None and new_buy_allowed and
                        key not in early_warned and
                        not bool(cfg.get('require_15m_orb', False))):
                        # A movement pre-trigger warning is misleading when the GUI
                        # requires a confirmed 15-minute ORB. In that mode, let the
                        # canonical ORB evaluator report the actual rejection reason.
                        _warn_arm = _pretrigger_entry_active(q, cfg)
                        _warn_min_px = max(0.0, float(cfg.get('minimum_price', 0) or 0))
                        _warn_min_vol = max(0.0, float(cfg.get('volume', 0) or 0))
                        _warn_min_turn = max(0.0, float(cfg.get('turnover_cr', 0) or 0))
                        _warn_filters_ok = (
                            float(q.get('ltp', 0) or 0) >= _warn_min_px and
                            float(q.get('volume', 0) or 0) >= _warn_min_vol and
                            float(q.get('turnover_cr', 0) or 0) >= _warn_min_turn
                        )
                        if _warn_arm.get('active') and _warn_filters_ok:
                            early_warned.add(key)
                            _warning = {
                                'symbol': sym, 'logic': logic_name,
                                'armed_datetime': q.get('time'),
                                'pre_level': _warn_arm.get('pre_level'),
                                'actual_level': _warn_arm.get('actual_level'),
                                'status': 'PAPER_EARLY_WARNING',
                            }
                            # Keep warnings in the process log only. Do not write
                            # them to the order feed, where they could look like a
                            # real paper BUY in the mobile Order Monitor.
                            print(f"EARLY BUY WARNING symbol={sym} logic={logic_name} price={float(q.get('ltp') or 0):.2f} pretrigger={float(_warn_arm.get('pre_level') or 0):.2f}; not a BUY, recheck all GUI conditions.", flush=True)

                    # Stage 1: persistently ARM the share at Open Movement - 0.15%.
                    # This state survives a fast tick, so a later volume/turnover
                    # qualification cannot be missed. It does not reserve an order slot.
                    if pos is None and pending is None and new_buy_allowed:
                        if gtt_enabled and key not in prearmed:
                            arm=_pretrigger_entry_active(q,cfg)
                            if arm.get('active'):
                                prearmed[key]={'symbol':sym,'logic':logic_name,'armed_datetime':q.get('time'),'pre_pct':arm['pre_pct'],'pre_level':arm['pre_level'],'actual_level':arm['actual_level'],'status':'PAPER_PREARMED'}
                                _mobile_order_event('PRETRIGGER_ARMED','PAPER',prearmed[key],'BUY',price=arm['actual_level'],status='PAPER_PREARMED')
                                print(f"PAPER PRE-ARMED {logic_name}: {sym} arm={arm['pre_pct']:.2f}% actual={arm['actual_level']:.2f} at {q['time']}")
                        arm=prearmed.get(key)
                        # Once armed, submit the virtual broker GTT as soon as all
                        # non-movement entry conditions are simultaneously valid.
                        if arm and _gtt_pretrigger_qualifies(q,cfg):
                            # A GTT/pre-arm is only an execution mechanism. It must NOT
                            # consume a first-cum-first-buy slot before the canonical
                            # full entry condition (including movement) is true.
                            if not _position_entry_matches(sym, q, logic_name, cfg):
                                continue
                            if not _universe_allows_symbol(selected_universe, sym, universe_cap):
                                continue
                            actual_level=float(arm['actual_level'])
                            qty=quantity(actual_level,cfg)
                            _gtt_order_value = float(qty) * float(_round_limit(actual_level)) if qty > 0 else 0.0
                            if qty>0:
                                _ok, _rank = _canonical_first_cum_first_buy_select(selected_universe, sym, universe_cap)
                                if not _ok:
                                    continue
                                limit=_gtt_limit_price(actual_level,cfg)
                                qty=quantity(limit,cfg)
                                if qty < 1:
                                    continue
                                pending={'symbol':sym,'logic':logic_name,'quantity':qty,'trigger_price':_round_limit(actual_level),'limit_price':limit,'armed_datetime':arm['armed_datetime'],'prearmed_datetime':arm['armed_datetime'],'rank':_rank,'status':'PAPER_GTT_PENDING'}
                                # A staged virtual GTT is NOT a completed BUY. Do not consume
                                # the order cap or per-logic quota until the simulated limit
                                # actually fills; otherwise one missed/gapped share can block
                                # the next share that satisfies the exact same strategy rules.
                                pending_gtts[key]=pending; triggered.add(key); prearmed.pop(key,None)
                                _mobile_order_event('GTT_ACCEPTED','PAPER',pending,'BUY',price=pending['trigger_price'],quantity_value=qty,status='PAPER_GTT_PENDING')
                                print(f"PAPER GTT READY {logic_name}: {sym} pending-only (no BUY slot reserved); prearm={arm['pre_pct']:.2f}% trigger={pending['trigger_price']:.2f} limit={limit:.2f} at {q['time']}")

                    # CANONICAL SHARED STRATEGY ENGINE: when GTT pre-trigger is disabled, Paper uses
                    # the same canonical entry evaluator as LIVE and Historical 1-second
                    # confirmation. The Top-N slot is reserved only when the actual BUY
                    # condition is true, never merely because a pre-trigger was reached.
                    if (pos is None and pending is None and new_buy_allowed and
                        not gtt_enabled and
                        (max_orders_for_logic(cfg)==0 or counts[logic_name] < max_orders_for_logic(cfg)) and
                        key not in triggered and _position_entry_matches(sym, q, logic_name, cfg)):
                        if not _universe_allows_symbol(selected_universe, sym, universe_cap):
                            continue
                        _paper_entry_price = _round_limit(q.get('ltp', 0))
                        qty=quantity(_paper_entry_price,cfg)
                        if qty < 1:
                            continue
                        _paper_order_value = float(qty) * float(_round_limit(q.get('ltp', 0)))
                        _ok, _rank = _canonical_first_cum_first_buy_select(selected_universe, sym, universe_cap)
                        if not _ok:
                            continue
                        fill=float(_paper_entry_price)
                        pos={'symbol':sym,'breeze_symbol':sym,'logic':logic_name,'logic_config':cfg,'quantity':qty,
                             'buy_actual_qty':qty,'buy_actual_price':fill,'buy_trade_datetime':q['time'],
                             'trade_date':now.date(),'mode':'paper_live','status':'PAPER_LIVE_OPEN',
                             'open':q['open'],'high':q['high'],'low':q['low'],'close':q['ltp'],
                             'previous_close':q.get('previous_close'),'volume':q['volume'],'turnover_cr':q['turnover_cr'],
                             'rank':_rank,'data_source':'Breeze live streaming quote + canonical first-cum-first-buy entry evaluator',
                             'trailing_active':False,'highest_price':fill,'trailing_stop':None}
                        sl_pct=float(cfg.get('initial_stop') or 0)
                        pos['stop_loss_trigger']=stop_price(fill,cfg) if sl_pct>0 else None
                        pos['stop_loss_limit']=max(0.05,pos['stop_loss_trigger']-0.05) if pos.get('stop_loss_trigger') else None
                        pos['stop_loss_order_id']=f"PAPER-SL-{logic_name.replace(' ','')}-{sym}-{q['time']:%H%M%S}" if pos.get('stop_loss_trigger') else None
                        pos['stop_loss_status']='PAPER_PROTECTIVE_SL_ACTIVE' if pos.get('stop_loss_trigger') else 'DISABLED'
                        positions[key]=pos; triggered.add(key); counts[logic_name]+=1; _total_buys+=1
                        record_paper_live_trades([pos])
                        _save_paper_engine_state(now.date(), positions, completed, counts, _total_buys, triggered, daily_loss_locked)
                        _mobile_order_event('ORDER_ACCEPTED','PAPER',pos,'BUY',price=fill,quantity_value=qty,status='PAPER_LIVE_OPEN')
                        print(f"PAPER BUY {logic_name} #{counts[logic_name]}: {sym} qty={qty} price={fill:.2f} at {q['time']} (canonical shared entry)")

                    # A virtual GTT that is still pending when BUY TIMEOUT is reached
                    # must not create a new position afterwards. Expire it and continue
                    # managing only already-bought positions.
                    pending=pending_gtts.get(key)
                    if pending is not None and not new_buy_allowed:
                        pending['status']='PAPER_GTT_EXPIRED_BUY_TIMEOUT'
                        pending['expired_datetime']=q.get('time')
                        del pending_gtts[key]
                        _mobile_order_event('GTT_EXPIRED','PAPER',pending,'BUY',price=pending.get('trigger_price'),quantity_value=pending.get('quantity'),status=pending['status'])
                        print(f"PAPER GTT EXPIRED BUY TIMEOUT {logic_name}: {sym} at {q.get('time')}")
                        pending=None

                    # Stage 2: virtual broker trigger. If price gaps through the trigger,
                    # do not require a separate ARM tick; the pending GTT itself is enough.
                    if pending is not None:
                        trigger=float(pending['trigger_price']); limit=float(pending['limit_price']); ltp=float(q['ltp'])
                        if ltp>=trigger:
                            if ltp<=limit:
                                if not _position_entry_matches(sym, q, logic_name, cfg):
                                    pending['status']='PAPER_GTT_CANCELLED_GUI_CONDITIONS_NOT_MET'
                                    pending['expired_datetime']=q.get('time')
                                    del pending_gtts[key]
                                    _mobile_order_event('GTT_EXPIRED','PAPER',pending,'BUY',
                                                        price=trigger,quantity_value=pending.get('quantity'),
                                                        reason='Full GUI entry conditions no longer pass at simulated fill',
                                                        status=pending['status'])
                                    print(f"PAPER GTT CANCELLED {logic_name}: {sym}; current tick no longer passes all GUI entry conditions; no BUY slot consumed.", flush=True)
                                    continue
                                # Backfill rule: re-check caps at actual fill time because
                                # other symbols may have filled while this GTT was pending.
                                _logic_cap=max_orders_for_logic(cfg)
                                if _logic_cap>0 and counts[logic_name]>=_logic_cap:
                                    pending['status']='PAPER_GTT_CANCELLED_BUY_CAP_REACHED'
                                    pending['expired_datetime']=q.get('time')
                                    del pending_gtts[key]
                                    _mobile_order_event('GTT_EXPIRED','PAPER',pending,'BUY',price=trigger,quantity_value=pending.get('quantity'),status=pending['status'])
                                    print(f"PAPER GTT CANCELLED {logic_name}: {sym}; buy cap reached before fill. Pending share did not consume a slot; other candidates continue under the same conditions.")
                                    continue
                                qty=int(pending['quantity']); fill=ltp
                                _filled_order_value = float(qty) * float(_round_limit(fill))
                                pos={'symbol':sym,'breeze_symbol':sym,'logic':logic_name,'logic_config':cfg,'quantity':qty,'buy_actual_qty':qty,'buy_actual_price':fill,'buy_trade_datetime':q['time'],'trade_date':now.date(),'mode':'paper_live','status':'PAPER_LIVE_OPEN','open':q['open'],'high':q['high'],'low':q['low'],'close':q['ltp'],'previous_close':q.get('previous_close'),'volume':q['volume'],'turnover_cr':q['turnover_cr'],'rank':pending.get('rank'),'data_source':'Breeze live streaming quote + Paper GTT + canonical first-cum-first-buy entry evaluator','trailing_active':False,'highest_price':fill,'trailing_stop':None,'gtt_trigger_price':trigger,'gtt_limit_price':limit,'gtt_armed_datetime':pending.get('armed_datetime'),'gtt_triggered_datetime':q.get('time')}
                                # Create the protective paper stop-loss at the exact moment
                                # the simulated BUY is created. It is monitored on every stream
                                # tick, so P&L protection starts with the position itself.
                                sl_pct=float(cfg.get('initial_stop') or 0)
                                pos['stop_loss_trigger']=stop_price(fill,cfg) if sl_pct>0 else None
                                pos['stop_loss_limit']=max(0.05, pos['stop_loss_trigger']-0.05) if pos.get('stop_loss_trigger') else None
                                pos['stop_loss_order_id']=f"PAPER-SL-{logic_name.replace(' ','')}-{sym}-{q['time']:%H%M%S}" if pos.get('stop_loss_trigger') else None
                                pos['stop_loss_status']='PAPER_PROTECTIVE_SL_ACTIVE' if pos.get('stop_loss_trigger') else 'DISABLED'
                                positions[key]=pos; del pending_gtts[key]
                                counts[logic_name]+=1; _total_buys+=1
                                record_paper_live_trades([pos])
                                _save_paper_engine_state(now.date(), positions, completed, counts, _total_buys, triggered, daily_loss_locked)
                                _mobile_order_event('GTT_TRIGGERED','PAPER',pos,'BUY',price=fill,quantity_value=qty,status='PAPER_LIVE_OPEN')
                                print(f"PAPER GTT TRIGGERED {logic_name}: {sym} trigger={trigger:.2f} fill={fill:.2f} at {q['time']}")
                            else:
                                pending['status']='PAPER_GTT_TRIGGERED_LIMIT_WAIT'; pending['triggered_datetime']=q.get('time')
                                _mobile_order_event('GTT_TRIGGERED','PAPER',pending,'BUY',price=trigger,quantity_value=pending['quantity'],status=pending['status'])
                                print(f"PAPER GTT MISSED {logic_name}: {sym} LTP={ltp:.2f} exceeds limit={limit:.2f}; no BUY slot consumed, so the next share qualifying under the same conditions can be bought.")
                        # A pending virtual GTT is not an open position yet.
                        if key not in positions: continue

                    pos=positions.get(key)
                    if not pos: continue
                    if key in _paper_pre_managed_keys:
                        continue
                    exit_now,sell,reason=_manage_paper_position(pos,q)
                    if exit_now:
                        pos['sell_actual_qty']=pos['quantity']; pos['sell_actual_price']=sell; pos['sell_trade_datetime']=q['time']; pos['exit_reason']=reason; pos['status']='PAPER_LIVE_EXITED'; record_paper_live_trades([pos]); completed.append(pos); del positions[key]
                        _mobile_order_event('ORDER_EXIT','PAPER',pos,'SELL',price=sell,quantity_value=pos['quantity'],reason=reason,status=pos['status'])
                        _save_paper_engine_state(now.date(), positions, completed, counts, _total_buys, triggered, daily_loss_locked)
                        print(f"PAPER SELL {logic_name}: {sym} qty={pos['quantity']} price={sell:.2f} reason={reason} at {q['time']}")
            if time.monotonic()-last_report>=15:
                last_report=time.monotonic(); print(f'AUTO PAPER MARKET: streaming={len(latest):,}; callbacks={_tick_diag["callbacks"]}; mapped_ticks={_tick_diag["mapped"]}; unmapped_ticks={_tick_diag["unmapped"]}; bad_price_ticks={_tick_diag["bad_price"]}; pending GTTs={len(pending_gtts)}; open positions={len(positions)}; buys={counts}; net_pnl_estimate=₹{_paper_session_net_pnl(completed, positions, latest):.2f}')
                _write_run_status('PAPER TRADING', 'RUNNING', f'Live ticks received={len(latest)}; subscriptions={len(subscribed)}; pending paper GTTs={len(pending_gtts)}; open positions={len(positions)}; BUY counts={counts}. Zero trades may mean no signal has qualified yet.', str(now.date()), sum(counts.values()))
            if time.monotonic() - _paper_state_last_save >= 2.0:
                _save_paper_engine_state(now.date(), positions, completed, counts, _total_buys, triggered, daily_loss_locked)
                _paper_state_last_save = time.monotonic()
            time.sleep(0.5)
    finally:
        if _remote_stop_requested():
            # Cancel queued warm-up jobs and prevent new jobs from being queued.
            # Running requests finish within the SDK/network timeout; wait=False
            # keeps cleanup responsive while their bounded calls unwind.
            try:
                for _state in list(_RUNTIME_MOMENTUM.values()):
                    _future = _state.get('rvol_seed_future')
                    if _future is not None and not _future.done():
                        _future.cancel()
                _RUNTIME_RVOL_SEED_EXECUTOR.shutdown(wait=False, cancel_futures=True)
                print('PAPER WARMUP CLEANUP: queued RVOL history jobs cancelled after stop request.', flush=True)
            except Exception as _warmup_cleanup_exc:
                print(f'PAPER WARMUP CLEANUP WARNING: {_warmup_cleanup_exc}', flush=True)
        for sym in symbols:
            try: breeze.unsubscribe_feeds(stock_token=f'4.1!{symbol_to_token[sym]}',get_exchange_quotes=True,get_market_depth=False)
            except Exception: pass
        try: breeze.ws_disconnect()
        except Exception: pass
    with tick_lock: final_snapshot=dict(latest)
    for key,pos in list(positions.items()):
        try:
            q=final_snapshot.get(pos['symbol'])
            if q and q.get('ltp',0)>0:
                quote_time=_align_datetime_timezone(q.get('time'), squareoff_dt)
                pos['sell_actual_qty']=pos['quantity']
                pos['sell_actual_price']=q['ltp']
                pos['sell_trade_datetime']=min(quote_time, squareoff_dt)
                pos['exit_reason']='3:20 PM pre-close square-off'
                pos['status']='PAPER_LIVE_SQUAREOFF_1520'
            else:
                pos['exit_reason']='3:20 PM square-off quote unavailable'
                pos['status']='PAPER_LIVE_SQUAREOFF_DATA_ERROR'
        except Exception as squareoff_exc:
            # One malformed quote must never abort cleanup for the remaining shares.
            pos['exit_reason']=f'3:20 PM square-off processing error: {squareoff_exc}'
            pos['status']='PAPER_LIVE_SQUAREOFF_DATA_ERROR'
            print(f"PAPER 15:20 square-off failed for {pos.get('symbol')}: {squareoff_exc}", flush=True)
        record_paper_live_trades([pos]); completed.append(pos)
        if pos.get('sell_actual_qty'):
            _mobile_order_event('ORDER_EXIT_1520','PAPER',pos,'SELL',price=pos.get('sell_actual_price'),quantity_value=pos.get('sell_actual_qty'),reason=pos.get('exit_reason'),status=pos.get('status'))
    positions.clear()
    for pos in completed:
        try:
            refresh_breeze_actual_charges(pos, breeze)
            apply_icici_charges(pos, exchange='NSE', breeze=breeze)
            pos['data_source']='Breeze live streaming quote + Breeze executed trade detail'
        except Exception as exc:
            pos['charge_error']=str(exc)
            pos['gross_pnl']=(pos.get('sell_actual_price',0)-pos.get('buy_actual_price',0))*pos.get('quantity',0)
            pos['simulated_total_charges']=0.0
            pos['net_pnl']=pos['gross_pnl']
    for pos in completed:
        record_paper_live_trades([pos])
        if pos.get('sell_actual_qty'):
            _mobile_order_event('ORDER_PNL_FINAL','PAPER',pos,'SELL',price=pos.get('sell_actual_price'),quantity_value=pos.get('sell_actual_qty'),reason=pos.get('exit_reason'),status=pos.get('status'))
    _write_mobile_order_state({}, {})
    _save_paper_engine_state(now.date(), {}, completed, counts, _total_buys, triggered, daily_loss_locked)
    try: update_overview_sheet()
    except Exception: pass
    print(f'AUTO PAPER MARKET COMPLETE: {len(completed)} paper trade rows recorded across enabled logics.')
    _write_run_status('PAPER TRADING', 'COMPLETED', f'Paper session completed. Completed trade rows={len(completed)}. See Paper Live Trading Shares for trade ledger; zero rows means no paper position was opened.', str(now.date()), len(completed))
    return completed

def _validate_auto_live_safety(enabled):
    """Validate the shared LIVE safety configuration for the streaming engine.

    The streaming LIVE path does not use run_orders(), so it must enforce the
    same explicit safety lock and per-run/order-value limits itself.
    """
    safety = load_trading_safety()
    if not LIVE_EXECUTION_CERTIFIED:
        raise RuntimeError('LIVE SAFETY LOCK: this r90 build is not certified for real-money execution. No LIVE orders may be submitted.')
    if not bool(safety.get('strict_strategy_parity', True)):
        raise RuntimeError(
            'LIVE SAFETY LOCK: strict_strategy_parity must remain enabled. '
            'Broker-side GTT/pretrigger paths are disabled until they use the same immutable authorization record. '
            'NO LIVE order was sent.'
        )
    if float(safety.get('max_daily_loss', 0) or 0) > 0:
        raise RuntimeError(
            'LIVE SAFETY LOCK: max_daily_loss is configured but broker-wide realized + unrealized '
            'P&L enforcement is not verified. NO LIVE order was sent.'
        )
    if not bool(safety.get('live_enabled', False)):
        raise RuntimeError(
            'LIVE SAFETY LOCK: live_enabled=false. Enable LIVE explicitly in '
            'Config/trading_safety.json after paper testing. NO LIVE order was sent.'
        )
    # GUI per-logic buy_amount and max_orders govern entry budget/count.
    # max_trade_value is retained only for backward-compatible config parsing;
    # it is deliberately not an independent cap.
    return safety


def _live_sell_limit_price(breeze, stock: str, desired_price: float) -> float:
    """Return an aggressive but valid NSE cash-equity sell limit.

    A target/stop decision is a minimum acceptable reference, but the actual
    order should remain executable if LTP has moved through that reference.
    """
    desired = float(desired_price or 0)
    if desired <= 0:
        raise RuntimeError('LIVE sell has no valid decision price.')
    try:
        quote = broker_call(f'LIVE sell quote {stock}', breeze.get_quotes,
                            stock_code=stock, exchange_code='NSE', product_type='cash')
        rows = quote.get('Success') or []
        ltp = float(rows[0].get('ltp') or 0) if rows and isinstance(rows[0], dict) else 0
    except Exception:
        ltp = 0.0
    if ltp > 0:
        desired = min(desired, ltp * 0.998)
    return max(0.05, math.floor(desired / 0.05) * 0.05)


def _place_live_sell(breeze, order: dict, decision_price: float, reason: str, user_remark: str,
                     timeout_seconds: int = 20) -> dict:
    """Place and verify an app-owned LIVE SELL before marking a position closed.

    The old auto-LIVE path referenced this function without defining it. This
    implementation also prevents a submitted-but-unfilled order from being
    reported as a completed trade.
    """
    stock = str(order.get('breeze_symbol') or order.get('symbol') or '').strip()
    qty = int(order.get('buy_actual_qty') or order.get('quantity') or 0)
    if not stock or qty <= 0:
        raise RuntimeError('LIVE sell requires a valid symbol and positive filled quantity.')
    limit = _live_sell_limit_price(breeze, stock, decision_price)
    response = broker_call(f'LIVE sell {stock}', breeze.place_order,
                           stock_code=stock, exchange_code='NSE', product='cash',
                           action='sell', order_type='limit', stoploss='',
                           quantity=str(qty), price=str(limit), validity='day',
                           user_remark=user_remark)
    success = response.get('Success') if isinstance(response, dict) else None
    oid = success.get('order_id') if isinstance(success, dict) else None
    if response.get('Status') != 200 or not oid:
        raise RuntimeError(f'LIVE sell was not accepted: {response}')
    order['sell_order_id'] = oid
    order['sell_limit_price'] = limit
    order['exit_reason'] = reason
    order['status'] = 'LIVE_SELL_ORDERED'
    deadline = time.monotonic() + max(1, int(timeout_seconds))
    terminal_bad = {'rejected', 'cancelled', 'expired'}
    while time.monotonic() < deadline:
        try:
            detail = broker_call(f'LIVE sell status {stock}', breeze.get_order_detail,
                                 exchange_code='NSE', order_id=oid)
            rows = detail.get('Success') or []
            row = rows[0] if rows and isinstance(rows[0], dict) else {}
            status = str(row.get('status') or '').strip().lower()
            if status == 'executed':
                executed_qty = int(float(row.get('quantity') or row.get('executed_quantity') or qty))
                executed_price = float(row.get('average_price') or row.get('execution_price') or limit)
                order['sell_actual_qty'] = executed_qty
                order['sell_actual_price'] = executed_price
                order['sell_trade_datetime'] = _parse_breeze_datetime(
                    row.get('exchange_time') or row.get('exchange_trade_time') or row.get('order_datetime')
                ) or datetime.now()
                order['status'] = 'LIVE_SELL_FILLED'
                return order
            if status in terminal_bad:
                raise RuntimeError(f'LIVE sell order {oid} became {status}.')
        except RuntimeError:
            raise
        except Exception as exc:
            order['sell_status_error'] = str(exc)
        time.sleep(1.0)

    # Never claim a timeout as a fill. Cancel the still-open order and surface
    # the failure so the caller can restore a protective stop.
    try:
        broker_call(f'Cancel timed-out LIVE sell {oid}', breeze.cancel_order,
                    exchange_code='NSE', order_id=oid)
    except Exception as exc:
        order['sell_cancel_error'] = str(exc)
    raise RuntimeError(f'LIVE sell order {oid} was accepted but not confirmed filled within {timeout_seconds}s.')


def _live_buy_submission_lock(logic_name: str, symbol: str, trade_date=None):
    """Persistent cross-process guard: at most one LIVE BUY submission per logic/symbol/day."""
    d = trade_date or datetime.now().date()
    lock_dir = ROOT / 'runtime_order_locks'
    lock_dir.mkdir(parents=True, exist_ok=True)
    safe_logic = re.sub(r'[^A-Za-z0-9_.-]+', '_', str(logic_name))
    safe_symbol = re.sub(r'[^A-Za-z0-9_.-]+', '_', str(symbol).upper())
    path = lock_dir / f'{d:%Y%m%d}_{safe_logic}_{safe_symbol}.buylock'
    try:
        fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, 'w', encoding='utf-8') as fh:
            json.dump({'date': str(d), 'logic': str(logic_name), 'symbol': str(symbol).upper(), 'created_at': datetime.now().isoformat(timespec='seconds')}, fh)
        return path
    except FileExistsError:
        return None

def _release_live_buy_submission_lock(path):
    if path:
        try: Path(path).unlink(missing_ok=True)
        except Exception: pass


def run_auto_live_market(universe: str = "NSE_ALL_EQ") -> list[dict]:
    """Automatic LIVE engine using every enabled logic configuration with the same Historical/Paper decision contract.

    A mobile STOP request uses a flag instead of killing this process, allowing
    the normal finally/15:25 square-off cleanup to run.
    """
    _clear_remote_stop()
    verify_canonical_strategy_parity()
    _refresh_latest_gui_logic_settings()
    now=_wait_for_next_paper_live_session('LIVE MARKET')
    print(f'AUTO LIVE MARKET: NSE CLOCK ACTIVE = {now:%Y-%m-%d %H:%M:%S %Z}')
    _live_market_gate()
    print(f'BUILD VERSION: {BUILD_VERSION}; MODE=LIVE; real broker orders may be sent only after explicit LIVE safety configuration.', flush=True)
    _log_runtime_build_identity('LIVE')
    breeze=require_breeze(); check_breeze_static_ip(os.getenv('BREEZE_STATIC_IP','')); _validate_customer_session(breeze,os.getenv('BREEZE_SESSION_TOKEN',''),defer_trade_date_mismatch=True)
    enabled=_enabled_runtime_logics()
    if not enabled: print('AUTO LIVE MARKET: all active logics are OFF.'); return []
    for _logic_name,_cfg in enabled:
        print(
            f'ACTIVE LIVE GUI CONFIG {_logic_name}: enabled={bool(_cfg.get("enabled", False))}; '
            f'budget_per_entry=₹{float(_cfg.get("buy_amount", 0) or 0):.2f}; '
            f'entry_window={_cfg.get("buy_entry_time", "09:15")}..{_cfg.get("buy_exit_time", "15:20")}; '
            f'movement={float(_cfg.get("open_movement", 0) or 0):.4f}%; '
            f'minimum_price=₹{float(_cfg.get("minimum_price", 0) or 0):.2f}; '
            f'VWAP={bool(_cfg.get("smart_require_vwap", False))}; '
            f'RVOL>{float(_cfg.get("smart_min_volume_ratio", 0) or 0):.4f}; '
            f'ORB15={bool(_cfg.get("require_15m_orb", False))}; '
            f'target={float(_cfg.get("profit_target", 0) or 0):.4f}%; '
            f'stop={float(_cfg.get("initial_stop", 0) or 0):.4f}%; '
            f'trailing={float(_cfg.get("trailing_stop", 0) or 0):.4f}%; '
            f'max_orders={max_orders_for_logic(_cfg)}', flush=True
        )
    universe_cap=_common_trade_universe_cap(enabled)
    shared_rows, shared_symbols = _load_shared_trade_universe(now.date(), enabled, universe_cap)
    selected_universe=set(shared_symbols)
    shared_rank={}
    print(f'AUTO LIVE MARKET: canonical first-cum-first-buy selector active; cap={universe_cap or "UNLIMITED"}.')
    safety=_validate_auto_live_safety(enabled)
    # No global per-order cap: each enabled GUI logic supplies buy_amount.
    print(
        'LIVE RISK CONFIG: explicit live_enabled=True; '
        'per-entry budget=GUI per-logic buy_amount; BUY count limit=GUI per-logic Maximum Orders; '
        'daily_loss_stop=DISABLED (max_daily_loss=0); positive daily-loss limits remain blocked '
        'until broker-wide realized + unrealized P&L enforcement is verified.',
        flush=True)
    nse_map=getattr(breeze,'stock_script_dict_list',None)
    if not nse_map or len(nse_map)<2 or not nse_map[1]: raise RuntimeError('AUTO LIVE MARKET: Breeze NSE security master is unavailable. NO LIVE ORDER was sent.')
    symbol_to_token=_nse_eq_token_universe(nse_map[1], getattr(breeze,'token_script_dict_list', [{} , {}])[1] if isinstance(getattr(breeze,'token_script_dict_list',None), (list, tuple)) and len(getattr(breeze,'token_script_dict_list',[])) > 1 else {})
    _selected_symbols = _nse_universe_symbols(universe)
    if _selected_symbols is not None:
        symbol_to_token = {k: v for k, v in symbol_to_token.items() if k in _selected_symbols}
        if not symbol_to_token:
            raise RuntimeError(f"Selected {universe} universe has no verified Breeze NSE EQ tokens; no LIVE run started.")
    symbols=sorted(symbol_to_token)
    print(f"AUTO LIVE MARKET: universe={universe}; enabled logics={', '.join(x[0] for x in enabled)}; verified shared scripts={len(symbols)}", flush=True)
    start_dt=datetime.combine(now.date(),datetime.min.time(),tzinfo=now.tzinfo).replace(hour=9,minute=15); squareoff_dt=datetime.combine(now.date(),datetime.min.time(),tzinfo=now.tzinfo).replace(hour=15,minute=20)
    while _nse_now()<start_dt:
        now_ist=_nse_now(); remaining=(start_dt-now_ist).total_seconds(); print(f'AUTO LIVE MARKET: NSE clock={now_ist:%H:%M:%S %Z}; waiting for 09:15 ({max(0,int(remaining))}s).'); time.sleep(min(30,max(1,remaining)))
    import threading
    tick_lock=threading.Lock(); latest={}; last_ltp_by_symbol={}; token_to_symbol={str(v).strip():k for k,v in symbol_to_token.items() if k in symbols}; nonlocal_last_tick=[time.monotonic()]
    def on_tick(tick):
        try:
            batch = tick if isinstance(tick, (list, tuple)) else (tick,)
            for item in batch:
                if not isinstance(item, dict): continue
                sym=_normalize_breeze_stream_symbol(item,token_to_symbol,symbols)
                if not sym: continue
                q=_paper_stream_quote_to_state(item)
                if not q or q.get('ltp',0)<=0: continue
                q['symbol']=sym
                # LIVE must seed genuine 09:15-09:29 candles before an ORB-enabled
                # logic can pass. Never infer or synthesize missing opening minutes.
                _orb_enabled = any(bool(_cfg.get('require_15m_orb', False)) for _, _cfg in enabled)
                if _orb_enabled and (q.get('time') or _nse_now()).time() >= datetime.strptime('09:30', '%H:%M').time():
                    _orb_stamp = q.get('time') or _nse_now()
                    _orb_key = (_orb_stamp.date().isoformat(), sym)
                    _orb_move = 0.0
                    _orb_priority_score = 99.0
                    _orb_px = float(q.get('ltp') or 0.0)
                    _orb_prev = float(q.get('previous_close') or 0.0)
                    _orb_open = float(q.get('open') or q.get('open_price') or 0.0)
                    if _orb_px > 0 and _orb_prev > 0:
                        _orb_move = ((_orb_px / _orb_prev) - 1.0) * 100.0
                    _orb_candidate = False
                    for _, _cfg in enabled:
                        if not bool(_cfg.get('require_15m_orb', False)):
                            continue
                        if _orb_px < float(_cfg.get('minimum_price', 0) or 0):
                            continue
                        if _orb_move < float(_cfg.get('open_movement', 0) or 0) - 1.5:
                            continue
                        if not direction_ok(_orb_open, _orb_prev, _cfg.get('open_direction', 'BOTH')):
                            continue
                        if price_band_enabled(_cfg) and not price_band_ok(_orb_open, _cfg):
                            continue
                        _orb_candidate = True
                        _orb_priority_score = min(_orb_priority_score,
                            _orb_seed_priority_score(_orb_move, _cfg.get('open_movement', 0)))
                    if _orb_candidate:
                        _orb_state = _LIVE_ORB15_STATE.setdefault(_orb_key, {
                            'minute': None, 'bar': None, 'orb_high': 0.0,
                            'orb_minutes_seen': set(), 'cum_volume': 0.0,
                            'cum_turnover': 0.0, 'last_signal': None,
                            'last_completed_close': 0.0, 'last_completed_vwap': 0.0,
                            'last_tick_time': None, 'last_stream_volume': None,
                            'vwap_seeded': False, 'vwap_source': None,
                        })
                        _orb_future = _orb_state.get('orb_seed_future')
                        _orb_attempts = int(_orb_state.get('orb_seed_attempts', 0) or 0)
                        _orb_retry_at = float(_orb_state.get('orb_seed_retry_at', 0.0) or 0.0)
                        if (_orb_future is None and not _orb_state.get('orb_seed_applied')
                                and _orb_attempts < 3 and time.monotonic() >= _orb_retry_at):
                            _queue_live_orb_seed(_orb_key, breeze, sym, _orb_stamp,
                                                 priority_score=_orb_priority_score)
                        _orb_future = _orb_state.get('orb_seed_future')
                        if _orb_future is not None and _orb_future.done() and not _orb_state.get('orb_seed_applied'):
                            _orb_queue_full = False
                            try:
                                _orb_result = _orb_future.result()
                            except Exception as _orb_exc:
                                if isinstance(_orb_exc, _HistoricalSeedQueueFull):
                                    _orb_queue_full = True
                                    _orb_state['orb_seed_attempts'] = max(0, int(_orb_state.get('orb_seed_attempts', 0) or 0) - 1)
                                    _orb_state['orb_seed_retry_at'] = time.monotonic() + 2.0
                                _orb_result = {'_seed_error': f'{type(_orb_exc).__name__}: {_orb_exc}'}
                            _orb_state['orb_seed_future'] = None
                            if isinstance(_orb_result, dict) and _orb_result.get('_orb_minutes_seen'):
                                _orb_state['orb_seed_applied'] = True
                                _orb_state['orb_high'] = float(_orb_result['_orb_high'])
                                _orb_state['orb_minutes_seen'] = set(_orb_result['_orb_minutes_seen'])
                                _orb_state['orb_ready'] = True
                                print(f'LIVE ORB READY: {sym}; high={_orb_state["orb_high"]:.2f}; 15/15 genuine minutes verified.', flush=True)
                            else:
                                _orb_state['orb_ready'] = False
                                _reason = _orb_result.get('_seed_error', 'invalid historical opening data') if isinstance(_orb_result, dict) else 'invalid historical opening data'
                                if int(_orb_state.get('orb_seed_attempts', 0)) < 3:
                                    _delay = 2.0 if _orb_queue_full else 20.0
                                    _orb_state['orb_seed_retry_at'] = time.monotonic() + _delay
                                    print(f'LIVE ORB WARMUP WARNING: {sym}; {_reason}; retry in {_delay:.0f}s.', flush=True)
                                else:
                                    _orb_state['orb_seed_applied'] = True
                                    print(f'LIVE ORB BLOCKED: {sym}; three attempts exhausted; {_reason}.', flush=True)
                            with _LIVE_ORB_SEED_SCHEDULED_LOCK:
                                _LIVE_ORB_SEED_SCHEDULED.discard(_orb_key)
                q=_update_live_orb15_state(sym,q)
                if q.get('orb15_reason') == 'OUT_OF_ORDER_TICK': continue
                q['_received_monotonic']=time.monotonic()
                with tick_lock:
                    q['previous_ltp']=last_ltp_by_symbol.get(sym)
                    last_ltp_by_symbol[sym]=float(q.get('ltp') or 0)
                    latest[sym]=q
                nonlocal_last_tick[0]=q['_received_monotonic']
        except Exception as exc: print(f'AUTO LIVE STREAM TICK ERROR: {type(exc).__name__}: {exc}')
    breeze.on_ticks=on_tick
    try:breeze.ws_connect()
    except Exception as exc:raise RuntimeError(f'AUTO LIVE MARKET: Breeze websocket connection failed. NO live order was sent. {exc}') from exc
    subscribed, feed_failures = _smart_subscribe_all_nse(breeze, symbols, symbol_to_token, 'AUTO LIVE MARKET')
    try:
        breeze.subscribe_feeds(get_order_notification=True)
        print('AUTO LIVE MARKET: order notifications subscribed.')
    except Exception as exc:
        print(f'AUTO LIVE MARKET: order notification subscription failed: {exc}')
    # GUI Maximum Orders is the sole BUY count limit, independently per logic.
    _total_buys=0
    positions={}; prearmed={}; triggered=set(); gtt_retry={}; buy_retry={}; counts={name:0 for name,_ in enabled}; completed=[]; last_report=0.0
    _live_gui_caps=', '.join(f'{name}={max_orders_for_logic(cfg) if max_orders_for_logic(cfg)>0 else "UNLIMITED"}' for name,cfg in enabled)
    print(f'LIVE GUI LOGIC BUY CAPS: {_live_gui_caps}; no independent aggregate order-count cap; BUY counts are enforced per GUI logic; SELL exits follow each position logic.')
    gtt_last_poll=0.0; gtt_status_cache={}; gtt_poll_error=''; last_feed_retry=0.0; last_reconnect=0.0; last_mobile_state=0.0
    try:
        while _nse_now()<squareoff_dt and not _remote_stop_requested():
            now_mono=time.monotonic()
            if now_mono-last_feed_retry>=10 and feed_failures:
                last_feed_retry=now_mono; _smart_retry_failed_subscriptions(breeze,feed_failures,symbol_to_token,'AUTO LIVE MARKET')
            if now_mono-nonlocal_last_tick[0]>8 and now_mono-last_reconnect>=15:
                last_reconnect=now_mono
                try:
                    _live_audit('FEED_HEARTBEAT_STALL',seconds=round(now_mono-nonlocal_last_tick[0],1))
                    print('AUTO LIVE MARKET: feed heartbeat stalled; reconnecting websocket and restoring subscriptions.')
                    _, feed_failures = _smart_reconnect_feeds(breeze,symbols,symbol_to_token,'AUTO LIVE MARKET')
                    try: breeze.subscribe_feeds(get_order_notification=True)
                    except Exception as _notify_exc: print(f'AUTO LIVE MARKET: order notification resubscribe failed: {_notify_exc}')
                    # Only the tick callback may refresh the feed heartbeat.
                except Exception as exc:
                    _live_audit('FEED_RECONNECT_FAILED',error=repr(exc))
                    print(f'AUTO LIVE MARKET: feed reconnect failed: {exc}')
            feed_age=time.monotonic()-nonlocal_last_tick[0]
            feed_healthy=feed_age <= 30.0
            if not feed_healthy and int(now_mono) % 10 == 0:
                print(f'AUTO LIVE MARKET: FEED UNHEALTHY; last tick {feed_age:.1f}s ago; new LIVE BUY decisions paused until fresh ticks resume.')
            with tick_lock:snapshot=dict(latest)
            _runtime_momentum_enrich(snapshot)
            if time.monotonic()-last_mobile_state>=1.0:
                _write_mobile_order_state(positions, snapshot); last_mobile_state=time.monotonic()
            # Poll the broker-side GTT book once per second for all pending GTTs.
            # This is one API call for the whole book, not one call per share.
            if positions and time.monotonic()-gtt_last_poll>=1.0:
                gtt_last_poll=time.monotonic()
                try:
                    gtt_status_cache=_gtt_status_map(_gtt_book_snapshot(breeze)); gtt_poll_error=''
                except Exception as exc:
                    gtt_poll_error=str(exc)
                    _live_audit('GTT_BOOK_POLL_FAILED',error=repr(exc))
            for sym,q in sorted(snapshot.items(), key=lambda item: (-_news_priority_score(item[0]), float((item[1] or {}).get('_received_monotonic', float('inf')) or float('inf')), str(item[0]))):
                if q.get('ltp',0)<=0 or q.get('open',0)<=0:continue
                for logic_name,cfg in enabled:
                    key=(logic_name,sym); pos=positions.get(key)
                    # BUY TIMEOUT RULE: after the configured Buy Exit Time, do not
                    # create any NEW LIVE BUY/GTT. Existing filled positions continue
                    # to be monitored until their target/stop/trailing exit or the 15:25 pre-close square-off.
                    buy_cutoff = q.get('time')
                    try:
                        xh,xm=[int(v) for v in str(cfg.get('buy_exit_time','15:20')).split(':')]
                        buy_cutoff = buy_cutoff.replace(hour=xh, minute=xm, second=0, microsecond=0)
                    except Exception:
                        buy_cutoff = None
                    new_buy_allowed = feed_healthy and bool(q.get('time')) and (buy_cutoff is None or q.get('time') < buy_cutoff) and entry_time_ok(q.get('time'),cfg)
                    gtt_enabled=(not bool(load_trading_safety().get('strict_strategy_parity',True))) and bool(cfg.get('use_gtt_pretrigger',True)) and float(cfg.get('open_movement',0) or 0)>0 and not bool(cfg.get('require_15m_orb',False))
                    if pos is None and (max_orders_for_logic(cfg)==0 or counts[logic_name]<max_orders_for_logic(cfg)) and key not in triggered and new_buy_allowed:
                        # Stage 1: persistently ARM at Open Movement - 0.15%. The arm
                        # survives a fast tick; no order is sent until all entry conditions
                        # are valid. This prevents the pre-trigger event from being missed.
                        if gtt_enabled and key not in prearmed:
                            arm=_pretrigger_entry_active(q,cfg)
                            if arm.get('active'):
                                prearmed[key]={'symbol':sym,'logic':logic_name,'armed_datetime':q.get('time'),'pre_pct':arm['pre_pct'],'pre_level':arm['pre_level'],'actual_level':arm['actual_level'],'status':'LIVE_PREARMED'}
                                _mobile_order_event('PRETRIGGER_ARMED','LIVE',prearmed[key],'BUY',price=arm['actual_level'],status='LIVE_PREARMED')
                                _live_audit('PRETRIGGER_ARMED',logic=logic_name,symbol=sym,arm_pct=arm['pre_pct'],pre_level=arm['pre_level'],trigger_price=arm['actual_level'])
                                print(f"LIVE PRE-ARMED {logic_name}: {sym} arm={arm['pre_pct']:.2f}% actual={arm['actual_level']:.2f} at {q['time']}")
                        arm=prearmed.get(key)
                        if arm and _gtt_pretrigger_qualifies(q,cfg) and time.monotonic() >= float((gtt_retry.get(key) or {}).get('next_retry', 0)):
                            # GTT is an execution mechanism only. The canonical
                            # first-cum-first-buy slot is reserved only after the
                            # complete entry evaluator is true.
                            if not _position_entry_matches(sym, q, logic_name, cfg):
                                continue
                            if not _universe_allows_symbol(selected_universe, sym, universe_cap):
                                continue
                            actual_level=float(arm['actual_level']); ltp=float(q.get('ltp',0) or 0); qty=quantity(actual_level,cfg)
                            gtt_value=float(qty)*float(_round_limit(actual_level)) if qty>0 else 0.0
                            if qty>0 and (max_orders_for_logic(cfg)==0 or counts[logic_name] < max_orders_for_logic(cfg)) :
                                _ok, _rank = _canonical_first_cum_first_buy_select(selected_universe, sym, universe_cap)
                                if not _ok:
                                    continue
                                try:
                                    # If price already crossed the real trigger, use the
                                    # direct path. Otherwise place the broker GTT now so a
                                    # fast move through the trigger is not lost.
                                    if ltp >= actual_level:
                                        price=_round_limit(ltp)
                                        qty=quantity(price,cfg)
                                        if qty < 1:
                                            continue
                                        auth=canonical_live_buy_authorization(symbol=sym,logic=logic_name,price=price,open_price=q.get('open'),previous_close=q.get('previous_close'),volume=q.get('volume'),turnover_cr=q.get('turnover_cr'),cfg=cfg,rank=_rank,decision_time=q.get('time'),market_state=q)
                                        _live_audit('LIVE_BUY_ATTEMPT',logic=logic_name,symbol=sym,quantity=qty,price=price,reason='PREARMED_TRIGGER_CROSSED',decision_hash=auth['decision_hash'])
                                        _buy_lock = _live_buy_submission_lock(logic_name, sym, now.date())

                                        if not _buy_lock:

                                            _live_audit('LIVE_DUPLICATE_BLOCKED',logic=logic_name,symbol=sym,reason='daily BUY submission lock already exists'); continue

                                        resp=_submit_authorized_live_buy(breeze,authorization=auth,stock_code=sym,quantity_value=qty,price=price,user_remark=f'BREEZE{logic_name[-1]}BUY')
                                        success=resp.get('Success') if isinstance(resp,dict) else None; oid=success.get('order_id') if isinstance(success,dict) else None
                                        if resp.get('Status')!=200 or not oid: raise RuntimeError(f'BUY rejected: {resp}')
                                        counts[logic_name]+=1; triggered.add(key); prearmed.pop(key,None)
                                        pos={'symbol':sym,'breeze_symbol':sym,'logic':logic_name,'logic_config':cfg,'quantity':qty,'buy_actual_qty':0,'buy_actual_price':price,'buy_order_id':oid,'buy_trade_datetime':q['time'],'trade_date':now.date(),'mode':'live','status':'LIVE_BUY_ORDERED','entry_state':'BUY_ORDER_PENDING','open':q['open'],'high':q['high'],'low':q['low'],'close':q['ltp'],'previous_close':q.get('previous_close'),'volume':q['volume'],'turnover_cr':q['turnover_cr'],'rank':_rank,'data_source':'Breeze live stream + prearmed canonical entry + first-cum-first-buy selector','trailing_active':False,'highest_price':price,'trailing_stop':None}
                                        positions[key]=pos; record_paper_live_trades([pos]); _mobile_order_event('ORDER_ACCEPTED','LIVE',pos,'BUY',price=price,quantity_value=qty,order_id=oid,status='LIVE_BUY_ORDERED')
                                        print(f"LIVE BUY PREARMED {logic_name} #{counts[logic_name]}: {sym} qty={qty} limit={price:.2f} order={oid}")
                                    else:
                                        gtt=_submit_entry_gtt(breeze,sym,qty,actual_level,cfg,logic_name)
                                        triggered.add(key); counts[logic_name]+=1; _total_buys+=1; prearmed.pop(key,None)
                                        pos={'symbol':sym,'breeze_symbol':sym,'logic':logic_name,'logic_config':cfg,'quantity':qty,'buy_actual_qty':0,'buy_actual_price':gtt['limit_price'],'buy_order_id':None,'gtt_order_id':gtt['gtt_order_id'],'gtt_trigger_price':gtt['trigger_price'],'gtt_limit_price':gtt['limit_price'],'buy_trade_datetime':q['time'],'trade_date':now.date(),'mode':'live','status':'LIVE_GTT_PENDING','entry_state':'GTT_ACTIVE','open':q['open'],'high':q['high'],'low':q['low'],'close':q['ltp'],'previous_close':q.get('previous_close'),'volume':q['volume'],'turnover_cr':q['turnover_cr'],'rank':_rank,'data_source':'Breeze live stream + broker-side GTT + first-cum-first-buy selector','trailing_active':False,'highest_price':gtt['limit_price'],'trailing_stop':None}
                                        positions[key]=pos; record_paper_live_trades([pos]); _mobile_order_event('GTT_ACCEPTED','LIVE',pos,'BUY',price=gtt['trigger_price'],quantity_value=qty,order_id=gtt['gtt_order_id'],status='LIVE_GTT_PENDING'); print(f"LIVE GTT READY {logic_name} #{counts[logic_name]}: {sym} prearm={arm['pre_pct']:.2f}% trigger={gtt['trigger_price']:.2f} gtt={gtt['gtt_order_id']}")
                                except Exception as exc:
                                    # First reconcile the broker GTT book. A timeout can mean the
                                    # broker accepted the GTT even though the client did not receive
                                    # the response; blindly retrying could create a duplicate GTT.
                                    adopted=None
                                    try:
                                        book=_gtt_status_map(_gtt_book_snapshot(breeze))
                                        for gid,gd in book.items():
                                            if str(gd.get('stock_code') or '').strip().upper()==sym.upper() and abs(float(gd.get('trigger_price') or 0)-float(actual_level)) <= 0.10:
                                                adopted=(gid,gd); break
                                    except Exception as reconcile_exc:
                                        _live_audit('GTT_RECONCILE_FAILED',logic=logic_name,symbol=sym,error=repr(reconcile_exc))
                                    if adopted:
                                        gid,gd=adopted; prearmed.pop(key,None); triggered.add(key); counts[logic_name]+=1; _total_buys+=1
                                        positions[key]={'symbol':sym,'breeze_symbol':sym,'logic':logic_name,'logic_config':cfg,'quantity':qty,'buy_actual_qty':0,'buy_actual_price':float(gd.get('limit_price') or actual_level),'buy_order_id':None,'gtt_order_id':gid,'gtt_trigger_price':float(gd.get('trigger_price') or actual_level),'gtt_limit_price':float(gd.get('limit_price') or actual_level),'buy_trade_datetime':q['time'],'trade_date':now.date(),'mode':'live','status':'LIVE_GTT_PENDING','entry_state':'GTT_ACTIVE','open':q['open'],'high':q['high'],'low':q['low'],'close':q['ltp'],'previous_close':q.get('previous_close'),'volume':q['volume'],'turnover_cr':q['turnover_cr'],'rank':_rank,'data_source':'Breeze live stream + reconciled broker GTT + first-cum-first-buy selector','trailing_active':False,'highest_price':float(gd.get('limit_price') or actual_level),'trailing_stop':None}
                                        _live_audit('GTT_RECONCILED_AFTER_ERROR',logic=logic_name,symbol=sym,gtt_order_id=gid)
                                    elif _explicit_order_rejection(exc):
                                        selected_universe.discard(sym.upper())
                                        attempt=int((gtt_retry.get(key) or {}).get('attempt',0))+1
                                        if attempt <= 3:
                                            gtt_retry[key]={'attempt':attempt,'next_retry':time.monotonic()+min(2.0*attempt,6.0)}
                                            _live_audit('GTT_RETRY_SCHEDULED',logic=logic_name,symbol=sym,attempt=attempt,error=repr(exc))
                                        else:
                                            _live_audit('GTT_RETRY_EXHAUSTED',logic=logic_name,symbol=sym,attempt=attempt,error=repr(exc))
                                    else:
                                        # Ambiguous transport/API failure: do not resend automatically.
                                        # Reconciliation on the next cycle must establish broker state first.
                                        gtt_retry[key]={'attempt':int((gtt_retry.get(key) or {}).get('attempt',0))+1,'next_retry':time.monotonic()+5.0}
                                        _live_audit('GTT_ERROR_REQUIRES_RECONCILIATION',logic=logic_name,symbol=sym,error=repr(exc))
                                    print(f'LIVE {logic_name} GTT/BUY handling {sym}: {exc}')
                            elif qty>0:
                                reason='GUI per-logic Maximum Orders reached' if (max_orders_for_logic(cfg)>0 and counts[logic_name] >= max_orders_for_logic(cfg)) else 'GUI per-logic investment/order conditions were not satisfied'
                                _live_audit('GTT_BUY_BLOCKED_SAFETY',logic=logic_name,symbol=sym,reason=reason,quantity=qty,order_value=gtt_value)
                        # Stage 2 fallback: if the market has already crossed the real
                        # trigger, use the same canonical entry evaluator.
                        elif (max_orders_for_logic(cfg)==0 or counts[logic_name] < max_orders_for_logic(cfg)) and _position_entry_matches(sym,q,logic_name,cfg) and time.monotonic() >= float((buy_retry.get(key) or {}).get('next_retry', 0)):
                            if not _universe_allows_symbol(selected_universe, sym, universe_cap):
                                continue
                            price=_round_limit(q['ltp'])
                            qty=quantity(price,cfg)
                            if qty<1:continue
                            try:
                                _live_audit('LIVE_BUY_ATTEMPT',logic=logic_name,symbol=sym,quantity=qty,price=price,reason='DIRECT_TRIGGER_FALLBACK')
                                order_value=float(qty)*float(price)  # audited against this logic's GUI buy_amount at submission
                                _ok, _rank = _canonical_first_cum_first_buy_select(selected_universe, sym, universe_cap)
                                if not _ok:
                                    continue
                                auth=canonical_live_buy_authorization(symbol=sym,logic=logic_name,price=price,open_price=q.get('open'),previous_close=q.get('previous_close'),volume=q.get('volume'),turnover_cr=q.get('turnover_cr'),cfg=cfg,rank=_rank,decision_time=q.get('time'),market_state=q)
                                _buy_lock = _live_buy_submission_lock(logic_name, sym, now.date())

                                if not _buy_lock:

                                    _live_audit('LIVE_DUPLICATE_BLOCKED',logic=logic_name,symbol=sym,reason='daily BUY submission lock already exists'); continue

                                resp=_submit_authorized_live_buy(breeze,authorization=auth,stock_code=sym,quantity_value=qty,price=price,user_remark=f'BREEZE{logic_name[-1]}BUY')
                                success=resp.get('Success') if isinstance(resp,dict) else None; oid=success.get('order_id') if isinstance(success,dict) else None
                                if resp.get('Status')!=200 or not oid:raise RuntimeError(f'BUY rejected: {resp}')
                                counts[logic_name]+=1; triggered.add(key); _total_buys+=1
                                pos={'symbol':sym,'breeze_symbol':sym,'logic':logic_name,'logic_config':cfg,'quantity':qty,'buy_actual_qty':0,'buy_actual_price':price,'buy_order_id':oid,'buy_trade_datetime':q['time'],'trade_date':now.date(),'mode':'live','status':'LIVE_BUY_ORDERED','entry_state':'BUY_ORDER_PENDING','open':q['open'],'high':q['high'],'low':q['low'],'close':q['ltp'],'previous_close':q.get('previous_close'),'volume':q['volume'],'turnover_cr':q['turnover_cr'],'rank':_rank,'data_source':'Breeze live streaming quote + live order + canonical first-cum-first-buy selector','trailing_active':False,'highest_price':price,'trailing_stop':None}
                                positions[key]=pos; record_paper_live_trades([pos]); _mobile_order_event('ORDER_ACCEPTED','LIVE',pos,'BUY',price=price,quantity_value=qty,order_id=oid,status='LIVE_BUY_ORDERED'); print(f"LIVE BUY {logic_name} #{counts[logic_name]}/{max_orders_for_logic(cfg) if max_orders_for_logic(cfg)>0 else '∞'}: {sym} qty={qty} limit={price:.2f} order={oid}")
                            except Exception as exc:
                                if _explicit_order_rejection(exc):
                                    selected_universe.discard(sym.upper())
                                    attempt=int((buy_retry.get(key) or {}).get('attempt',0))+1
                                    if attempt <= 3:
                                        buy_retry[key]={'attempt':attempt,'next_retry':time.monotonic()+min(1.5*attempt,5.0)}
                                        _live_audit('LIVE_BUY_RETRY_SCHEDULED',logic=logic_name,symbol=sym,attempt=attempt,error=repr(exc))
                                    else:
                                        _live_audit('LIVE_BUY_RETRY_EXHAUSTED',logic=logic_name,symbol=sym,attempt=attempt,error=repr(exc))
                                else:
                                    # An unknown/timeout response may mean the broker accepted the
                                    # order. Do not resend blindly; this avoids duplicate positions.
                                    buy_retry[key]={'attempt':int((buy_retry.get(key) or {}).get('attempt',0))+1,'next_retry':time.monotonic()+15.0}
                                    _live_audit('LIVE_BUY_ERROR_REQUIRES_RECONCILIATION',logic=logic_name,symbol=sym,error=repr(exc))
                                print(f'LIVE {logic_name} BUY FAILED {sym}: {exc}')
                    pos=positions.get(key)
                    if not pos or pos.get('status') in {'LIVE_SELL_ORDERED','LIVE_TARGET_ORDERED'}:continue
                    if pos.get('status') == 'LIVE_GTT_PENDING':
                        # A broker-side GTT that has not triggered by BUY TIMEOUT is
                        # cancelled/expired. It must never become a new BUY after the
                        # configured entry window. Already-filled positions are handled
                        # below and continue until target/stop/15:25.
                        if not new_buy_allowed:
                            try:
                                if pos.get('gtt_order_id'):
                                    broker_call(f'Cancel BUY-timeout GTT {sym}', breeze.cancel_order, exchange_code='NSE', order_id=pos['gtt_order_id'])
                            except Exception as exc:
                                print(f'LIVE BUY TIMEOUT: could not cancel pending GTT {sym}: {exc}')
                            pos['status']='LIVE_GTT_EXPIRED_BUY_TIMEOUT'
                            pos['exit_reason']='BUY timeout before GTT trigger'
                            completed.append(pos)
                            del positions[key]
                            _live_audit('GTT_EXPIRED_BUY_TIMEOUT',logic=logic_name,symbol=sym,gtt_order_id=pos.get('gtt_order_id'))
                            continue
                        gs=gtt_status_cache.get(str(pos.get('gtt_order_id')))
                        if gs:
                            st=str(gs.get('status') or '').lower()
                            fresh=gs.get('fresh_order_id')
                            if fresh:
                                pos['buy_order_id']=fresh; pos['entry_state']='BUY_ORDER_PENDING'; pos['status']='LIVE_BUY_ORDERED_FROM_GTT'; pos['gtt_status']=st
                                _live_audit('GTT_TRIGGERED',logic=logic_name,symbol=sym,gtt_order_id=pos.get('gtt_order_id'),fresh_order_id=fresh,trigger_price=pos.get('gtt_trigger_price'),limit_price=pos.get('gtt_limit_price'))
                                _mobile_order_event('GTT_TRIGGERED','LIVE',pos,'BUY',price=pos.get('gtt_limit_price'),quantity_value=pos.get('quantity'),order_id=fresh,status=pos['status'])
                                print(f"LIVE GTT TRIGGERED {logic_name}: {sym} gtt={pos.get('gtt_order_id')} order={fresh}")
                            elif st in {'rejected','cancelled','expired'}:
                                # A broker-side GTT can be accepted first and later rejected/expired.
                                # Treat that as a recoverable entry failure while the BUY window is
                                # still open: restore PRE-ARM and let the bounded retry path place a
                                # fresh GTT/direct BUY. Do not consume an order slot for a GTT that
                                # never triggered.
                                if new_buy_allowed:
                                    attempts=int((gtt_retry.get(key) or {}).get('terminal_attempts',0))+1
                                    if attempts <= 3:
                                        gtt_retry[key]={'attempt':attempts,'terminal_attempts':attempts,'next_retry':time.monotonic()+min(1.0*attempts,4.0)}
                                        _retry_open=float(q.get('open') or 0)
                                        _retry_prev=float(q.get('previous_close') or q.get('prev_close') or 0)
                                        _retry_pre_pct,_retry_pre_level=_gtt_pretrigger_level(_retry_open,cfg,_retry_prev)
                                        prearmed[key]={'symbol':sym,'logic':logic_name,'armed_datetime':pos.get('gtt_armed_datetime') or pos.get('buy_trade_datetime') or q.get('time'),'pre_pct':_retry_pre_pct,'pre_level':_retry_pre_level,'actual_level':entry_level(_retry_open,cfg,_retry_prev),'status':'LIVE_PREARMED_RETRY'}
                                        triggered.discard(key); counts[logic_name]=max(0,counts[logic_name]-1)
                                        del positions[key]
                                        _live_audit('GTT_TERMINAL_RETRY',logic=logic_name,symbol=sym,gtt_order_id=pos.get('gtt_order_id'),status=st,attempt=attempts)
                                        continue
                                pos['status']='LIVE_GTT_FAILED'; completed.append(pos); del positions[key]; triggered.discard(key); _live_audit('GTT_TERMINAL_UNFILLED',logic=logic_name,symbol=sym,gtt_order_id=pos.get('gtt_order_id'),status=st); continue
                        continue
                    if pos.get('buy_actual_qty',0)<=0:
                        try:
                            detail=broker_call(f'Live {logic_name} BUY status {sym}',breeze.get_order_detail,exchange_code='NSE',order_id=pos['buy_order_id']); rows=detail.get('Success') or []
                            if rows:
                                st=str(rows[0].get('status','')).strip().lower()
                                if st=='executed':
                                    fill=rows[0]; pos['fill']=fill; pos['buy_actual_qty']=int(float(fill.get('executed_quantity') or fill.get('quantity') or pos['quantity'])); pos['buy_actual_price']=float(fill.get('average_price') or pos['buy_actual_price']); pos['buy_trade_datetime']=_parse_breeze_datetime(fill.get('exchange_time') or fill.get('order_datetime')) or q['time']
                                    sl=_attach_stop_with_retry(breeze,pos,fill); pos['stop_loss_order_id']=sl['order_id']; pos['stop_loss']=sl; pos['status']='LIVE_BUY_FILLED_STOPLOSS_SENT'; record_paper_live_trades([pos]); _mobile_order_event('ORDER_BUY_FILLED','LIVE',pos,'BUY',price=pos['buy_actual_price'],quantity_value=pos['buy_actual_qty'],order_id=pos.get('buy_order_id'),status=pos['status']); print(f"LIVE {logic_name} FILLED {sym} qty={pos['buy_actual_qty']} price={pos['buy_actual_price']:.2f}; stop={sl.get('trigger'):.2f}")
                                elif st in {'rejected','cancelled','expired','partially executed','partially executed and cancelled','partially executed and expired'}:
                                    # Never discard a partial fill. If any quantity actually
                                    # executed, it is a real position and must receive protection.
                                    executed_qty=0
                                    try:
                                        executed_qty=int(float(rows[0].get('executed_quantity') or 0))
                                    except Exception:
                                        executed_qty=0
                                    if executed_qty <= 0 and st.startswith('partially'):
                                        try:
                                            td=_trade_detail_safe(breeze, pos.get('buy_order_id'))
                                            executed_qty=int(float((td or {}).get('executed_quantity') or 0))
                                            if executed_qty > 0:
                                                rows[0]['average_price']=rows[0].get('average_price') or (td or {}).get('execution_price') or (td or {}).get('average_price')
                                        except Exception:
                                            executed_qty=0
                                    if executed_qty > 0:
                                        fill=dict(rows[0]); fill['executed_quantity']=executed_qty
                                        pos['fill']=fill; pos['buy_actual_qty']=executed_qty
                                        pos['buy_actual_price']=float(fill.get('average_price') or pos.get('buy_actual_price') or q.get('ltp') or 0)
                                        pos['buy_trade_datetime']=_parse_breeze_datetime(fill.get('exchange_time') or fill.get('order_datetime')) or q['time']
                                        sl=_attach_stop_with_retry(breeze,pos,fill); pos['stop_loss_order_id']=sl['order_id']; pos['stop_loss']=sl
                                        pos['status']='LIVE_PARTIAL_BUY_FILLED_STOPLOSS_SENT'
                                        record_paper_live_trades([pos]); _mobile_order_event('ORDER_BUY_PARTIAL_FILLED','LIVE',pos,'BUY',price=pos['buy_actual_price'],quantity_value=executed_qty,order_id=pos.get('buy_order_id'),status=pos['status'])
                                        _live_audit('PARTIAL_BUY_PROTECTED',logic=logic_name,symbol=sym,executed_quantity=executed_qty,order_id=pos.get('buy_order_id'),stop_loss_order_id=sl['order_id'])
                                    else:
                                        if new_buy_allowed:
                                            attempts=int((buy_retry.get(key) or {}).get('terminal_attempts',0))+1
                                            if attempts <= 3:
                                                buy_retry[key]={'attempt':attempts,'terminal_attempts':attempts,'next_retry':time.monotonic()+min(1.0*attempts,4.0)}
                                                triggered.discard(key)
                                                positions.pop(key,None)
                                                _live_audit('BUY_TERMINAL_RETRY',logic=logic_name,symbol=sym,status=st,attempt=attempts)
                                                continue
                                        pos['status']='LIVE_BUY_NOT_FILLED'; completed.append(pos); del positions[key]; triggered.discard(key)
                                    continue
                        except Exception as exc:pos['last_order_check_error']=str(exc)
                    if pos.get('buy_actual_qty',0)>0:
                        # Protective-stop watchdog: a filled position is never considered
                        # protected unless a real broker stop order id exists. Retry missing
                        # protection before normal exit/trailing decisions.
                        if float((pos.get('logic_config') or {}).get('initial_stop') or 0) > 0 and not _protective_stop_is_valid(pos) and not pos.get('stop_loss_reconciliation_required'):
                            try:
                                fill=pos.get('fill') or {'average_price':pos.get('buy_actual_price')}
                                sl=_attach_stop_with_retry(breeze,pos,fill,attempts=2)
                                pos['stop_loss']=sl; pos['stop_loss_order_id']=sl['order_id']; pos['status']='LIVE_PROTECTIVE_STOP_RESTORED'
                                _live_audit('PROTECTIVE_STOP_WATCHDOG_RESTORED',logic=logic_name,symbol=sym,order_id=sl['order_id'],trigger=sl['trigger'])
                            except Exception as exc:
                                pos['status']='LIVE_UNPROTECTED_POSITION'; pos['stop_loss_error']=str(exc)
                                _live_audit('LIVE_UNPROTECTED_POSITION',logic=logic_name,symbol=sym,error=repr(exc))
                                # Do not submit target/trailing replacement sells while the
                                # position is unprotected; keep the watchdog retrying next loop.
                                continue
                        pos['high']=max(float(pos.get('high') or 0),q['high']); pos['low']=min(float(pos.get('low') or q.get('low') or q['ltp']),q.get('low') or q['ltp'])
                        # SAME chronological EXIT decision path as Historical Shadow/Paper.
                        # Only broker order/protective-stop mechanics remain Live-specific.
                        _should_exit, decision_price, decision_reason = canonical_position_exit_decision(pos, q)
                        if decision_price is not None:
                            old_stop_id=pos.get('stop_loss_order_id')
                            try:
                                if old_stop_id:
                                    broker_call(f'Cancel exit stop {sym}',breeze.cancel_order,exchange_code='NSE',order_id=old_stop_id)
                                _place_live_sell(breeze,pos,decision_price,decision_reason,f'BREEZE{logic_name[-1]}EXIT')
                                completed.append(pos); del positions[key]; continue
                            except Exception as exc:
                                print(f'LIVE {logic_name} exit sell failed {sym}: {exc}')
                                # The protective stop was cancelled before the target/exit
                                # sell. If the replacement sell was not confirmed filled,
                                # immediately restore protection rather than leaving a naked
                                # position.
                                if not pos.get('sell_actual_qty'):
                                    try:
                                        restore_qty=int(pos.get('buy_actual_qty') or pos.get('quantity') or 0)
                                        if restore_qty>0:
                                            restore=dict(pos); restore['quantity']=restore_qty
                                            fill=pos.get('fill') or {'average_price':pos.get('buy_actual_price') or pos.get('limit_price')}
                                            sl=submit_stoploss_after_fill(breeze,restore,fill,pos.get('logic_config'))
                                            pos['stop_loss']=sl; pos['stop_loss_order_id']=sl.get('order_id'); pos['status']='LIVE_EXIT_FAILED_STOP_RESTORED'
                                    except Exception as restore_exc:
                                        pos['status']='LIVE_UNPROTECTED_EXIT_FAILURE'; pos['exit_error']=str(exc); pos['stop_restore_error']=str(restore_exc)
                                        _live_audit('LIVE_UNPROTECTED_POSITION',logic=logic_name,symbol=sym,error=repr(restore_exc))
                        elif pos.get('trailing_active') and float(pos.get('trailing_stop') or 0)>0:
                            new_stop=float(pos['trailing_stop'])
                            old_stop=float(pos.get('_last_submitted_trailing_stop') or 0)
                            if new_stop > old_stop:
                                try:
                                    if pos.get('stop_loss_order_id'): broker_call(f'Cancel {logic_name} trailing stop {sym}',breeze.cancel_order,exchange_code='NSE',order_id=pos['stop_loss_order_id'])
                                    trigger=max(0.05,math.floor(new_stop/0.05)*0.05); limit=max(0.05,trigger-0.05)
                                    resp=broker_call(f'{logic_name} trailing stop {sym}',breeze.place_order,stock_code=sym,exchange_code='NSE',product='cash',action='sell',order_type='stoploss',stoploss=str(trigger),quantity=str(pos['buy_actual_qty']),price=str(limit),validity='day',user_remark=f'BREEZE{logic_name[-1]}TRAIL')
                                    success=resp.get('Success') if isinstance(resp,dict) else None; pos['stop_loss_order_id']=success.get('order_id') if isinstance(success,dict) else None; pos['_last_submitted_trailing_stop']=trigger
                                except Exception as exc: print(f'LIVE {logic_name} trailing update failed {sym}: {exc}')
            if time.monotonic()-last_report>=30:
                last_report=time.monotonic(); print(f'AUTO LIVE MARKET: streaming={len(latest):,}; open positions={len(positions)}; buys={counts}')
            time.sleep(0.5)
    finally:
        for sym in symbols:
            try:breeze.unsubscribe_feeds(stock_token=f'4.1!{symbol_to_token[sym]}',get_exchange_quotes=True,get_market_depth=False)
            except Exception:pass
        try:breeze.ws_disconnect()
        except Exception:pass
    if _remote_stop_requested():
        print('AUTO LIVE MARKET: mobile STOP requested; entering safe cleanup and square-off.')
    # Mandatory 15:20 pre-close square-off for all app-owned live positions.
    squareoff_live_app_positions(breeze,list(positions.values()))
    for key,pos in list(positions.items()):
        if pos.get('sell_actual_qty'):pos['sell_trade_datetime']=pos.get('sell_trade_datetime') or datetime.now();pos['status']='LIVE_EOD_SQUAREOFF_FILLED' if pos.get('status')=='LIVE_EOD_SQUAREOFF_FILLED' else 'LIVE_EOD_SQUAREOFF_ORDERED'
        completed.append(pos)
        record_paper_live_trades([pos])
        if pos.get('sell_actual_qty'):
            _mobile_order_event('ORDER_EXIT_1520','LIVE',pos,'SELL',price=pos.get('sell_actual_price'),quantity_value=pos.get('sell_actual_qty'),reason=pos.get('exit_reason') or '15:25 square-off',order_id=pos.get('sell_order_id'),status=pos.get('status'))
    positions.clear()
    for pos in completed:
        try:
            refresh_breeze_actual_charges(pos, breeze)
            apply_icici_charges(pos, exchange='NSE', breeze=breeze)
            pos['data_source']='Breeze live stream + Breeze executed trades + shared ICICI charge engine'
        except Exception as exc:pos['charge_error']=str(exc)
    if completed:
        record_paper_live_trades(completed)
        for pos in completed:
            if pos.get('sell_actual_qty'):
                _mobile_order_event('ORDER_PNL_FINAL','LIVE',pos,'SELL',price=pos.get('sell_actual_price'),quantity_value=pos.get('sell_actual_qty'),reason=pos.get('exit_reason'),order_id=pos.get('sell_order_id'),status=pos.get('status'))
        _write_mobile_order_state({}, {})
        record_historical_results(completed)
        try:update_overview_sheet()
        except Exception:pass
    send_final_whatsapp_report(completed, 'Live', now.date())
    print(f'AUTO LIVE MARKET COMPLETE: {len(completed)} live trade rows recorded across enabled logics.')
    return completed

def _parse_cli_date(text: str) -> date:
    text = str(text).strip()
    for fmt in ('%Y-%m-%d','%d-%m-%Y','%d/%m/%Y','%Y/%m/%d','%d.%m.%Y','%d-%b-%Y','%d %b %Y','%d-%B-%Y','%d/%m/%y'):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise ValueError(f'Invalid date: {text}')


def _monthly_workbook_path(month_text: str) -> Path:
    """Return the standalone Excel workbook for YYYY-MM."""
    y, m = [int(x) for x in str(month_text).split('-', 1)]
    _migrate_legacy_historical_month(y, m)
    _historical_month_dir(y, m).mkdir(parents=True, exist_ok=True)
    return _historical_month_dir(y, m) / f'Trading_Data_{y:04d}-{m:02d}.xlsx'


def _ensure_monthly_workbook(month_text: str) -> Path:
    """Create/open a canonical monthly workbook from current engine output only.

    Never migrate or copy rows from the old workbook/template.  A workbook that
    was not created by this canonical output layer is reset to headers before
    any current-run data is written.
    """
    path = _monthly_workbook_path(month_text)
    CANONICAL_TITLE = 'Breeze NSE Trading - Canonical Excel Output v1'
    required = {'YTD Historical Data','Selected Date','NSE Data','Paper Live Trading Shares','Run Status'}

    reset = True
    if path.exists():
        try:
            probe = load_workbook(path, read_only=False, data_only=False)
            reset = probe.properties.title != CANONICAL_TITLE
            probe.close()
        except Exception:
            reset = True

    if reset:
        wb = Workbook()
        ws = wb.active
        ws.title = 'YTD Historical Data'
        ws.append(HISTORICAL_HEADERS)
        wb.create_sheet('Selected Date').append(HISTORICAL_HEADERS)
        wb.create_sheet('NSE Data').append(NSE_HEADERS)
        wb.create_sheet('Paper Live Trading Shares').append(PAPER_LIVE_HEADERS)
        wb.create_sheet('Run Status').append(RUN_STATUS_HEADERS)
    else:
        wb = load_workbook(path)
        # Keep only the four canonical output sheets. Existing rows are retained
        # only when they were produced by this same canonical writer.
        for name in list(wb.sheetnames):
            if name not in required:
                del wb[name]
        for name, headers in (
            ('YTD Historical Data', HISTORICAL_HEADERS),
            ('Selected Date', HISTORICAL_HEADERS),
            ('NSE Data', NSE_HEADERS),
            ('Paper Live Trading Shares', PAPER_LIVE_HEADERS),
            ('Run Status', RUN_STATUS_HEADERS),
        ):
            if name not in wb.sheetnames:
                wb.create_sheet(name)
                wb[name].append(headers)
    wb.properties.title = CANONICAL_TITLE
    wb.properties.subject = 'Generated only from current NSE/Breeze data and canonical enabled-logic engine results'
    wb.properties.creator = 'Breeze NSE Trading canonical Excel output'
    wb._sheets=[wb['YTD Historical Data'], wb['Selected Date'], wb['NSE Data'], wb['Paper Live Trading Shares'], wb['Run Status']]
    wb.calculation.fullCalcOnLoad=True
    wb.calculation.forceFullCalc=True
    wb.calculation.calcMode='auto'
    wb.save(path)
    return path


def _read_existing_nse_data() -> pd.DataFrame:
    """Read existing NSE data only from the monthly Trading_Data workbooks."""
    frames=[]
    candidates=sorted(HISTORICAL_YEAR_DIR.glob('*/Month/*/Trading_Data_*.xlsx'))
    seen=set()
    for path in candidates:
        try:
            if str(path.resolve()) in seen:
                continue
            seen.add(str(path.resolve()))
            x=pd.read_excel(path, sheet_name='NSE Data')
            if not x.empty:
                frames.append(x)
        except Exception as exc:
            print(f'NSE Data existing workbook read skipped: {path.name}: {exc}')
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _upsert_nse_sheet_in_monthly_workbook(month_text: str, market_df: pd.DataFrame) -> Path:
    """Persist the daily NSE screening data into Trading_Data_YYYY-MM.xlsx.

    Historical replay loads the official daily bhavcopy itself.  That data must
    also be visible in the workbook's NSE Data sheet; otherwise a historical run
    can contain trade rows while NSE Data remains blank.  This helper updates only
    NSE Data and preserves the three other sheets.
    """
    path=_ensure_monthly_workbook(month_text)
    headers=NSE_HEADERS
    incoming=market_df.copy() if isinstance(market_df,pd.DataFrame) else pd.DataFrame()
    if incoming.empty:
        return path
    # Normalize common NSE column spellings so cached screen output always lands
    # in the workbook even if a provider returns lower-case field names.
    # Normalize without creating duplicate target columns.  screen_from_raw()
    # supplies both raw turnover and already-converted Turnover Cr; prefer the
    # canonical Turnover Cr value and only fall back to raw turnover when the
    # canonical field is absent.
    if 'Turnover Cr' in incoming.columns:
        incoming=incoming.drop(columns=['Turnover (₹)'], errors='ignore')
    incoming=incoming.rename(columns={
        'symbol':'Symbol','SYMBOL':'Symbol','open':'Open','OPEN':'Open',
        'high':'High','HIGH':'High','low':'Low','LOW':'Low',
        'close':'Close','CLOSE':'Close','previous_close':'Previous Close',
        'Previous close':'Previous Close','PREVCLOSE':'Previous Close',
        'volume':'Volume','VOLUME':'Volume','turnover_cr':'Turnover in Cr',
        'Turnover Cr':'Turnover in Cr',
        'movement_pct':'Movement %','MOVEMENT %':'Movement %',
        'source':'Source'
    })
    # If a provider supplied only raw rupee turnover, convert it to crore here.
    if 'Turnover in Cr' not in incoming.columns and 'Turnover (₹)' in incoming.columns:
        incoming['Turnover in Cr']=pd.to_numeric(incoming['Turnover (₹)'], errors='coerce')/10000000.0
        incoming=incoming.drop(columns=['Turnover (₹)'], errors='ignore')
    for h in headers:
        if h not in incoming.columns: incoming[h]=None
    incoming['Source']='NSE'
    incoming['Downloaded At']=datetime.now()
    # Continuous serial numbers are assigned ONLY to shares that pass at least
    # one enabled logic. Non-qualified shares stay blank.
    if 'Eligible?' in incoming.columns:
        incoming['Logic Serial No.']=None
        mask=incoming['Eligible?'].fillna(False).astype(bool)
        incoming.loc[mask,'Logic Serial No.']=range(1, int(mask.sum())+1)
    else:
        incoming['Logic Serial No.']=None
    incoming=incoming[headers]

    wb=load_workbook(path)
    ws=wb['NSE Data'] if 'NSE Data' in wb.sheetnames else wb.create_sheet('NSE Data')
    existing=[]
    if ws.max_row>1:
        old_headers=[ws.cell(1,c).value for c in range(1,ws.max_column+1)]
        # Map existing cells by header name. This safely migrates both the old
        # Date-first layout and the new Serial-first layout without column shifts.
        for rr in ws.iter_rows(min_row=2,values_only=True):
            row_map={str(h): v for h,v in zip(old_headers,rr) if h is not None}
            existing.append([row_map.get(h) for h in headers])
    rows=list(existing)
    for _,r in incoming.iterrows():
        rows.append([_excel_safe(r.get(h)) for h in headers])
    if rows:
        df=pd.DataFrame(rows,columns=headers)
        df['Date']=df['Date'].astype(str).str[:10]
        df['Symbol']=df['Symbol'].astype(str).str.strip().str.upper()
        df=df.drop_duplicates(subset=['Date','Symbol'],keep='last')
        df['_move']=pd.to_numeric(df['Movement %'],errors='coerce')
        df=df.sort_values(['Date','_move','Symbol'],ascending=[True,False,True],na_position='last').drop(columns=['_move'])
    else:
        df=pd.DataFrame(columns=headers)
    ws.delete_rows(1,ws.max_row)
    ws.append(headers)
    for _,r in df.iterrows(): ws.append([_excel_safe(r.get(h)) for h in headers])
    ws.freeze_panes='A2'; ws.auto_filter.ref=ws.dimensions; ws.sheet_view.showGridLines=False
    for cell in ws[1]:
        cell.font=Font(bold=True,color='FFFFFF'); cell.fill=PatternFill('solid',fgColor='1F4E78'); cell.alignment=Alignment(horizontal='center',vertical='center',wrap_text=True)
    widths=[16,12,16,14,14,14,14,16,16,18,14,18,18,12,22]
    for i,w in enumerate(widths,1): ws.column_dimensions[get_column_letter(i)].width=w
    for rr in range(2,ws.max_row+1):
        ws.cell(rr,1).number_format='0'
        ws.cell(rr,2).number_format='dd-mm-yyyy'
        ws.cell(rr,11).number_format='0.00%'
    wb.calculation.fullCalcOnLoad=True; wb.calculation.forceFullCalc=True; wb.calculation.calcMode='auto'
    wb.save(path)
    return path


def _write_nse_month_workbook(month_text: str, market_df: pd.DataFrame) -> Path:
    """Persist NSE data using one canonical column order everywhere."""
    if not month_text:
        month_text=date.today().strftime('%Y-%m')
    path=_ensure_monthly_workbook(month_text)
    headers=NSE_HEADERS
    incoming=market_df.copy() if isinstance(market_df,pd.DataFrame) else pd.DataFrame()
    if not incoming.empty:
        incoming=incoming.rename(columns={
            'symbol':'Symbol','SYMBOL':'Symbol','open':'Open','OPEN':'Open',
            'high':'High','HIGH':'High','low':'Low','LOW':'Low',
            'close':'Close','CLOSE':'Close','previous_close':'Previous Close',
            'Previous close':'Previous Close','PREVCLOSE':'Previous Close',
            'volume':'Volume','VOLUME':'Volume','turnover_cr':'Turnover in Cr',
            'Turnover Cr':'Turnover in Cr','movement_pct':'Movement %','MOVEMENT %':'Movement %',
            'source':'Source'
        })
        if 'Turnover in Cr' not in incoming.columns and 'Turnover (₹)' in incoming.columns:
            incoming['Turnover in Cr']=pd.to_numeric(incoming['Turnover (₹)'],errors='coerce')/10000000.0
        for h in headers:
            if h not in incoming.columns: incoming[h]=None
        incoming['Source']='NSE'; incoming['Downloaded At']=datetime.now()
        incoming['Logic Serial No.']=None
        if 'Eligible?' in incoming.columns:
            mask=incoming['Eligible?'].fillna(False).astype(bool)
            incoming.loc[mask,'Logic Serial No.']=range(1,int(mask.sum())+1)
        incoming=incoming[headers]
    else:
        incoming=pd.DataFrame(columns=headers)

    # Read current workbook rows and normalize by header name, so an older
    # Date-first workbook can never shift Date into the Serial No. column.
    existing=pd.DataFrame(columns=headers)
    try:
        mwb=load_workbook(path,read_only=True,data_only=True)
        if 'NSE Data' in mwb.sheetnames:
            vals=list(mwb['NSE Data'].values)
            if len(vals)>1:
                old=pd.DataFrame(vals[1:],columns=list(vals[0]))
                for h in headers:
                    if h not in old.columns: old[h]=None
                existing=old[headers]
        mwb.close()
    except Exception as exc:
        print(f'NSE Data current-workbook read skipped: {exc}')

    combined=pd.concat([existing,incoming],ignore_index=True)
    if not combined.empty:
        combined['Date']=combined['Date'].apply(_excel_date)
        combined['Symbol']=combined['Symbol'].astype(str).str.strip().str.upper()
        combined=combined.drop_duplicates(subset=['Date','Symbol'],keep='last')
        combined['_move']=pd.to_numeric(combined['Movement %'],errors='coerce')
        combined=combined.sort_values(['Date','_move','Symbol'],ascending=[True,False,True],na_position='last').drop(columns=['_move'])
        # Re-number continuously across the final displayed NSE rows for each
        date_key=combined['Date'].astype(str)
        combined['Logic Serial No.']=None
        if 'Eligible?' in combined.columns:
            mask=combined['Eligible?'].fillna(False).astype(bool)
        else:
            # Preserve serial values already calculated for incoming/current rows.
            mask=combined['Logic Serial No.'].notna()
        for d in date_key.drop_duplicates().tolist():
            idx=combined.index[(date_key==d) & mask]
            combined.loc[idx,'Logic Serial No.']=range(1,len(idx)+1)
        combined=combined[headers]

    wb=load_workbook(path)
    ws=wb['NSE Data'] if 'NSE Data' in wb.sheetnames else wb.create_sheet('NSE Data')
    ws.delete_rows(1,ws.max_row); ws.append(headers)
    for _,r in combined.iterrows(): ws.append([_excel_safe(r.get(h)) for h in headers])
    ws.freeze_panes='A2'; ws.auto_filter.ref=ws.dimensions; ws.sheet_view.showGridLines=False; ws.sheet_view.zoomScale=90
    for cell in ws[1]:
        cell.font=Font(bold=True,color='FFFFFF',size=10); cell.fill=PatternFill('solid',fgColor='17365D'); cell.alignment=Alignment(horizontal='center',vertical='center',wrap_text=True)
    ws.row_dimensions[1].height=34
    widths=[16,13,14,14,14,14,14,16,16,18,14,18,22]
    for i,w in enumerate(widths,1): ws.column_dimensions[get_column_letter(i)].width=w
    for rr in range(2,ws.max_row+1):
        ws.cell(rr,1).number_format='0'; ws.cell(rr,2).number_format='dd-mm-yyyy'; ws.cell(rr,11).number_format='0.00%'
    _ensure_paper_live_sheet(wb)
    wb._sheets=[wb['YTD Historical Data'],wb['Selected Date'],wb['NSE Data'],wb['Paper Live Trading Shares'],wb['Run Status']]
    wb.save(path)
    return path

def run_nse_date_range(start_text: str, end_text: str) -> None:
    start, end = _parse_cli_date(start_text), _parse_cli_date(end_text)
    if end < start:
        raise RuntimeError('End date cannot be before start date.')
    session = _new_nse_session()
    frames=[]
    day=start
    while day<=end:
        if _is_nse_trading_day(day):
            try:
                raw=_load_cached_daily(day)
                if raw is None:
                    print(f'NSE daily {day}: downloading official UDiFF bhavcopy')
                    raw=bhavcopy_for_date(day,session); _save_cached_daily(day,raw)
                else:
                    print(f'NSE daily {day}: using cached NSE bhavcopy')
                market,_=screen_from_raw(raw)
                market.insert(0,'Date',day.isoformat())
                frames.append(market)
                print(f'NSE daily {day}: {len(market):,} NSE EQ rows; {int(market["Eligible?"].sum() if "Eligible?" in market.columns else 0):,} shares pass at least one configured logic')
            except Exception as exc:
                print(f'NSE daily {day}: ERROR {exc}')
        day+=timedelta(days=1)
    if not frames:
        raise RuntimeError('No NSE trading-day data was downloaded for the requested range.')
    out=pd.concat(frames,ignore_index=True)
    out['Movement %']=pd.to_numeric(out['Movement %'],errors='coerce')
    # Write the downloaded NSE data to BOTH the monthly Trading_Data workbook
    # and the main workbook's NSE Data sheet.  The main workbook is what the GUI
    # opens/displayers use, so leaving it untouched makes the NSE sheet appear blank
    # even though the download itself succeeded.
    path=_write_nse_month_workbook('',out)
    try:
        main_wb=load_workbook(WORKBOOK)
        if 'NSE Data' not in main_wb.sheetnames:
            ws_main=main_wb.create_sheet('NSE Data')
        else:
            ws_main=main_wb['NSE Data']
        headers=NSE_HEADERS
        ws_main.delete_rows(1, ws_main.max_row)
        ws_main.append(headers)
        now=datetime.now()
        eligible_mask=out['Eligible?'].fillna(False).astype(bool) if 'Eligible?' in out.columns else pd.Series(False,index=out.index)
        eligible_serial=0
        for _, r in out.iterrows():
            is_eligible=bool(r.get('Eligible?',False))
            if is_eligible:
                eligible_serial += 1
                serial=eligible_serial
            else:
                serial=None
            dt=_excel_safe(r.get('Date'))
            vals=[serial, dt, r.get('Symbol'), r.get('Open'), r.get('High'), r.get('Low'),
                  r.get('Close'), r.get('Previous close'), r.get('Volume'),
                  r.get('Turnover Cr') if 'Turnover Cr' in out.columns else r.get('Turnover (₹)'),
                  r.get('Movement %'), 'NSE', now]
            ws_main.append([_excel_safe(v) for v in vals])
        ws_main.freeze_panes='A2'; ws_main.auto_filter.ref=ws_main.dimensions
        ws_main.sheet_view.showGridLines=False; ws_main.sheet_view.zoomScale=90
        for cell in ws_main[1]:
            cell.font=Font(bold=True,color='FFFFFF',size=10)
            cell.fill=PatternFill('solid',fgColor='17365D')
            cell.alignment=Alignment(horizontal='center',vertical='center',wrap_text=True)
            cell.border=Border(bottom=Side(style='medium',color='2F75B5'))
        ws_main.row_dimensions[1].height=34
        widths=[16,13,14,14,14,14,14,16,16,18,14,18,22]
        for i,w in enumerate(widths,1): ws_main.column_dimensions[get_column_letter(i)].width=w
        for rr in range(2,ws_main.max_row+1):
            fill='FFFFFF' if rr % 2 == 0 else 'F5F9FD'
            for cc in range(1,ws_main.max_column+1):
                ws_main.cell(rr,cc).fill=PatternFill('solid',fgColor=fill)
                ws_main.cell(rr,cc).alignment=Alignment(vertical='center')
            ws_main.cell(rr,2).number_format='dd-mm-yyyy'
            ws_main.cell(rr,11).number_format='0.00%'
            ws_main.row_dimensions[rr].height=21
        main_wb.save(WORKBOOK)
        print(f'Main NSE Data sheet updated: {len(out):,} rows -> {WORKBOOK}')
    except Exception as exc:
        print(f'Main NSE Data sheet update ERROR: {exc}')
    try:
        saved_rows=max(0, pd.read_excel(path,sheet_name='NSE Data').shape[0])
    except Exception:
        saved_rows=0
    print(f'NSE Data complete: {len(out):,} incoming rows; {saved_rows:,} total rows in ONE workbook: {path}')


def _populate_monthly_nse_from_cache(month_text: str, range_start: date, range_end: date) -> None:
    """Upsert cached NSE data into the same single NSE Data workbook."""
    y,m=[int(x) for x in month_text.split('-',1)]
    first=date(y,m,1); next_m=date(y + (m==12), 1 if m==12 else m+1, 1); last=next_m-timedelta(days=1)
    lo=max(first,range_start); hi=min(last,range_end)
    frames=[]; day=lo
    while day<=hi:
        if _is_nse_trading_day(day):
            raw=_load_cached_daily(day)
            if raw is not None:
                market,_=screen_from_raw(raw); market.insert(0,'Date',day.isoformat()); frames.append(market)
        day+=timedelta(days=1)
    if frames:
        df=pd.concat(frames,ignore_index=True)
        df['Movement %']=pd.to_numeric(df['Movement %'],errors='coerce')
    else:
        df=pd.DataFrame(columns=['Date','Symbol','Company Name','Open','Close','Previous close','High','Volume','Turnover (₹)','Movement %'])
    path=_write_nse_month_workbook(month_text,df)
    print(f'NSE Data cache refresh: {path}')

def run_live_shadow_historical(start_text: str, end_text: str, interval: str = 'full', universe: str = 'NSE_ALL_EQ') -> None:
    """Replay the LIVE decision flow against historical market data, without orders.

    LIVE SHADOW is intentionally an execution-free validation mode. It uses the
    current GUI/canonical strategy configuration and the same historical replay
    engine used to validate the intraday selection/entry/exit sequence. No broker
    order API is called. The resulting workbook/log is therefore a safe way to
    inspect what LIVE would have decided for a past session.
    """
    print('\n' + '='*92)
    print('LIVE SHADOW — HISTORICAL REPLAY (NO BROKER ORDERS)')
    print('Uses current GUI strategy + canonical intraday decision flow.')
    print('Maximum Orders is a TOTAL intraday BUY cap; a SELL does NOT reopen capacity.')
    print('='*92)
    verify_canonical_strategy_parity()
    run_historical_date_range(start_text, end_text, interval=interval, historical_share_limit=None, universe=universe)
    print(f'LIVE SHADOW UNIVERSE: {universe}')
    print('LIVE SHADOW COMPLETE: historical decisions replayed; NO real BUY/SELL orders were sent.')


def run_historical_date_range(start_text: str, end_text: str, interval: str = '1minute', historical_share_limit: int | None = None, symbol_allowlist: Iterable[str] | None = None, universe: str = "NSE_ALL_EQ") -> None:
    start, end = _parse_cli_date(start_text), _parse_cli_date(end_text)
    if end < start:
        raise RuntimeError('End date cannot be before Start Date.')
    if historical_share_limit is not None:
        historical_share_limit=int(historical_share_limit)
        if historical_share_limit < 1:
            raise RuntimeError('Command-line Historical Shares override must be 1 or greater when supplied.')
    interval=str(interval).strip().lower()
    if interval not in {'full','1minute','1second','hybrid'}:
        raise RuntimeError('Historical interval must be full, 1minute, 1second or hybrid.')
    if symbol_allowlist is None:
        symbol_allowlist = _nse_universe_symbols(universe)
    _full_interval_all_qualified = interval == 'full'
    if _full_interval_all_qualified:
        # Full means complete 1-minute AND complete 1-second session data for
        # every symbol that passes the GUI daily qualification stage.
        # Trade ranking/replay still observes the existing Historical Shares and
        # Maximum Orders caps; only the historical DATA DOWNLOAD scope is widened.
        replay_interval = 'hybrid'
        print('Historical FULL MODE: downloading complete 1-minute + 1-second session candles for EVERY GUI-qualified symbol. Strategy ranking and trade-output caps remain unchanged; 1-second downloads are cache-first and checkpointed per symbol.')
    else:
        replay_interval = interval
    months=[]; cur=date(start.year,start.month,1)
    while cur<=end:
        months.append(cur.strftime('%Y-%m'))
        cur=date(cur.year+(cur.month==12),1 if cur.month==12 else cur.month+1,1)
    _scope_label = f'{universe} ({len(set(str(x).strip().upper() for x in symbol_allowlist if str(x).strip()))} symbols)' if symbol_allowlist is not None else 'NSE_ALL_EQ'
    print(f"Historical EXACT DATE RANGE: {start} through {end}; mode={interval}; shares={_scope_label}; storage={YTD_HISTORICAL_CACHE / '<YYYY>' / '<YYYY-MM>' / '<YYYY-MM-DD>'}.")
    for _month in months:
        _write_run_status('HISTORICAL DOWNLOAD/REPLAY', 'RUNNING', f'Historical data cache is stored under {YTD_HISTORICAL_CACHE}; trade rows are saved to this month workbook when produced.', _month, None, _ensure_monthly_workbook(_month))
    total=0
    created_workbooks=0
    for month in months:
        y,m=[int(x) for x in month.split('-',1)]; next_m=date(y+(m==12),1 if m==12 else m+1,1)
        month_start=max(start,date(y,m,1)); month_end=min(end,next_m-timedelta(days=1))
        print(f'Historical exact range: processing {month_start} through {month_end}')
        results=run_historical_replay_month(
            month,
            range_start=month_start,
            range_end=month_end,
            interval=replay_interval,
            historical_share_limit=historical_share_limit,
            symbol_allowlist=symbol_allowlist,
            full_interval_all_qualified=_full_interval_all_qualified
        )
        # Historical results have already been persisted date-by-date by the replay
        # engine. Do not rewrite the entire month here; this preserves incremental
        # saves and avoids losing completed dates if a later date fails.
        if results:
            path=_ensure_monthly_workbook(month)
            total += len(results)
            created_workbooks += 1
            _write_run_status('HISTORICAL DOWNLOAD/REPLAY', 'COMPLETED', f'Historical replay produced {len(results)} result rows. Raw historical cache files are under {YTD_HISTORICAL_CACHE}.', f'{month_start} to {month_end}', len(results), path)
            print(
                f'Monthly workbook ready: {path.name}; '
                f'historical result rows={len(results):,}; '
                f'final workbook batch save completed.'
            )
        else:
            # Keep a workbook/status row even when the strategy produced no trades.
            # This distinguishes a completed no-signal run from a failed/no-output run.
            _status_path = _ensure_monthly_workbook(month)
            _write_run_status('HISTORICAL DOWNLOAD/REPLAY', 'COMPLETED - NO TRADE SIGNALS', f'No historical trade rows were produced for {month_start} through {month_end}. Historical CSV/cache files, if downloaded, remain under {YTD_HISTORICAL_CACHE}. Check Process Log for download/API errors and the enabled strategy filters.', f'{month_start} to {month_end}', 0, _status_path)
            print(f'Historical exact range: no historical trade results for {month_start} through {month_end}; status saved to {_status_path}.')
    print(f'Historical exact range complete: {start} through {end}; {total:,} result rows across {created_workbooks} monthly workbook(s).')


def _resolve_single_share_input(text: str) -> tuple[str, str]:
    """Resolve an NSE symbol or a company-name fragment to (symbol, Breeze stock code)."""
    query = str(text or '').strip()
    if not query:
        raise RuntimeError('Company / NSE symbol is required.')
    q = re.sub(r'\s+', ' ', query).upper()
    security = _load_security_cache()
    master = _load_nse_company_master()
    # Common company names that differ from the listed NSE trading symbol.
    # In particular, users commonly type "Vedanta" while NSE lists it as VEDL.
    aliases = {
        'VEDANTA': 'VEDL',
        'VEDANTA LIMITED': 'VEDL',
        'TATA STEEL': 'TATASTEEL',
        'VODAFONE IDEA': 'IDEA',
        'VODAFONE IDEA LIMITED': 'IDEA',
    }
    alias_symbol = aliases.get(q)
    if alias_symbol and alias_symbol in master:
        return alias_symbol, (security.get(alias_symbol) or alias_symbol)
    # Exact NSE symbol match first.
    if q in master:
        return q, (security.get(q) or q)
    # Prefer an exact company-name match over a substring match. This avoids
    # ambiguous results such as VEDANTA matching VEDL plus similarly named firms.
    def _normalise_company_name(value: str) -> str:
        value = re.sub(r'[^A-Z0-9]+', ' ', str(value or '').upper()).strip()
        value = re.sub(r'\b(LIMITED|LTD|INDIA|INDUSTRIES)\b', '', value)
        return re.sub(r'\s+', ' ', value).strip()
    normalised_query = _normalise_company_name(q)
    exact_company_matches = sorted(
        sym for sym, name in master.items()
        if _normalise_company_name(name) == normalised_query
    )
    if len(exact_company_matches) == 1:
        sym = exact_company_matches[0]
        return sym, (security.get(sym) or sym)
    # Company-name fragment, case-insensitive.
    matches=[]
    for sym,name in master.items():
        if q in str(name).upper():
            matches.append(sym)
    matches=sorted(set(matches))
    if len(matches)==1:
        sym=matches[0]
        return sym, (security.get(sym) or sym)
    if len(matches)>1:
        raise RuntimeError('Company name matches multiple NSE symbols: ' + ', '.join(matches[:20]) + (', ...' if len(matches)>20 else '') + '. Paste the exact NSE symbol.')
    # Fail closed: an arbitrary symbol must not pass merely because it looks
    # syntactically valid. Search results must come from NSE's SERIES=EQ master.
    if not master:
        raise RuntimeError(
            'NSE EQ share search is unavailable because the official NSE equity master '
            'could not be verified. Check the internet connection and retry; no symbol was accepted.'
        )
    raise RuntimeError(
        f'"{query}" is not a verified NSE EQ-series share. Search an NSE EQ-listed company or exact EQ symbol.'
    )


def _extract_breeze_isec_stock_code(payload) -> str:
    """Read Breeze's short stock code from either flat or wrapped get_names payloads."""
    wanted = {'isecstockcode'}
    def walk(value):
        if isinstance(value, dict):
            for key, item in value.items():
                norm = re.sub(r'[^a-z0-9]', '', str(key).lower())
                if norm in wanted and isinstance(item, (str, int, float)):
                    candidate = str(item).strip().upper()
                    if candidate and candidate not in {'NONE', 'NULL', 'NAN'}:
                        return candidate
            for item in value.values():
                found = walk(item)
                if found:
                    return found
        elif isinstance(value, (list, tuple)):
            for item in value:
                found = walk(item)
                if found:
                    return found
        return ''
    return walk(payload)


# Verified Breeze short-code corrections for NSE symbols that are not named
# identically in the Breeze security master. The security-cache/get_names result
# always takes precedence over this fallback.
_HISTORICAL_BREEZE_CODE_FALLBACKS = {
    'TATAMOTORS': ('TATMOT',),
    'LTIM': ('LTINFO', 'LARSMI', 'LTI', 'LTIMIN'),
}


def _resolve_historical_breeze_stock(breeze, symbol: str, stock_cache: dict | None = None, refresh: bool = False) -> tuple[str, str]:
    """Resolve an NSE ticker to Breeze's ISEC short code; never assume the ticker is the code."""
    sym=str(symbol or '').strip().upper()
    cache=stock_cache if isinstance(stock_cache, dict) else _load_security_cache()
    stock=str(cache.get(sym) or '').strip().upper()
    if stock and not refresh:
        return sym, stock
    if breeze is None:
        fallback = next(iter(_HISTORICAL_BREEZE_CODE_FALLBACKS.get(sym, ())), '')
        return sym, fallback or stock or sym
    last_error = ''
    # Retry transient/empty get_names responses; a single empty response must
    # not permanently poison the mapping cache with NSE ticker == Breeze code.
    for attempt in range(1, 4):
        try:
            with _NSE50_MAPPING_LOCK:
                names=breeze.get_names(exchange_code='NSE',stock_code=sym) or {}
            resolved=_extract_breeze_isec_stock_code(names)
            if resolved:
                cache[sym]=resolved
                _save_security_cache(cache)
                company_name = _extract_company_name_from_breeze_payload(names, sym)
                if company_name:
                    with _COMPANY_NAME_CACHE_LOCK:
                        _load_company_name_fallback_cache()
                        _COMPANY_NAME_LOOKUP_CACHE[sym] = company_name
                        try:
                            _save_company_name_fallback_cache()
                        except Exception as cache_exc:
                            print(f'Company-name fallback cache save warning: {type(cache_exc).__name__}: {cache_exc}')
                print(f'Historical SYMBOL MAPPING: {sym} -> {resolved} (get_names attempt {attempt}).', flush=True)
                return sym,resolved
            last_error = f'get_names returned no isec_stock_code; response_keys={list(names.keys()) if isinstance(names, dict) else type(names).__name__}'
        except Exception as exc:
            last_error = f'{type(exc).__name__}: {exc}'
        if attempt < 3:
            time.sleep(1.25 * attempt)
    fallback_codes = _HISTORICAL_BREEZE_CODE_FALLBACKS.get(sym, ())
    if fallback_codes:
        resolved = str(fallback_codes[0]).strip().upper()
        cache[sym] = resolved
        _save_security_cache(cache)
        print(f'Historical SYMBOL MAPPING FALLBACK: {sym} -> {resolved}; get_names retries exhausted ({last_error}).', flush=True)
        return sym, resolved
    print(f'Historical SYMBOL MAPPING UNRESOLVED: {sym}; get_names retries exhausted ({last_error}); ticker fallback will be attempted but not treated as verified.', flush=True)
    return sym, sym


def _safe_filename_part(value: str, fallback: str = 'UNKNOWN') -> str:
    """Normalize a company/symbol for a stable Windows-safe filename."""
    text = re.sub(r'\s+', '_', str(value or '').strip())
    text = re.sub(r'[^A-Za-z0-9_.&-]+', '_', text)
    text = re.sub(r'_+', '_', text).strip('._-')
    return text[:100] or fallback


def _company_name_for_symbol(symbol: str, breeze=None) -> str:
    """Resolve official company name from NSE master, then Breeze get_names."""
    stock_symbol = str(symbol or '').strip().upper()
    if not stock_symbol:
        return 'UNKNOWN'
    try:
        name = str(_load_nse_company_master().get(stock_symbol) or '').strip()
        if name:
            return name
    except Exception as exc:
        print(f'Historical NSE company-master lookup warning for {stock_symbol}: {type(exc).__name__}: {exc}')
    name = _company_name_from_breeze(stock_symbol, breeze=breeze)
    return name or stock_symbol
def _canonical_full_historical_path(outdir: Path, day: date, symbol: str) -> Path:
    """Return ONE user-facing historical CSV named by date and NSE ticker only.

    Company-name resolution is intentionally excluded from the download path.
    The CSV contains both 1-minute and 1-second rows, distinguished by interval.
    """
    sym = _safe_filename_part(str(symbol).strip().upper(), 'SYMBOL')
    return outdir / f'{day:%Y-%m-%d}_{sym}_historical.csv'


def _legacy_canonical_full_historical_path(outdir: Path, day: date, symbol: str) -> Path:
    """Previous canonical filename used before company names were included."""
    safe = _safe_filename_part(str(symbol).strip().upper())
    return outdir / f'{day:%Y-%m-%d}_{safe}_historical.csv'


def _read_canonical_full_historical(path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Read and validate a canonical combined historical CSV."""
    if not path.exists():
        return pd.DataFrame(), pd.DataFrame()
    df = pd.read_csv(path)
    if 'interval' not in df.columns or '_dt' not in df.columns:
        raise ValueError('canonical historical CSV is missing interval/_dt columns')
    df['_dt'] = pd.to_datetime(df['_dt'], errors='coerce')
    df = df[df['_dt'].notna()].copy()
    for c in ('open', 'high', 'low', 'close', 'volume'):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    frames = {}
    for interval in ('1minute', '1second'):
        part = df[df['interval'].astype(str).str.lower().eq(interval)].copy()
        frames[interval] = part.sort_values('_dt').drop_duplicates('_dt', keep='last').reset_index(drop=True)
    return frames['1minute'], frames['1second']



_PREVIOUS_CLOSE_CACHE: dict[tuple[str, str], float | None] = {}


def _previous_close_for_historical_day(day: date, symbol: str) -> float | None:
    """Get the prior NSE session close for a symbol, cached per date/symbol.

    Prefer an authenticated Breeze 1-day candle strictly before `day`; fall back
    to the most recent cached intraday close. Never use the current session close.
    """
    key = (str(symbol or '').strip().upper(), day.isoformat())
    if key in _PREVIOUS_CLOSE_CACHE:
        return _PREVIOUS_CLOSE_CACHE[key]
    result = None

    # Check cache/bhavcopy first to avoid 51 individual 1day API calls during historical downloads
    if result is None and key[0]:
        aliases = {key[0]}
        try:
            aliases.update(_historical_symbol_aliases(key[0]))
        except Exception:
            pass
        for offset in range(1, 15):
            prior_day = day - timedelta(days=offset)
            try:
                daily_raw = _load_cached_daily(prior_day)
                if daily_raw is not None and not daily_raw.empty:
                    colmap = {re.sub(r'[^A-Z0-9]', '', str(c).upper()): c for c in daily_raw.columns}
                    sym_col = next((colmap[k] for k in ('SYMBOL','TCKRSYMB','STOCKCODE') if k in colmap), None)
                    close_col = next((colmap[k] for k in ('CLOSE','CLSPRIC','CLOSEPRICE') if k in colmap), None)
                    if sym_col and close_col:
                        rows = daily_raw.copy()
                        rows['_sym_norm'] = rows[sym_col].astype(str).str.strip().str.upper()
                        rows['_close_norm'] = pd.to_numeric(rows[close_col], errors='coerce')
                        rows = rows[rows['_sym_norm'].isin(aliases) & (rows['_close_norm'] > 0)]
                        if not rows.empty:
                            result = float(rows.iloc[-1]['_close_norm'])
                            break
            except Exception:
                pass
            if result is not None:
                break

    # Offline/cache-only fallback. Search every supported cache layout for the
    # most recent prior NSE session, including the date-folder layout and the
    # one-share export layout. Include known Breeze/NSE symbol aliases.
    if result is None and key[0]:
        aliases = {key[0]}
        try:
            aliases.update(_historical_symbol_aliases(key[0]))
        except Exception:
            pass
        safe_aliases = {re.sub(r'[^A-Za-z0-9_.-]+', '_', x) for x in aliases if x}
        for offset in range(1, 15):
            prior_day = day - timedelta(days=offset)
            candidates = []
            try:
                prior_cache_dir = _cache_day_dir(prior_day)
                if prior_cache_dir.is_dir():
                    candidates.extend(prior_cache_dir.glob('*_historical.csv'))
                one_share_month = ONE_SHARE_HISTORICAL_CACHE / f'{prior_day:%Y-%m}'
                if one_share_month.is_dir():
                    candidates.extend(one_share_month.glob(f'{prior_day:%Y-%m-%d}_*_historical.csv'))
            except Exception:
                pass
            # Also consult the NSE daily bhavcopy cache. Its close is an
            # authoritative previous-session close when intraday data is absent.
            try:
                daily_raw = _load_cached_daily(prior_day)
                if daily_raw is not None and not daily_raw.empty:
                    sym_col = next((c for c in ('Symbol', 'symbol', 'stock_code', 'Stock Code') if c in daily_raw.columns), None)
                    close_col = next((c for c in ('Close', 'close', 'CLOSE') if c in daily_raw.columns), None)
                    if sym_col and close_col:
                        match = daily_raw[daily_raw[sym_col].astype(str).str.strip().str.upper().isin(aliases)].copy()
                        match['_close_num'] = pd.to_numeric(match[close_col], errors='coerce')
                        match = match[match['_close_num'] > 0]
                        if not match.empty:
                            result = float(match.iloc[-1]['_close_num'])
            except Exception:
                pass
            if result is not None:
                break
            # Filename matching is deliberately alias-aware; contents are still
            # validated against the requested date and symbol before use.
            for candidate in dict.fromkeys(candidates):
                try:
                    if not any(candidate.name.upper().endswith(f'_{a}_HISTORICAL.CSV'.upper()) for a in safe_aliases):
                        continue
                    old = pd.read_csv(candidate)
                    if 'interval' in old.columns:
                        old = old[old['interval'].astype(str).str.lower().eq('1minute')]
                    dt_col = 'datetime' if 'datetime' in old.columns else ('_dt' if '_dt' in old.columns else None)
                    if not dt_col or 'close' not in old.columns:
                        continue
                    old['_dt_tmp'] = pd.to_datetime(old[dt_col], errors='coerce')
                    old['_close_num'] = pd.to_numeric(old['close'], errors='coerce')
                    old = old[(old['_dt_tmp'].dt.date == prior_day) & (old['_close_num'] > 0)].sort_values('_dt_tmp')
                    if not old.empty:
                        result = float(old.iloc[-1]['_close_num'])
                        break
                except Exception:
                    continue
            if result is not None:
                break

    # If the previous-session daily bhavcopy is not already cached, retrieve it
    # once for the date and reuse it for every symbol. This avoids requiring a
    # manually downloaded previous-day intraday CSV and avoids one broker request
    # per symbol for Previous Close.
    if result is None and key[0]:
        try:
            prior_day = _previous_trading_day(day)
            daily_raw = _load_cached_daily(prior_day)
            if daily_raw is None or daily_raw.empty:
                daily_raw = bhavcopy_for_date(prior_day, _new_nse_session())
                if daily_raw is not None and not daily_raw.empty:
                    _save_cached_daily(prior_day, daily_raw)
                    print(f'Previous-close support: downloaded and cached NSE bhavcopy for {prior_day}.')
            if daily_raw is not None and not daily_raw.empty:
                colmap = {re.sub(r'[^A-Z0-9]', '', str(c).upper()): c for c in daily_raw.columns}
                sym_col = next((colmap[k] for k in ('SYMBOL','TCKRSYMB','STOCKCODE') if k in colmap), None)
                close_col = next((colmap[k] for k in ('CLOSE','CLSPRIC','CLOSEPRICE') if k in colmap), None)
                series_col = next((colmap[k] for k in ('SERIES','SCTYSRS') if k in colmap), None)
                if sym_col and close_col:
                    rows = daily_raw.copy()
                    rows['_sym_norm'] = rows[sym_col].astype(str).str.strip().str.upper()
                    rows['_close_norm'] = pd.to_numeric(rows[close_col], errors='coerce')
                    rows = rows[rows['_sym_norm'].isin(aliases) & (rows['_close_norm'] > 0)]
                    if series_col:
                        rows = rows[rows[series_col].astype(str).str.strip().str.upper().eq('EQ')]
                    if not rows.empty:
                        result = float(rows.iloc[-1]['_close_norm'])
        except Exception as exc:
            print(f'Previous-close NSE bhavcopy fallback warning for {key[0]} {day}: {type(exc).__name__}: {exc}')

    # Do not negative-cache a miss: a Breeze session or a prior-day file may
    # become available later in the same run (or after the user downloads it).
    if result is not None and result > 0:
        _PREVIOUS_CLOSE_CACHE[key] = float(result)
    else:
        print(f'Previous close unavailable for {key[0]} {day}; lookup remains retryable.')
    return result


def _add_previous_close_column(frame: pd.DataFrame, day: date | None, symbol: str, lookup: bool = True) -> pd.DataFrame:
    """Ensure every downloaded candle row has a previous_close column."""
    if frame is None:
        return frame
    out = frame.copy()
    if 'previous_close' not in out.columns:
        out['previous_close'] = pd.NA
    existing = pd.to_numeric(out['previous_close'], errors='coerce')
    valid = existing[existing > 0]
    value = float(valid.iloc[0]) if not valid.empty else (
        _previous_close_for_historical_day(day, symbol) if lookup and day is not None else None)
    if value is not None and value > 0:
        out['previous_close'] = existing.where(existing > 0, float(value))
    return out


def _add_orb15_vwap_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Add historical ORB15 high and session VWAP using live-compatible definitions.

    ORB15 is the maximum high of the complete 09:15-09:29 NSE opening range;
    it is exposed from 09:30 onward only (no look-ahead during the opening range).
    VWAP prefers an exchange average-traded-price field when supplied by the
    feed; otherwise it uses minute typical-price x volume, cumulatively per day.
    """
    if frame is None or frame.empty:
        return frame
    out = frame.copy()
    dt_col = '_dt' if '_dt' in out.columns else ('datetime' if 'datetime' in out.columns else None)
    if dt_col is None:
        out['ORB15'] = pd.NA
        out['VWAP'] = pd.NA
        return out
    dt = pd.to_datetime(out[dt_col], errors='coerce')
    close = pd.to_numeric(out.get('close', pd.Series(index=out.index, dtype=float)), errors='coerce')
    high = pd.to_numeric(out.get('high', pd.Series(index=out.index, dtype=float)), errors='coerce')
    low = pd.to_numeric(out.get('low', pd.Series(index=out.index, dtype=float)), errors='coerce')
    volume = pd.to_numeric(out.get('volume', pd.Series(index=out.index, dtype=float)), errors='coerce').fillna(0).clip(lower=0)
    typical = (high + low + close) / 3.0
    avg_col = next((c for c in ('avgPrice', 'avg_traded_price', 'average_traded_price', 'average_price', 'vwap') if c in out.columns), None)
    if avg_col is not None:
        avg = pd.to_numeric(out[avg_col], errors='coerce')
        # A row-level exchange average is useful as a VWAP snapshot; where only
        # candle values exist, calculate the cumulative session VWAP below.
        row_turnover = avg * volume
    else:
        row_turnover = typical * volume
    valid = dt.notna() & (volume > 0) & row_turnover.notna() & (row_turnover > 0)
    group_key = dt.dt.date
    cum_vol = volume.where(valid, 0).groupby(group_key).cumsum()
    cum_turn = row_turnover.where(valid, 0).groupby(group_key).cumsum()
    calculated_vwap = (cum_turn / cum_vol.where(cum_vol > 0)).where(cum_vol > 0)
    if avg_col is not None:
        # Exchange avgPrice is already session cumulative VWAP-equivalent.
        calculated_vwap = pd.to_numeric(out[avg_col], errors='coerce').where(
            pd.to_numeric(out[avg_col], errors='coerce') > 0, calculated_vwap)
    out['VWAP'] = calculated_vwap

    orb_values = pd.Series(pd.NA, index=out.index, dtype='object')
    for session_day in pd.unique(group_key.dropna()):
        day_mask = group_key.eq(session_day)
        opening_mask = day_mask & dt.dt.strftime('%H:%M').between('09:15', '09:29')
        opening_high = high.loc[opening_mask].max()
        if pd.notna(opening_high) and float(opening_high) > 0:
            # The level becomes available at 09:30, matching the live state machine.
            after_open = day_mask & (dt.dt.hour * 60 + dt.dt.minute >= 570)
            orb_values.loc[after_open] = float(opening_high)
    out['ORB15'] = orb_values
    return out


def _add_company_rvol_columns(frame: pd.DataFrame, day: date | None, symbol: str) -> pd.DataFrame:
    """Add NSE company name and live-compatible rolling RVOL.

    RVOL follows the live scanner's rolling 5-minute-volume method: current
    bucket volume is projected to a full five minutes and divided by the median
    of up to the previous 20 completed five-minute buckets. It remains blank
    until at least five completed buckets and 60 seconds of the current bucket.
    """
    if frame is None or frame.empty:
        return frame
    out = frame.copy()
    # Historical CSVs are ticker-only: never resolve NSE/Breeze company names.
    out = out.drop(columns=['company_name', 'Company Name'], errors='ignore')

    dt_col = '_dt' if '_dt' in out.columns else ('datetime' if 'datetime' in out.columns else None)
    out['RVOL'] = pd.NA
    if dt_col is not None and 'volume' in out.columns:
        dt = pd.to_datetime(out[dt_col], errors='coerce')
        vol = pd.to_numeric(out['volume'], errors='coerce').fillna(0).clip(lower=0)
        work = pd.DataFrame({'_dt': dt, '_vol': vol}, index=out.index)
        valid = work['_dt'].notna()
        work = work.loc[valid].sort_values('_dt')
        if not work.empty:
            work['_bucket'] = work['_dt'].dt.floor('5min')
            work['_session'] = work['_dt'].dt.date
            rvol = pd.Series(index=out.index, dtype='float64')
            for _, session in work.groupby('_session', sort=True):
                bucket_volumes = session.groupby('_bucket')['_vol'].sum().sort_index()
                for bucket, bucket_rows in session.groupby('_bucket', sort=True):
                    completed = bucket_volumes.loc[bucket_volumes.index < bucket].tail(20)
                    if len(completed) < 5:
                        continue
                    median_volume = float(completed.median())
                    if median_volume <= 0:
                        continue
                    elapsed = (bucket_rows['_dt'] - bucket).dt.total_seconds().clip(lower=0, upper=300)
                    elapsed_fraction = elapsed / 300.0
                    cumulative = bucket_rows['_vol'].cumsum()
                    values = (cumulative / (median_volume * elapsed_fraction)).where(elapsed_fraction >= (60.0 / 300.0))
                    rvol.loc[bucket_rows.index] = values
            out['RVOL'] = rvol.reindex(out.index)

    # 52-week high/low are intentionally excluded from historical CSV exports.
    return out


def _drop_52_week_export_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Remove 52-week high/low fields from user-facing historical downloads.

    Keep 52-week strategy calculations and settings intact; this only cleans
    exported/cached historical candle columns, including legacy aliases.
    """
    if frame is None or frame.empty:
        return frame
    def is_52w_column(column) -> bool:
        key = ''.join(ch for ch in str(column).lower() if ch.isalnum())
        has_period = ('52w' in key) or ('52week' in key)
        has_level = ('high' in key) or ('low' in key)
        return has_period and has_level
    remove = [column for column in frame.columns if is_52w_column(column)]
    return frame.drop(columns=remove, errors='ignore') if remove else frame


def _write_canonical_full_historical(path: Path, minute: pd.DataFrame, second: pd.DataFrame, day: date | None = None, symbol: str = '') -> None:
    """Atomically write both intervals into the same stable CSV filename."""
    frames = []
    for interval, frame in (('1minute', minute), ('1second', second)):
        if frame is None or frame.empty:
            continue
        out = frame.copy()
        out = _add_previous_close_column(out, day, symbol)
        out['interval'] = interval
        if '_dt' in out.columns:
            out['_dt'] = pd.to_datetime(out['_dt'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
        frames.append(out)
    if not frames:
        raise ValueError('no historical data available to save')
    combined = pd.concat(frames, ignore_index=True, sort=False)
    if '_dt' in combined.columns:
        combined['_sort_dt'] = pd.to_datetime(combined['_dt'], errors='coerce')
        combined = combined.sort_values(['interval', '_sort_dt']).drop(columns=['_sort_dt'])
    combined = combined.drop_duplicates(subset=['interval', '_dt'], keep='last').reset_index(drop=True)
    # Add the same ORB15 level and VWAP fields consumed by live entry logic.
    enriched_parts = []
    for _interval, _part in combined.groupby('interval', sort=False):
        _part = _add_orb15_vwap_columns(_part)
        _part = _add_company_rvol_columns(_part, day, symbol)
        enriched_parts.append(_part)
    combined = pd.concat(enriched_parts, ignore_index=True, sort=False)
    combined = _drop_52_week_export_columns(combined)
    # User-facing single-share export schema: keep the internal _dt field for
    # cache compatibility, while adding the requested datetime/turnover columns.
    if '_dt' in combined.columns:
        combined['datetime'] = pd.to_datetime(combined['_dt'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
    if 'exchange_code' not in combined.columns:
        combined['exchange_code'] = 'NSE'
    else:
        combined['exchange_code'] = combined['exchange_code'].fillna('NSE')
    if 'stock_code' not in combined.columns:
        combined['stock_code'] = ''
    if 'is_actual' not in combined.columns:
        combined['is_actual'] = True
    if '_cum_volume' not in combined.columns:
        combined['_cum_volume'] = pd.to_numeric(combined.get('volume', 0), errors='coerce').fillna(0).groupby(combined['interval']).cumsum()
    if '_candle_turnover_rupees' not in combined.columns:
        close = pd.to_numeric(combined.get('close', 0), errors='coerce').fillna(0)
        volume = pd.to_numeric(combined.get('volume', 0), errors='coerce').fillna(0)
        combined['_candle_turnover_rupees'] = close * volume
    if '_cum_turnover_cr' not in combined.columns:
        combined['_cum_turnover_cr'] = (combined['_candle_turnover_rupees'].groupby(combined['interval']).cumsum() / 10000000.0)
    # Use exactly the same candle columns for 1-minute and 1-second rows.
    # The interval column distinguishes candle granularity; 1-second records
    # retain their original timestamps and OHLCV values (they are NOT resampled).
    schema = [
        'close', 'datetime', 'exchange_code', 'high', 'low', 'open',
        'stock_code', 'volume', 'is_actual', '_cum_volume',
        '_candle_turnover_rupees', '_cum_turnover_cr', 'previous_close',
        'VWAP', 'ORB15', 'company_name', 'RVOL',
        'interval',
    ]
    if '_dt' not in combined.columns and 'datetime' in combined.columns:
        combined['_dt'] = pd.to_datetime(combined['datetime'], errors='coerce')
    if 'datetime' not in combined.columns and '_dt' in combined.columns:
        combined['datetime'] = pd.to_datetime(combined['_dt'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
    if 'exchange_code' not in combined.columns:
        combined['exchange_code'] = 'NSE'
    else:
        combined['exchange_code'] = combined['exchange_code'].fillna('NSE')
    if 'stock_code' not in combined.columns:
        combined['stock_code'] = str(symbol or '').strip().upper()
    else:
        combined['stock_code'] = combined['stock_code'].fillna(str(symbol or '').strip().upper())
    if 'is_actual' not in combined.columns:
        combined['is_actual'] = True
    else:
        combined['is_actual'] = combined['is_actual'].fillna(True).astype(bool)
    for col in schema:
        if col not in combined.columns:
            combined[col] = pd.NA
    # Recalculate cumulative fields separately for each interval so second-level
    # and minute-level running totals do not contaminate one another.
    combined['_sort_dt'] = pd.to_datetime(combined['_dt'], errors='coerce')
    combined = combined.sort_values(['_sort_dt', 'interval']).reset_index(drop=True)
    volume = pd.to_numeric(combined['volume'], errors='coerce').fillna(0)
    close = pd.to_numeric(combined['close'], errors='coerce')
    combined['_candle_turnover_rupees'] = close * volume
    combined['_cum_volume'] = volume.groupby(combined['interval']).cumsum()
    combined['_cum_turnover_cr'] = (
        combined['_candle_turnover_rupees'].fillna(0).groupby(combined['interval']).cumsum() / 10000000.0
    )
    combined['datetime'] = pd.to_datetime(combined['_dt'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
    # Keep _dt as a private compatibility field for the canonical cache reader.
    combined = combined[schema + ['_dt']]
    combined = combined.drop(columns=['_sort_dt'], errors='ignore')
    tmp = path.with_suffix('.tmp.csv')
    combined.to_csv(tmp, index=False)
    tmp.replace(path)


def _full_second_session_coverage(frame: pd.DataFrame | None, day: date) -> bool:
    """Require actual 1-second candles near both ends of the NSE session.

    Breeze returns traded candles, not synthetic timestamps for every second.
    This conservative coverage check catches minute-only/partial second caches
    without inventing missing ticks. The first and last actual ticks may be up
    to one minute inside the official session boundaries.
    """
    if frame is None or frame.empty:
        return False
    dt_col = '_dt' if '_dt' in frame.columns else ('datetime' if 'datetime' in frame.columns else None)
    if not dt_col:
        return False
    values = pd.to_datetime(frame[dt_col], errors='coerce').dropna()
    if values.empty:
        return False
    start = datetime.combine(day, datetime.min.time()).replace(hour=9, minute=15)
    end = datetime.combine(day, datetime.min.time()).replace(hour=15, minute=30)
    return values.min().to_pydatetime() <= start + timedelta(minutes=1) and values.max().to_pydatetime() >= end - timedelta(minutes=1)


def _download_full_historical_share(breeze, symbol: str, stock: str, day: date, outdir: Path | None = None) -> tuple[pd.DataFrame, pd.DataFrame, Path, Path]:
    global _BREEZE_52W_SESSION
    # Ensure enrichment lookups use the authenticated session for this download,
    # including when a cached CSV needs backfilling.
    if breeze is not None:
        _BREEZE_52W_SESSION = breeze
    """Single canonical full-session downloader.

    IMPORTANT: one stable CSV is the final user-facing file for a symbol/date.
    If that CSV is complete, Breeze is never called again.  If older versions
    left separate 1-minute/1-second files, they are imported into the canonical
    CSV once and then the old duplicate files are not used for downloading.
    """
    if outdir is None or outdir == ONE_SHARE_HISTORICAL_CACHE:
        # Store canonical date-wise CSVs directly in the month folder.
        outdir = ONE_SHARE_HISTORICAL_CACHE / f'{day:%Y-%m}'
    outdir.mkdir(parents=True, exist_ok=True)
    canonical_path = _canonical_full_historical_path(outdir, day, symbol)
    legacy_canonical_path = _legacy_canonical_full_historical_path(outdir, day, symbol)

    # 1) AUTHORITATIVE CACHE: same date+symbol filename every run; no Breeze call.
    # If the previous symbol-only canonical file exists, migrate it once instead of
    # downloading the same market data again.
    if not canonical_path.exists() and legacy_canonical_path.exists():
        try:
            legacy_minute, legacy_second = _read_canonical_full_historical(legacy_canonical_path)
            if not legacy_minute.empty and not legacy_second.empty:
                _write_canonical_full_historical(canonical_path, legacy_minute, legacy_second, day=day, symbol=symbol)
                try:
                    legacy_canonical_path.unlink()
                except OSError:
                    pass
                print(f'Historical FINAL CSV RENAMED: {legacy_canonical_path.name} -> {canonical_path.name}; NO BREEZE DOWNLOAD.')
        except Exception as exc:
            print(f'Historical legacy canonical migration warning {legacy_canonical_path.name}: {exc}')

    if canonical_path.exists():
        try:
            minute_cached, second_cached = _read_canonical_full_historical(canonical_path)
            if not minute_cached.empty and _full_second_session_coverage(second_cached, day):
                # Backfill existing full-interval files as well as new downloads.
                existing_head = pd.read_csv(canonical_path, nrows=20)
                existing_pc = pd.to_numeric(existing_head.get('previous_close', pd.Series(dtype=float)), errors='coerce')
                required_enrichment = ('previous_close', 'ORB15', 'VWAP', 'RVOL')
                missing_enrichment = [c for c in required_enrichment if c not in existing_head.columns]
                obsolete_52w = [c for c in ('52W_HIGH', '52W_LOW') if c in existing_head.columns]
                previous_close_missing = not existing_pc.gt(0).any()
                if missing_enrichment or previous_close_missing or obsolete_52w:
                    _write_canonical_full_historical(canonical_path, minute_cached, second_cached, day=day, symbol=symbol)
                    # Return the enriched frames too, not stale pre-backfill cache frames.
                    minute_cached, second_cached = _read_canonical_full_historical(canonical_path)
                    print(
                        f'Historical ENRICHMENT BACKFILL: updated existing file {canonical_path.name}; '
                        f'missing_columns={missing_enrichment}; removed_columns={obsolete_52w}; previous_close_missing={previous_close_missing}.'
                    )
                print(
                    f'Historical FINAL CSV REUSE: {symbol} {day}; '
                    f'{canonical_path.name}; 1m={len(minute_cached):,}; 1s={len(second_cached):,}; '
                    'NO BREEZE DOWNLOAD.'
                )
                return minute_cached, second_cached, canonical_path, canonical_path
            print(f'Historical FINAL CSV INCOMPLETE: {canonical_path.name}; only missing interval will be downloaded.')
        except Exception as exc:
            print(f'Historical FINAL CSV READ WARNING {canonical_path.name}: {exc}; rebuilding only the invalid/missing data.')

    # 2) ONE-TIME MIGRATION: recognize the old two-file layout so an existing
    # download is not downloaded again just because the filename changed.
    old_minute = outdir / f'{day:%Y-%m-%d}_{symbol}_1minute_full_{HISTORICAL_CACHE_VERSION}.csv'
    old_second = outdir / f'{day:%Y-%m-%d}_{symbol}_1second_full_{HISTORICAL_CACHE_VERSION}.csv'
    minute = pd.DataFrame()
    second = pd.DataFrame()
    for old_path, key in ((old_minute, 'minute'), (old_second, 'second')):
        if old_path.exists():
            try:
                frame = _candle_dataframe(pd.read_csv(old_path).to_dict('records'))
                if frame is not None and not frame.empty:
                    if key == 'minute':
                        minute = frame
                    else:
                        second = frame
            except Exception as exc:
                print(f'Historical OLD CACHE WARNING {old_path.name}: {exc}')

    # 3) Download ONLY what is missing.  Existing old/canonical data is reused.
    if minute.empty:
        minute = _historical_session_candles(breeze, stock, day, '1minute')
    if minute is None or minute.empty:
        raise RuntimeError(f'No usable 1-minute Breeze historical data returned for {symbol} on {day}.')

    if second.empty:
        second = _historical_session_candles(breeze, stock, day, '1second')
    if second is None or second.empty:
        raise RuntimeError(f'1-minute data is available for {symbol}, but no usable 1-second Breeze historical data was returned for {day}.')
    if not _full_second_session_coverage(second, day):
        try:
            _sec_dt = pd.to_datetime(second['_dt'] if '_dt' in second.columns else second['datetime'], errors='coerce').dropna()
            _sec_span = f'{_sec_dt.min():%H:%M:%S}-{_sec_dt.max():%H:%M:%S}' if not _sec_dt.empty else 'unknown'
        except Exception:
            _sec_span = 'unknown'
        raise RuntimeError(
            f'1-second data is partial for {symbol} on {day} (coverage {_sec_span}); '
            'not marking this symbol complete. Rerun Full mode to retry cached/missing chunks.'
        )

    # 4) Final output: EXACTLY ONE stable CSV for this symbol/date.
    _write_canonical_full_historical(canonical_path, minute, second, day=day, symbol=symbol)
    # All intervals for this date are now safely consolidated; discard temporary
    # request checkpoints to keep the cache tidy. If the job failed before here,
    # checkpoints remain available for a fast resume.
    removed_checkpoints = _remove_redundant_request_cache_files(day, stock)
    if removed_checkpoints:
        print(f'Historical REQUEST CHECKPOINT CLEANUP: {stock} {day}; removed={removed_checkpoints}.')
    # Remove the old split final-download files after successful consolidation.
    # They are no longer authoritative and keeping them would make the folder
    # look as if the same historical data had been downloaded multiple times.
    for old_path in (old_minute, old_second):
        if old_path.exists() and old_path.resolve() != canonical_path.resolve():
            try:
                old_path.unlink()
                print(f'Historical OLD DUPLICATE REMOVED: {old_path.name}')
            except Exception as exc:
                print(f'Historical OLD DUPLICATE REMOVE WARNING: {old_path.name}: {exc}')
    print(
        f'Historical FINAL CSV SAVE: {symbol} -> {stock} {day}; '
        f'{canonical_path.name}; 1m={len(minute):,}; 1s={len(second):,}.'
    )
    return minute, second, canonical_path, canonical_path


NSE50_HISTORICAL_SYMBOLS = (
    'ADANIENT', 'ADANIPORTS', 'APOLLOHOSP', 'ASIANPAINT', 'AXISBANK',
    'BAJAJ-AUTO', 'BAJFINANCE', 'BAJAJFINSV', 'BEL', 'BHARTIARTL',
    'CIPLA', 'COALINDIA', 'DRREDDY', 'EICHERMOT', 'ETERNAL',
    'GRASIM', 'HCLTECH', 'HDFCBANK', 'HDFCLIFE', 'HEROMOTOCO',
    'HINDALCO', 'HINDUNILVR', 'ICICIBANK', 'INDUSINDBK', 'INFY',
    'ITC', 'JIOFIN', 'JSWSTEEL', 'KOTAKBANK', 'LT', 'LTIM', 'M&M',
    'MARUTI', 'NESTLEIND', 'NTPC', 'ONGC', 'POWERGRID', 'RELIANCE',
    'SBILIFE', 'SBIN', 'SHRIRAMFIN', 'SUNPHARMA', 'TATACONSUM',
    'TATAMOTORS', 'TATASTEEL', 'TCS', 'TECHM', 'TITAN', 'TRENT',
    'ULTRACEMCO', 'WIPRO'
)



def _nse50_has_usable_candles(frame: pd.DataFrame | None) -> bool:
    """True only when at least one real, internally consistent traded OHLCV row exists."""
    if frame is None or frame.empty:
        return False
    actual = frame.copy()
    if 'is_actual' in actual.columns:
        actual = actual[actual['is_actual'].fillna(False).astype(bool)]
    if actual.empty:
        return False
    valid = _valid_historical_ohlcv_mask(actual)
    dt_col = 'datetime' if 'datetime' in actual.columns else ('_dt' if '_dt' in actual.columns else None)
    if dt_col:
        valid &= pd.to_datetime(actual[dt_col], errors='coerce').notna()
    # A partially valid cache is not a complete download: reject it so the
    # caller refreshes the interval instead of silently retaining bad rows.
    return bool(len(valid) > 0 and valid.all())


def _remove_nse50_legacy_alias_files(day: date, symbol: str, stock_cache: dict[str, str]) -> int:
    """Remove legacy duplicate filenames for an NSE50 symbol after canonical save."""
    official = str(symbol or '').strip().upper()
    breeze_code = str((stock_cache or {}).get(official) or '').strip().upper()
    if not official or not breeze_code or breeze_code == official:
        return 0
    day_dir = _cache_day_dir(day)
    if not day_dir.is_dir():
        return 0
    alias_suffix = f'_{re.sub(r"[^A-Za-z0-9_.-]+", "_", breeze_code)}_historical.csv'.upper()
    official_suffix = f'_{re.sub(r"[^A-Za-z0-9_.-]+", "_", official)}_historical.csv'.upper()
    removed = 0
    for candidate in day_dir.glob('*_historical.csv'):
        upper_name = candidate.name.upper()
        if upper_name.endswith(alias_suffix) and not upper_name.endswith(official_suffix):
            try:
                candidate.unlink()
                removed += 1
                print(f'NSE 50 DUPLICATE ALIAS REMOVED: {candidate.name}; canonical symbol={official}.')
            except OSError as exc:
                print(f'NSE 50 duplicate alias cleanup warning: {candidate.name}: {exc}')
    return removed


def _remove_empty_previous_month_cache_dirs(start_day: date) -> None:
    """Remove only empty prior-month cache directories left by older lookups.

    Never deletes CSVs or other user data.
    """
    prior_month = date(start_day.year - (1 if start_day.month == 1 else 0),
                       12 if start_day.month == 1 else start_day.month - 1, 1)
    candidates = [
        _historical_month_dir(prior_month.year, prior_month.month),
        _historical_month_dir(prior_month.year, prior_month.month) / f'{prior_month:%Y-%m-%d}',
    ]
    for folder in candidates:
        try:
            if folder.is_dir() and not any(folder.iterdir()):
                folder.rmdir()
                print(f'NSE 50 CLEANUP: removed empty unnecessary prior-month folder {folder}.')
        except OSError:
            pass


def run_nse50_historical_range(start_text: str, end_text: str, interval: str = 'full') -> None:
    """Download selected NIFTY 50 intraday history using bounded parallel workers."""
    start_day = _parse_cli_date(start_text)
    end_day = _parse_cli_date(end_text)
    if end_day < start_day:
        raise ValueError('End date cannot be before start date.')
    _remove_empty_previous_month_cache_dirs(start_day)
    interval = str(interval or 'full').strip().lower()
    if interval not in {'full', 'hybrid', '1minute', '1second'}:
        raise ValueError('NSE 50 interval must be full, hybrid, 1minute or 1second.')
    wanted = ('1minute', '1second') if interval == 'full' else (('1minute', '1second') if interval == 'hybrid' else (interval,))
    breeze = require_breeze()
    stock_cache = _load_security_cache()
    total_days = sum(1 for d in _daterange(start_day, end_day) if _is_nse_trading_day(d))
    symbols = list(NSE50_HISTORICAL_SYMBOLS)
    workers = min(12, max(1, len(symbols)))
    print(f'NSE 50 HISTORICAL DOWNLOAD START: {start_day} to {end_day}; symbols={len(symbols)}; trading_days={total_days}; interval={interval}; requested={",".join(wanted)}; cache-first=YES; workers={workers}; REST pacing=0.58s shared (about 88/minute; workers overlap network latency).', flush=True)
    downloaded = reused = failed = 0

    def _download_one(day, index, symbol):
        """Process one symbol independently; each worker owns its output CSV."""
        outdir = _cache_day_dir(day)
        outdir.mkdir(parents=True, exist_ok=True)
        # Prefer an existing canonical file without resolving company names again.
        suffix = f'_{str(symbol).strip().upper()}_historical.csv'.upper()
        candidates = [p for p in outdir.glob(f'{day:%Y-%m-%d}_*_historical.csv') if p.name.upper().endswith(suffix)]
        final_path = candidates[0] if candidates else None
        try:
            cached_minute, cached_second = _read_canonical_full_historical(final_path) if final_path else (pd.DataFrame(), pd.DataFrame())
        except Exception:
            cached_minute, cached_second = pd.DataFrame(), pd.DataFrame()
        existing = {'1minute': cached_minute, '1second': cached_second}
        for intrvl in ('1minute', '1second'):
            if not _nse50_has_usable_candles(existing[intrvl]):
                existing[intrvl] = pd.DataFrame()
            elif intrvl == '1minute' and not _nse50_minute_coverage_ok(existing[intrvl], max_gap_minutes=5):
                print(f'NSE 50 MINUTE CACHE INCOMPLETE: {symbol} {day}; gap >5 minutes; forcing full-session broker refresh.', flush=True)
                existing[intrvl] = pd.DataFrame()
        for intrvl in wanted:
            if existing[intrvl].empty:
                cached = _load_cached_intraday(day, symbol, intrvl)
                if _nse50_has_usable_candles(cached) and (intrvl != '1minute' or _nse50_minute_coverage_ok(cached, max_gap_minutes=5)):
                    existing[intrvl] = cached
        missing = [x for x in wanted if not _nse50_has_usable_candles(existing[x])]
        # Resolve only when needed for a missing request or when a canonical CSV
        # must be created from interval cache files. The same get_names payload
        # seeds the company-name cache, avoiding a second API call.
        stock = str(stock_cache.get(str(symbol).strip().upper()) or '').strip()
        if missing or final_path is None:
            _, stock = _resolve_historical_breeze_stock(breeze, symbol, stock_cache)
        if final_path is None:
            final_path = _canonical_full_historical_path(outdir, day, symbol)
        if not missing:
            # Per-interval cache files may exist without the combined CSV. Publish
            # the canonical file once, then count it as cache reuse.
            _write_canonical_full_historical(final_path, existing['1minute'], existing['1second'], day=day, symbol=symbol)
            _remove_nse50_legacy_alias_files(day, symbol, stock_cache)
            return ('reused', index, symbol, final_path.name, {x: len(existing[x]) for x in wanted})
        for intrvl in missing:
            # Try the resolved Breeze short code first. If Breeze returns an
            # empty HTTP-200 payload, refresh get_names and retry using a distinct
            # verified/known short-code candidate before recording a failure.
            candidates = []
            if stock:
                candidates.append(str(stock).strip().upper())
            candidates.extend(str(x).strip().upper() for x in _HISTORICAL_BREEZE_CODE_FALLBACKS.get(str(symbol).upper(), ()))
            candidates.append(str(symbol).strip().upper())
            candidates = list(dict.fromkeys(x for x in candidates if x))
            candles = pd.DataFrame()
            tried = set()
            for attempt, candidate in enumerate(candidates, 1):
                if candidate in tried:
                    continue
                tried.add(candidate)
                print(f'NSE 50 REQUEST: {symbol} via Breeze stock_code={candidate} {intrvl} on {day}; worker={index}; candidate={attempt}/{len(candidates)}.', flush=True)
                for retry in range(1, 4):
                    candles = _historical_session_candles(breeze, candidate, day, intrvl, strict_minute_coverage=(intrvl == '1minute'))
                    if _nse50_has_usable_candles(candles):
                        if candidate != stock:
                            stock = candidate
                            with _NSE50_MAPPING_LOCK:
                                stock_cache[str(symbol).strip().upper()] = candidate
                                _save_security_cache(stock_cache)
                            print(f'NSE 50 STOCK CODE RECOVERED: {symbol} -> {candidate}; saved verified historical route.', flush=True)
                        break
                    if retry < 3:
                        print(f'NSE 50 EMPTY RETRY: {symbol} stock_code={candidate} interval={intrvl} date={day}; attempt={retry}/3.', flush=True)
                        time.sleep(1.25 * retry)
                if _nse50_has_usable_candles(candles):
                    break
                # Refresh the symbol mapping after an empty response, then add
                # the newly resolved Breeze code if it differs from prior tries.
                if attempt == 1:
                    try:
                        _, refreshed = _resolve_historical_breeze_stock(breeze, symbol, stock_cache, refresh=True)
                        if refreshed and refreshed not in tried:
                            candidates.append(refreshed)
                    except Exception as map_exc:
                        print(f'NSE 50 STOCK CODE REFRESH WARNING: {symbol}: {map_exc}', flush=True)
            if not _nse50_has_usable_candles(candles):
                raise RuntimeError(f'No usable OHLC candles returned for {symbol} on {day} ({intrvl}) after stock-code refresh and bounded retries; blank placeholder data was not saved.')
            existing[intrvl] = candles
        _write_canonical_full_historical(final_path, existing['1minute'], existing['1second'], day=day, symbol=symbol)
        _remove_nse50_legacy_alias_files(day, symbol, stock_cache)
        return ('downloaded', index, symbol, final_path.name, {x: len(existing[x]) for x in wanted})

    for day in _daterange(start_day, end_day):
        if not _is_nse_trading_day(day):
            print(f'NSE 50 SKIP NON-TRADING DAY: {day}', flush=True)
            continue
        print(f'NSE 50 DAY START: {day}; parallel_workers={workers}.', flush=True)
        # The REST request pacer is global and shared by every worker. Six workers
        # overlap network latency, but actual Breeze request starts remain below
        # the documented 100/minute ceiling. The same worker count is already used
        # by the historical replay engine in this application.
        old_timeout = socket.getdefaulttimeout()
        socket.setdefaulttimeout(35)
        globals()['_HISTORICAL_PARALLEL_SOCKET_TIMEOUT_ACTIVE'] = True
        day_downloaded = day_reused = day_failed = 0
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix='nse50-hist') as pool:
                futures = {pool.submit(_download_one, day, i, sym): (i, sym) for i, sym in enumerate(symbols, 1)}
                completed = 0
                for future in concurrent.futures.as_completed(futures):
                    i, sym = futures[future]
                    completed += 1
                    try:
                        status, idx, symbol, filename, counts = future.result()
                        if status == 'reused':
                            day_reused += 1
                            reused += 1
                            print(f'NSE 50 PROGRESS {day} {completed}/{len(symbols)} {symbol}: CACHE REUSED; {filename}.', flush=True)
                        else:
                            day_downloaded += 1
                            downloaded += 1
                            count_text = ', '.join(f'{k}={v:,}' for k, v in counts.items())
                            print(f'NSE 50 PROGRESS {day} {completed}/{len(symbols)} {symbol}: SAVED {filename}; {count_text}.', flush=True)
                    except Exception as exc:
                        day_failed += 1
                        failed += 1
                        print(f'NSE 50 ERROR {day} {i}/{len(symbols)} {sym}: {type(exc).__name__}: {exc}', flush=True)
                    if completed % 5 == 0 or completed == len(symbols):
                        print(f'NSE 50 LIVE PROGRESS {day}: {completed}/{len(symbols)} completed; downloaded={day_downloaded}; cache_reused={day_reused}; failed={day_failed}.', flush=True)
        finally:
            globals()['_HISTORICAL_PARALLEL_SOCKET_TIMEOUT_ACTIVE'] = False
            socket.setdefaulttimeout(old_timeout)

    print(f'NSE 50 HISTORICAL DOWNLOAD COMPLETE: downloaded={downloaded}; cache_reused={reused}; failed={failed}; symbols={len(symbols)}; interval={interval}; range={start_day} to {end_day}.', flush=True)

    # This action is intentionally data-only. Do not start a second historical
    # replay/download pass after the NSE 50 CSVs have been saved. Users can run
    # Historical Replay separately when they want P&L/workbook calculations.

    month_cursor = date(start_day.year, start_day.month, 1)
    while month_cursor <= end_day:
        legacy = _historical_month_dir(month_cursor.year, month_cursor.month) / f'{month_cursor:%Y-%m}_NIFTY50_Historical.xlsx'
        if legacy.exists():
            try:
                legacy.unlink()
                print(f'NSE 50 LEGACY RAW EXCEL REMOVED: {legacy}')
            except Exception as exc:
                print(f'NSE 50 LEGACY RAW EXCEL REMOVE WARNING: {legacy}: {exc}')
        month_cursor = date(month_cursor.year + (month_cursor.month == 12), 1 if month_cursor.month == 12 else month_cursor.month + 1, 1)
    print('NSE 50 OUTPUT COMPLETE: canonical historical CSV data saved; no replay or P&L workbook generated.', flush=True)


def _daterange(start_day: date, end_day: date):
    current = start_day
    while current <= end_day:
        yield current
        current += timedelta(days=1)


def run_historical_single_share_range(symbol_text: str, start_text: str, end_text: str, interval: str = 'full') -> None:
    """Download one share over a date range and publish one CSV per trading day.

    Each daily CSV keeps the canonical 13-column historical schema, including
    ``interval`` so 1-minute and 1-second candles remain distinguishable. User-facing
    date-named CSVs are placed directly inside the month folder (no date subfolders).
    Canonical date-wise CSVs are stored directly in month folders; no date subfolders
    or range-wide combined CSV are generated by this button.
    """
    start_day = _parse_cli_date(start_text)
    end_day = _parse_cli_date(end_text)
    if start_day > end_day:
        raise RuntimeError('Start date must be on or before end date.')
    days = [d for d in _daterange(start_day, end_day) if _is_nse_trading_day(d)]
    if not days:
        raise RuntimeError(f'No NSE trading days found between {start_day} and {end_day}.')
    interval = str(interval or 'full').strip().lower()
    if interval not in {'full', '1minute', '1second'}:
        raise RuntimeError('One-share interval must be full, 1minute or 1second.')
    print(f'ONE-SHARE HISTORICAL DATE RANGE: input={symbol_text!r}; start={start_day}; end={end_day}; trading_days={len(days)}; weekends/holidays skipped; output=ONE CSV PER DATE; intervals={interval}.')
    symbol, _stock = _resolve_single_share_input(symbol_text)
    failures = []
    completed = []
    # Reuse one authenticated Breeze session for the entire date-range job.
    # Initialize lazily so a fully cached range does not require credentials.
    breeze = None
    resolved_stock = None
    for index, day in enumerate(days, 1):
        print(f'\nDATE PROGRESS: {index}/{len(days)} | date={day} | symbol={symbol} | requested={interval}', flush=True)
        try:
            cache_day_dir = ONE_SHARE_HISTORICAL_CACHE / f'{day:%Y-%m}'
            cached_daily_path = _canonical_full_historical_path(cache_day_dir, day, symbol)
            existing = pd.DataFrame()
            if cached_daily_path.exists():
                try:
                    existing = pd.read_csv(cached_daily_path)
                except Exception as cache_exc:
                    print(f'CACHE READ WARNING: {cached_daily_path.name}: {cache_exc}; will rebuild missing data.', flush=True)
                    existing = pd.DataFrame()
            if interval == 'full':
                # A date is complete only when both intervals have usable rows.
                labels = existing['interval'].astype(str).str.strip().str.lower() if not existing.empty and 'interval' in existing.columns else pd.Series(dtype=str)
                minute_rows = int(labels.eq('1minute').sum())
                second_rows = int(labels.eq('1second').sum())
                if minute_rows > 0 and second_rows > 0:
                    print(f'ONE-SHARE CACHE COMPLETE: {symbol} {day}; 1m={minute_rows:,} rows; 1s={second_rows:,} rows; download skipped.', flush=True)
                else:
                    print(f'ONE-SHARE CACHE INCOMPLETE: {symbol} {day}; 1m={minute_rows:,}; 1s={second_rows:,}; only missing interval(s) will be fetched.', flush=True)
                    run_historical_single_share(symbol_text, day.isoformat(), interval='full', export_options=False)
            else:
                # Single-interval mode fetches only the requested missing interval,
                # while preserving any other interval already stored in the canonical CSV.
                cached_daily_path.parent.mkdir(parents=True, exist_ok=True)
                if not existing.empty and 'interval' in existing.columns:
                    labels = existing['interval'].astype(str).str.strip().str.lower()
                    minute_frame = existing[labels.eq('1minute')].copy()
                    second_frame = existing[labels.eq('1second')].copy()
                else:
                    minute_frame = pd.DataFrame()
                    second_frame = pd.DataFrame()
                selected = minute_frame if interval == '1minute' else second_frame
                if not selected.empty:
                    print(f'ONE-SHARE CACHE REUSE: {symbol} {day} interval={interval}; rows={len(selected):,}; no download.', flush=True)
                else:
                    if breeze is None:
                        breeze = require_breeze()
                        symbol, resolved_stock = _resolve_single_share_input(symbol_text)
                        symbol, resolved_stock = _resolve_historical_breeze_stock(breeze, symbol, _load_security_cache())
                    stock = resolved_stock
                    print(f'ONE-SHARE DOWNLOAD: {symbol} {day} interval={interval} only; requesting missing data.', flush=True)
                    old_cache_only = os.environ.get('BREEZE_HISTORICAL_CACHE_ONLY')
                    os.environ['BREEZE_HISTORICAL_CACHE_ONLY'] = '0'
                    try:
                        selected = _historical_session_candles(breeze, stock, day, interval)
                    finally:
                        if old_cache_only is None:
                            os.environ.pop('BREEZE_HISTORICAL_CACHE_ONLY', None)
                        else:
                            os.environ['BREEZE_HISTORICAL_CACHE_ONLY'] = old_cache_only
                    if selected is None or selected.empty:
                        raise RuntimeError(f'No usable {interval} Breeze historical data returned for {symbol} on {day}.')
                    if interval == '1minute':
                        minute_frame = selected
                    else:
                        second_frame = selected
                _write_canonical_full_historical(cached_daily_path, minute_frame, second_frame, day=day, symbol=symbol)
                removed_checkpoints = _remove_redundant_request_cache_files(day, resolved_stock or symbol)
                if removed_checkpoints:
                    print(f'REQUEST CHECKPOINT CLEANUP: {symbol} {day}; removed={removed_checkpoints}.', flush=True)
            if not cached_daily_path.exists():
                legacy_path = _legacy_canonical_full_historical_path(cache_day_dir, day, symbol)
                if legacy_path.exists():
                    cached_daily_path = legacy_path
            if not cached_daily_path.exists():
                raise FileNotFoundError(f'Daily historical CSV was not created: {cached_daily_path}')
            # The month folder is the ONLY storage location for one-share daily CSVs.
            # Migrate any older date-folder CSVs into the month folder, then remove
            # empty date folders so repeated runs do not recreate the old structure.
            month_output_dir = ONE_SHARE_HISTORICAL_CACHE / f'{day:%Y-%m}'
            month_output_dir.mkdir(parents=True, exist_ok=True)
            daily_path = month_output_dir / cached_daily_path.name
            if cached_daily_path.resolve() != daily_path.resolve():
                shutil.copy2(cached_daily_path, daily_path)
            for old_date_dir in month_output_dir.iterdir():
                if old_date_dir.is_dir() and len(old_date_dir.name) == 10:
                    for old_csv in old_date_dir.glob('*.csv'):
                        target_csv = month_output_dir / old_csv.name
                        if not target_csv.exists():
                            shutil.copy2(old_csv, target_csv)
                    try:
                        if not any(old_date_dir.iterdir()):
                            old_date_dir.rmdir()
                    except OSError:
                        pass
            if not daily_path.exists():
                raise FileNotFoundError(f'Daily historical CSV was not created: {daily_path}')
            # Validate that interval labels exist and the file contains daily data.
            check = pd.read_csv(daily_path, usecols=lambda c: c in {'datetime', 'interval'})
            if check.empty or 'interval' not in check.columns:
                raise ValueError(f'Daily CSV is empty or missing interval column: {daily_path}')
            intervals = sorted(check['interval'].dropna().astype(str).unique().tolist())
            print(f'ONE-SHARE DATE FILE READY: {daily_path} ({len(check):,} rows; intervals={intervals}).', flush=True)
            completed.append((day, daily_path, len(check), intervals))
        except Exception as exc:
            failures.append((day, str(exc)))
            print(f'WARNING: {symbol_text} {day} failed: {type(exc).__name__}: {exc}', flush=True)

    print(f'\nRANGE SUMMARY: requested trading dates={len(days)} | ready={len(completed)} | failed={len(failures)} | skipped as cache-complete are included in ready. Combined file not created.', flush=True)
    print(f'ONE-SHARE DATE RANGE COMPLETE: {len(completed)}/{len(days)} daily CSV files ready; combined file not created.', flush=True)
    if completed:
        print('DAILY FILES:')
        for day, path, rows, intervals in completed:
            print(f'  {day}: {path} ({rows:,} rows; intervals={",".join(intervals)})')
    if failures:
        print('FAILED DATES: ' + '; '.join(f'{day}: {error}' for day, error in failures))
        raise RuntimeError(f'{len(failures)} date(s) failed during the one-share historical range download. See log above.')


def _nearest_monthly_option_expiry(day: date) -> date:
    """Approximate NSE stock-option monthly expiry (Tuesday since Sep-2025, Thursday before)."""
    weekday = 1 if day >= date(2025, 9, 1) else 3  # Tuesday / Thursday
    month_last = date(day.year + (day.month == 12), 1 if day.month == 12 else day.month + 1, 1) - timedelta(days=1)
    expiry = month_last
    while expiry.weekday() != weekday:
        expiry -= timedelta(days=1)
    if expiry < day:
        next_month = date(day.year + (day.month == 12), 1 if day.month == 12 else day.month + 1, 1)
        month_last = date(next_month.year + (next_month.month == 12), 1 if next_month.month == 12 else next_month.month + 1, 1) - timedelta(days=1)
        expiry = month_last
        while expiry.weekday() != weekday:
            expiry -= timedelta(days=1)
    return expiry


def _option_strike_step(symbol: str, price: float) -> float:
    """Known common strike steps for the supported shares; conservative fallback by price band."""
    steps = {'VEDL': 10.0, 'TATASTEEL': 5.0, 'IDEA': 1.0}
    if symbol.upper() in steps:
        return steps[symbol.upper()]
    if price < 100:
        return 5.0
    if price < 500:
        return 10.0
    return 20.0


def run_equity_options_historical_range(symbol_text: str, start_text: str, end_text: str) -> None:
    """Download multiple equities into separate per-share, per-date CSV files."""
    start_day = _parse_cli_date(start_text)
    end_day = _parse_cli_date(end_text)
    if start_day > end_day:
        raise RuntimeError('Start Date must be on or before End Date.')

    # Accept one or several symbols separated by /, comma, semicolon, or whitespace-delimited plus.
    raw_symbols = [x.strip() for x in re.split(r'[/,;]+', str(symbol_text or '')) if x.strip()]
    if not raw_symbols:
        raise RuntimeError('Enter one or more symbols, e.g. VEDL/TATASTEEL/VODE.')
    aliases = {'VODE': 'IDEA', 'VOD': 'IDEA', 'VODAFONE': 'IDEA', 'VODAFONE IDEA': 'IDEA'}
    resolved = []
    for raw in raw_symbols:
        query = aliases.get(raw.strip().upper(), raw.strip())
        symbol, stock = _resolve_single_share_input(query)
        if symbol.upper() in ('VODE', 'VOD'):
            symbol = 'IDEA'
            stock = 'IDEA'
        if not any(existing[0] == symbol for existing in resolved):
            resolved.append((symbol, stock))
    breeze = require_breeze()
    # Resolve NSE symbols to the broker's canonical equity stock_code (e.g. VEDL -> VEDLIM).
    # The NSE symbol remains the user-facing symbol and the options underlying identifier.
    stock_cache = _load_security_cache()
    resolved_with_broker_codes = []
    for symbol, _stock_hint in resolved:
        mapped_symbol, broker_stock = _resolve_historical_breeze_stock(breeze, symbol, stock_cache)
        resolved_with_broker_codes.append((mapped_symbol, broker_stock))
    resolved = resolved_with_broker_codes
    outroot = ROOT / 'Data' / 'Historical_Data' / 'Equity_Options' / 'Combined_Datewise'
    outroot.mkdir(parents=True, exist_ok=True)
    days = [d for d in _daterange(start_day, end_day) if _is_nse_trading_day(d)]
    if not days:
        raise RuntimeError(f'No NSE trading days found between {start_day} and {end_day}.')
    print(f'EQUITY + OPTIONS DOWNLOAD: symbols={", ".join(s for s, _ in resolved)}; {start_day} to {end_day}; ATM +/- 3 strikes; CALL + PUT; {len(days)} trading days.')
    failures = []
    output_files = []
    for n, day in enumerate(days, 1):
        day_rows = {symbol: [] for symbol, _ in resolved}
        print(f'[{n}/{len(days)}] Combined equity and options for {day}...', flush=True)
        from_dt = datetime.combine(day, datetime.strptime('09:15', '%H:%M').time())
        to_dt = datetime.combine(day, datetime.strptime('15:30', '%H:%M').time())
        for symbol, stock in resolved:
            try:
                eq_resp = broker_call(
                    f'Equity/options equity history {symbol} {day}', breeze.get_historical_data_v2,
                    interval='1minute', from_date=_utc_iso_for_local(from_dt), to_date=_utc_iso_for_local(to_dt),
                    stock_code=stock, exchange_code='NSE', product_type='cash')
                eq_data = eq_resp.get('Success') or []
                eq_df = _candle_dataframe(eq_data)
                if not eq_data:
                    err_detail = eq_resp.get('Error') or eq_resp.get('error') or eq_resp.get('Status') or 'empty Success payload'
                    print(f'  EQUITY API EMPTY: {symbol} broker_stock={stock}; response={str(err_detail)[:300]}')
                if eq_df is None or eq_df.empty:
                    cache_dir = ONE_SHARE_HISTORICAL_CACHE / f'{day:%Y-%m}'
                    cpath = _canonical_full_historical_path(cache_dir, day, symbol)
                    cached_minute, _ = _read_canonical_full_historical(cpath) if cpath.exists() else (pd.DataFrame(), pd.DataFrame())
                    eq_df = cached_minute
                if eq_df is None or eq_df.empty:
                    raise RuntimeError('No equity candles returned; cannot calculate ATM strike.')
                eq_df = eq_df.copy()
                close_col = 'close' if 'close' in eq_df.columns else 'Close'
                eq_df[close_col] = pd.to_numeric(eq_df[close_col], errors='coerce')
                eq_df = eq_df[eq_df[close_col].notna()]
                if eq_df.empty:
                    raise RuntimeError('Equity history contains no valid close prices.')
                day_close = float(eq_df.iloc[-1][close_col])
                eq_df['data_type'] = 'EQUITY'
                eq_df['symbol'] = symbol
                eq_df['underlying_symbol'] = symbol
                eq_df['trade_date'] = day.isoformat()
                eq_df['right'] = ''
                eq_df['strike_price'] = pd.NA
                eq_df['expiry_date'] = ''
                eq_df['underlying_close'] = day_close
                eq_df['atm_strike'] = pd.NA
                day_rows[symbol].extend(eq_df.to_dict('records'))

                step = _option_strike_step(symbol, day_close)
                atm = round(day_close / step) * step
                expiry = _nearest_monthly_option_expiry(day)
                expiry_iso = f'{expiry:%Y-%m-%d}T07:00:00.000Z'
                strikes = [round(atm + offset * step, 2) for offset in range(-3, 4)]
                for right in ('call', 'put'):
                    for strike in strikes:
                        try:
                            opt_records = []
                            for method_name in ('get_historical_data_v2', 'get_historical_data'):
                                method = getattr(breeze, method_name, None)
                                if not callable(method):
                                    continue
                                opt_resp = broker_call(
                                    f'Breeze {method_name} option {symbol} {right} {strike} {expiry}', method,
                                    interval='1minute', from_date=_utc_iso_for_local(from_dt), to_date=_utc_iso_for_local(to_dt),
                                    stock_code=stock, exchange_code='NFO', product_type='options',
                                    expiry_date=expiry_iso, right=right, strike_price=str(strike))
                                opt_records = opt_resp.get('Success') or []
                                if opt_records:
                                    break
                                err_detail = opt_resp.get('Error') or opt_resp.get('error') or opt_resp.get('Status') or 'empty Success payload'
                                print(f'  OPTION EMPTY: {symbol} {right.upper()} strike={strike:g} expiry={expiry} via {method_name}; response={str(err_detail)[:250]}')
                            opt_df = _candle_dataframe(opt_records) if opt_records else pd.DataFrame()
                            if opt_df is None or opt_df.empty:
                                continue
                            opt_df = opt_df.copy()
                            opt_df['data_type'] = 'OPTION'
                            opt_df['symbol'] = symbol
                            opt_df['underlying_symbol'] = symbol
                            opt_df['trade_date'] = day.isoformat()
                            opt_df['expiry_date'] = expiry.isoformat()
                            opt_df['right'] = right.upper()
                            opt_df['strike_price'] = strike
                            opt_df['underlying_close'] = day_close
                            opt_df['atm_strike'] = atm
                            day_rows[symbol].extend(opt_df.to_dict('records'))
                        except Exception as exc:
                            failures.append(f'{day} {symbol} {right.upper()} {strike:g} expiry {expiry}: {exc}')
                            print(f'  OPTION FAILED: {symbol} {right.upper()} strike={strike:g} expiry={expiry}: {exc}')
            except Exception as exc:
                failures.append(f'{day} {symbol} equity/ATM: {exc}')
                print(f'  SHARE FAILED: {day} {symbol}: {type(exc).__name__}: {exc}')
        for symbol, _stock in resolved:
            rows = day_rows.get(symbol, [])
            if rows:
                raw = pd.DataFrame(rows)
                eq = raw[raw.get('data_type', pd.Series(index=raw.index, dtype=str)).eq('EQUITY')].copy()
                opt = raw[raw.get('data_type', pd.Series(index=raw.index, dtype=str)).eq('OPTION')].copy()
                # Standard wide schema: 12 equity + 7 CALL + 7 PUT + 2 index context = 28 columns.
                base_cols = ['close','datetime','exchange_code','high','low','open','stock_code','volume',
                             'is_actual','_cum_volume','_candle_turnover_rupees','_cum_turnover_cr']
                call_cols = ['call_open','call_high','call_low','call_close','call_volume','call_strike','call_expiry']
                put_cols = ['put_open','put_high','put_low','put_close','put_volume','put_strike','put_expiry']
                out_cols = base_cols + call_cols + put_cols + ['nse50_close','sector_close']
                if not eq.empty:
                    if '_dt' not in eq.columns:
                        eq['_dt'] = pd.to_datetime(eq.get('datetime'), errors='coerce')
                    eq['_dt'] = pd.to_datetime(eq['_dt'], errors='coerce')
                    eq = eq[eq['_dt'].notna()].sort_values('_dt').drop_duplicates('_dt', keep='last')
                    eq['datetime'] = eq['_dt'].dt.strftime('%Y-%m-%d %H:%M:%S')
                    eq['is_actual'] = eq.get('is_actual', pd.Series(True, index=eq.index)).fillna(True)
                    eq['volume'] = pd.to_numeric(eq.get('volume', pd.Series(0, index=eq.index)), errors='coerce').fillna(0)
                    eq['_cum_volume'] = eq.get('_cum_volume', eq['volume'].cumsum())
                    eq['_candle_turnover_rupees'] = eq.get('_candle_turnover_rupees', pd.to_numeric(eq['close'], errors='coerce') * eq['volume'])
                    eq['_cum_turnover_cr'] = eq.get('_cum_turnover_cr', eq['_candle_turnover_rupees'].cumsum() / 10000000.0)
                    eq_by_dt = {pd.Timestamp(r['_dt']): r for r in eq.to_dict('records')}
                    option_map = {}
                    if not opt.empty:
                        opt['_dt'] = pd.to_datetime(opt.get('_dt', opt.get('datetime')), errors='coerce')
                        opt['strike_price'] = pd.to_numeric(opt.get('strike_price'), errors='coerce')
                        for _, r in opt[opt['_dt'].notna()].iterrows():
                            right = str(r.get('right', '')).upper()
                            strike = float(r.get('strike_price')) if pd.notna(r.get('strike_price')) else None
                            if right in ('CALL','PUT') and strike is not None:
                                option_map[(right, strike, pd.Timestamp(r['_dt']))] = r.to_dict()
                    step = _option_strike_step(symbol, float(pd.to_numeric(eq['close'], errors='coerce').dropna().iloc[-1]))
                    atm = round(float(pd.to_numeric(eq['close'], errors='coerce').dropna().iloc[-1]) / step) * step
                    strikes = [round(atm + k * step, 2) for k in range(-3, 4)]
                    wide_rows = []
                    for strike in strikes:
                        for dt, erow in eq_by_dt.items():
                            out = {c: erow.get(c, pd.NA) for c in base_cols}
                            for prefix, right in (('call','CALL'),('put','PUT')):
                                o = option_map.get((right, strike, dt), {})
                                for fld in ('open','high','low','close','volume'):
                                    out[f'{prefix}_{fld}'] = o.get(fld, pd.NA) if o else pd.NA
                                out[f'{prefix}_strike'] = strike if o else pd.NA
                                out[f'{prefix}_expiry'] = o.get('expiry_date', pd.NA) if o else pd.NA
                            out['nse50_close'] = erow.get('nse50_close', pd.NA)
                            out['sector_close'] = erow.get('sector_close', pd.NA)
                            wide_rows.append(out)
                    result = pd.DataFrame(wide_rows, columns=out_cols)
                else:
                    result = pd.DataFrame(columns=out_cols)
                share_dir = outroot / symbol
                share_dir.mkdir(parents=True, exist_ok=True)
                day_path = share_dir / f'{day:%Y-%m-%d}_{symbol}_EQUITY_CALL_PUT.csv'
                result.to_csv(day_path, index=False)
                output_files.append(str(day_path))
                populated = int(result['call_close'].notna().sum()) + int(result['put_close'].notna().sum())
                print(f'  SAVED SHARE FILE: {day_path} ({len(result):,} rows; {len(result.columns)} columns; option close values={populated})')
            else:
                print(f'  NO DATA SAVED for {symbol} on {day}.')
    print(f'PER-SHARE DATE-WISE CSV FILES: {len(output_files)}; folder={outroot}')
    print('Each file contains one underlying share only, with any returned EQUITY and OPTION rows. Empty/unsupported option contracts are logged and are never fabricated.')
    if failures:
        print(f'WARNING: {len(failures)} equity/option request(s) failed or returned errors. First 30:')
        for failure in failures[:30]:
            print('  ' + failure)
    if not output_files:
        raise RuntimeError('No equity or option data was downloaded; check Breeze credentials, symbols, API access, and contract parameters.')

def _download_single_share_equity_call_put_csv(breeze, symbol: str, stock: str, day: date, minute: pd.DataFrame, canonical_path: Path) -> Path:
    """Create a single 28-column CSV: one row per minute/strike with equity + CALL + PUT + index context.

    Seven strikes (ATM +/- 3 steps) are represented as repeated underlying rows,
    with the matching call and put contract fields populated on each row.
    The canonical minute/second cache remains untouched.
    """
    base_cols = ['close','datetime','exchange_code','high','low','open','stock_code','volume',
                 'is_actual','_cum_volume','_candle_turnover_rupees','_cum_turnover_cr']
    call_cols = ['call_open','call_high','call_low','call_close','call_volume','call_strike','call_expiry']
    put_cols = ['put_open','put_high','put_low','put_close','put_volume','put_strike','put_expiry']
    index_cols = ['nse50_close', 'sector_close']
    out_cols = base_cols + call_cols + put_cols + index_cols
    if minute is None or minute.empty:
        raise RuntimeError('Cannot download CALL/PUT history because equity 1-minute candles are empty.')
    eq = minute.copy()
    if '_dt' not in eq.columns:
        if 'datetime' in eq.columns:
            eq['_dt'] = pd.to_datetime(eq['datetime'], errors='coerce')
        else:
            raise RuntimeError('Equity candles have no datetime column for option timestamp matching.')
    eq['_dt'] = pd.to_datetime(eq['_dt'], errors='coerce')
    eq = eq[eq['_dt'].notna()].copy()
    eq['close'] = pd.to_numeric(eq.get('close'), errors='coerce')
    eq = eq[eq['close'].notna()].sort_values('_dt').drop_duplicates('_dt', keep='last')
    if eq.empty:
        raise RuntimeError('Equity candles have no valid close prices for ATM calculation.')
    # Calculate ATM from the actual underlying session close, not a hard-coded price.
    underlying_close = float(eq.iloc[-1]['close'])
    step = _option_strike_step(symbol, underlying_close)
    atm = round(underlying_close / step) * step
    strikes = [round(atm + offset * step, 2) for offset in range(-3, 4)]
    expiry = _nearest_monthly_option_expiry(day)
    expiry_iso = f'{expiry:%Y-%m-%d}T07:00:00.000Z'
    print(f'ONE-SHARE OPTIONS: {symbol}; close={underlying_close:g}; ATM={atm:g}; step={step:g}; strikes={strikes}; expiry={expiry}.')

    # Normalize equity schema; 12 equity fields are retained, no interval column
    # because this output is specifically the 1-minute equity+options joined file.
    eq['datetime'] = eq['_dt'].dt.strftime('%Y-%m-%d %H:%M:%S')
    eq['exchange_code'] = eq.get('exchange_code', pd.Series('NSE', index=eq.index)).fillna('NSE')
    eq['stock_code'] = eq.get('stock_code', pd.Series(stock, index=eq.index)).fillna(stock)
    eq['is_actual'] = eq.get('is_actual', pd.Series(True, index=eq.index)).fillna(True)
    eq['volume'] = pd.to_numeric(eq.get('volume', pd.Series(0, index=eq.index)), errors='coerce').fillna(0)
    eq['_cum_volume'] = eq['_cum_volume'] if '_cum_volume' in eq.columns else eq['volume'].cumsum()
    eq['_candle_turnover_rupees'] = eq['_candle_turnover_rupees'] if '_candle_turnover_rupees' in eq.columns else eq['close'] * eq['volume']
    eq['_cum_turnover_cr'] = eq['_cum_turnover_cr'] if '_cum_turnover_cr' in eq.columns else eq['_candle_turnover_rupees'].cumsum() / 10000000.0
    for c in ('open','high','low'):
        if c not in eq.columns: eq[c] = pd.NA
    eq_by_dt = {pd.Timestamp(r['_dt']): r for r in eq.to_dict('records')}
    option_map = {}
    for right in ('call', 'put'):
        for strike in strikes:
            records = []
            params = dict(interval='1minute', from_date=_utc_iso_for_local(datetime.combine(day, datetime.strptime('09:15','%H:%M').time())),
                          to_date=_utc_iso_for_local(datetime.combine(day, datetime.strptime('15:30','%H:%M').time())),
                          stock_code=stock, exchange_code='NFO', product_type='options', expiry_date=expiry_iso,
                          right=right, strike_price=str(strike))
            # V2 is preferred; some Breeze SDK/API combinations return empty V2
            # payloads for historical stock options, so try the documented V1 endpoint too.
            for method_name in ('get_historical_data_v2', 'get_historical_data'):
                method = getattr(breeze, method_name, None)
                if not callable(method):
                    continue
                try:
                    response = broker_call(f'Breeze {method_name} option {symbol} {right} {strike} {expiry}', method, **params)
                    records = response.get('Success') or []
                    if records:
                        break
                    detail = response.get('Error') or response.get('error') or response.get('Status') or 'empty Success payload'
                    print(f'  OPTION EMPTY: {symbol} {right.upper()} strike={strike:g} expiry={expiry} via {method_name}; {str(detail)[:200]}')
                except Exception as exc:
                    print(f'  OPTION REQUEST ERROR: {symbol} {right.upper()} strike={strike:g} expiry={expiry} via {method_name}: {exc}')
            opt_df = _candle_dataframe(records) if records else pd.DataFrame()
            normalized = {}
            if opt_df is not None and not opt_df.empty:
                if '_dt' not in opt_df.columns and 'datetime' in opt_df.columns:
                    opt_df['_dt'] = pd.to_datetime(opt_df['datetime'], errors='coerce')
                opt_df['_dt'] = pd.to_datetime(opt_df['_dt'], errors='coerce')
                for row in opt_df[opt_df['_dt'].notna()].to_dict('records'):
                    normalized[pd.Timestamp(row['_dt'])] = row
            option_map[(right, strike)] = normalized
            if normalized:
                print(f'  OPTION DATA OK: {symbol} {right.upper()} strike={strike:g} expiry={expiry}; candles={len(normalized)}.')
    rows = []
    for strike in strikes:
        for dt, erow in eq_by_dt.items():
            row = {c: erow.get(c, pd.NA) for c in base_cols}
            row['nse50_close'] = erow.get('nse50_close', pd.NA)
            row['sector_close'] = erow.get('sector_close', pd.NA)
            call = option_map.get(('call', strike), {}).get(dt, {})
            put = option_map.get(('put', strike), {}).get(dt, {})
            for prefix, opt in (('call', call), ('put', put)):
                for field in ('open','high','low','close','volume'):
                    value = opt.get(field, pd.NA) if opt else pd.NA
                    row[f'{prefix}_{field}'] = value
                row[f'{prefix}_strike'] = strike if opt else pd.NA
                row[f'{prefix}_expiry'] = expiry.isoformat() if opt else pd.NA
            rows.append(row)
    result = pd.DataFrame(rows, columns=out_cols)
    # If no option request returned any candles, don't silently present an equity-only
    # file as a successful combined download.
    if not any(result[c].notna().any() for c in ('call_close','put_close')):
        raise RuntimeError('Breeze returned no CALL or PUT candles for any of the 7 strikes. Check option symbol/expiry support; equity cache was preserved. See OPTION EMPTY/REQUEST ERROR lines above.')
    target = canonical_path.with_name(f'{day:%Y-%m-%d}_{symbol}_EQUITY_CALL_PUT.csv')
    tmp = target.with_suffix('.tmp.csv')
    result.to_csv(tmp, index=False)
    tmp.replace(target)
    print(f'ONE-SHARE EQUITY+CALL+PUT CSV: {target} ({len(result):,} rows; {len(out_cols)} columns; option prices populated={int(result["call_close"].notna().sum()) + int(result["put_close"].notna().sum()):,}).')
    return target


def run_historical_single_share(symbol_text: str, date_text: str, interval: str = 'full', export_options: bool = True) -> None:
    """Download one requested NSE share using the same canonical historical downloader."""
    day=_parse_cli_date(date_text)
    if not _is_nse_trading_day(day):
        raise RuntimeError(f'{day} is not an NSE equity trading day.')
    symbol,stock=_resolve_single_share_input(symbol_text)
    breeze=require_breeze()
    symbol,stock=_resolve_historical_breeze_stock(breeze,symbol,_load_security_cache())
    print(f'ONE-SHARE HISTORICAL REQUEST: input={symbol_text!r}; NSE symbol={symbol}; Breeze stock={stock}; date={day}; mode=FULL 1MIN + FULL 1SEC; canonical downloader=YES.')
    # Explicit user-requested downloads must be allowed to call Breeze even if the
    # GUI normally enables cache-only mode for offline replay. Restore the setting
    # immediately after this job so backtests remain cache-only.
    _old_cache_only = os.environ.get('BREEZE_HISTORICAL_CACHE_ONLY')
    os.environ['BREEZE_HISTORICAL_CACHE_ONLY'] = '0'
    try:
        minute,second,final_path,_=_download_full_historical_share(breeze,symbol,stock,day,outdir=ONE_SHARE_HISTORICAL_CACHE)
    finally:
        if _old_cache_only is None:
            os.environ.pop('BREEZE_HISTORICAL_CACHE_ONLY', None)
        else:
            os.environ['BREEZE_HISTORICAL_CACHE_ONLY'] = _old_cache_only
    print(f'ONE-SHARE FINAL CSV: {final_path} ({len(minute)+len(second):,} rows; 1m={len(minute):,}; 1s={len(second):,})')
    # Single-day downloads may additionally export the equity+options view.
    # Date-range downloads suppress that extra export and retain one canonical
    # historical CSV per trading date.
    if export_options:
        combined_option_path = _download_single_share_equity_call_put_csv(breeze, symbol, stock, day, minute, final_path)
        print(f'ONE-SHARE EQUITY+OPTIONS EXPORT: {combined_option_path}')
    try:
        daily=_load_cached_daily(day)
        daily_open=None; prev_close=None
        row=pd.DataFrame()
        if daily is not None and not daily.empty:
            row=daily[daily['Symbol'].astype(str).str.upper().eq(symbol.upper())]
            if not row.empty:
                daily_open=float(row.iloc[0]['Open']); prev_close=float(row.iloc[0]['Previous close'])
        if daily_open is not None:
            for lname in ACTIVE_LOGIC_NAMES:
                cfg=get_logic(lname)
                if not cfg.get('enabled',False):
                    continue
                diag=_historical_trigger_diagnostic(minute,day,cfg,lname,symbol,daily_open=daily_open,daily_previous_close=prev_close)
                _print_historical_trigger_diagnostic(diag)
    except Exception as exc:
        print(f'ONE-SHARE DIAGNOSTIC WARNING: {exc}')
    print(f'ONE-SHARE HISTORICAL COMPLETE: {symbol} {day}. FINAL single CSV: {_canonical_full_historical_path(ONE_SHARE_HISTORICAL_CACHE, day, symbol).name}. Existing final CSV is reused on every later run.')


def main():
    p=argparse.ArgumentParser(); p.add_argument('--verify-strategy-parity', action='store_true', help='Run deterministic canonical strategy self-test; never sends orders.'); p.add_argument('--mode', choices=['paper','live'], default='paper'); p.add_argument('--i-understand-live', action='store_true'); p.add_argument('--orders', action='store_true', help='Send paper/live orders after refreshing data'); p.add_argument('--backtest', action='store_true', help='Test all historical signals over the holding period'); p.add_argument('--live-shadow-historical', nargs=2, metavar=('START','END'), help='Replay the LIVE decision flow on historical data with zero broker orders.'); p.add_argument('--holding-days', type=int, default=5, help='Backtest holding period in trading days (default: 5)'); p.add_argument('--backfill-month', help='Explicitly load every eligible NSE share for every trading day in YYYY-MM, e.g. 2026-09'); p.add_argument('--paper-replay', action='store_true', help='Run paper validation through the same historical replay engine on the latest actual NSE session'); p.add_argument('--historical-test-month', help='Run the primary Breeze intraday validation engine for YYYY-MM, e.g. 2026-09'); p.add_argument('--universe', choices=['NSE50','NSE100','NSE500','NSE_ALL_EQ'], default='NSE_ALL_EQ', help='NSE equity universe used consistently by Paper, Live and Historical modes.'); p.add_argument('--auto-paper-market', action='store_true', help='Start automatic all-enabled-logic paper market mode at 09:00, trade from 09:15 and square off at 15:20'); p.add_argument('--auto-live-market', action='store_true', help='Start automatic all-enabled-logic LIVE market mode at 09:15 and square off at 15:20'); p.add_argument('--historical-month-combined', help='One-click monthly eligibility + intraday P&L test for YYYY-MM'); p.add_argument('--historical-month-range', help='Process every month from YYYY-MM through the latest/current month, e.g. 2026-07'); p.add_argument('--nse-range', nargs=2, metavar=('START','END'), help='Download official NSE daily data for an exact date range'); p.add_argument('--historical-range', nargs=2, metavar=('START','END'), help='Run historical validation for an exact date range'); p.add_argument('--nse50-historical-range', nargs=2, metavar=('START','END'), help='Download only NIFTY 50 historical candles for an exact date range'); p.add_argument('--nse50-historical-interval', choices=['full','hybrid','1minute','1second'], default='full', help='NIFTY 50 download interval; 1minute downloads only 1-minute candles.'); p.add_argument('--historical-interval', choices=['full','hybrid','1minute','1second'], default='full', help='Historical mode: full uses the same full 1-minute + full 1-second downloader as the One-Share Historical Download (default: full).'); p.add_argument('--historical-share-limit', type=int, default=None, help='Optional command-line hard ceiling on total unique historical shares for each date.'); p.add_argument('--historical-share', nargs=2, metavar=('COMPANY_OR_SYMBOL','DATE'), help='Download one specific NSE share for one date: symbol/company name + YYYY-MM-DD.'); p.add_argument('--historical-share-range', nargs=3, metavar=('COMPANY_OR_SYMBOL','START','END'), help='Download one NSE share for an inclusive date range; weekends and NSE holidays are skipped.'); p.add_argument('--equity-options-range', nargs=3, metavar=('COMPANY_OR_SYMBOL','START','END'), help='Download equity plus historical CALL/PUT options at ATM +/- 3 strikes for an inclusive date range.'); p.add_argument('--historical-share-interval', choices=['full','1minute','1second'], default='full', help='Single-share interval: full downloads 1-minute + 1-second; 1minute or 1second downloads only that interval.'); args=p.parse_args()
    if args.verify_strategy_parity:
        print(json.dumps(verify_canonical_strategy_parity(), indent=2)); return
    if args.live_shadow_historical:
        run_live_shadow_historical(*args.live_shadow_historical, interval=args.historical_interval, universe=args.universe); return
    if args.equity_options_range:
        run_equity_options_historical_range(*args.equity_options_range); return
    if args.historical_share_range:
        run_historical_single_share_range(*args.historical_share_range, interval=args.historical_share_interval); return
    if args.historical_share:
        run_historical_single_share(*args.historical_share, interval=args.historical_share_interval); return
    if args.nse_range:
        run_nse_date_range(*args.nse_range); return
    if args.nse50_historical_range:
        run_nse50_historical_range(*args.nse50_historical_range, interval=args.nse50_historical_interval); return
    if args.historical_range:
        run_historical_date_range(*args.historical_range, interval=args.historical_interval, historical_share_limit=args.historical_share_limit, universe=args.universe); return
    if args.auto_paper_market:
        run_auto_paper_market(args.universe); return
    if args.auto_live_market:
        if not args.i_understand_live: raise RuntimeError('LIVE requires --i-understand-live.')
        run_auto_live_market(args.universe); return
    if args.backfill_month:
        backfill_month(args.backfill_month); return
    if args.paper_replay:
        run_paper_replay_latest(); return
    if args.historical_month_combined:
        run_historical_month_combined(args.historical_month_combined); return
    if args.historical_month_range:
        run_historical_month_range(args.historical_month_range); return
    if args.historical_test_month:
        run_historical_replay_month(args.historical_test_month); return
    if args.backtest:
        if args.holding_days < 1: raise RuntimeError('Holding days must be at least 1.')
        backtest(args.holding_days); return
    market, top, asof = screen(); write_workbook(market, top, asof)
    print(f'Refreshed {len(market):,} NSE EQ rows for {asof}; {len(top)} qualifying shares selected (ALL eligible shares; no Top-10 sheet or execution limit).')
    if args.orders:
        if args.mode == 'paper':
            result=run_paper_replay_latest()
        else:
            result=run_orders(top,args.mode,args.i_understand_live); record_orders(result)
        print(json.dumps(result, indent=2, default=str))
    else: print('No orders sent. Add --orders after reviewing the refreshed workbook.')

# ================= FRESH EXCEL OUTPUT LAYER =================
# All active Excel writes are delegated to excel_output.py. The trading/replay
# engine does not own workbook schemas or cell formatting.

def _fresh_target_for_results(results, workbook_path=None):
    if workbook_path: return Path(workbook_path)
    dates=[_excel_date(r.get('trade_date')) for r in (results or []) if r.get('trade_date')]
    if dates:
        d=max(dates); return _fresh_monthly_path(MONTHLY_WORKBOOK_DIR, d.strftime('%Y-%m'))
    return WORKBOOK

def record_historical_results(results: list[dict], workbook_path: Path | None = None, replace_dates: set[str] | None = None) -> None:
    """Write Historical/Paper results through the single formula-based Excel layer.

    This is the only active historical Excel writer.  It preserves the four-sheet
    template, replaces the selected-date view, updates YTD, and leaves P&L to the
    Excel formulas in excel_output.py.
    """
    target = Path(workbook_path) if workbook_path else None
    if target is None:
        dates=[_excel_date(r.get('trade_date')) for r in (results or []) if r.get('trade_date')]
        target=_monthly_workbook_path(max(dates).strftime('%Y-%m')) if dates else WORKBOOK
    _fresh_write_historical(results or [], target, replace_dates)
    print(f'FORMULA EXCEL HISTORICAL SAVED: {target} | rows={sum(1 for r in (results or []) if r.get("trade_date"))}')


# Compact build: Excel output and strategy configuration are embedded in this module.
_fresh_write_historical = write_historical
_fresh_write_nse = write_nse
_fresh_write_paper_live = write_paper_live
_fresh_monthly_path = monthly_path

if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        import traceback as _traceback
        print('FATAL PROCESS ERROR: ' + type(exc).__name__ + ': ' + str(exc), flush=True)
        if '--auto-paper-market' in sys.argv:
            try: _write_run_status('PAPER TRADING', 'ERROR', f'{type(exc).__name__}: {exc}', str(_nse_now().date()))
            except Exception: pass
        if '--historical-range' in sys.argv:
            try: _write_run_status('HISTORICAL DOWNLOAD/REPLAY', 'ERROR', f'{type(exc).__name__}: {exc}', 'See Process Log')
            except Exception: pass
        _traceback.print_exc()
        sys.exit(1)
