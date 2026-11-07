from __future__ import annotations
import csv, io, json, os, re, sys, zipfile, subprocess, shutil
from pathlib import Path
from datetime import datetime
try:
    import tkinter as tk
    from tkinter import ttk, filedialog, messagebox
except ImportError:
    tk = None
    ttk = None
    filedialog = None
    messagebox = None
import pandas as pd

PNL_CALCULATOR_BUILD = 'r97-symbol-aware-multilogic-parity'
ROOT=Path(__file__).resolve().parent
CFG_PATH=ROOT/'Config'/'logic_settings.json'
def load_cfg():
    """Load the exact settings saved by the main GUI; never silently substitute strategy defaults."""
    try:
        raw=json.loads(CFG_PATH.read_text(encoding='utf-8'))
        if not isinstance(raw, dict):
            return {}
        return {f'Logic {i}': raw[f'Logic {i}'] for i in range(1, 9) if isinstance(raw.get(f'Logic {i}'), dict)}
    except (OSError, json.JSONDecodeError):
        # An empty result makes the GUI ask the user to open/save Trading Logic.
        # Falling back to hard-coded defaults could produce misleading P&L.
        return {}

def parse_time(v, fallback):
    s=str(v or fallback).strip()
    if re.fullmatch(r'\d{1,2}:\d{2}',s):
        h,m=map(int,s.split(':'))
        if 0<=h<=23 and 0<=m<=59: return f'{h:02d}:{m:02d}'
    return fallback

def read_frame(raw, name):
    df=pd.read_csv(io.BytesIO(raw), low_memory=False)
    df.columns=[str(c).strip().lower() for c in df.columns]
    aliases={'timestamp':'datetime','date_time':'datetime','time':'datetime','date':'datetime','vol':'volume','qty':'volume','prev_close':'previous_close','prevclose':'previous_close','previousclose':'previous_close','previous close':'previous_close','previous closing price':'previous_close','turnover cr':'turnover_cr','turnover (cr)':'turnover_cr','turnover lacs':'turnover_lacs','symbol':'stock_code','ticker':'stock_code','trading_symbol':'stock_code','trading symbol':'stock_code','stock code':'stock_code'}
    df=df.rename(columns={c:aliases.get(c,c) for c in df.columns})
    # Multiple source headers can map to the same canonical name (for example
    # timestamp + date_time + time -> datetime). Collapse those duplicates
    # before selecting columns or calling pd.to_datetime; otherwise pandas
    # raises: ValueError: cannot assemble with duplicate keys.
    if df.columns.duplicated().any():
        merged = {}
        for col in dict.fromkeys(df.columns):
            same = df.loc[:, df.columns == col]
            series = same.iloc[:, 0].copy()
            for j in range(1, same.shape[1]):
                series = series.combine_first(same.iloc[:, j])
            merged[col] = series
        df = pd.DataFrame(merged, index=df.index)
    needed=['datetime','open','high','low','close','volume']
    if not set(needed).issubset(df.columns): raise ValueError('Missing columns: '+', '.join(sorted(set(needed)-set(df.columns))))
    df['datetime']=pd.to_datetime(df['datetime'],errors='coerce')
    # Breeze timestamps are normally naive IST. If the source explicitly carries
    # a timezone, normalize it to NSE local time before grouping by trading date.
    try:
        if isinstance(df['datetime'].dtype, pd.DatetimeTZDtype):
            df['datetime']=df['datetime'].dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)
    except (TypeError, ValueError):
        pass
    for c in ['open','high','low','close','volume']: df[c]=pd.to_numeric(df[c],errors='coerce')
    df=df.dropna(subset=['datetime','open','high','low','close'])
    df=df[(df['open']>0)&(df['high']>0)&(df['low']>0)&(df['close']>0)]
    df['volume']=df['volume'].fillna(0).clip(lower=0)
    dedupe_keys = ['stock_code', 'datetime'] if 'stock_code' in df.columns else ['datetime']
    df=df.sort_values(dedupe_keys).drop_duplicates(dedupe_keys, keep='last').reset_index(drop=True)
    if df.empty: raise ValueError('No valid OHLC rows')
    stem=Path(name).stem
    stem=re.sub(r'_historical$','',stem,flags=re.I)
    bits=stem.split('_')
    # Prefer an explicit symbol column when the CSV supplies one. Canonical
    # Historical filenames are YYYY-MM-DD_<symbol>_historical.csv (legacy company-name filenames are also recognized).
    # using bits[1] incorrectly treated the company name as the ticker.
    if 'stock_code' in df and df['stock_code'].notna().any():
        df['stock_code']=df['stock_code'].astype(str).str.strip().str.upper()
        symbol=str(df['stock_code'].dropna().iloc[0]).strip().upper()
    elif len(bits)>=3 and re.fullmatch(r'\d{4}-\d{2}-\d{2}',bits[0]):
        symbol=bits[-1].strip().upper()
    else:
        symbol=bits[-1].strip().upper()
    if not symbol:
        raise ValueError(f'Could not determine symbol from CSV filename or stock_code column: {name}')
    return df,symbol

def files_from_paths(paths):
    out=[]; errors=[]
    for p in paths:
        try:
            if p.lower().endswith('.zip'):
                with zipfile.ZipFile(p) as z:
                    names=[n for n in z.namelist() if n.lower().endswith('.csv') and not n.endswith('/')]
                    preferred=[n for n in names if n.lower().endswith('_historical.csv')]
                    for n in (preferred or names):
                        out.append((n,z.read(n)))
            else:
                out.append((Path(p).name,Path(p).read_bytes()))
        except Exception as e: errors.append((p,str(e)))
    return out,errors


# Keep this order aligned with BreezeTrading.CONDITION_RANK_DEFAULTS.
CALCULATOR_RANK_DEFAULTS = {
    "minimum_price": 1, "entry_time": 2, "vwap": 3, "orb15": 4,
    "rvol": 5, "movement": 6, "investment": 7, "profit_target": 8,
    "stop_loss": 9, "max_trades": 10, "open_direction": 11,
    "price_big": 12, "price_small": 13, "turnover": 14, "volume": 15,
}

def condition_rank_sequence(cfg):
    """Return GUI condition keys in the same explicit-rank-then-rank-zero order."""
    raw = cfg.get('condition_ranks', {}) if isinstance(cfg, dict) else {}
    ranks = {}
    for key, default in CALCULATOR_RANK_DEFAULTS.items():
        try:
            value = float(raw.get(key, default)) if isinstance(raw, dict) else float(default)
            ranks[key] = int(value) if value.is_integer() and 0 <= value <= 15 else default
        except (TypeError, ValueError, OverflowError):
            ranks[key] = default
    # Match BreezeTrading.normalize_condition_ranks exactly: explicit positive
    # ranks must be unique. If a stale/manual config contains duplicates, fall
    # back to the shared documented defaults rather than silently inventing a
    # different order in CSV history than in Paper/Live. Rank 0 remains valid
    # for any number of conditions and sorts after explicit ranks.
    positive = [rank for rank in ranks.values() if rank > 0]
    if len(positive) != len(set(positive)):
        ranks = dict(CALCULATOR_RANK_DEFAULTS)
    return sorted(CALCULATOR_RANK_DEFAULTS, key=lambda k: (ranks[k] == 0, ranks[k] if ranks[k] > 0 else CALCULATOR_RANK_DEFAULTS[k], CALCULATOR_RANK_DEFAULTS[k]))

def format_rank_sequence(cfg):
    raw = cfg.get('condition_ranks', {}) if isinstance(cfg, dict) else {}
    order = condition_rank_sequence(cfg)
    return ' -> '.join(f"{(raw.get(k, CALCULATOR_RANK_DEFAULTS[k]) if isinstance(raw, dict) else CALCULATOR_RANK_DEFAULTS[k])}: {k}" for k in order)

_CANONICAL_ENGINE = None

def canonical_engine():
    """Load the exact engine used by Historical/Paper/Live, from this folder."""
    global _CANONICAL_ENGINE
    if _CANONICAL_ENGINE is None:
        import importlib.util
        engine_path = ROOT / 'BreezeTrading.py'
        spec = importlib.util.spec_from_file_location('BreezeTrading_Canonical_Engine', engine_path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f'Cannot load canonical trading engine: {engine_path}')
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        _CANONICAL_ENGINE = module
    return _CANONICAL_ENGINE


def calc_symbol(df, date, symbol, cfg, target_pct=None, stop_pct=None, use_orb=None, use_vwap=None,
                use_movement=None, use_volume=None, use_turnover=None, use_price_band=None,
                entry_override='', exit_override='', previous_close=None, logic_name=None):
    """Calculate one symbol/day by calling BreezeTrading's canonical historical replay.

    This intentionally does not maintain a second implementation of entry/exit rules.
    The replay engine uses the same canonical BUY/SELL evaluators as Paper and Live.
    Input is 1-minute OHLCV, so intraminute tick order/fills remain an approximation.
    """
    if df is None or df.empty:
        return None, {'Date':date,'Symbol':symbol,'Status':'NO DATA','Reason':'No candles for date'}
    try:
        day_obj = datetime.strptime(str(date), '%Y-%m-%d').date()
        x = df.copy().sort_values('datetime').reset_index(drop=True)
        x = x[pd.to_datetime(x['datetime'], errors='coerce').dt.strftime('%Y-%m-%d') == str(date)].copy()
        if x.empty:
            return None, {'Date':date,'Symbol':symbol,'Status':'NO DATA','Reason':'No candles for date'}
        try:
            pc = float(previous_close) if previous_close is not None and float(previous_close) > 0 else None
        except (TypeError, ValueError):
            pc = None
        if pc is None:
            if 'previous_close' in x.columns:
                vals = pd.to_numeric(x['previous_close'], errors='coerce')
                vals = vals[vals > 0]
                if not vals.empty:
                    pc = float(vals.iloc[0])
        if pc is None:
            return None, {'Date':date,'Symbol':symbol,'Status':'MISSING PREVIOUS CLOSE',
                          'Reason':'Canonical entry rules require the previous trading close. Include prior-session candles or previous_close.'}

        # Normalize source turnover to rupees; Breeze's canonical engine expects rupees.
        if 'turnover_cr' in x.columns and 'turnover' not in x.columns:
            x['turnover'] = pd.to_numeric(x['turnover_cr'], errors='coerce').fillna(0.0) * 10000000.0
        elif 'turnover_lacs' in x.columns and 'turnover' not in x.columns:
            x['turnover'] = pd.to_numeric(x['turnover_lacs'], errors='coerce').fillna(0.0) * 100000.0
        elif 'turnover' in x.columns:
            x['turnover'] = pd.to_numeric(x['turnover'], errors='coerce').fillna(0.0)

        candles = x.drop(columns=['_dt'], errors='ignore').rename(columns={'datetime':'_dt'}).copy()
        candles['_dt'] = pd.to_datetime(candles['_dt'], errors='coerce')
        candles = candles.dropna(subset=['_dt','open','high','low','close'])
        if candles.empty:
            return None, {'Date':date,'Symbol':symbol,'Status':'NO DATA','Reason':'No valid timestamp/OHLC candles'}
        session = candles[(candles['_dt'].dt.strftime('%H:%M') >= '09:15') &
                          (candles['_dt'].dt.strftime('%H:%M') <= '15:24')]
        if session.empty:
            return None, {'Date':date,'Symbol':symbol,'Status':'NO DATA','Reason':'No NSE regular-session candles (09:15–15:24)'}
        session = session.sort_values('_dt').reset_index(drop=True)
        day_open = float(session.iloc[0]['open'])
        day_high = float(pd.to_numeric(session['high'], errors='coerce').max())
        day_volume = float(pd.to_numeric(session.get('volume', 0), errors='coerce').fillna(0).sum())
        day_turnover_cr = None
        if 'turnover' in session.columns:
            day_turnover_cr = float(pd.to_numeric(session['turnover'], errors='coerce').fillna(0).sum()) / 10000000.0

        engine = canonical_engine()
        # Use the engine's normalized copy of the persisted GUI configuration.
        logic_name = str(logic_name or 'Logic 1')
        cfg = engine.get_logic(logic_name)
        result = engine._simulate_logic_from_candles_uncached(
            None, symbol, symbol, day_obj, session, logic_name,
            budget=float(cfg.get('buy_amount', 0) or 0), interval='1minute',
            daily_open=day_open, daily_high=day_high, daily_previous_close=pc,
            daily_volume=day_volume, daily_turnover_cr=day_turnover_cr)
        status = str(result.get('status') or 'UNKNOWN')
        audit_base = {'Date':date,'Symbol':symbol,'Status':status,
                      'Reason':str(result.get('exit_reason') or result.get('entry_audit',{}).get('reason') or result.get('data_source') or status),
                      'Data Source':result.get('data_source','Breeze canonical historical replay'),
                      'Parity':'Canonical Live/Paper BUY/SELL decisions; 1-minute candle fill approximation'}
        if status != 'HISTORICAL_REPLAY_COMPLETE':
            return None, audit_base

        buy_dt = pd.Timestamp(result.get('buy_trade_datetime'))
        sell_dt = pd.Timestamp(result.get('sell_trade_datetime'))
        buy_price = float(result.get('buy_actual_price') or 0)
        sell_price = float(result.get('sell_actual_price') or 0)
        qty = int(result.get('buy_actual_qty') or 0)
        gross = float(result.get('gross_pnl') or 0.0)
        charges = float(result.get('total_charges') or result.get('simulated_total_charges') or 0.0)
        net = float(result.get('net_pnl')) if result.get('net_pnl') is not None else gross - charges
        pnl_pct = float(result.get('pnl_pct')) * 100.0 if result.get('pnl_pct') is not None else (net / (buy_price * qty) * 100.0 if buy_price > 0 and qty > 0 else 0.0)
        target = buy_price * (1.0 + float(cfg.get('profit_target',0) or 0)/100.0)
        stop = buy_price * (1.0 - float(cfg.get('initial_stop',0) or 0)/100.0)
        trade = {
            'Date':date,'Symbol':symbol,'Signal Time':buy_dt,'Buy Date & Time':buy_dt,
            'Buy Price':buy_price,'Buy Quantity':qty,'Capital Used ₹':buy_price*qty,
            'Profit Target Price':target,'Stop Loss Price':stop,
            'Sell Date & Time':sell_dt,'Sell Price':sell_price,
            'Exit Reason':result.get('exit_reason') or status,
            'Gross P&L ₹':gross,'Brokerage ₹':float(result.get('brokerage') or 0.0),'STT ₹':float(result.get('stt') or 0.0),'Exchange Charges ₹':float(result.get('exchange_turnover_charges') or 0.0),'SEBI Charges ₹':float(result.get('sebi_charges') or 0.0),'GST ₹':float(result.get('gst') or 0.0),'Stamp Duty ₹':float(result.get('stamp_duty') or 0.0),'Other Charges ₹':float(result.get('total_other_charges') or 0.0),'Total Charges ₹':charges,'P&L ₹':net,'P&L %':pnl_pct,'Charge Plan':result.get('charge_plan','Prime 999'),'Charge Source':result.get('charge_source','ICICI intraday formula'),
            'Condition Evaluation Order':format_rank_sequence(cfg),
            'Data Source':result.get('data_source','Breeze canonical historical replay'),
            'Parity':'CANONICAL LIVE/PAPER BUY/SELL decision path; 1-minute OHLC replay approximation',
            'Broker Charge Source':result.get('charge_source','Engine simulated charges'),
            'First Exit Event':result.get('first_exit_event','')
        }
        audit_base.update({'Signal Time':buy_dt,'Entry Price':buy_price,
                           'Status':'TRADE EXECUTED','Reason':result.get('exit_reason') or 'Canonical historical replay complete',
                           'Gross P&L ₹':gross,'Brokerage ₹':float(result.get('brokerage') or 0.0),'STT ₹':float(result.get('stt') or 0.0),'Exchange Charges ₹':float(result.get('exchange_turnover_charges') or 0.0),'SEBI Charges ₹':float(result.get('sebi_charges') or 0.0),'GST ₹':float(result.get('gst') or 0.0),'Stamp Duty ₹':float(result.get('stamp_duty') or 0.0),'Other Charges ₹':float(result.get('total_other_charges') or 0.0),'Total Charges ₹':charges,'Net P&L ₹':net})
        return trade, audit_base
    except Exception as exc:
        return None, {'Date':date,'Symbol':symbol,'Status':'CALCULATION ERROR',
                      'Reason':f'{type(exc).__name__}: {exc}'}

class App:
    def __init__(self,root,embedded=False,on_back=None,on_logic=None):
        self.root=root; self.embedded=embedded; self.on_back=on_back; self.on_logic=on_logic
        if not embedded:
            root.title('Jai Shree Ram — CSV P&L Calculation'); root.geometry('1060x760')
        self.cfgs=load_cfg()
        self.paths=[]
        self.output_path=None
        self.logic=tk.StringVar(value=next((k for k,v in self.cfgs.items() if v.get('enabled')), next(iter(self.cfgs),'Logic 1')))
        self.target=tk.StringVar(value=''); self.stop=tk.StringVar(value=''); self.cap=tk.StringVar(value='')
        self.orb=tk.BooleanVar(value=False); self.vwap=tk.BooleanVar(value=False); self.movement=tk.BooleanVar(value=False)
        self.volume=tk.BooleanVar(value=False); self.turnover=tk.BooleanVar(value=False); self.priceband=tk.BooleanVar(value=False)
        self.entry=tk.StringVar(value=''); self.exit=tk.StringVar(value='')
        self.status=tk.StringVar(value='Select one ZIP or multiple CSV files to begin.')
        self._build()
    def _build(self):
        # Breeze terminal palette: deep navy canvas, blue panels, orange accent,
        # and high-contrast text. Use namespaced ttk styles so the calculator
        # does not restyle the rest of the trading terminal.
        style=ttk.Style(self.root)
        try:
            style.theme_use('clam')
        except Exception:
            pass
        style.configure('BreezeCSV.TFrame', background='#081321')
        style.configure('BreezeCSV.TLabel', background='#081321', foreground='#EAF2FA',
                        font=('Segoe UI',9))
        style.configure('BreezeCSV.Title.TLabel', background='#081321', foreground='#F4F8FC',
                        font=('Segoe UI',18,'bold'))
        style.configure('BreezeCSV.Muted.TLabel', background='#081321', foreground='#91A9BD',
                        font=('Segoe UI',9))
        style.configure('BreezeCSV.TButton', background='#17324B', foreground='#F4F8FC',
                        bordercolor='#294963', lightcolor='#294963', darkcolor='#10243A',
                        padding=(10,7), font=('Segoe UI',9,'bold'))
        style.map('BreezeCSV.TButton',
                  background=[('active','#24516F'),('pressed','#10243A')],
                  foreground=[('disabled','#71869A')])
        outer=ttk.Frame(self.root,padding=12,style='BreezeCSV.TFrame'); outer.pack(fill='both',expand=True)
        try:
            self.root.configure(bg='#081321')
        except Exception:
            pass
        if self.embedded:
            nav=ttk.Frame(outer,style='BreezeCSV.TFrame'); nav.pack(fill='x',pady=(0,6))
            if self.on_back: ttk.Button(nav,text='← Back to Historical Trading',command=self.on_back,style='BreezeCSV.TButton').pack(side='left')
            if self.on_logic: ttk.Button(nav,text='Trading Logic — All Conditions',command=self.on_logic,style='BreezeCSV.TButton').pack(side='left',padx=(8,0))
        ttk.Label(outer,text='CSV Historical P&L Calculator',style='BreezeCSV.Title.TLabel').pack(anchor='w')
        tk.Frame(outer,bg='#FF8A1D',height=3,width=54).pack(anchor='w',pady=(5,8))
        ttk.Label(outer,text='LIVE-EQUIVALENT HISTORICAL REPLAY: uses each enabled saved Trading Logic BUY/SELL rules, per-logic position sizing and the ICICI intraday charge engine. Select the historical ZIP/CSV files and calculate. One-minute OHLC data cannot reproduce tick order, queue position, partial fills or exact live slippage. If target and stop are both touched within one candle, candle data cannot prove which happened first.',wraplength=1100,style='BreezeCSV.Muted.TLabel').pack(anchor='w',pady=(0,10))
        row=ttk.Frame(outer,style='BreezeCSV.TFrame'); row.pack(fill='x',pady=4)
        ttk.Button(row,text='Select ZIP / CSV files',command=self.pick,style='BreezeCSV.TButton').pack(side='left')
        self.filelabel=ttk.Label(row,text='No files selected',style='BreezeCSV.Muted.TLabel'); self.filelabel.pack(side='left',padx=10)
        self.logic_status=ttk.Label(outer,text='',style='BreezeCSV.Muted.TLabel'); self.logic_status.pack(anchor='w',pady=(5,2))
        ttk.Button(outer,text='Refresh saved Trading Logic',command=self.refresh_logic_summary,style='BreezeCSV.TButton').pack(anchor='w',pady=(2,6))
        action_row=ttk.Frame(outer,style='BreezeCSV.TFrame'); action_row.pack(fill='x',pady=8)
        ttk.Button(action_row,text='Calculate date-wise P&L',command=self.calculate,style='BreezeCSV.TButton').pack(side='left')
        ttk.Button(action_row,text='Open P&L Excel',command=self.open_pnl_excel,style='BreezeCSV.TButton').pack(side='left',padx=(8,0))
        ttk.Button(action_row,text='Clear All Logs',command=self.clear_logs,style='BreezeCSV.TButton').pack(side='left',padx=(8,0))
        ttk.Label(outer,textvariable=self.status,style='BreezeCSV.Muted.TLabel').pack(anchor='w',pady=4)
        self.log=tk.Text(outer,height=24,wrap='word',bg='#0D1B2A',fg='#EAF2FA',insertbackground='#FFFFFF',selectbackground='#24516F',relief='flat',highlightthickness=1,highlightbackground='#294963',highlightcolor='#FF8A1D',font=('Consolas',9)); self.log.pack(fill='both',expand=True,pady=4)
        ttk.Label(outer,text='Historical results replay every enabled saved logic through the canonical historical decision path. Charges are calculated from actual simulated BUY and SELL turnover using the saved ICICI plan. 1-minute OHLC replay is an approximation, not a broker contract note. If target and stop both fall inside one candle, the sequence is unknowable from OHLC alone.',wraplength=1100,style='BreezeCSV.Muted.TLabel').pack(anchor='w')
        self.refresh_logic_summary()
    def refresh_logic_summary(self):
        # Always read the persisted configuration written by the main Trading Logic screen.
        self.cfgs=load_cfg()
        enabled=[(f'Logic {i}', self.cfgs[f'Logic {i}']) for i in range(1, 9) if isinstance(self.cfgs.get(f'Logic {i}'), dict) and bool(self.cfgs[f'Logic {i}'].get('enabled',False))]
        if enabled:
            details=[]
            for k,v in enabled:
                cap = v.get('historical_shares',0) or v.get('max_orders',0) or 'unlimited'
                conditions = [
                    f"{k}: {v.get('name',k)}",
                    f"Enabled={bool(v.get('enabled',False))}",
                    f"Direction={v.get('open_direction','BOTH')}",
                    f"Movement={v.get('open_movement',0)}%",
                    f"Movement-only={bool(v.get('movement_only_mode',False))}",
                    f"Min price=₹{v.get('minimum_price',0)}",
                    f"Condition rank sequence={format_rank_sequence(v)}",
                    f"Price band={bool(v.get('use_price_band',False))} (big ₹{v.get('price_big',0)}, small ₹{v.get('price_small',0)})",
                    f"Turnover={v.get('turnover_cr',0)} Cr",
                    f"Volume={v.get('volume',0)}",
                    f"Entry={v.get('buy_entry_time','')}",
                    f"Last entry={v.get('buy_exit_time','')}",
                    f"ORB15={bool(v.get('require_15m_orb',False))}",
                    f"VWAP={bool(v.get('smart_require_vwap',False))}",
                    f"Target={v.get('profit_target',0)}%",
                    f"Stop={v.get('initial_stop',0)}%",
                    f"Trailing={v.get('trailing_stop',0)}% (activation {v.get('activation',0)}%)",
                    f"Buy amount=₹{v.get('buy_amount',0)}",
                    f"Daily cap={cap}",
                    f"MA filter={bool(v.get('ma_filter_enabled',False))} [200={bool(v.get('ma_filter_200',False))}, 100={bool(v.get('ma_filter_100',False))}, 20={bool(v.get('ma_filter_20',False))}, 5={bool(v.get('ma_filter_5',False))}, cross={bool(v.get('ma_filter_cross_5',False))}]",
                    f"52W high/low={bool(v.get('require_52w_high_breakout',False))}/{bool(v.get('require_52w_low_breakdown',False))}",
                    f"Weak-move exit={bool(v.get('weak_move_exit_enabled',False))}",
                    f"SMART mode={bool(v.get('smart_mode',False))}, score={v.get('smart_min_score',0)}, vol ratio={v.get('smart_min_volume_ratio',0)}, momentum={v.get('smart_momentum_pct',0)}%/{v.get('smart_momentum_lookback_minutes',0)}m"
                ]
                details.append(' | '.join(conditions))
            self.logic_status.config(text='Enabled Trading Logic(s) — live from saved Trading Logic: ' + '\n\n'.join(details), wraplength=1320, justify='left')
        else:
            self.logic_status.config(text='No Trading Logic is enabled. Enable at least one strategy on the Trading Logic screen and Save.')
    def on_logic_changed(self, _event=None):
        self.apply_logic_defaults()
    def apply_logic_defaults(self):
        cfg=self.cfgs.get(self.logic.get(),{})
        # All initial values are populated from the selected GUI logic configuration.
        self.target.set(str(cfg.get('profit_target',0) or 0))
        self.stop.set(str(cfg.get('initial_stop',0) or 0))
        hist_cap=int(cfg.get('historical_shares',0) or 0)
        order_cap=int(cfg.get('max_orders',0) or 0)
        _positive_caps=[v for v in (hist_cap,order_cap) if v>0]
        cap=min(_positive_caps) if _positive_caps else 999999
        self.cap.set(str(cap))
        self.orb.set(bool(cfg.get('require_15m_orb',False)))
        self.vwap.set(bool(cfg.get('smart_require_vwap',False)))
        self.movement.set(float(cfg.get('open_movement',0) or 0)>0)
        self.volume.set(float(cfg.get('volume',0) or 0)>0)
        self.turnover.set(float(cfg.get('turnover_cr',0) or 0)>0)
        self.priceband.set(bool(cfg.get('use_price_band',False)))
        self.entry_display.config(text=parse_time(cfg.get('buy_entry_time'),'09:15'))
        self.exit_display.config(text=parse_time(cfg.get('buy_exit_time'),'11:20'))
        # Show every configured strategy field so selecting Logic 1..8 exposes the real settings, not just a few checkboxes.
        labels={
          'name':'Strategy name','enabled':'Enabled','open_direction':'Opening direction','open_movement':'Movement %',
          'movement_only_mode':'Movement-only entry','price_big':'Maximum price','price_small':'Minimum price','use_price_band':'Price band filter',
          'volume':'Minimum volume','turnover_cr':'Minimum turnover (Cr)','buy_amount':'Investment per trade (₹)','buy_entry_time':'First entry time','buy_exit_time':'Last entry time',
          'profit_target':'Profit target %','initial_stop':'Initial stop %','trailing_stop':'Trailing stop %','activation':'Trailing activation %',
          'historical_shares':'Historical shares cap','max_orders':'Max orders/day','require_15m_orb':'15-minute ORB required',
          'smart_min_score':'SMART minimum score','smart_min_volume_ratio':'Minimum volume ratio','smart_require_vwap':'Require VWAP','smart_require_breakout':'Require breakout',
          'smart_require_market_up':'Require market up','smart_momentum_pct':'Momentum threshold %','smart_momentum_lookback_minutes':'Momentum lookback minutes',
          'smart_rank_top_pct':'Top momentum rank %','require_52w_high_breakout':'52-week high breakout','require_52w_low_breakdown':'52-week low breakdown',
          'ma_filter_enabled':'Moving-average filter enabled','ma_filter_200':'MA 200','ma_filter_100':'MA 100','ma_filter_20':'MA 20','ma_filter_5':'MA 5','ma_filter_cross_5':'MA 5 cross',
          'weak_move_exit_enabled':'Weak-move exit','weak_move_high_threshold':'Weak-move high threshold %','weak_move_stop':'Weak-move stop %','shared_universe_mode':'Universe mode'}
        lines=[f"{labels.get(k,k.replace('_',' ').title())}: {v}" for k,v in cfg.items()]
        self.conditions.configure(state='normal'); self.conditions.delete('1.0','end'); self.conditions.insert('1.0','  |  '.join(lines)); self.conditions.configure(state='disabled')
    def on_logic_changed(self, event=None):
        """Populate controls from the selected GUI logic; no strategy defaults are silently substituted."""
        cfg=self.cfgs.get(self.logic.get(), {})
        if not cfg: return
        self.target.set(str(cfg.get('profit_target', 0) or 0))
        self.stop.set(str(cfg.get('initial_stop', 0) or 0))
        cap=cfg.get('historical_shares', 0)
        if not isinstance(cap,(int,float)) or cap <= 0: cap=cfg.get('max_orders',0)
        self.cap.set(str(int(cap)) if isinstance(cap,(int,float)) and cap>0 else '999999')
        self.orb.set(bool(cfg.get('require_15m_orb', False)))
        self.vwap.set(bool(cfg.get('smart_require_vwap', False)))
        self.movement.set(bool(cfg.get('movement_only_mode', False)))
        self.volume.set(bool(float(cfg.get('volume',0) or 0)>0))
        self.turnover.set(bool(float(cfg.get('turnover_cr',0) or 0)>0))
        self.priceband.set(bool(cfg.get('use_price_band',False)))

    def clear_logs(self):
        self.log.delete('1.0','end')
        self.status.set('Logs cleared. Select files and calculate date-wise P&L when ready.')

    def open_pnl_excel(self):
        candidates=[]
        if self.output_path:
            candidates.append(Path(self.output_path))
        if self.paths:
            candidates.append(Path(self.paths[0]).expanduser().resolve().parent/'Breeze_CSV_PnL_Results.xlsx')
        path=next((p for p in candidates if p.exists()),None)
        if path is None:
            messagebox.showinfo('P&L Excel not found','Calculate date-wise P&L first. The Excel file will be saved beside the selected input files.')
            return
        try:
            if sys.platform.startswith('win'):
                os.startfile(str(path))
            elif sys.platform == 'darwin':
                subprocess.Popen(['open',str(path)])
            else:
                subprocess.Popen(['xdg-open',str(path)])
            self.status.set(f'Opened P&L Excel: {path}')
        except Exception as e:
            messagebox.showerror('Could not open Excel',f'File saved at:\n{path}\n\n{e}')

    def pick(self):
        p=filedialog.askopenfilenames(title='Select ZIP or CSV files',filetypes=[('ZIP and CSV','*.zip *.csv'),('ZIP files','*.zip'),('CSV files','*.csv')])
        if p:
            self.paths=list(p); self.filelabel.config(text=f'{len(p)} file(s) selected')
    def logmsg(self,s):
        self.log.insert('end',s+'\n'); self.log.see('end'); self.root.update_idletasks()
    def calculate(self):
        if not self.paths: messagebox.showerror('No files','Select ZIP or CSV files first.'); return
        # Re-read the exact saved settings every run so edits in Trading Logic drive Excel immediately.
        self.cfgs=load_cfg()
        logics=[(f'Logic {i}', self.cfgs[f'Logic {i}']) for i in range(1, 9) if isinstance(self.cfgs.get(f'Logic {i}'), dict) and bool(self.cfgs[f'Logic {i}'].get('enabled',False))]
        if not logics:
            messagebox.showerror('No enabled strategies','Open Trading Logic, enable at least one logic, click Save, then calculate again.'); return
        bad=[]
        for logic_name,cfg in logics:
            try:
                target=float(cfg.get('profit_target',0)); stop=float(cfg.get('initial_stop',0))
                if target<=0 or stop<=0: bad.append(f'{logic_name}: target and stop must be positive')
            except Exception: bad.append(f'{logic_name}: invalid target/stop')
        if bad:
            messagebox.showerror('Invalid Trading Logic','\n'.join(bad)); return
        files,errors=files_from_paths(self.paths); grouped={}; prev_close_for_day={}
        self.log.delete('1.0','end'); self.status.set(f'Reading {len(files)} CSV files...')
        self.root.update_idletasks()
        for i,(name,raw) in enumerate(files,1):
            try:
                df,symbol=read_frame(raw,name)
                # Accept both one-symbol-per-file Breeze exports and combined CSVs
                # carrying a Symbol/Stock_Code column; never merge different tickers.
                if 'stock_code' in df.columns and df['stock_code'].nunique(dropna=True)>1:
                    symbol_frames=[(str(sym).strip().upper(),part.copy()) for sym,part in df.groupby('stock_code',sort=False) if str(sym).strip()]
                else:
                    symbol_frames=[(symbol,df)]
                for symbol,symbol_df in symbol_frames:
                    symbol_df=symbol_df.copy()
                    symbol_df['_date_key']=symbol_df.datetime.dt.strftime('%Y-%m-%d')
                    all_dates=sorted(symbol_df['_date_key'].dropna().unique())
                    for d in all_dates:
                        key=(d,symbol)
                        prior=symbol_df[symbol_df['_date_key']<d].sort_values('datetime')
                        pc=float(prior.iloc[-1]['close']) if not prior.empty else None
                        day_frame=symbol_df[symbol_df['_date_key']==d].drop(columns=['_date_key']).copy()
                        # Some monthly 1-minute exports include the previous session close
                        # on every row. Use it for the first date when no earlier candle is
                        # present in the uploaded archive.
                        if pc is None and 'previous_close' in day_frame.columns:
                            vals=pd.to_numeric(day_frame['previous_close'],errors='coerce')
                            vals=vals[vals>0]
                            if not vals.empty: pc=float(vals.iloc[0])
                        if key not in grouped or ('historical' in name.lower() and 'historical' not in grouped[key][0].lower()):
                            grouped[key]=(name,day_frame)
                            prev_close_for_day[key]=pc
            except Exception as e: errors.append((name,str(e)))
            if i%100==0: self.status.set(f'Parsing {i}/{len(files)} files...'); self.root.update_idletasks()
        # Carry previous close across separate per-day CSVs for each symbol.
        by_symbol={}
        for (day,sym),(name,daydf) in grouped.items():
            by_symbol.setdefault(sym,[]).append((day,float(daydf.sort_values('datetime').iloc[-1]['close'])))
        for sym,rows in by_symbol.items():
            rows.sort(key=lambda z:z[0]); prior_close=None
            for day,_close in rows:
                if prior_close is not None and prev_close_for_day.get((day,sym)) is None: prev_close_for_day[(day,sym)]=prior_close
                prior_close=_close
        # Index once by date and cache daily OHLC metadata. This avoids rescanning
        # every symbol for every date/logic and avoids reopening source CSVs per trade.
        grouped_by_date={}
        day_stats={}
        for (day,sym),(source_name,daydf) in grouped.items():
            grouped_by_date.setdefault(day,[]).append((sym,source_name,daydf))
            if daydf is not None and not daydf.empty:
                _sorted=daydf.sort_values('datetime')
                _hi_idx=_sorted['high'].idxmax(); _lo_idx=_sorted['low'].idxmin()
                day_stats[(day,sym)]={
                    'open':float(_sorted.iloc[0]['open']),
                    'high':float(_sorted['high'].max()),
                    'high_time':_sorted.loc[_hi_idx,'datetime'].to_pydatetime(),
                    'low':float(_sorted['low'].min()),
                    'low_time':_sorted.loc[_lo_idx,'datetime'].to_pydatetime(),
                }
            else:
                day_stats[(day,sym)]={}
        for day_rows in grouped_by_date.values():
            day_rows.sort(key=lambda item:item[0])
        trades=[]; audit=[]; summaries=[]; dates=sorted(grouped_by_date)
        for logic_name,cfg in logics:
            # Rank details are included in the Excel output; keep the screen log concise.
            target=float(cfg.get('profit_target',0)); stop=float(cfg.get('initial_stop',0))
            hist_cap=int(cfg.get('historical_shares',0) or 0); order_cap=int(cfg.get('max_orders',0) or 0)
            _positive_caps=[v for v in (hist_cap,order_cap) if v>0]
            cap=max(_positive_caps) if _positive_caps else 999999
            logic_trades=[]
            for d in dates:
                cand=[]
                date_rows=grouped_by_date.get(d,())
                for sym,name,df in date_rows:
                    trade,aud=calc_symbol(df,d,sym,cfg,target,stop,previous_close=prev_close_for_day.get((d,sym)),logic_name=logic_name)
                    if aud:
                        aud['Trading Logic']=logic_name; aud['Source File']=name; audit.append(aud)
                    if trade:
                        trade['Trading Logic']=logic_name; trade['Source File']=name; cand.append(trade)
                cand.sort(key=lambda x:(x['Signal Time'],x['Symbol']))
                sel=cand[:cap]; logic_trades.extend(sel); trades.extend(sel)
                for tr in cand[cap:]:
                    audit.append({'Date':d,'Symbol':tr['Symbol'],'Trading Logic':logic_name,'Source File':tr['Source File'],'Status':'QUALIFIED - NOT SELECTED','Reason':f'{logic_name} daily cap {cap}; later signal'})
                _gross_profit=sum(max(0.0,float(x.get('Gross P&L ₹',x.get('P&L ₹',0)) or 0)) for x in sel)
                _gross_loss=sum(abs(min(0.0,float(x.get('Gross P&L ₹',x.get('P&L ₹',0)) or 0))) for x in sel)
                _net_pnl=sum(float(x.get('P&L ₹',0) or 0) for x in sel)
                summaries.append({'Date':d,'Trading Logic':logic_name,'Symbols Scanned':len(date_rows),
                                  'Eligible Signals':len(cand),'Qualified Signals':len(cand),'Selected Trades':len(sel),
                                  'Total Trades':len(sel),'Profit Trades':sum(float(x.get('P&L ₹',0) or 0)>0 for x in sel),
                                  'Loss Trades':sum(float(x.get('P&L ₹',0) or 0)<0 for x in sel),
                                  'Profit Targets':sum(x.get('First Exit Event')=='PROFIT_TARGET' for x in sel),
                                  'Stop Losses':sum(x.get('First Exit Event') in ('INITIAL_STOP','TRAILING_STOP','WEAK_MOVE') for x in sel),
                                  'Market-Close Exits':sum(x.get('First Exit Event')=='15:25_SQUARE_OFF' for x in sel),
                                  'Winners':sum(x['P&L %']>0 for x in sel),'Losers':sum(x['P&L %']<0 for x in sel),
                                  'Gross Profit ₹':_gross_profit,'Gross Loss ₹':_gross_loss,
                                  'Net P&L % (weighted)':(_net_pnl / sum(float(x.get('Capital Used ₹',0) or 0) for x in sel) * 100.0) if sum(float(x.get('Capital Used ₹',0) or 0) for x in sel)>0 else 0.0,
                                  'Capital Used ₹':sum(x.get('Capital Used ₹',0) for x in sel),
                                  'Net P&L ₹':_net_pnl,'Total Charges ₹':sum(float(x.get('Total Charges ₹',0) or 0) for x in sel)})
        # Keep exactly one row per date + trading logic. Duplicate rows from
        # repeated scan callbacks are merged before rendering/exporting.
        _logic_date_rows={}
        for _row in summaries:
            _key=(str(_row.get('Date','')),_row.get('Trading Logic',''))
            if _key not in _logic_date_rows:
                _logic_date_rows[_key]=dict(_row)
            else:
                _existing=_logic_date_rows[_key]
                for _field,_value in _row.items():
                    if _field not in ('Date','Trading Logic') and isinstance(_value,(int,float)):
                        _existing[_field]=float(_existing.get(_field,0) or 0)+float(_value or 0)
        summaries=[_logic_date_rows[_key] for _key in sorted(_logic_date_rows)]
        if not trades: self.status.set('Completed — no trades qualified. Check enabled conditions, ranking order and source data.')
        else: self.status.set(f'Completed: {len(trades)} trades across {len(dates)} dates using {len(logics)} enabled logic(s).')
        # Render the same date-wise and grand totals directly in the GUI log.
        self.logmsg('')
        self.logmsg('='*112)
        self.logmsg('DATE-WISE TRADING SUMMARY')
        self.logmsg('='*112)
        self.logmsg(f"{'DATE':<12} {'LOGIC':<18} {'ELIGIBLE':>9} {'TRADES':>8} {'PROFIT':>8} {'LOSS':>8} {'GROSS +₹':>14} {'GROSS -₹':>14} {'NET ₹':>14}")
        self.logmsg('-'*112)
        for _item in summaries:
            self.logmsg(f"{_item['Date']:<12} {_item['Trading Logic']:<18} {int(_item.get('Eligible Signals',0)):>9} {int(_item.get('Total Trades',0)):>8} {int(_item.get('Profit Trades',0)):>8} {int(_item.get('Loss Trades',0)):>8} {float(_item.get('Gross Profit ₹',0)):>14,.2f} {float(_item.get('Gross Loss ₹',0)):>14,.2f} {float(_item.get('Net P&L ₹',0)):>14,.2f}")
        _eligible_total=sum(int(x.get('Eligible Signals',0) or 0) for x in summaries)
        _profit_total=sum(float(x.get('Gross Profit ₹',0) or 0) for x in summaries)
        _loss_total=sum(float(x.get('Gross Loss ₹',0) or 0) for x in summaries)
        _net_total=sum(float(x.get('Net P&L ₹',0) or 0) for x in summaries)
        self.logmsg('-'*112)
        self.logmsg(f"{'ALL DATES':<12} {'GRAND TOTAL':<18} {_eligible_total:>9} {len(trades):>8} {sum(int(x.get('Profit Trades',0) or 0) for x in summaries):>8} {sum(int(x.get('Loss Trades',0) or 0) for x in summaries):>8} {_profit_total:>14,.2f} {_loss_total:>14,.2f} {_net_total:>14,.2f}")
        self.logmsg('='*112)
        # Save workbook as an optional export; the screen summary above is the primary result.
        # If multiple inputs are selected from different folders, the first input
        # determines the output folder. Existing output is replaced on rerun.
        try:
            output_dir=Path(self.paths[0]).expanduser().resolve().parent
            out=str(output_dir / 'Breeze_CSV_PnL_Results.xlsx')
        except Exception as e:
            messagebox.showerror('Output folder error', f'Could not resolve the selected input folder:\n{e}')
            return
        try:
            from openpyxl import Workbook
            from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
            from openpyxl.utils import get_column_letter

            # Match the user's Book1.xlsx layout. Only enabled conditions receive
            # rank columns; every condition contributes Name / Value / Yes-No.
            base_headers = [
                'Date','Symbol','Company Name','Previous close','Day open','Day high','Day high time',
                'Day Low','Day low time','Signal Time','Buy Date & Time','Buy Price','Buy Quantity',
                'Capital Used ₹','Profit Target Price','Stop Loss Price','Sell Date & Time','Sell Price',
                'Exit Reason','P&L %','P&L ₹','Condition Evaluation Order','Gross P&L ₹','Brokerage ₹','STT ₹','Exchange Charges ₹','SEBI Charges ₹','GST ₹','Stamp Duty ₹','Other Charges ₹','Total Charges ₹','Data Source / Parity','Broker Charge Source','Charge Plan','Trading Logic'
            ]
            condition_labels = {
                'minimum_price':'Share price','entry_time':'Entry time','vwap':'VWAP','orb15':'15-minute ORB',
                'rvol':'RVOL','movement':'Movement from previous close','investment':'Investment per trade',
                'profit_target':'Profit target','stop_loss':'Stop-loss','max_trades':'Maximum trades per day',
                'open_direction':'Open direction','price_big':'Price big','price_small':'Price small',
                'turnover':'Turnover','volume':'Volume'
            }
            def active_condition_keys(cfg):
                # Mirror the actual enabled GUI gates/values; don't add empty columns.
                active = {
                    'minimum_price': float(cfg.get('minimum_price',0) or 0) > 0,
                    'entry_time': bool(cfg.get('buy_entry_time')) and bool(cfg.get('buy_exit_time')),
                    'vwap': bool(cfg.get('smart_require_vwap',False)),
                    'orb15': bool(cfg.get('require_15m_orb',False)),
                    'rvol': float(cfg.get('smart_min_volume_ratio',0) or 0) > 0,
                    'movement': float(cfg.get('open_movement',0) or 0) > 0,
                    'investment': float(cfg.get('buy_amount',0) or 0) > 0,
                    'profit_target': float(cfg.get('profit_target',0) or 0) > 0,
                    'stop_loss': float(cfg.get('initial_stop',0) or 0) > 0,
                    'max_trades': float(cfg.get('historical_shares',0) or cfg.get('max_orders',0) or 0) > 0,
                    'open_direction': str(cfg.get('open_direction','BOTH')).upper() in ('UP','DOWN','BOTH'),
                    'price_big': bool(cfg.get('use_price_band',False)) and float(cfg.get('price_big',0) or 0) > 0,
                    'price_small': bool(cfg.get('use_price_band',False)) and float(cfg.get('price_small',0) or 0) > 0,
                    'turnover': float(cfg.get('turnover_cr',0) or 0) > 0,
                    'volume': float(cfg.get('volume',0) or 0) > 0,
                }
                return [k for k in condition_rank_sequence(cfg) if active.get(k,False)]
            def cond_value(key, cfg):
                mapping = {
                    'minimum_price': cfg.get('minimum_price',0),
                    'entry_time': f"{parse_time(cfg.get('buy_entry_time'),'09:15')}–{parse_time(cfg.get('buy_exit_time'),'14:00')}",
                    'vwap': 'Price above VWAP',
                    'orb15': '15-minute breakout',
                    'rvol': float(cfg.get('smart_min_volume_ratio',0) or 0),
                    'movement': float(cfg.get('open_movement',0) or 0),
                    'investment': cfg.get('buy_amount',0),
                    'profit_target': float(cfg.get('profit_target',0) or 0),
                    'stop_loss': float(cfg.get('initial_stop',0) or 0),
                    'max_trades': cfg.get('historical_shares',0) or cfg.get('max_orders',0) or 'Unlimited',
                    'open_direction': cfg.get('open_direction','BOTH'),
                    'price_big': cfg.get('price_big',0),
                    'price_small': cfg.get('price_small',0),
                    'turnover': cfg.get('turnover_cr',0),
                    'volume': cfg.get('volume',0)
                }
                return mapping.get(key,'')
            cfg_by_logic=dict(logics)
            wb=Workbook()
            ws=wb.active
            ws.title='P&L Output'
            enabled_by_logic = {name: active_condition_keys(cfg) for name,cfg in logics}
            # A single dynamic set of condition columns is based on the union of
            # enabled conditions across the selected strategies. Each trade row
            # fills only conditions enabled for its own strategy.
            ordered_conditions=[]
            for name,cfg in logics:
                for key in enabled_by_logic[name]:
                    if key not in ordered_conditions: ordered_conditions.append(key)
            headers=base_headers[:]
            for rank,key in enumerate(ordered_conditions,1):
                headers.extend([f'Rank {rank} Conditions name',f'Rank {rank} Conditions',f'Rank {rank} Conditions Yes/No'])
            ws.append(headers)
            # Date-wise summary with one row per logic/date plus one grand total.
            summary_ws=wb.create_sheet('Date-wise Summary')
            summary_headers=['Date','Trading Logic','Symbols Scanned','Eligible Signals','Total Trades',
                             'Profit Trades','Loss Trades','Profit Targets','Stop Losses',
                             'Market-Close Exits','Gross Profit ₹','Gross Loss ₹','Total Charges ₹','Net P&L ₹',
                             'Net P&L % (weighted)','Capital Used ₹']
            summary_ws.append(summary_headers)
            for item in summaries:
                summary_ws.append([item.get(h,0 if h not in ('Date','Trading Logic') else '') for h in summary_headers])
            def _sum_summary(rows,label,day):
                fields=('Symbols Scanned','Eligible Signals','Total Trades','Profit Trades','Loss Trades',
                        'Profit Targets','Stop Losses','Market-Close Exits','Gross Profit ₹','Gross Loss ₹',
                        'Total Charges ₹','Net P&L ₹','Net P&L % (weighted)','Capital Used ₹')
                result={'Date':day,'Trading Logic':label}
                for field in fields:
                    result[field]=sum(float(r.get(field,0) or 0) for r in rows)
                cap_total=result.get('Capital Used ₹',0.0)
                result['Net P&L % (weighted)']=(result.get('Net P&L ₹',0.0)/cap_total*100.0) if cap_total else 0.0
                return result
            _grand_total=_sum_summary(summaries,'GRAND TOTAL','ALL DATES')
            summary_ws.append([_grand_total.get(h,0 if h not in ('Date','Trading Logic') else '') for h in summary_headers])
            summary_ws.freeze_panes='A2'
            summary_ws.auto_filter.ref=summary_ws.dimensions
            summary_ws.sheet_view.showGridLines=False
            for c in summary_ws[1]:
                c.fill=PatternFill('solid',fgColor='12344A'); c.font=Font(color='FFFFFF',bold=True)
                c.alignment=Alignment(wrap_text=True,vertical='center')
            summary_ws.row_dimensions[1].height=34
            for col in range(1,summary_ws.max_column+1):
                summary_ws.column_dimensions[get_column_letter(col)].width=22 if col in (2,11,12,13,14,15,16) else 17
            for rr in range(2,summary_ws.max_row+1):
                for cc in (11,12,13,14,15,16): summary_ws.cell(rr,cc).number_format='#,##0.00'
                if str(summary_ws.cell(rr,2).value).startswith(('ALL LOGICS','GRAND TOTAL')):
                    for cc in range(1,summary_ws.max_column+1):
                        summary_ws.cell(rr,cc).font=Font(bold=True,color='FF8A1D')
            audit_ws=wb.create_sheet('Signal Audit')
            audit_headers=['Date','Symbol','Trading Logic','Source File','Status','Reason','Signal Time','Entry Price','Gross P&L ₹','Brokerage ₹','STT ₹','Exchange Charges ₹','SEBI Charges ₹','GST ₹','Stamp Duty ₹','Other Charges ₹','Total Charges ₹','Net P&L ₹','Data Source','Parity']
            audit_ws.append(audit_headers)
            for item in audit:
                audit_ws.append([item.get(h,'') for h in audit_headers])
            audit_ws.freeze_panes='A2'; audit_ws.auto_filter.ref=audit_ws.dimensions
            audit_ws.sheet_view.showGridLines=False
            for c in audit_ws[1]:
                c.fill=PatternFill('solid',fgColor='12344A'); c.font=Font(color='FFFFFF',bold=True)
                c.alignment=Alignment(wrap_text=True,vertical='center')
            audit_ws.row_dimensions[1].height=32
            for col in range(1,audit_ws.max_column+1):
                audit_ws.column_dimensions[get_column_letter(col)].width=28 if col in (6,12,13) else 19
            for rr in range(2,audit_ws.max_row+1):
                for cc in (9,10,11): audit_ws.cell(rr,cc).number_format='#,##0.00'

            settings=wb.create_sheet('Strategy Settings')
            settings.append(['Logic','Condition key','Condition value','Enabled'])
            settings_rows={}
            for logic_name,cfg in logics:
                for key in ordered_conditions:
                    val=cond_value(key,cfg)
                    settings.append([logic_name,key,str(val),key in enabled_by_logic[logic_name]])
                    settings_rows[(logic_name,key)] = settings.max_row
            settings.sheet_state='hidden'
            for tr in trades:
                logic_name=tr.get('Trading Logic','')
                cfg=cfg_by_logic.get(logic_name,{})
                active_keys=enabled_by_logic.get(logic_name,[])
                date=tr.get('Date',''); symbol=tr.get('Symbol','')
                src_name=tr.get('Source File','')
                _stats=day_stats.get((date,symbol),{})
                prev_close=prev_close_for_day.get((date,symbol))
                day_open=_stats.get('open'); day_high=_stats.get('high')
                day_low=_stats.get('low'); day_high_time=_stats.get('high_time')
                day_low_time=_stats.get('low_time')
                row=[
                    date,symbol,cfg.get('company_name',''),prev_close,day_open,day_high,day_high_time,
                    day_low,day_low_time,tr.get('Signal Time'),tr.get('Buy Date & Time'),tr.get('Buy Price'),
                    tr.get('Buy Quantity'),tr.get('Capital Used ₹'),tr.get('Profit Target Price'),tr.get('Stop Loss Price'),tr.get('Sell Date & Time'),tr.get('Sell Price'),tr.get('Exit Reason'),
                    tr.get('P&L %'),tr.get('P&L ₹'),tr.get('Condition Evaluation Order',''),tr.get('Gross P&L ₹'),tr.get('Brokerage ₹'),tr.get('STT ₹'),tr.get('Exchange Charges ₹'),tr.get('SEBI Charges ₹'),tr.get('GST ₹'),tr.get('Stamp Duty ₹'),tr.get('Other Charges ₹'),tr.get('Total Charges ₹'),tr.get('Parity'),tr.get('Broker Charge Source'),tr.get('Charge Plan'),logic_name
                ]
                for key in ordered_conditions:
                    if key in active_keys:
                        row.extend([condition_labels.get(key,key),None,None])
                    else:
                        row.extend(['','',''])
                ws.append(row)
                rr=ws.max_row
                buy_col=12; qty_col=13; capital_col=14; target_col=15; stop_col=16; sell_col=18; pct_col=20; pnl_col=21
                # Formula references point to a hidden settings sheet included in this workbook,
                # so no missing external workbook/sheet reference is needed.
                invest_ref = repr(float(cfg.get('buy_amount',0) or 0))
                # Values in these columns come directly from the canonical replay result.
                # Do not replace them with formulas: that would discard actual engine sizing,
                # simulated charges, and net P&L.
                # Condition values are formula-linked to the hidden settings sheet;
                # Yes/No fields are formula-driven. Entry conditions which do not
                # have raw metric columns in the user's template are marked YES
                # because only fully qualified historical signals enter this sheet.
                for idx,key in enumerate(ordered_conditions):
                    name_col=len(base_headers)+idx*3+1
                    value_col=name_col+1
                    yes_col=name_col+2
                    if key not in active_keys: continue
                    sr=settings_rows.get((logic_name,key))
                    if sr:
                        ws.cell(rr,value_col, f"='Strategy Settings'!$C${sr}")
                    if key=='minimum_price':
                        cond_formula=f'=IF(L{rr}>=VALUE({get_column_letter(value_col)}{rr}),"YES","NO")'
                    elif key=='entry_time':
                        cond_formula=f'=IF(AND(MOD(K{rr},1)>=TIMEVALUE(LEFT({get_column_letter(value_col)}{rr},5)),MOD(K{rr},1)<=TIMEVALUE(RIGHT({get_column_letter(value_col)}{rr},5))),"YES","NO")'
                    elif key=='investment':
                        cond_formula=f'=IF(N{rr}<={invest_ref},"YES","NO")'
                    elif key=='profit_target':
                        cond_formula=f'=IF(O{rr}>L{rr},"YES","NO")'
                    elif key=='stop_loss':
                        cond_formula=f'=IF(P{rr}<L{rr},"YES","NO")'
                    elif key=='max_trades':
                        _caps=[int(float(v or 0)) for v in (cfg.get('historical_shares',0),cfg.get('max_orders',0)) if int(float(v or 0))>0]
                        cap=max(_caps) if _caps else 999999
                        cond_formula=f'=IF(COUNTIFS($A$2:A{rr},A{rr},$AA$2:AA{rr},AA{rr})<={cap},"YES","NO")'
                    elif key=='open_direction':
                        if str(cfg.get('open_direction','BOTH')).upper()=='UP':
                            cond_formula=f'=IF(E{rr}>D{rr},"YES","NO")'
                        elif str(cfg.get('open_direction','BOTH')).upper()=='DOWN':
                            cond_formula=f'=IF(E{rr}<D{rr},"YES","NO")'
                        else: cond_formula=f'=IF(B{rr}<>"","YES","NO")'
                    elif key=='price_big':
                        cond_formula=f'=IF(E{rr}<={get_column_letter(value_col)}{rr},"YES","NO")'
                    elif key=='price_small':
                        cond_formula=f'=IF(E{rr}>={get_column_letter(value_col)}{rr},"YES","NO")'
                    elif key=='movement':
                        cond_formula=f'=IFERROR(IF((L{rr}/D{rr}-1)*100>=VALUE(SUBSTITUTE({get_column_letter(value_col)}{rr},"%","")),"YES","NO"),"N/A")'
                    else:
                        cond_formula=f'=IF(B{rr}<>"","YES","NO")'
                    ws.cell(rr,yes_col,cond_formula)
            ws.freeze_panes='A2'
            ws.auto_filter.ref=ws.dimensions
            ws.sheet_view.showGridLines=False
            header_fill=PatternFill('solid',fgColor='12344A')
            header_font=Font(color='FFFFFF',bold=True)
            thin=Side(style='thin',color='D7E2EA')
            for c in ws[1]:
                c.fill=header_fill; c.font=header_font; c.alignment=Alignment(wrap_text=True,vertical='center')
                c.border=Border(bottom=thin)
            ws.row_dimensions[1].height=34
            for col in range(1,ws.max_column+1):
                letter=get_column_letter(col)
                ws.column_dimensions[letter].width=16 if col>22 else (30 if col in (2,3,19,22) else 22 if col >= 23 else 15)
            for rr in range(2,ws.max_row+1):
                for cc in (12,13,14,15,16,18,20,21):
                    ws.cell(rr,cc).number_format='#,##0.00'
                for cc in (7,9,10,11,17):
                    ws.cell(rr,cc).number_format='yyyy-mm-dd hh:mm:ss'
            wb.calculation.fullCalcOnLoad=True
            wb.calculation.forceFullCalc=True
            wb.calculation.calcMode='auto'
            wb.save(out)
            self.output_path=out
            # Excel recalculates formulas when the workbook opens. Avoid a blocking
            # headless office conversion here; it can add minutes to large runs.
        except Exception as e:
            messagebox.showerror('Excel export failed', f'Could not save the Excel workbook to:\n{out}\n\n{e}')
            self.logmsg('Optional Excel export failed (screen results are available): '+str(e))
            self.status.set('Calculation completed — screen results are available; optional Excel export failed.')
            return
        if errors:
            self.logmsg(f'File errors: {len(errors)} file(s) could not be read.')
        self.logmsg(f'P&L Excel saved: {out}')
        self.logmsg(f'Completed | Logic(s): {len(logics)} | Dates: {len(dates)} | Trades selected: {len(trades)}')
        self.status.set('Completed — individual logic results and one grand total are displayed; Excel is optional.')

if __name__=='__main__':
    root=tk.Tk(); App(root); root.mainloop()
