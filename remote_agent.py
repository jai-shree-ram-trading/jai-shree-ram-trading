"""Breeze Remote Agent - mobile control for Historical, Paper and Live modes.

The laptop remains the only trading engine. The phone sends authenticated commands
through a private VPN such as Tailscale. Breeze credentials remain on Windows.
"""
from __future__ import annotations
import base64, ctypes, json, os, subprocess, threading, time, socket, hmac
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, unquote, parse_qs

ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / 'Config' / 'remote_config.json'
STATE = ROOT / 'remote_state.json'
SCRIPT = ROOT / 'BreezeTrading.py'
STOP_FLAG = ROOT / 'remote_stop.flag'
ORDER_FEED = ROOT / 'Data' / 'Mobile_Order_Feed.jsonl'
ORDER_STATE = ROOT / 'Data' / 'Mobile_Order_State.json'
REMOTE_COMMAND = ROOT / 'remote_command.json'
APP_UPDATE_META_CANDIDATES = [
    ROOT / 'AndroidApp' / 'app_update.json',
    ROOT / 'app_update.json',
]
MOBILE_PAGE = '<!doctype html>\n<html><head>\n<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">\n<meta name="theme-color" content="#0A1220"><title>Breeze Trading System</title>\n<style>\n*{box-sizing:border-box}body{font-family:Segoe UI,Arial,sans-serif;background:#060B14;margin:0;padding:12px;color:#E8EEF7}.card{max-width:1600px;margin:auto;background:#0E1828;border-radius:22px;padding:16px;box-shadow:0 10px 30px #0006}h1{font-size:22px;margin:0;color:#E8EEF7}h2{font-size:18px;margin:0 0 8px}.sub{color:#8EA3B8;font-size:13px;margin-top:3px}.top{display:flex;justify-content:space-between;gap:10px;align-items:center}.online{background:#123326;color:#6EE7A1;padding:10px 12px;border-radius:14px;font-weight:800;font-size:12px;text-align:center}.offline{background:#3A171A;color:#FF8F8F;padding:10px 12px;border-radius:14px;font-weight:800;font-size:12px;text-align:center}.tabs{display:grid;grid-template-columns:repeat(3,1fr);gap:7px;margin:15px 0}.tab{padding:11px 5px;border:0;border-radius:12px;background:#132238;color:#C9D6E5;font-weight:800}.tab.active{background:#155EEF;color:#fff}.panel{display:none}.panel.active{display:block}.box{background:#111F32;border:1px solid #203650;border-radius:15px;padding:13px;margin:10px 0}.grid{display:grid;grid-template-columns:1fr 1fr;gap:9px}label{font-size:12px;font-weight:800;display:block;margin:6px 0 4px}input,select{width:100%;padding:12px;border:1px solid #cbd5e1;border-radius:10px;font-size:15px;background:#0E1828}button{width:100%;padding:13px;border:0;border-radius:11px;font-size:15px;font-weight:800;margin-top:9px}.hist{background:#155EEF;color:#fff}.paper{background:#10a35a;color:#fff}.live{background:#c62828;color:#fff}.stop{background:#e53935;color:#fff}.sleep{background:#606770;color:#fff}.refresh{background:#455a64;color:#fff}.warn{background:#0E18284d6;border:1px solid #f1ce76;padding:11px;border-radius:12px;font-size:12px;line-height:1.45}.danger{background:#351719;border:1px solid #7A3034;padding:11px;border-radius:12px;font-size:12px;line-height:1.45}.status{padding:13px;border-radius:14px;background:#10243A;line-height:1.55;font-size:13px}.progress{height:10px;background:#1A2A3F;border-radius:99px;overflow:hidden;margin-top:7px}.bar{height:100%;width:0;background:#22C55E}.small{font-size:11px;color:#8EA3B8;line-height:1.45}.files{font-size:12px}.file{padding:8px 0;border-bottom:1px solid #e7edf2;display:flex;justify-content:space-between;gap:6px}.file a{font-weight:800;color:#1976d2}.footer{margin-top:12px;text-align:center;font-size:11px;color:#8EA3B8}.check{display:flex;align-items:flex-start;gap:8px;font-size:12px;margin-top:9px}.check input{width:auto;margin-top:2px}.orders{display:grid;grid-template-columns:1fr 1fr;gap:8px;padding-right:0}.order{border:1px solid #203650;border-radius:14px;padding:11px;background:#0E1828;box-shadow:0 2px 8px #0000000b}.order.buy{border-left:5px solid #10a35a}.order.sell{border-left:5px solid #e53935}.order-top{display:flex;justify-content:space-between;align-items:center;gap:8px;font-weight:800}.order-symbol{font-size:15px;color:#E8EEF7}.badge{display:inline-flex;align-items:center;padding:4px 8px;border-radius:999px;font-size:10px;font-weight:900;letter-spacing:.3px}.badge.buy{background:#123326;color:#6EE7A1}.badge.sell{background:#3A171A;color:#FF8F8F}.order-price{font-size:15px;font-weight:900;white-space:nowrap}.order-meta{font-size:11px;color:#8EA3B8;line-height:1.55;margin-top:6px}.order-extra{display:flex;flex-wrap:wrap;gap:5px;margin-top:7px}.chip{background:#16263A;border-radius:999px;padding:4px 7px;font-size:10px;color:#A7B7C8}.pnl{font-weight:900;margin-top:7px;font-size:13px}.pnl.pos{color:#087443}.pnl.neg{color:#a32626}.trade-card{border:1px solid #203650;border-radius:12px;background:#0E1828;box-shadow:0 2px 8px #0000000b;overflow:hidden}.trade-table-wrap{width:100%;overflow-x:auto;-webkit-overflow-scrolling:touch}.trade-table{width:100%;min-width:1250px;border-collapse:separate;border-spacing:0;table-layout:fixed}.trade-table th{position:sticky;top:0;z-index:2;background:#132238;color:#91A6BC;font-size:10px;text-transform:uppercase;letter-spacing:.25px;padding:9px 8px;text-align:left;border-bottom:1px solid #dce6ef;white-space:nowrap}.trade-table td{padding:9px 8px;border-bottom:1px solid #edf1f4;vertical-align:top;font-size:11px;background:#0E1828}.trade-table tbody tr:hover td{background:#132238}.trade-table tr:last-child td{border-bottom:0}.trade-rank{font-weight:900;color:#91A6BC;text-align:center}.trade-symbol{font-weight:900;color:#E8EEF7;font-size:13px}.trade-qty,.trade-buy,.trade-sell{font-weight:800;white-space:nowrap}.trade-time{font-size:10px;color:#8EA3B8;line-height:1.35;margin-top:2px;white-space:nowrap}.trade-reason{font-size:10px;line-height:1.35;color:#A7B7C8;min-width:280px}.trade-header-note{font-size:10px;color:#8EA3B8;margin-top:4px}.trade-pnl{font-weight:900;white-space:nowrap}.trade-pnl.pos{color:#087443}.trade-pnl.neg{color:#a32626}.trade-pnl.zero{color:#91A6BC}.trade-table th:first-child,.trade-table td:first-child{position:sticky;left:0;z-index:3}.trade-table th:first-child{z-index:4;background:#132238}.trade-table td:first-child{background:#0E1828}@media(max-width:700px){.trade-table{min-width:1150px}.trade-table th,.trade-table td{padding:7px 6px}.trade-table th{font-size:9px}.trade-table td{font-size:10px}.trade-symbol{font-size:12px}.trade-time{font-size:9px}.trade-reason{font-size:9px;min-width:250px}}.trade-head{display:flex;justify-content:space-between;align-items:center;gap:8px}.trade-num{font-size:16px;font-weight:900;color:#E8EEF7}.trade-sub{font-size:10px;color:#8EA3B8;margin-top:2px}.trade-result{white-space:nowrap}.trade-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:6px;margin-top:8px}.trade-grid>div{background:#111F32;border-radius:9px;padding:7px;min-width:0}.trade-grid span{display:block;font-size:9px;font-weight:900;color:#7E93A8}.trade-grid b{display:block;font-size:12px;margin-top:2px;color:#E8EEF7}.trade-grid small{display:block;font-size:9px;color:#7E93A8;margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.open-chip{background:#0E18284d6;color:#8a5a00}.trace-head{display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:9px}.trace-count{font-size:11px;color:#8EA3B8;font-weight:800}.trace-tools{display:grid;grid-template-columns:1fr 1fr;gap:7px;margin:8px 0}.trace-tools input,.trace-tools select{padding:9px;font-size:12px}.summary-grid{display:grid;grid-template-columns:repeat(5,1fr);gap:6px;margin:10px 0}.summary-item{background:#0E1828;border:1px solid #203650;border-radius:12px;padding:8px;text-align:center}.summary-value{font-size:16px;font-weight:900;color:#E8EEF7}.summary-label{font-size:9px;color:#8EA3B8;margin-top:2px}.empty{font-size:12px;color:#8EA3B8;padding:12px;text-align:center}.mobile-log{height:62vh;min-height:360px;overflow:auto;background:#07101C;border:1px solid #203650;border-radius:12px;padding:9px;font-family:Consolas,Menlo,monospace;font-size:11px;line-height:1.55;white-space:pre-wrap;word-break:break-word}.log-line{padding:6px 7px;border-bottom:1px solid #15263A}.log-time{color:#6E879F}.log-msg{color:#D8E2EE}.log-ok{color:#6EE7A1}.log-warn{color:#F5C76B}.log-err{color:#FF8F8F}.log-empty{color:#7E93A8;padding:14px;text-align:center}.trace-note{font-size:10px;color:#7E93A8;margin-top:6px}@media(max-width:700px){.orders{grid-template-columns:1fr}}@media(max-width:430px){.summary-grid{grid-template-columns:repeat(2,1fr)}.trade-grid{grid-template-columns:repeat(2,1fr)}.trace-tools{grid-template-columns:1fr}}\n\n.logic-grid{display:grid;grid-template-columns:1fr 1fr;gap:9px}.logic-section{margin-top:10px}.logic-card{background:#0E1828;border:1px solid #203650;border-radius:15px;padding:13px;margin:10px 0}.logic-title{display:flex;justify-content:space-between;align-items:center;gap:8px}.logic-badge{padding:5px 9px;border-radius:999px;background:#172A40;color:#8EA3B8;font-size:10px;font-weight:900}.logic-badge.on{background:#123326;color:#6EE7A1}.logic-save{background:#155EEF;color:#fff}.logic-reset{background:#455a64;color:#fff}.logic-note{font-size:11px;color:#8EA3B8;line-height:1.45}.logic-field{margin-bottom:7px}.logic-field label{margin-bottom:4px}.logic-field input,.logic-field select{background:#091321;color:#E8EEF7;border-color:#29435F}.logic-check{display:flex;align-items:center;gap:8px;background:#091321;border:1px solid #203650;border-radius:10px;padding:10px;font-size:12px}.logic-check input{width:auto}.logic-status{margin-top:9px;padding:10px;border-radius:10px;background:#10243A;font-size:12px;line-height:1.4}.logic-danger{color:#F5C76B}.logic-top{display:grid;grid-template-columns:1fr 1fr;gap:8px}.logic-chip{font-size:10px;padding:6px 8px;border-radius:999px;background:#16263A;color:#A7B7C8;display:inline-block;margin:3px 3px 0 0}@media(max-width:430px){.logic-grid,.logic-top{grid-template-columns:1fr}}\n</style></head><body>\n<div class="card">\n<div class="top"><div><h1>📈 Breeze Trading System</h1><div class="sub">Mobile Control • Home Laptop</div></div><div id="conn" class="online">CHECKING…</div></div>\n<div class="tabs"><button class="tab active" onclick="tab(\'history\',this)">📅 Historical</button><button class="tab" onclick="tab(\'trade\',this)">⚡ Trading</button><button class="tab" onclick="tab(\'histlog\',this);loadLog(\'historical\')">📜 Hist Log</button><button class="tab" onclick="tab(\'tradelog\',this);loadLog(\'trading\')">🟢 Live/Paper Log</button><button class="tab" onclick="tab(\'logic\',this);loadMobileLogic()">⚙️ Logic</button><button class="tab" onclick="tab(\'access\',this)">🔐 Access</button></div>\n\n<section id="home" class="panel">\n<div class="box"><div class="trade-head"><h2 style="margin:0">Current Status</h2><button id="refreshBtn" class="refresh" type="button" onclick="manualRefresh()" style="width:auto;margin-top:0;padding:10px 15px">↻ Refresh</button></div><div id="status" class="status">Checking home laptop connection…</div><div class="small" style="margin-top:7px"><b>Live clock:</b> <span id="liveClock">—</span> • <b>Last refresh:</b> <span id="lastRefresh">—</span></div><div class="progress"><div id="bar" class="bar"></div></div></div>\n</section>\n\n<section id="history" class="panel active">\n<div class="box"><h2>Historical Data</h2><div class="grid"><div><label>From Date</label><input id="start" type="date" value="2026-09-01"></div><div><label>To Date</label><input id="end" type="date" value="2026-09-23"></div></div><div class="grid"><div><label>Data</label><select id="interval"><option value="hybrid">Hybrid 1m → 1s</option><option value="1minute">1 Minute</option><option value="1second">1 Second</option></select></div><div><label>Maximum Shares Override (0 = GUI Logic)</label><input id="limit" type="number" min="0" value="0"></div></div><button class="hist" onclick="runHist()">▶ START HISTORICAL</button><button class="stop" onclick="stopJob()">⏹ STOP</button></div>\n<div class="box"><h2>Latest Excel</h2><div id="files" class="files">Loading…</div></div>\n</section>\n\n<section id="trade" class="panel">\n<div class="box"><h2>🔔 Early BUY Popup Alerts</h2><div class="small">Free popup warnings for developing setups. Alerts never place orders; the trading engine must revalidate all GUI conditions before a paper BUY.</div><button class="refresh" type="button" onclick="enableMobilePopups()">🔔 ENABLE MOBILE POPUPS</button><div id="popupAlertStatus" class="small" style="margin-top:7px">Popup notifications not enabled.</div></div>\n<div class="box"><h2>Paper Trading</h2><div class="small">Uses the same enabled Logic 1–8 settings saved on the laptop. Buys/sells are simulated and positions are processed through the normal engine.</div><button class="paper" onclick="startPaper()">🟢 START PAPER TRADING FROM MOBILE</button></div>\n<div class="box"><div class="trace-head"><h2 style="margin:0">📋 Order Monitor</h2><div id="traceCount" class="trace-count">Loading…</div></div><div class="small"><b>One trade = one row.</b> BUY and intraday SELL are combined, so a 20-share limit will show at most 20 trades — not 40 events.</div><div class="box" id="mobilePnlStatus"><h2>P&L STATUS</h2><div class="grid"><div class="summary-item" style="text-align:left"><div class="summary-label">Mode</div><div id="pnlMode" class="summary-value">PAPER</div></div><div class="summary-item" style="text-align:left"><div class="summary-label">Open Positions</div><div id="pnlOpen" class="summary-value">0</div></div><div class="summary-item" style="text-align:left"><div class="summary-label">Completed</div><div id="pnlCompleted" class="summary-value">0</div></div><div class="summary-item" style="text-align:left"><div class="summary-label">Winning</div><div id="pnlWinning" class="summary-value">0</div></div><div class="summary-item" style="text-align:left"><div class="summary-label">Losing</div><div id="pnlLosing" class="summary-value">0</div></div><div class="summary-item" style="text-align:left"><div class="summary-label">Gross P&L</div><div id="pnlGross" class="summary-value">₹0.00</div></div><div class="summary-item" style="text-align:left"><div class="summary-label">Charges</div><div id="pnlCharges" class="summary-value">₹0.00</div></div><div class="summary-item" style="text-align:left"><div class="summary-label">Net P&L</div><div id="pnlNet" class="summary-value">₹0.00</div></div></div><div style="margin-top:14px"><div class="summary-label" style="font-size:12px">Last Trade</div><div id="pnlLastTrade" style="font-weight:800;margin-top:8px">No completed trade yet</div></div><div id="pnlUpdated" class="small" style="margin-top:10px">Waiting for trading data…</div></div><div class="summary-grid"><div class="summary-item"><div id="sumEvents" class="summary-value">0</div><div class="summary-label">TRADES TODAY</div></div><div class="summary-item"><div id="sumBuys" class="summary-value">0</div><div class="summary-label">BUY SHARES</div></div><div class="summary-item"><div id="sumSells" class="summary-value">0</div><div class="summary-label">SOLD</div></div><div class="summary-item"><div id="sumOpen" class="summary-value">0</div><div class="summary-label">OPEN</div></div><div class="summary-item"><div id="sumCompleted" class="summary-value">0</div><div class="summary-label">COMPLETED</div></div><div class="summary-item"><div id="sumWins" class="summary-value pnl pos">0</div><div class="summary-label">WIN TRADES</div></div><div class="summary-item"><div id="sumLosses" class="summary-value pnl neg">0</div><div class="summary-label">LOSS TRADES</div></div><div class="summary-item"><div id="sumGross" class="summary-value">₹0.00</div><div class="summary-label">GROSS P&L</div></div><div class="summary-item"><div id="sumCharges" class="summary-value">₹0.00</div><div class="summary-label">CHARGES</div></div><div class="summary-item"><div id="sumPnl" class="summary-value">₹0.00</div><div class="summary-label">NET P&L</div></div></div><div class="trace-tools"><input id="orderSearch" type="search" placeholder="Search company name…" oninput="renderOrders()"><select id="orderFilter" onchange="renderOrders()"><option value="ALL">All trades</option><option value="OPEN">Open trades</option><option value="CLOSED">Closed trades</option></select></div><div id="orders" class="orders"><div class="empty">Loading order history…</div></div><div class="trace-note">Closed-trade P&L comes from SELL events; open trades show live estimated P&L from the latest Breeze price. Paper and LIVE events remain traceable individually.</div></div>\n<div class="box"><h2>Quick Controls</h2><div id="mobileActionMsg" class="small" style="display:none;margin:8px 0;padding:10px;border-radius:10px;background:#10243A"></div><button class="paper" onclick="startPaper()">🟢 START PAPER TRADING FROM MOBILE</button><button class="live" onclick="startLive()">🔴 START LIVE TRADING</button><button class="stop" onclick="stopJob()">⏹ STOP / SAFE STOP</button><button class="refresh" onclick="refreshAll()">🔄 REFRESH STATUS</button></div>\n<div class="warn">Paper Trading uses market data but sends <b>no real orders</b>. Live Trading sends real BUY/SELL orders through your saved Breeze account.</div>\n\n<div class="box"><h2>Live Trading</h2><div class="danger"><b>REAL MONEY:</b> this button can place real BUY and SELL orders through ICICI Direct Breeze. The laptop uses the same saved Logic 1–4 configuration and normal 15:25 square-off.</div><label class="check"><input id="liveConfirm" type="checkbox"> I understand that Live Trading sends real orders from my home laptop.</label><button class="live" onclick="startLive()">🔴 START LIVE TRADING FROM MOBILE</button><button class="stop" onclick="stopJob()">⏹ SAFE STOP LIVE / PAPER</button></div>\n<div class="box"><h2>Home Laptop</h2><button class="sleep" onclick="sleepPC()">💤 SLEEP HOME LAPTOP</button><div class="small">Do not sleep the laptop while trading is running. A sleeping laptop cannot receive normal internet commands. Remote wake requires Wake-on-LAN setup.</div></div>\n</section>\n\n<section id="histlog" class="panel">\n<div class="box"><div class="trace-head"><h2 style="margin:0">📜 Historical Data Process Log</h2><button class="refresh" type="button" onclick="loadLog(\'historical\')" style="width:auto;margin-top:0;padding:10px 15px">↻ Refresh</button></div><div class="small">Historical download, cache, replay, scoring and completion messages. This feed is separate from Live/Paper trading.</div><div id="histLogMeta" class="trace-note">Loading…</div><div id="histLog" class="mobile-log"></div></div>\n</section>\n<section id="tradelog" class="panel">\n<div class="box"><div class="trace-head"><h2 style="margin:0">🟢 Live / Paper Trading Process Log</h2><button class="refresh" type="button" onclick="loadLog(\'trading\')" style="width:auto;margin-top:0;padding:10px 15px">↻ Refresh</button></div><div class="small">Paper and Live trading status, signals, orders, fills, exits, errors and safe-stop messages.</div><div id="tradeLogMeta" class="trace-note">Loading…</div><div id="tradeLog" class="mobile-log"></div></div>\n</section>\n<section id="logic" class="panel">\n<div class="box"><div class="trace-head"><h2 style="margin:0">⚙️ Trading Logic</h2><button class="refresh" type="button" onclick="loadMobileLogic()" style="width:auto;margin-top:0;padding:10px 15px">↻ Refresh</button></div><div class="logic-note">Edit the same canonical Logic 1–8 configuration used by the laptop. Saving here writes directly to <b>Config/logic_settings.json</b>. Saved changes are written to the laptop immediately. Running PAPER sessions reload conditions and enabled logics automatically; open positions keep their original stop/target settings. LIVE and Historical jobs use saved settings on the next run.</div><div id="logicStatus" class="logic-status">Loading logic…</div><div class="logic-card"><h3 style="margin:0 0 6px">Shared condition priority</h3><div class="logic-note">Same shared priority order as the laptop GUI. Use 1–15 for explicit order; 0 means default order. Non-zero ranks must be unique.</div><div id="conditionRankControls" class="logic-grid" style="margin-top:9px"></div><h3 style="margin:14px 0 6px">Shared moving-average filters</h3><div class="logic-note">These six controls are shared across Logic 1–8, matching the laptop GUI.</div><div id="sharedMAControls" class="logic-grid" style="margin-top:9px"></div><h3 style="margin:14px 0 6px">Shared exit / GTT controls</h3><div class="logic-note">These controls are shared across Logic 1–8, matching the laptop GUI.</div><div id="sharedExecutionControls" class="logic-grid" style="margin-top:9px"></div></div><div id="logicCards"></div></div>\n</section>\n<section id="smart" class="panel">\n<div class="box"><div class="trade-head"><h2 style="margin:0">🤖 Smart Trade Validation</h2><button class="refresh" onclick="loadSmart()" style="width:auto;margin-top:0">↻ Refresh</button></div>\n<div id="smartGate" class="status">Loading paper validation…</div><div id="smartDaily" class="status" style="margin-top:8px">No completed market-close result yet.</div>\n<div class="small" style="margin-top:7px">Smart LIVE remains locked until the paper/shadow evidence gate is satisfied. A profitable sample is evidence, not a guarantee.</div></div>\n<div class="box"><h2>Paper Validation</h2><div class="summary-grid"><div class="summary-item"><div id="svTrades" class="summary-value">—</div><div class="summary-label">Closed Trades</div></div><div class="summary-item"><div id="svWin" class="summary-value">—</div><div class="summary-label">Win Rate</div></div><div class="summary-item"><div id="svPf" class="summary-value">—</div><div class="summary-label">Profit Factor</div></div><div class="summary-item"><div id="svDd" class="summary-value">—</div><div class="summary-label">Max Drawdown</div></div><div class="summary-item"><div id="svNet" class="summary-value">—</div><div class="summary-label">Net P&L</div></div></div></div>\n<div class="box"><h2>Current Smart Ideas • Top 30</h2><div id="smartIdeas" class="orders"><div class="empty">No Smart ideas available yet. Start the Smart scanner on the laptop.</div></div></div>\n<div class="box warn"><b>LIVE gate:</b> The mobile screen is for monitoring. It does not bypass the laptop\'s final Breeze validation, protective-stop checks, or manual confirmation.</div>\n</section>\n<section id="access" class="panel">\n<div class="box"><h2>🔐 Mobile Login</h2><div class="small">This phone is paired using the secure token in the QR link. If the token is missing, enter the pairing token below. The token is not your Breeze password.</div><label>Pairing Token</label><input id="pairToken" type="password" placeholder="Paste pairing token if needed"><button class="hist" onclick="mobileLogin()">🔐 LOGIN / CONNECT</button><div id="loginMsg" class="status" style="margin-top:9px">Checking connection…</div></div>\n<div class="box"><h2>🖥 Chrome Remote Desktop</h2><div class="small">Use Chrome Remote Desktop when you want to see and control the complete Windows desktop, including the Breeze GUI.</div><button class="hist" onclick="openCRD()">🖥 OPEN CHROME REMOTE DESKTOP</button><div class="warn" style="margin-top:9px">Chrome Remote Desktop must be installed and the home laptop must be registered once. The trading program cannot create that first-time Google authorization automatically.</div></div>\n<div class="box"><h2>🔑 Breeze Login</h2><div class="small">Breeze API credentials stay on the home laptop. For security, use the desktop Breeze login/credential screen rather than transmitting API secrets through the mobile browser.</div><button class="paper" onclick="alert(\'Open the Breeze GUI on the home laptop to enter or update App Key, Secret Key and Session Token. The credentials are stored encrypted for the Windows account.\')">🔑 OPEN BREEZE LOGIN IN GUI</button></div>\n</section>\n<div class="footer">🔐 One-time paired phone • Private connection via Tailscale • Breeze credentials stay on the home laptop</div>\n</div>\n<div id="earlyAlertToast" role="alert" aria-live="assertive" style="display:none;position:fixed;left:12px;right:12px;bottom:16px;z-index:99999;background:#10243A;border:2px solid #F5C76B;color:#fff;padding:15px;border-radius:16px;box-shadow:0 8px 30px #000a"><div style="font-weight:900;color:#F5C76B;margin-bottom:6px">✅ BUY CONDITIONS PASSED</div><div id="earlyAlertText" style="font-size:13px;line-height:1.5;white-space:pre-line"></div><button type="button" onclick="dismissEarlyAlert()" style="background:#155EEF;color:#fff">DISMISS</button></div>\n<script>\nconst hashToken=(location.hash.startsWith(\'#token=\')?decodeURIComponent(location.hash.slice(7)):\'\');const queryToken=new URLSearchParams(location.search).get(\'token\')||\'\';const token=hashToken||queryToken||localStorage.getItem(\'breeze_token\')||\'\';if(token)localStorage.setItem(\'breeze_token\',token);const base=location.origin;const H=()=>({\'Content-Type\':\'application/json\',\'X-Remote-Token\':localStorage.getItem(\'breeze_token\')||\'\'});\nasync function api(p,m=\'GET\',d=null){const t=encodeURIComponent(localStorage.getItem(\'breeze_token\')||\'\');const sep=p.includes(\'?\')?\'&\':\'?\';let r=await fetch(base+p+sep+\'token=\'+t,{method:m,headers:H(),body:d?JSON.stringify(d):undefined});let x={};try{x=await r.json()}catch(e){}if(!r.ok)throw Error(x.error||(\'HTTP \'+r.status));return x}\nfunction tab(id,el){document.querySelectorAll(\'.panel\').forEach(x=>x.classList.remove(\'active\'));document.getElementById(id).classList.add(\'active\');document.querySelectorAll(\'.tab\').forEach(x=>x.classList.remove(\'active\'));el.classList.add(\'active\')}\nasync function fetchWithTimeout(url,opts={},ms=3500){const c=new AbortController();const t=setTimeout(()=>c.abort(),ms);try{return await fetch(url,{...opts,signal:c.signal,cache:"no-store"})}finally{clearTimeout(t)}}\nasync function status(){try{let h=await fetchWithTimeout(base+"/health?ts="+Date.now(),{},3500);if(!h.ok)throw Error("HTTP "+h.status);let hp=await h.json();document.getElementById("conn").className="online";document.getElementById("conn").innerHTML="🟢 LAPTOP<br>ONLINE";document.getElementById("status").innerHTML="<b>ONLINE</b><br>Home laptop is reachable.";document.getElementById("bar").style.width="0%";try{let x=await api("/status");let mode=(x.mode||"IDLE").toUpperCase();let msg=x.status||"IDLE";if(x.job_running)msg="<b>"+mode+"</b> — "+msg;else if(mode==="IDLE")msg="<b>IDLE</b> — Trading engine is not running";const detail=x.error||x.message||x.last_error||"";document.getElementById("status").innerHTML="<b>"+msg+"</b><br>Server time: "+(x.server_time||hp.time||"")+(x.time?"<br>State update: "+x.time:"")+(detail?"<br>"+esc(detail):"");document.getElementById("bar").style.width=x.job_running?"60%":"0%";if(x.job_running)document.getElementById("status").scrollIntoView({block:"nearest"});}catch(e){document.getElementById("status").innerHTML="<b>ONLINE</b><br>Mobile agent is reachable. Status details are temporarily unavailable.";document.getElementById("bar").style.width="0%";}}catch(e){document.getElementById("conn").className="offline";document.getElementById("conn").innerHTML="🔴 OFFLINE";document.getElementById("status").textContent="Home laptop is not reachable. Make sure laptop is ON, internet is connected, and Tailscale is ON.";document.getElementById("bar").style.width="0%"}}\nasync function files(){try{let x=await api(\'/files\');document.getElementById(\'files\').innerHTML=x.files.length?x.files.slice(0,8).map(f=>{const t=encodeURIComponent(localStorage.getItem(\'breeze_token\')||\'\');return `<div class="file"><span>${f.name}</span><a href="/download/${encodeURIComponent(f.name)}?token=${t}">OPEN</a></div>`}).join(\'\'):\'No Excel file yet.\'}catch(e){document.getElementById(\'files\').textContent=\'Not available\'}}\nfunction mobileLogin(){const v=document.getElementById(\'pairToken\').value.trim();if(v){localStorage.setItem(\'breeze_token\',v);document.getElementById(\'loginMsg\').textContent=\'Token saved. Checking home laptop…\';}else if(!localStorage.getItem(\'breeze_token\')){document.getElementById(\'loginMsg\').textContent=\'Scan the GUI QR code or enter the pairing token.\';return;}status().then(()=>{document.getElementById(\'loginMsg\').textContent=\'Connected to home laptop.\'}).catch(()=>{})}\nfunction openCRD(){window.open(\'https://remotedesktop.google.com/access\',\'_blank\',\'noopener,noreferrer\')}\nlet ORDER_CACHE=[];\nfunction esc(v){return String(v??"").replace(/[&<>"]/g,m=>({"&":"&amp;","<":"&lt;",">":"&gt;","\\"":"&quot;"}[m]));}\nfunction fmtTime(v){const s=String(v||"");if(!s)return "—";const d=new Date(s);if(Number.isNaN(d.getTime()))return s.replace("T"," ").slice(0,19);return d.toLocaleString([], {day:"2-digit",month:"short",hour:"2-digit",minute:"2-digit",second:"2-digit",hour12:false});}function fmtTimeOnly(v){const s=String(v||"");if(!s)return "—";const d=new Date(s);if(Number.isNaN(d.getTime())){const m=s.match(/([0-9]{1,2}:[0-9]{2}:[0-9]{2})(?:\\s*(AM|PM))?/i);return m?m[1]+(m[2]?" "+m[2].toUpperCase():""):s.replace("T"," ").slice(11,19);}return d.toLocaleTimeString([], {hour:"numeric",minute:"2-digit",second:"2-digit",hour12:true});}\nfunction num(v){const n=Number(v);return Number.isFinite(n)?n:0;}\nfunction todayKey(){const now=new Date();return now.getFullYear()+"-"+String(now.getMonth()+1).padStart(2,"0")+"-"+String(now.getDate()).padStart(2,"0");}\nfunction groupTrades(events){\n  const map=new Map();\n  for(const e of (events||[])){\n    const side=String(e.side||"").toUpperCase();\n    if(side!=="BUY"&&side!=="SELL")continue;\n    const id=String(e.trade_id||`${e.mode||""}-${e.logic||""}-${e.symbol||""}-${side}-${e.time||""}`);\n    let t=map.get(id);\n    if(!t){t={trade_id:id,mode:e.mode||"",logic:e.logic||"",symbol:e.symbol||"",company_name:e.company_name||e.symbol||"",quantity:num(e.quantity),buy:null,sell:null,status:"OPEN",reason:"",gross_pnl:null,charges:null,net_pnl:null,current_price:null,unrealized_net_pnl:null};map.set(id,t);}\n    if(e.company_name)t.company_name=e.company_name; if(side==="BUY") t.buy=e;\n    if(side==="SELL"){t.sell=e;t.reason=e.reason||t.reason;t.status="CLOSED";t.gross_pnl=e.gross_pnl!=null?num(e.gross_pnl):t.gross_pnl;t.charges=e.charges!=null?num(e.charges):t.charges;t.net_pnl=e.net_pnl!=null?num(e.net_pnl):t.net_pnl;if(!t.buy&&e.buy_time)t.buy={side:"BUY",mode:e.mode,logic:e.logic,symbol:e.symbol,quantity:num(e.buy_quantity||e.quantity),price:num(e.buy_price),time:e.buy_time,trade_id:id};}\n    if(!t.quantity)t.quantity=num(e.quantity);\n  }\n  return Array.from(map.values()).sort((a,b)=>String(b.buy?.time||b.sell?.time||"").localeCompare(String(a.buy?.time||a.sell?.time||"")));\n}\nfunction applyLivePositions(trades, livePositions){const map=new Map((livePositions||[]).map(x=>[String(x.trade_id||""),x]));for(const t of trades){const p=map.get(String(t.trade_id||""));if(!p)continue;if(p.company_name)t.company_name=p.company_name;if(p.current_price!=null)t.current_price=num(p.current_price);if(p.net_pnl!=null)t.unrealized_net_pnl=num(p.net_pnl);if(p.current_time)t.current_time=p.current_time;}return trades;}\nfunction renderOrders(){\n  const search=(document.getElementById("orderSearch")?.value||"").trim().toUpperCase();\n  const filter=(document.getElementById("orderFilter")?.value||"ALL").toUpperCase();\n  const trades=applyLivePositions(groupTrades(ORDER_CACHE), window.LIVE_POSITIONS||[]);\n  const today=todayKey();\n  const todayTrades=trades.filter(t=>String(t.buy?.time||t.sell?.time||"").slice(0,10)===today);\n  const buys=todayTrades.filter(t=>t.buy).length;\n  const sells=todayTrades.filter(t=>t.sell).length;\n  const open=todayTrades.filter(t=>!t.sell).length;\n  const completed=todayTrades.filter(t=>!!t.sell);\n  const gross=completed.reduce((a,t)=>a+(t.gross_pnl!=null?num(t.gross_pnl):0),0);\n  const charges=completed.reduce((a,t)=>a+(t.charges!=null?num(t.charges):0),0);\n  const net=todayTrades.reduce((a,t)=>a+(t.net_pnl!=null?num(t.net_pnl):(t.unrealized_net_pnl!=null?num(t.unrealized_net_pnl):0)),0);\n  const wins=completed.filter(t=>t.net_pnl!=null&&num(t.net_pnl)>0).length;\n  const losses=completed.filter(t=>t.net_pnl!=null&&num(t.net_pnl)<0).length;\n  const setPnl=(id,value)=>{const el=document.getElementById(id);if(el)el.textContent=value;};\n  setPnl("pnlOpen",open); setPnl("pnlCompleted",completed.length); setPnl("pnlWinning",wins); setPnl("pnlLosing",losses);\n  setPnl("pnlGross","₹"+gross.toFixed(2)); setPnl("pnlCharges","₹"+charges.toFixed(2)); setPnl("pnlNet","₹"+net.toFixed(2));\n  const last=completed.slice().sort((a,b)=>String(b.sell?.time||"").localeCompare(String(a.sell?.time||"")))[0];\n  setPnl("pnlLastTrade",last?`${last.company_name||last.symbol||"Share"} • ${last.sell?.reason||"Completed"} • ₹${num(last.net_pnl).toFixed(2)}`:"No completed trade yet");\n  setPnl("pnlUpdated","Updated "+new Date().toLocaleTimeString());\n  document.getElementById("sumEvents").textContent=todayTrades.length;\n  document.getElementById("sumBuys").textContent=buys;\n  document.getElementById("sumSells").textContent=sells;\n  const openEl=document.getElementById("sumOpen");if(openEl)openEl.textContent=open;\n  const completedEl=document.getElementById("sumCompleted");if(completedEl)completedEl.textContent=completed.length;\n  const winsEl=document.getElementById("sumWins");if(winsEl)winsEl.textContent=wins;\n  const lossesEl=document.getElementById("sumLosses");if(lossesEl)lossesEl.textContent=losses;\n  const grossEl=document.getElementById("sumGross");if(grossEl)grossEl.textContent="₹"+gross.toFixed(2);\n  const chargesEl=document.getElementById("sumCharges");if(chargesEl)chargesEl.textContent="₹"+charges.toFixed(2);\n  const pnl=document.getElementById("sumPnl");pnl.textContent="₹"+net.toFixed(2);pnl.className="summary-value "+(net>0?"pnl pos":net<0?"pnl neg":"");\n  document.getElementById("traceCount").textContent=todayTrades.length+" trades today • BUY+SELL combined";\n  const rows=trades.filter(t=>{const company=String(t.company_name||t.symbol||"").toUpperCase();const sideOk=filter==="ALL"||(filter==="OPEN"&&!t.sell)||(filter==="CLOSED"&&!!t.sell);return sideOk&&(!search||company.includes(search));});\n  rows.sort((a,b)=>String(a.buy?.time||a.sell?.time||"").localeCompare(String(b.buy?.time||b.sell?.time||"")));\n  document.getElementById("orders").innerHTML=rows.length?`<div class="trade-card"><div class="trade-table-wrap"><table class="trade-table"><colgroup><col style="width:5%"><col style="width:9%"><col style="width:7%"><col style="width:10%"><col style="width:11%"><col style="width:10%"><col style="width:10%"><col style="width:11%"><col style="width:21%"><col style="width:6%"></colgroup><thead><tr><th>Ranking</th><th>Symbol</th><th>Quantity</th><th>Buy Price</th><th>Buy Date &amp; Time</th><th>Sell Quantity</th><th>Sell Price</th><th>Sell Date &amp; Time</th><th>Exit Reason</th><th>Net P&amp;L</th></tr></thead><tbody>${rows.map((t,i)=>{\n    const b=t.buy,s=t.sell,qty=num(t.quantity||b?.quantity||s?.quantity);\n    const buyPrice=b?num(b.price):null,sellPrice=s?num(s.price):null;\n    const sellQty=s?num(s.quantity||qty):null;\n    const realized=t.net_pnl!=null?num(t.net_pnl):null;\n    const unrealized=t.unrealized_net_pnl!=null?num(t.unrealized_net_pnl):null;\n    const netVal=realized!==null?realized:unrealized;\n    const pnlText=netVal===null?"—":(netVal<0?"-":"")+"₹"+Math.abs(netVal).toFixed(2);\n    const pnlClass=netVal===null?"zero":netVal>0?"pos":netVal<0?"neg":"zero";\n    const status=t.sell?"CLOSED":"OPEN";\n    const reason=t.reason||"—";\n    return `<tr><td class="trade-rank">${i+1}</td><td><div class="trade-symbol">${esc(t.symbol||t.company_name||"-")}</div></td><td class="trade-qty">${qty||"—"}</td><td class="trade-buy">${buyPrice===null?"—":"₹"+buyPrice.toFixed(2)}</td><td><div class="trade-time">${b?fmtTimeOnly(b.time):"—"}</div></td><td class="trade-qty">${sellQty===null?"—":sellQty}</td><td class="trade-sell">${sellPrice===null?"—":"₹"+sellPrice.toFixed(2)}</td><td><div class="trade-time">${s?fmtTimeOnly(s.time):"—"}</div></td><td><div class="trade-reason">${esc(reason)}</div></td><td><div class="trade-pnl ${pnlClass}">${pnlText}</div></td></tr>`;\n  }).join("")}</tbody></table></div></div>`:"<div class=\\"empty\\">No matching trades.</div>";\n}\nfunction logClass(msg){const u=String(msg||\'\').toUpperCase();if(u.includes(\'ERROR\')||u.includes(\'FAILED\')||u.includes(\'REJECT\'))return \'log-err\';if(u.includes(\'WARN\')||u.includes(\'BLOCKED\')||u.includes(\'SKIP\'))return \'log-warn\';if(u.includes(\'SUCCESS\')||u.includes(\'COMPLETE\')||u.includes(\'FILLED\')||u.includes(\'STARTED\'))return \'log-ok\';return \'log-msg\'}\nlet seenEarlyAlertKeys=new Set();\nfunction enableMobilePopups(){const st=document.getElementById("popupAlertStatus");if(!(\'Notification\' in window)){if(st)st.textContent="Browser notifications unsupported; in-page popups work while dashboard is open.";return;}Notification.requestPermission().then(p=>{if(st)st.textContent=p==="granted"?"Browser notifications enabled; keep dashboard open.":"Permission not granted; in-page popups work while dashboard is open.";});}\nfunction dismissEarlyAlert(){const el=document.getElementById("earlyAlertToast");if(el)el.style.display="none";}\nfunction inspectEarlyWarnings(rows){for(const r of (rows||[])){const msg=String(r.message||" ").trim();const early=/EARLY BUY WARNING\\s+symbol=([A-Z0-9._-]+)\\s+logic=(.*?)\\s+price=([0-9.]+)\\s+pretrigger=([0-9.]+)/i.exec(msg);const confirmed=/^(?:PAPER BUY|LIVE BUY)\\s+Logic\\s+\\d+\\s+#\\d+(?:\\/[^: ]+)?:/i.test(msg);if(!early&&!confirmed)continue;const key=String(r.id||r.event_id||r.time||"")+"|"+msg;if(seenEarlyAlertKeys.has(key))continue;seenEarlyAlertKeys.add(key);if(seenEarlyAlertKeys.size>500)seenEarlyAlertKeys=new Set([...seenEarlyAlertKeys].slice(-250));let symbol="See trading log",detail="",title="BreezeTrading: BUY confirmed";if(early){symbol=early[1];title="BreezeTrading: BUY setup approaching";detail="BUY SETUP APPROACHING — not yet a buy\\n"+symbol+" • "+early[2]+"\\nPrice ₹"+Number(early[3]).toFixed(2)+" • Pre-trigger ₹"+Number(early[4]).toFixed(2)+"\\nPreliminary filters passed; final BUY conditions will be checked again.";}else{symbol=(msg.match(/:\\s*([A-Z0-9._-]+)\\s+(?:qty=|quantity=)/i)||msg.match(/\\b([A-Z][A-Z0-9._-]{1,})\\b(?=\\s+qty=)/i)||[])[1]||symbol;detail="BUY conditions passed — confirmed order event\\n"+symbol+"\\n"+msg;}const box=document.getElementById("earlyAlertToast"),txt=document.getElementById("earlyAlertText");if(txt)txt.textContent=detail;if(box)box.style.display="block";if("Notification" in window&&Notification.permission==="granted"){try{new Notification(title,{body:detail});}catch(e){}}}}\nasync function pollEarlyWarnings(){try{const x=await api(\'/process-log?feed=trading&limit=100\');inspectEarlyWarnings(x.logs||[]);}catch(e){}}\nasync function loadLog(kind){const box=document.getElementById(kind===\'historical\'?\'histLog\':\'tradeLog\');const meta=document.getElementById(kind===\'historical\'?\'histLogMeta\':\'tradeLogMeta\');if(!box)return;box.innerHTML=\'<div class="log-empty">Loading…</div>\';try{const x=await api(\'/process-log?feed=\'+encodeURIComponent(kind)+\'&limit=500\');const rows=x.logs||[];meta.textContent=(x.feed_name||kind.toUpperCase())+\' • \'+rows.length+\' messages • \'+(x.updated||\'\');if(!rows.length){box.innerHTML=\'<div class="log-empty">No process messages yet.</div>\';return;}box.innerHTML=rows.map(e=>{const msg=esc(e.message||\'\');const cls=logClass(msg);return \'<div class="log-line"><span class="log-time">[\'+esc(e.time||\'\')+\']</span> <span class="\'+cls+\'">\'+msg+\'</span></div>\';}).join(\'\');box.scrollTop=box.scrollHeight;}catch(e){meta.textContent=\'Unable to read process log\';box.innerHTML=\'<div class="log-empty log-err">\'+esc(e.message||\'Log unavailable\')+\'</div>\'}}\nasync function orders(){try{let x=await api("/orders");ORDER_CACHE=x.orders||[];window.LIVE_POSITIONS=x.live_positions||[];renderOrders()}catch(e){document.getElementById("orders").innerHTML="<div class=\\"empty\\">Order trace unavailable.</div>";document.getElementById("traceCount").textContent="Unavailable"}}\nfunction updateLiveClock(){const el=document.getElementById("liveClock");if(!el)return;const d=new Date();el.textContent=d.toLocaleString([], {year:"numeric",month:"2-digit",day:"2-digit",hour:"2-digit",minute:"2-digit",second:"2-digit",hour12:false});}updateLiveClock();setInterval(updateLiveClock,1000);\n\nconst LOGIC_FIELDS=[\n [\'name\',\'Name\',\'text\'],[\'enabled\',\'Enabled\',\'checkbox\'],[\'open_direction\',\'Direction\',\'select:BOTH,UP,DOWN\'],[\'open_movement\',\'Open movement %\',\'number\'],[\'minimum_price\',\'Minimum price ₹\',\'number\'],[\'price_big\',\'Price max\',\'number\'],[\'price_small\',\'Price min\',\'number\'],[\'use_price_band\',\'Use price band\',\'checkbox\'],[\'require_15m_orb\',\'Require 15-minute ORB breakout\',\'checkbox\'],[\'require_52w_high_breakout\',\'Require 52-week high breakout\',\'checkbox\'],[\'require_52w_low_breakdown\',\'Require 52-week low breakdown\',\'checkbox\'],[\'turnover_cr\',\'Turnover ₹ Cr\',\'number\'],[\'volume\',\'Volume minimum\',\'number\'],[\'buy_amount\',\'Buy amount ₹\',\'number\'],[\'buy_entry_time\',\'Buy entry time\',\'time\'],[\'buy_exit_time\',\'Last entry time\',\'time\'],[\'profit_target\',\'Profit target %\',\'number\'],[\'initial_stop\',\'Initial stop %\',\'number\'],[\'activation\',\'Trail activation %\',\'number\'],[\'trailing_stop\',\'Trailing stop %\',\'number\'],[\'historical_shares\',\'Historical shares\',\'number\'],[\'max_orders\',\'Max orders\',\'number\'],[\'smart_mode\',\'Smart mode\',\'checkbox\'],[\'smart_min_score\',\'Smart minimum score\',\'number\'],[\'smart_min_volume_ratio\',\'Smart volume ratio\',\'number\'],[\'smart_rank_by\',\'Rank by\',\'select:movement,momentum_15m\'],[\'smart_rank_limit\',\'Rank limit (0=off)\',\'number\'],[\'smart_momentum_pct\',\'15m momentum %\',\'number\'],[\'smart_momentum_lookback_minutes\',\'Momentum lookback minutes\',\'number\'],[\'smart_require_vwap\',\'Require VWAP\',\'checkbox\'],[\'smart_require_breakout\',\'Require breakout\',\'checkbox\'],[\'smart_require_market_up\',\'Require market up\',\'checkbox\'],[\'smart_breakout_lookback\',\'Breakout lookback\',\'number\']\n];\nlet MOBILE_LOGICS={};\nconst CONDITION_RANK_FIELDS=[[\'minimum_price\',\'Minimum price\'],[\'entry_time\',\'Entry time\'],[\'vwap\',\'VWAP\'],[\'orb15\',\'ORB15\'],[\'rvol\',\'Relative volume\'],[\'movement\',\'Movement\'],[\'investment\',\'Investment\'],[\'profit_target\',\'Profit target\'],[\'stop_loss\',\'Stop loss\'],[\'max_trades\',\'Maximum trades\'],[\'open_direction\',\'Direction\'],[\'price_big\',\'Maximum price\'],[\'price_small\',\'Minimum band price\'],[\'turnover\',\'Turnover\'],[\'volume\',\'Volume\']];\nfunction renderConditionRanks(){const root=document.getElementById(\'conditionRankControls\');if(!root)return;const first=MOBILE_LOGICS[Object.keys(MOBILE_LOGICS).find(k=>k.startsWith(\'Logic \'))]||{};const ranks=first.condition_ranks||{};root.innerHTML=CONDITION_RANK_FIELDS.map(([k,label])=>`<div class=\'logic-field\'><label for=\'rank_${k}\'>${logicEsc(label)} rank (0–15)</label><input id=\'rank_${k}\' type=\'number\' min=\'0\' max=\'15\' step=\'1\' value=\'${logicEsc(ranks[k]??0)}\'></div>`).join(\'\');}\nfunction readConditionRanks(){const out={};for(const [k] of CONDITION_RANK_FIELDS){const el=document.getElementById(\'rank_\'+k);if(el)out[k]=Number(el.value);}return out;}\nconst SHARED_MA_FIELDS=[[\'ma_filter_enabled\',\'Enable MA filter\'],[\'ma_filter_5\',\'5-period MA\'],[\'ma_filter_20\',\'20-period MA\'],[\'ma_filter_100\',\'100-period MA\'],[\'ma_filter_200\',\'200-period MA\'],[\'ma_filter_cross_5\',\'5-period MA cross\']];\nfunction renderSharedMA(){const root=document.getElementById(\'sharedMAControls\');if(!root)return;const first=MOBILE_LOGICS[Object.keys(MOBILE_LOGICS).find(k=>k.startsWith(\'Logic \'))]||{};root.innerHTML=SHARED_MA_FIELDS.map(([k,label])=>`<label class=\'logic-check\'><input id=\'shared_${k}\' type=\'checkbox\' ${first[k]?\'checked\':\'\'}><span>${logicEsc(label)}</span></label>`).join(\'\');}\nfunction readSharedMA(){const out={};for(const [k] of SHARED_MA_FIELDS){const el=document.getElementById(\'shared_\'+k);if(el)out[k]=!!el.checked;}return out;}\nconst SHARED_EXEC_FIELDS=[[\'weak_move_exit_enabled\',\'Weak-move exit\',\'checkbox\'],[\'weak_move_high_threshold\',\'Weak-move high threshold %\',\'number\'],[\'weak_move_stop\',\'Weak-move stop %\',\'number\'],[\'use_gtt_pretrigger\',\'Use GTT pretrigger\',\'checkbox\'],[\'gtt_pretrigger_gap\',\'GTT pretrigger gap %\',\'number\'],[\'gtt_limit_buffer\',\'GTT limit buffer %\',\'number\']];\nfunction renderSharedExecution(){const root=document.getElementById(\'sharedExecutionControls\');if(!root)return;const first=MOBILE_LOGICS[Object.keys(MOBILE_LOGICS).find(k=>k.startsWith(\'Logic \'))]||{};root.innerHTML=SHARED_EXEC_FIELDS.map(([k,label,type])=>logicField(\'SHARED\',k,label,type,first[k])).join(\'\');}\nfunction readSharedExecution(){const out={};for(const [k,,type] of SHARED_EXEC_FIELDS){const el=document.getElementById(\'lg_SHARED_\'+k);if(el)out[k]=type===\'checkbox\'?!!el.checked:Number(el.value||0);}return out;}\nfunction logicEsc(v){return esc(v==null?\'\':String(v));}\nfunction logicField(logic,key,label,type,value){const id=\'lg_\'+logic.replace(/\\s/g,\'_\')+\'_\'+key; if(type===\'checkbox\') return `<div class="logic-field"><label class="logic-check"><input id="${id}" type="checkbox" ${value?\'checked\':\'\'}> <span>${logicEsc(label)}</span></label></div>`; if(type.startsWith(\'select:\')){const opts=type.slice(7).split(\',\');return `<div class="logic-field"><label for="${id}">${logicEsc(label)}</label><select id="${id}">${opts.map(o=>`<option value="${o}" ${String(value||\'\')===o?\'selected\':\'\'}>${o}</option>`).join(\'\')}</select></div>`;} const step=type===\'number\'?\'any\':\'\';return `<div class="logic-field"><label for="${id}">${logicEsc(label)}</label><input id="${id}" type="${type}" ${step?`step="${step}"`:\'\'} value="${logicEsc(value)}"></div>`;}\nfunction renderMobileLogic(){const root=document.getElementById(\'logicCards\');if(!root)return;const names=Object.keys(MOBILE_LOGICS).filter(k=>k.startsWith(\'Logic \'));root.innerHTML=names.map(logic=>{const c=MOBILE_LOGICS[logic]||{};const fields=LOGIC_FIELDS.map(([k,l,t])=>logicField(logic,k,l,t,c[k])).join(\'\');return `<div class="logic-card"><div class="logic-title"><div><b>${logicEsc(logic)}</b><div class="logic-note">${logicEsc(c.name||\'Custom logic\')}</div></div><span class="logic-badge ${c.enabled?\'on\':\'\'}">${c.enabled?\'ENABLED\':\'DISABLED\'}</span></div><div class="logic-grid" style="margin-top:9px">${fields}</div><button class="logic-save" onclick="saveMobileLogic(\'${logic}\')">💾 SAVE ${logic}</button><button class="logic-reset" onclick="resetMobileLogic(\'${logic}\')">↺ RESET ${logic}</button></div>`}).join(\'\');}\nfunction readMobileLogic(logic){const c={...(MOBILE_LOGICS[logic]||{})};for(const [k,,t] of LOGIC_FIELDS){const el=document.getElementById(\'lg_\'+logic.replace(/\\s/g,\'_\')+\'_\'+k);if(!el)continue;if(t===\'checkbox\')c[k]=!!el.checked;else if(t===\'number\')c[k]=Number(el.value||0);else c[k]=el.value;}return c;}\nasync function loadMobileLogic(){const st=document.getElementById(\'logicStatus\');if(st)st.textContent=\'Loading canonical laptop logic…\';try{const x=await api(\'/logic\');MOBILE_LOGICS=x.logics||{};renderConditionRanks();renderSharedMA();renderSharedExecution();renderMobileLogic();if(st)st.innerHTML=\'<b>Synced.</b> Mobile and laptop use the same Config/logic_settings.json.\';}catch(e){if(st)st.innerHTML=\'<span class="logic-danger">\'+logicEsc(e.message||\'Unable to load logic\')+\'</span>\';}}\nasync function saveMobileLogic(logic){const st=document.getElementById(\'logicStatus\');const cfg=readMobileLogic(logic);if(!confirm(`Save ${logic} to the home laptop?\\n\\nThis changes the canonical trading logic used by the laptop.`))return;try{if(st)st.textContent=`Saving ${logic}…`;const x=await api(\'/logic\',\'POST\',{logic,config:cfg,condition_ranks:readConditionRanks(),shared_ma_filters:readSharedMA(),shared_execution_controls:readSharedExecution()});MOBILE_LOGICS=x.logics||MOBILE_LOGICS;renderConditionRanks();renderSharedMA();renderSharedExecution();renderMobileLogic();if(st)st.innerHTML=\'<b>Saved.</b> \'+logic+\' is now stored on the laptop.\';}catch(e){if(st)st.innerHTML=\'<span class="logic-danger">Save failed: \'+logicEsc(e.message||\'Unable to save\')+\'</span>\';}}\nasync function resetMobileLogic(logic){if(!confirm(`Reset ${logic} to the laptop\'s built-in default?`))return;try{const x=await api(\'/logic\',\'POST\',{logic,reset:true});MOBILE_LOGICS=x.logics||MOBILE_LOGICS;renderConditionRanks();renderSharedMA();renderSharedExecution();renderMobileLogic();const st=document.getElementById(\'logicStatus\');if(st)st.innerHTML=\'<b>Reset.</b> \'+logic+\' restored to default.\';}catch(e){const st=document.getElementById(\'logicStatus\');if(st)st.innerHTML=\'<span class="logic-danger">\'+logicEsc(e.message||\'Unable to reset\')+\'</span>\';}}\n\nasync function loadSmart(){try{const x=await api(\'/smart-validation\');const g=x.gate||{};const el=document.getElementById(\'smartGate\');if(el){el.innerHTML=(g.eligible?\'🟢 <b>LIVE Smart unlocked:</b> \':\'🟠 <b>LIVE Smart locked:</b> \')+logicEsc(g.reason||\'Paper validation required.\');el.style.borderColor=g.eligible?\'#1f7a4c\':\'#8a5a00\';}const dr=x.daily_result||{};const day=document.getElementById(\'smartDaily\');if(day){day.innerHTML=dr.date?`<b>Last market-close result • ${logicEsc(dr.date)}</b><br>Trades ${dr.closed_trades||0} • Win ${Number(dr.winrate||0).toFixed(1)}% • PF ${Number(dr.profit_factor||0).toFixed(2)} • Net ₹${Number(dr.net_pnl||0).toLocaleString(undefined,{maximumFractionDigits:2})}<br><span class="small">Auto-finalized at ${logicEsc(dr.completed_at||\'\')} • ${dr.open_positions_closed_at_market_close||0} position(s) closed at market close.</span>`:\'No completed market-close result yet.\';}for(const [id,val] of [[\'svTrades\',g.trades],[\'svWin\',(g.winrate||0).toFixed(1)+\'%\'],[\'svPf\',(g.pf||0).toFixed(2)],[\'svDd\',(g.dd||0).toFixed(2)+\'%\'],[\'svNet\',\'₹\'+Number(g.net||0).toLocaleString(undefined,{maximumFractionDigits:2})]]){const n=document.getElementById(id);if(n)n.textContent=val==null?\'—\':val;}const box=document.getElementById(\'smartIdeas\');const ideas=x.ideas||[];if(!ideas.length){box.innerHTML=\'<div class="empty">No qualifying Smart setup right now.</div>\';return;}box.innerHTML=ideas.slice(0,10).map((i,n)=>`<div class="order buy"><div class="order-top"><span class="order-symbol">#${n+1} ${logicEsc(i.symbol)}</span><span class="badge buy">SCORE ${logicEsc(i.score)}</span></div><div class="order-price">BUY ₹${Number(i.entry||0).toLocaleString(undefined,{minimumFractionDigits:2})}</div><div class="order-meta">Stop ₹${Number(i.stop||0).toLocaleString(undefined,{minimumFractionDigits:2})} • Target ₹${Number(i.target||0).toLocaleString(undefined,{minimumFractionDigits:2})}<br>R:R ${Number(i.rr||0).toFixed(2)} • Move ${Number(i.pct||0).toFixed(2)}% • Qty ${logicEsc(i.qty||0)}</div><div class="order-extra"><span class="chip">${logicEsc(i.logic||\'\')}</span><span class="chip">${logicEsc(i.setup||\'\')}</span></div></div>`).join(\'\');}catch(e){const el=document.getElementById(\'smartGate\');if(el)el.textContent=\'Smart validation unavailable: \'+(e.message||e);}}\n\nlet refreshBusy=false;\nasync function refreshAll(){if(refreshBusy)return;refreshBusy=true;const btn=document.getElementById("refreshBtn");if(btn){btn.disabled=true;btn.textContent="↻ Refreshing…";}try{await Promise.all([status(),files(),orders(),loadLog(\'historical\'),loadLog(\'trading\')]);const el=document.getElementById("lastRefresh");if(el)el.textContent=new Date().toLocaleTimeString([], {hour:"numeric",minute:"2-digit",second:"2-digit",hour12:true});}finally{refreshBusy=false;if(btn){btn.disabled=false;btn.textContent="↻ Refresh";}}}\nasync function manualRefresh(){await refreshAll()}\nasync function runHist(){try{let x=await api(\'/historical\',\'POST\',{start:document.getElementById(\'start\').value,end:document.getElementById(\'end\').value,interval:document.getElementById(\'interval\').value,share_limit:Number(document.getElementById(\'limit\').value)});alert(x.message||\'Started\');refreshAll()}catch(e){alert(e.message)}}\nasync function mobileMsg(text,isError=false){const el=document.getElementById(\'mobileActionMsg\');if(!el)return;el.style.display=\'block\';el.textContent=text;el.style.background=isError?\'#ffe8e8\':\'#eaf4ff\';el.style.color=isError?\'#a32626\':\'#204a68\'}\nasync function startPaper(){mobileMsg(\'Starting PAPER trading on the home laptop…\');try{let x=await api(\'/paper\',\'POST\',{});mobileMsg(x.message||\'Paper trading started.\');refreshAll()}catch(e){mobileMsg(e.message||\'Paper trading could not be started.\',true)}}\nasync function startLive(){if(!document.getElementById(\'liveConfirm\').checked){mobileMsg(\'Please tick the LIVE confirmation box first.\',true);return}if(!confirm(\'FINAL CONFIRMATION\\n\\nStart LIVE trading on your home laptop?\\n\\nThis can place REAL BUY and SELL orders through Breeze.\'))return;mobileMsg(\'Starting LIVE trading on the home laptop…\');try{let x=await api(\'/live\',\'POST\',{confirm:true,mobile_confirm:true});mobileMsg(x.message||\'LIVE trading started.\');refreshAll()}catch(e){mobileMsg(e.message||\'LIVE trading could not be started.\',true)}}\nasync function stopJob(){if(!confirm(\'Request a safe stop? For Paper/Live the laptop will perform its normal cleanup before stopping.\'))return;try{let x=await api(\'/stop\',\'POST\',{});alert(x.message||\'Stop requested\');refreshAll()}catch(e){alert(e.message)}}\nasync function sleepPC(){if(!confirm(\'Put the home laptop to sleep? Make sure no trading job is running.\'))return;try{await api(\'/sleep\',\'POST\',{});alert(\'Sleep requested.\')}catch(e){alert(e.message)}}\nrefreshAll();pollEarlyWarnings();setInterval(pollEarlyWarnings,5000);\n</script></body></html>\n'
APP_UPDATE_APK_CANDIDATES = [
    ROOT / 'AndroidApp' / 'BreezeTrading.apk',
    ROOT / 'BreezeTrading.apk',
]

def _load_app_update_meta():
    for p in APP_UPDATE_META_CANDIDATES:
        try:
            if p.exists():
                data=json.loads(p.read_text(encoding='utf-8'))
                if isinstance(data,dict):
                    data.setdefault('enabled', True)
                    data.setdefault('version_code', 0)
                    data.setdefault('version_name', '')
                    data.setdefault('mandatory', False)
                    data.setdefault('release_notes', '')
                    return data
        except Exception:
            continue
    return {'enabled':False,'version_code':0,'version_name':'','mandatory':False,'release_notes':'','apk_filename':'BreezeTrading.apk'}

DEFAULT = {
    'listen_host': '127.0.0.1',
    'listen_port': 8765,
    'token': '',
    'allow_live': False,
    'auto_sleep_after_job': False,
}

def load_cfg():
    cfg = DEFAULT.copy()
    if CONFIG.exists():
        try: cfg.update(json.loads(CONFIG.read_text(encoding='utf-8')))
        except Exception: pass
    if not cfg.get('token'):
        cfg['token'] = base64.urlsafe_b64encode(os.urandom(24)).decode().rstrip('=')
        CONFIG.write_text(json.dumps(cfg, indent=2), encoding='utf-8')
    return cfg

CFG = load_cfg()

def choose_listen_port(host, preferred):
    # Keep the normal 8765 port when available. If another process is using it,
    # automatically select the next free local port instead of failing startup.
    for port in range(int(preferred), int(preferred) + 6):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((host, port))
            s.close()
            return port
        except OSError:
            try: s.close()
            except Exception: pass
    raise RuntimeError(f'No free mobile-control port found from {preferred} to {int(preferred)+5}.')

LOCK = threading.Lock()
PROC = None
CURRENT_JOB = None
SERVER = None


def _tailscale_exe() -> str | None:
    candidates = [
        os.environ.get('TAILSCALE_EXE',''),
        r'C:\Program Files\Tailscale\tailscale.exe',
        r'C:\Program Files (x86)\Tailscale\tailscale.exe',
        'tailscale.exe',
        'tailscale',
    ]
    for item in candidates:
        if not item:
            continue
        try:
            if os.path.isabs(item) and not Path(item).exists():
                continue
            if os.path.isabs(item):
                return item
            r=subprocess.run([item,'version'],capture_output=True,text=True,timeout=3)
            if r.returncode == 0:
                return item
        except Exception:
            continue
    return None


def ensure_tailscale_ready(timeout=15) -> tuple[bool,str]:
    """Start the Tailscale service/app and return the active 100.x IPv4 address."""
    if os.name != 'nt':
        return False, ''
    try:
        subprocess.run(['sc.exe','start','Tailscale'],capture_output=True,text=True,timeout=5)
    except Exception:
        pass
    exe=_tailscale_exe()
    if exe:
        try:
            subprocess.Popen([exe,'up'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,
                             creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
        except Exception:
            pass
        # Start the desktop UI as well when installed; it does not block the agent.
        for gui in (r'C:\Program Files\Tailscale\tailscale-ipn.exe', r'C:\Program Files (x86)\Tailscale\tailscale-ipn.exe'):
            if Path(gui).exists():
                try:
                    subprocess.Popen([gui],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,
                                     creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
                except Exception:
                    pass
                break
    deadline=time.time()+max(1,timeout)
    while time.time()<deadline:
        if exe:
            try:
                r=subprocess.run([exe,'ip','-4'],capture_output=True,text=True,timeout=3)
                for line in (r.stdout or '').splitlines():
                    ip=line.strip().split()[0] if line.strip() else ''
                    if ip.startswith('100.'):
                        return True,ip
            except Exception:
                pass
        time.sleep(0.5)
    return False,''


def _read_order_feed(limit=100):
    rows=[]
    try:
        if not ORDER_FEED.exists():
            return rows
        lines=ORDER_FEED.read_text(encoding='utf-8',errors='replace').splitlines()
        for line in lines[-max(1,int(limit)):]:
            try:
                item=json.loads(line)
                if isinstance(item,dict): rows.append(item)
            except Exception:
                continue
    except Exception:
        pass
    rows.reverse()
    return rows


def _read_order_state():
    try:
        if not ORDER_STATE.exists(): return []
        data=json.loads(ORDER_STATE.read_text(encoding='utf-8',errors='replace'))
        rows=data.get('positions',[]) if isinstance(data,dict) else []
        return rows if isinstance(rows,list) else []
    except Exception:
        return []

def save_state(**kw):
    running = PROC is not None and PROC.poll() is None
    state = {
        'computer': os.environ.get('COMPUTERNAME','HOME-PC'),
        'online': True,
        'time': datetime.now().isoformat(timespec='seconds'),
        'job_running': running,
        'job_type': CURRENT_JOB.get('type') if isinstance(CURRENT_JOB,dict) else None,
        'mode': CURRENT_JOB.get('mode') if isinstance(CURRENT_JOB,dict) else 'IDLE',
    }
    state.update(kw)
    STATE.write_text(json.dumps(state, indent=2), encoding='utf-8')


def dpapi_unprotect(blob: bytes) -> bytes:
    class Blob(ctypes.Structure):
        _fields_=[('cbData',ctypes.c_ulong),('pbData',ctypes.POINTER(ctypes.c_byte))]
    buf=ctypes.create_string_buffer(blob); src=Blob(len(blob),ctypes.cast(buf,ctypes.POINTER(ctypes.c_byte))); out=Blob()
    if not ctypes.windll.crypt32.CryptUnprotectData(ctypes.byref(src),None,None,None,None,1,ctypes.byref(out)):
        raise ctypes.WinError()
    try: return ctypes.string_at(out.pbData,out.cbData)
    finally: ctypes.windll.kernel32.LocalFree(out.pbData)


def load_breeze_env():
    p = Path(os.environ.get('LOCALAPPDATA', str(ROOT))) / 'BreezeNSEHelper' / 'credentials.dat'
    if not p.exists():
        raise RuntimeError('Breeze credentials are not saved on this Windows user account.')
    data=json.loads(dpapi_unprotect(p.read_bytes()).decode('utf-8'))
    env=os.environ.copy()
    env.update({'BREEZE_API_KEY':data.get('api_key',''), 'BREEZE_API_SECRET':data.get('api_secret',''), 'BREEZE_SESSION_TOKEN':data.get('session_token','')})
    if not all(env[k] for k in ('BREEZE_API_KEY','BREEZE_API_SECRET','BREEZE_SESSION_TOKEN')):
        raise RuntimeError('Saved Breeze credentials are incomplete.')
    return env


def start_job(job):
    global PROC, CURRENT_JOB
    with LOCK:
        if PROC is not None and PROC.poll() is None:
            raise RuntimeError(f"{CURRENT_JOB.get('mode','Another job')} is already running.")
        if REMOTE_COMMAND.exists():
            try:
                old=json.loads(REMOTE_COMMAND.read_text(encoding='utf-8'))
                if old.get('status') in {'REQUESTED','RUNNING'}:
                    raise RuntimeError('A mobile command is already waiting or running on the home laptop.')
            except RuntimeError:
                raise
            except Exception:
                pass
        if job.get('type') == 'live' and not bool(CFG.get('allow_live', False)):
            raise RuntimeError('Mobile LIVE control is disabled in remote_config.json.')
        if STOP_FLAG.exists():
            try: STOP_FLAG.unlink()
            except Exception: pass
        command=dict(job)
        command.update({'request_id':base64.urlsafe_b64encode(os.urandom(12)).decode().rstrip('='),
                        'status':'REQUESTED','requested_at':datetime.now().isoformat(timespec='seconds')})
        REMOTE_COMMAND.write_text(json.dumps(command,indent=2),encoding='utf-8')
        CURRENT_JOB=dict(job)
        save_state(status='COMMAND SENT', started=command['requested_at'], job_running=True,
                   message=f"Mobile {job.get('mode',job.get('type','')).upper()} command sent to the home laptop GUI.")
        return command

def _watch_job(proc,out):
    global PROC, CURRENT_JOB
    code=proc.wait(); out.write(datetime.now().isoformat()+f' END code={code}\n'); out.close()
    with LOCK:
        PROC=None
        old=CURRENT_JOB
        CURRENT_JOB=None
    save_state(status='COMPLETED' if code==0 else 'ERROR', exit_code=code, last_job=old)


def stop_job():
    global PROC, CURRENT_JOB
    with LOCK:
        try:
            state=json.loads(STATE.read_text(encoding='utf-8')) if STATE.exists() else {}
        except Exception:
            state={}
        if PROC is None or PROC.poll() is not None:
            if bool(state.get('job_running')) or str(state.get('mode','')).upper() in {'PAPER','LIVE'}:
                STOP_FLAG.write_text('stop',encoding='utf-8')
                save_state(status='STOP REQUESTED', job_running=True, mode=state.get('mode','IDLE'), message='Mobile safe-stop requested; laptop trading engine is cleaning up.')
                return True, 'Stop requested on the home laptop. The trading engine will clean up safely.'
            return False, 'No Paper/Live job is running on the home laptop.'
        mode=(CURRENT_JOB or {}).get('type')
        if mode in {'live','paper'}:
            STOP_FLAG.write_text('stop',encoding='utf-8')
            save_state(status='STOP REQUESTED', message='Graceful market stop requested; engine is cleaning up safely.')
            return True, 'Stop requested. The market engine will clean up safely.'
        pid=PROC.pid
        if os.name=='nt': subprocess.run(['taskkill','/PID',str(pid),'/T','/F'],capture_output=True)
        else: PROC.terminate()
        return True, 'Job stopped.'

def auth(handler):
    # Write/destructive commands must carry the token in a header. Read-only GET
    # endpoints retain query-token compatibility for existing QR/download links,
    # but command endpoints never accept credentials from the URL.
    header_token=str(handler.headers.get('X-Remote-Token','')).strip()
    if header_token:
        return bool(CFG.get('token')) and hmac.compare_digest(header_token, str(CFG['token']).strip())
    path=urlparse(handler.path).path
    if getattr(handler,'command','GET') != 'GET' or path in {'/shutdown'}:
        return False
    try:
        query_token=parse_qs(urlparse(handler.path).query).get('token',[''])[0]
    except Exception:
        query_token=''
    return bool(query_token) and hmac.compare_digest(query_token, str(CFG.get('token','')).strip())



def _load_mobile_logics():
    from BreezeTrading import DEFAULT_LOGICS, load_logics
    cfg=load_logics()
    out={}
    for name in DEFAULT_LOGICS:
        base=dict(DEFAULT_LOGICS[name])
        base.update(cfg.get(name,{}))
        out[name]=base
    return out

def _save_mobile_logic(logic, incoming=None, reset=False, condition_ranks=None, shared_ma_filters=None, shared_execution_controls=None):
    import math
    from BreezeTrading import DEFAULT_LOGICS, CONDITION_RANK_DEFAULTS, load_logics, save_logics
    if logic not in DEFAULT_LOGICS:
        raise ValueError('Unknown logic. Select one of Logic 1 through Logic 8.')
    cfg=load_logics()
    if reset:
        _existing_ranks=dict(cfg.get(logic,{}).get('condition_ranks',{}))
        cfg[logic]=dict(DEFAULT_LOGICS[logic])
        if _existing_ranks: cfg[logic]['condition_ranks']=_existing_ranks
    else:
        if not isinstance(incoming,dict): raise ValueError('Logic configuration must be an object.')
        current=dict(cfg.get(logic, DEFAULT_LOGICS[logic]))
        allowed=set(DEFAULT_LOGICS[logic].keys())
        for k,v in incoming.items():
            if k in allowed: current[k]=v
        current['enabled']=bool(current.get('enabled',False))
        current['name']=str(current.get('name','')).strip()[:120] or DEFAULT_LOGICS[logic].get('name',logic)
        # Normalize numeric controls and reject non-finite values.
        numeric=['open_movement','minimum_price','price_big','price_small','turnover_cr','volume','buy_amount','profit_target','initial_stop','activation','trailing_stop','historical_shares','max_orders','gtt_pretrigger_gap','gtt_limit_buffer','weak_move_high_threshold','weak_move_stop','smart_min_score','smart_min_volume_ratio','smart_breakout_lookback','smart_momentum_lookback_minutes','smart_momentum_pct','smart_rank_limit']
        for k in numeric:
            try:
                val=float(current.get(k,0))
                if not math.isfinite(val): raise ValueError
                if k in ('volume','historical_shares','max_orders','smart_breakout_lookback','smart_momentum_lookback_minutes','smart_rank_limit'): val=int(max(0,round(val)))
                elif k in ('smart_min_score',): val=int(max(0,round(val)))
                else: val=max(0.0,val)
                current[k]=val
            except Exception: raise ValueError(f'Invalid numeric value for {k}.')
        if current.get('open_direction') not in ('BOTH','UP','DOWN'): raise ValueError('Direction must be BOTH, UP or DOWN.')
        if current.get('shared_universe_mode') not in ('ALGORITHM','STRICT','SHARED'): raise ValueError('Universe mode must be ALGORITHM, STRICT or SHARED.')
        current['smart_rank_by']=str(current.get('smart_rank_by','movement') or 'movement').strip().lower()
        if current['smart_rank_by'] not in ('movement','momentum_15m'): raise ValueError('Rank by must be movement or momentum_15m.')
        for k in ('buy_entry_time','buy_exit_time'):
            val=str(current.get(k,''))
            if len(val)!=5 or val[2] != ':' or not val.replace(':','').isdigit(): raise ValueError(f'Invalid time for {k}.')
        # SMART scoring/ranking is retired; the old `config` reference here was
        # undefined and caused every mobile save to fail with NameError.
        current['smart_mode'] = False
        for k in ('use_price_band','use_gtt_pretrigger','weak_move_exit_enabled','movement_only_mode','ma_filter_enabled','ma_filter_5','ma_filter_20','ma_filter_100','ma_filter_200','ma_filter_cross_5','smart_require_vwap','smart_require_breakout','smart_require_market_up'):
            current[k]=bool(current.get(k,False))
        cfg[logic]=current
    if condition_ranks is not None:
        if not isinstance(condition_ranks, dict):
            raise ValueError('Condition ranking must be an object.')
        normalized_ranks={}
        for key, default_rank in CONDITION_RANK_DEFAULTS.items():
            raw=condition_ranks.get(key, default_rank)
            try:
                number=float(raw)
                if not math.isfinite(number) or not number.is_integer(): raise ValueError
                rank=int(number)
            except Exception:
                raise ValueError(f'Ranking for {key} must be an integer from 0 to 15.')
            if rank < 0 or rank > 15:
                raise ValueError(f'Ranking for {key} must be from 0 to 15.')
            normalized_ranks[key]=rank
        active=[v for v in normalized_ranks.values() if v > 0]
        if len(active) != len(set(active)):
            raise ValueError('Non-zero condition ranks must be unique. Rank 0 may be repeated.')
        # The laptop GUI stores one shared priority order in every logic row.
        for row in cfg.values():
            if isinstance(row, dict): row['condition_ranks']=dict(normalized_ranks)
    if shared_ma_filters is not None:
        if not isinstance(shared_ma_filters, dict):
            raise ValueError('Shared moving-average settings must be an object.')
        _ma_keys=('ma_filter_enabled','ma_filter_5','ma_filter_20','ma_filter_100','ma_filter_200','ma_filter_cross_5')
        _ma_values={key:bool(shared_ma_filters.get(key,False)) for key in _ma_keys}
        for row in cfg.values():
            if isinstance(row, dict): row.update(_ma_values)
    if shared_execution_controls is not None:
        if not isinstance(shared_execution_controls, dict):
            raise ValueError('Shared exit/GTT settings must be an object.')
        _shared_bool=('weak_move_exit_enabled','use_gtt_pretrigger')
        _shared_num=('weak_move_high_threshold','weak_move_stop','gtt_pretrigger_gap','gtt_limit_buffer')
        _shared_values={key:bool(shared_execution_controls.get(key,False)) for key in _shared_bool}
        for key in _shared_num:
            try:
                value=float(shared_execution_controls.get(key,0))
                if not math.isfinite(value) or value < 0: raise ValueError
                _shared_values[key]=value
            except Exception:
                raise ValueError(f'Shared setting {key} must be a finite non-negative number.')
        if _shared_values['gtt_pretrigger_gap'] >= 100:
            raise ValueError('GTT pretrigger gap must be less than 100%.')
        for row in cfg.values():
            if isinstance(row, dict): row.update(_shared_values)
    cfg['_schema_version']=13
    save_logics(cfg)
    return _load_mobile_logics()

class Handler(BaseHTTPRequestHandler):
    def _send(self, code, obj, content='application/json'):
        body=json.dumps(obj).encode() if content=='application/json' else obj.encode()
        self.send_response(code); self.send_header('Content-Type',content); self.send_header('Content-Length',str(len(body))); self.send_header('X-Content-Type-Options','nosniff'); self.send_header('Cache-Control','no-store'); self.send_header('Referrer-Policy','no-referrer'); self.end_headers(); self.wfile.write(body)
    def do_GET(self):
        path=urlparse(self.path).path
        if path=='/':
            self._send(200,MOBILE_PAGE,'text/html; charset=utf-8'); return
        if path=='/health':
            return self._send(200,{'ok':True,'online':True,'time':datetime.now().isoformat(timespec='seconds'),'port':CFG.get('active_port',CFG.get('listen_port',8765))})
        if path=='/shutdown':
            if not auth(self): return self._send(401,{'error':'unauthorized'})
            self._send(200,{'ok':True,'message':'Mobile agent shutdown requested.'})
            server=SERVER
            if server is not None:
                threading.Thread(target=server.shutdown,daemon=True).start()
            return
        if path=='/status':
            if not auth(self): return self._send(401,{'error':'unauthorized'})
            try: state=json.loads(STATE.read_text())
            except Exception: state={'online':True,'status':'IDLE'}
            if PROC is not None:
                state['job_running']=PROC.poll() is None
                state['mode']=CURRENT_JOB.get('type','IDLE') if isinstance(CURRENT_JOB,dict) else state.get('mode','IDLE')
            else:
                state['job_running']=bool(state.get('job_running',False))
                state['mode']=state.get('mode','IDLE')
            state['server_time']=datetime.now().isoformat(timespec='seconds')
            return self._send(200,state)
        if path=='/files':
            if not auth(self): return self._send(401,{'error':'unauthorized'})
            files=[]
            for p in ROOT.rglob('*.xlsx'):
                try:
                    rel=p.relative_to(ROOT)
                    files.append({'name':str(rel).replace('\\','/'),'size':p.stat().st_size,'modified':datetime.fromtimestamp(p.stat().st_mtime).isoformat(timespec='seconds')})
                except Exception: pass
            return self._send(200,{'files':sorted(files,key=lambda x:x['modified'],reverse=True)[:20]})
        if path=='/logic':
            if not auth(self): return self._send(401,{'error':'unauthorized'})
            try: return self._send(200,{'ok':True,'logics':_load_mobile_logics()})
            except Exception as e: return self._send(500,{'error':str(e)})
        if path=='/process-log':
            if not auth(self): return self._send(401,{'error':'unauthorized'})
            q=parse_qs(urlparse(self.path).query)
            feed=str(q.get('feed',['trading'])[0]).lower()
            try: limit=min(1000,max(1,int(q.get('limit',['500'])[0])))
            except Exception: limit=500
            log_path=(ROOT/'Data'/'Mobile_Historical_Process_Log.jsonl') if feed=='historical' else (ROOT/'Data'/'Mobile_Live_Paper_Process_Log.jsonl')
            rows=[]
            if log_path.exists():
                try:
                    lines=log_path.read_text(encoding='utf-8').splitlines()[-limit:]
                    for line in lines:
                        try: rows.append(json.loads(line))
                        except Exception: pass
                except Exception: pass
            return self._send(200,{'feed':feed,'feed_name':'HISTORICAL DATA' if feed=='historical' else 'LIVE / PAPER TRADING','logs':rows,'updated':datetime.now().isoformat(timespec='seconds')})
        if path=='/smart-validation':
            if not auth(self): return self._send(401,{'error':'unauthorized'})
            # Smart Trade persistence is disabled; return an empty, in-memory status.
            gate={'eligible':False,'trades':0,'winrate':0.0,'pf':0.0,'dd':0.0,'net':0.0,
                  'reason':'Smart Trade validation persistence is disabled in this build.'}
            return self._send(200,{'ok':True,'gate':gate,'ideas':[],'daily_result':{}})
        if path=='/breeze-status':
            if not auth(self): return self._send(401,{'error':'unauthorized'})
            has_creds = False
            try:
                env = load_breeze_env()
                has_creds = bool(env.get('BREEZE_API_KEY') and env.get('BREEZE_SESSION_TOKEN'))
            except Exception:
                has_creds = False
            return self._send(200, {'ok': True, 'has_credentials': has_creds, 'server_time': datetime.now().isoformat()})
        if path=='/orders':
            if not auth(self): return self._send(401,{'error':'unauthorized'})
            return self._send(200,{'orders':_read_order_feed(2000),'live_positions':_read_order_state()})
        if path=='/app-update':
            if not auth(self): return self._send(401,{'error':'unauthorized'})
            meta=_load_app_update_meta()
            apk=next((p for p in APP_UPDATE_APK_CANDIDATES if p.exists()),None)
            meta['apk_available']=bool(apk)
            meta['apk_size']=apk.stat().st_size if apk else 0
            return self._send(200,meta)
        if path=='/app-update/apk':
            if not auth(self): return self._send(401,{'error':'unauthorized'})
            apk=next((p for p in APP_UPDATE_APK_CANDIDATES if p.exists()),None)
            if apk is None: return self._send(404,{'error':'APK update is not installed on the laptop yet.'})
            body=apk.read_bytes()
            self.send_response(200)
            self.send_header('Content-Type','application/vnd.android.package-archive')
            self.send_header('Content-Disposition','attachment; filename="BreezeTrading.apk"')
            self.send_header('Content-Length',str(len(body)))
            self.end_headers(); self.wfile.write(body); return
        if path.startswith('/download/'):
            if not auth(self): return self._send(401,{'error':'unauthorized'})
            name=unquote(path[len('/download/'):])
            if not name.lower().endswith('.xlsx') or name.startswith('/') or '..' in Path(name).parts:
                return self._send(400,{'error':'invalid file'})
            p=(ROOT/name).resolve()
            if not p.exists() or ROOT.resolve() not in p.parents: return self._send(404,{'error':'file not found'})
            body=p.read_bytes(); self.send_response(200); self.send_header('Content-Type','application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'); self.send_header('Content-Disposition',f'attachment; filename="{p.name}"'); self.send_header('Content-Length',str(len(body))); self.end_headers(); self.wfile.write(body); return
        self._send(404,{'error':'not found'})
    def do_POST(self):
        if not auth(self): return self._send(401,{'error':'unauthorized'})
        path=urlparse(self.path).path
        if path=='/shutdown':
            self._send(200,{'ok':True,'message':'Mobile agent shutdown requested.'})
            server=SERVER
            if server is not None:
                threading.Thread(target=server.shutdown,daemon=True).start()
            return
        try:
            n = int(self.headers.get('Content-Length', '0'))
        except (TypeError, ValueError):
            return self._send(400, {'error':'invalid Content-Length'})
        if n < 0 or n > 1_048_576:
            return self._send(413, {'error':'request body too large'})
        raw=self.rfile.read(n)
        try: data=json.loads(raw or b'{}')
        except Exception: return self._send(400,{'error':'invalid JSON'})
        try:
            if path=='/logic':
                logic=str(data.get('logic','')).strip()
                if data.get('reset') is True:
                    logics=_save_mobile_logic(logic, reset=True)
                    return self._send(200,{'ok':True,'message':f'{logic} reset.','logics':logics})
                logics=_save_mobile_logic(logic, incoming=data.get('config'), condition_ranks=data.get('condition_ranks'), shared_ma_filters=data.get('shared_ma_filters'), shared_execution_controls=data.get('shared_execution_controls'))
                return self._send(200,{'ok':True,'message':f'{logic} saved to the laptop.','logics':logics})
            if path=='/historical':
                for k in ('start','end'):
                    if not data.get(k): raise ValueError(f'Missing {k}.')
                _limit = int(data.get('share_limit', 0) or 0)
                start_job({'type':'historical','mode':'HISTORICAL','start':data['start'],'end':data['end'],'interval':data.get('interval','hybrid'),'share_limit':_limit})
                return self._send(200,{'ok':True,'message':'Historical job started.'})
            if path=='/paper':
                start_job({'type':'paper','mode':'PAPER'})
                return self._send(200,{'ok':True,'message':'Paper trading started on the home laptop.'})
            if path=='/live':
                if data.get('confirm') is not True: raise ValueError('LIVE confirmation is required.')
                start_job({'type':'live','mode':'LIVE'})
                return self._send(200,{'ok':True,'message':'LIVE trading started on the home laptop.'})
            if path=='/nse':
                for k in ('start','end'):
                    if not data.get(k): raise ValueError(f'Missing {k}.')
                start_job({'type':'nse','mode':'NSE','start':data['start'],'end':data['end']}); return self._send(200,{'ok':True})
            if path=='/stop':
                ok,msg=stop_job(); return self._send(200,{'ok':ok,'message':msg})
            if path=='/sleep':
                if PROC is not None and PROC.poll() is None: raise RuntimeError('Cannot sleep while a job is running.')
                subprocess.Popen(['rundll32.exe','powrprof.dll,SetSuspendState','0,1,0']); return self._send(200,{'ok':True})
            return self._send(404,{'error':'not found'})
        except Exception as e:
            return self._send(400,{'error':str(e)})
    def log_message(self,*args): pass


def main():
    save_state(status='STARTING', mode='IDLE')
    # Bring Tailscale up automatically before the mobile page is advertised.
    # If Tailscale is not installed/logged in, the local mobile server still starts;
    # the GUI will only advertise a Tailscale URL when a real 100.x address exists.
    try:
        ts_ok, ts_ip = ensure_tailscale_ready(timeout=20)
        CFG['tailscale_online'] = bool(ts_ok)
        CFG['tailscale_ip'] = ts_ip or ''
        CONFIG.write_text(json.dumps(CFG, indent=2), encoding='utf-8')
    except Exception:
        CFG['tailscale_online'] = False
        CFG['tailscale_ip'] = ''
    # Reuse the Tailscale result from the startup probe; do not probe/start twice.
    tailscale_ok=bool(CFG.get('tailscale_online',False))
    tailscale_ip=str(CFG.get('tailscale_ip','') or '')
    try:
        requested_port=int(CFG.get('listen_port',8765))
        active_port=choose_listen_port(CFG.get('listen_host','0.0.0.0'), requested_port)
        CFG['active_port']=active_port
        CONFIG.write_text(json.dumps(CFG, indent=2), encoding='utf-8')
        srv=ThreadingHTTPServer((CFG['listen_host'],active_port),Handler)
        global SERVER
        SERVER=srv
    except Exception as e:
        (ROOT/'remote_start_error.txt').write_text(str(e), encoding='utf-8')
        save_state(status='ERROR', mode='IDLE', error=str(e))
        raise
    save_state(status='IDLE', mode='IDLE', tailscale_online=tailscale_ok, tailscale_ip=tailscale_ip, active_port=active_port)
    print(f'Remote Agent listening on {CFG["listen_host"]}:{active_port}')
    if tailscale_ok:
        print(f'Tailscale ready: http://{tailscale_ip}:{active_port}/')
    else:
        print('Tailscale not ready; mobile page can still be used on LAN if reachable.')
    print('Mobile controls: Historical, Paper, Live, Stop, Sleep. Private VPN strongly recommended.')
    try:
        srv.serve_forever()
    finally:
        try: srv.server_close()
        except Exception: pass
        try: save_state(status='OFFLINE', mode='IDLE', online=False)
        except Exception: pass

if __name__=='__main__': main()
