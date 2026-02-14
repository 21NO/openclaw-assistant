#!/root/.openclaw/workspace/projects/bitcoin_trader_llm/venv/bin/python
import pyupbit
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
from pathlib import Path
import pytz

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / 'logs'
TRADE_CSV = LOG_DIR / 'backtest_trades_20260213_175157.csv'
BACKTEST_JSON = LOG_DIR / 'backtest_20260213_175157.json'

# parameters
SYMBOL = 'KRW-BTC'
RAW_INTERVAL = 'minutes15'
FETCH_COUNT = 200

# analysis windows (minutes)
W1 = 45  # 3 * 15min
W2 = 90  # 6 * 15min
W3 = 180 # 12 * 15min

# TP/SL base
TP_PCT = 0.015
SL_PCT = 0.008

# helper
def parse_dt(s):
    if s is None or (isinstance(s, float) and np.isnan(s)):
        return None
    if isinstance(s, pd.Timestamp):
        try:
            return s.to_pydatetime()
        except Exception:
            return s
    try:
        return datetime.fromisoformat(s)
    except Exception:
        try:
            return pd.to_datetime(s).to_pydatetime()
        except Exception:
            return None

# load trades
trades_df = pd.read_csv(TRADE_CSV)
# keep rows with pnl not null
trades_df = trades_df[trades_df['pnl'].notna()].copy()
trades_df['pnl'] = trades_df['pnl'].astype(float)
trades_df['entry_time_dt'] = trades_df['entry_time'].apply(parse_dt)
trades_df['exit_time_dt'] = trades_df['exit_time'].apply(parse_dt)

# read backtest json for start/end
with open(BACKTEST_JSON,'r',encoding='utf-8') as f:
    bt = json.load(f)
start = bt['result']['start']
end = bt['result']['end']
start_dt = datetime.fromisoformat(start)
end_dt = datetime.fromisoformat(end)
# expand window margins
fetch_start = start_dt - timedelta(days=2)
fetch_end = end_dt + timedelta(days=1)

print('Fetching OHLCV', SYMBOL, RAW_INTERVAL, 'from', fetch_start, 'to', fetch_end)

# fetch OHLCV in pages
all_chunks = []
to_dt = fetch_end
attempts = 0
while True:
    attempts += 1
    if attempts > 400:
        break
    to_str = to_dt = to_dt = to_dt
    try:
        to_str = to_dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        to_str = fetch_end.strftime('%Y-%m-%d %H:%M:%S')
    try:
        df = pyupbit.get_ohlcv(SYMBOL, interval=RAW_INTERVAL, count=FETCH_COUNT, to=to_str)
    except Exception as e:
        print('pyupbit.get_ohlcv failed:', e)
        break
    if df is None or df.empty:
        break
    df = df.reset_index().rename(columns={'index':'timestamp','open':'open','high':'high','low':'low','close':'close','volume':'volume'})
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    all_chunks.append(df)
    earliest = df['timestamp'].iloc[0]
    if earliest <= fetch_start:
        break
    to_dt = earliest - timedelta(seconds=1)

if not all_chunks:
    raise SystemExit('No OHLCV fetched')

big = pd.concat(all_chunks, ignore_index=True)
big = big.drop_duplicates(subset='timestamp').sort_values('timestamp').reset_index(drop=True)
mask = (big['timestamp'] >= fetch_start) & (big['timestamp'] <= fetch_end)
ohlcv15 = big.loc[mask].copy().reset_index(drop=True)
# normalize tz: make tz-naive
if pd.api.types.is_datetime64tz_dtype(ohlcv15['timestamp']):
    ohlcv15['timestamp'] = ohlcv15['timestamp'].dt.tz_convert('Asia/Seoul').dt.tz_localize(None)
else:
    # assume naive in local timezone already
    ohlcv15['timestamp'] = ohlcv15['timestamp']

ohlcv15.set_index('timestamp', inplace=True)

# resample to 30m and 60m
ohlcv30 = ohlcv15.resample('30min', label='right', closed='right').agg({'open':'first','high':'max','low':'min','close':'last','volume':'sum'}).dropna()
ohlcv60 = ohlcv15.resample('60min', label='right', closed='right').agg({'open':'first','high':'max','low':'min','close':'last','volume':'sum'}).dropna()

# resample 1H and 4H for HTF
ohlcv1h = ohlcv15.resample('60min', label='right', closed='right').agg({'open':'first','high':'max','low':'min','close':'last','volume':'sum'}).dropna()
ohlcv4h = ohlcv15.resample('240min', label='right', closed='right').agg({'open':'first','high':'max','low':'min','close':'last','volume':'sum'}).dropna()

# helper to get bars within minutes after a timestamp
def bars_within(df, ts, minutes):
    if ts is None:
        return pd.DataFrame()
    start = ts
    end = ts + timedelta(minutes=minutes)
    return df[(df.index > start) & (df.index <= end)]

# identify stop trades
stops = trades_df[trades_df['reason']=='stop'].copy()
# compute additional thresholds
stops['entry_price'] = stops['entry_price'].astype(float)
# compute top20 worst losses among losses
losses = trades_df[trades_df['pnl']<=0].copy()
losses['abs_loss'] = -losses['pnl']
losses_sorted = losses.sort_values('abs_loss', ascending=False).reset_index(drop=True)
N = len(trades_df)
num_top20 = max(1, int(round(N * 0.2)))
# careful: top20 should apply among all trades? user wants top20 손실군 - we'll use top20 of losses by abs loss
num_top20_losses = max(1, int(round(len(losses) * 0.2)))
# but earlier we treated top20 as 20% of all trades; user wants both definitions; we'll produce both: top20_by_trades and top20_of_losses
num_top20_by_trades = max(1, int(round(N * 0.2)))

# get top20 by absolute loss among losses
top20_losses = losses_sorted.head(num_top20_losses)
# Also top20_by_trades indices
losses_by_trade_sorted = losses.sort_values('abs_loss', ascending=False).reset_index(drop=True)
top20_by_trades = losses_by_trade_sorted.head(num_top20_by_trades)

# function to compute recovery metrics for a set of trades
def compute_recovery_metrics(trade_subset, ohlcv_df, windows_minutes=(W1,W2), thresholds_pct=(0.0,0.005,0.01)):
    rows = []
    for _, r in trade_subset.iterrows():
        entry_price = float(r['entry_price'])
        exit_time = r['exit_time_dt']
        if exit_time is None:
            continue
        # For each window, for each threshold check if any high >= entry_price*(1+threshold)
        res = {'entry_time': r['entry_time'], 'exit_time': r['exit_time'], 'entry_price': entry_price, 'amount': r['amount'], 'pnl': r['pnl'], 'exit_dt': exit_time}
        for w in windows_minutes:
            bars = bars_within(ohlcv_df, exit_time, w)
            highs = bars['high'].values if not bars.empty else np.array([])
            for th in thresholds_pct:
                target = entry_price * (1.0 + th)
                hit = bool((highs >= target).any())
                res[f'win_within_{w}min_thresh_{int(th*10000)/100:.2f}%'] = hit
        # also store hour (KST)
        # convert exit_time naive to KST timezone for hour grouping
        try:
            # assume exit_time naive in local KST; we'll treat as KST
            exit_kst = exit_time
            hour = exit_kst.hour
        except Exception:
            hour = None
        res['exit_hour_kst'] = hour
        rows.append(res)
    return pd.DataFrame(rows)

# compute recoveries on 15m, 30m, 60m by using respective df inputs
print('Computing recovery metrics on 15m...')
stops_metrics_15 = compute_recovery_metrics(stops, ohlcv15, windows_minutes=(W1,W2), thresholds_pct=(0.0,0.005,0.01))
print('Computing recovery metrics on 30m...')
stops_metrics_30 = compute_recovery_metrics(stops, ohlcv30, windows_minutes=(W1,W2), thresholds_pct=(0.0,0.005,0.01))
print('Computing recovery metrics on 60m...')
stops_metrics_60 = compute_recovery_metrics(stops, ohlcv60, windows_minutes=(W1,W2), thresholds_pct=(0.0,0.005,0.01))

# aggregate function
def aggregate_recovery(df_metrics):
    out = {}
    total = len(df_metrics)
    out['count'] = total
    for w in (W1,W2):
        for th in (0.0,0.005,0.01):
            col = f'win_within_{w}min_thresh_{int(th*10000)/100:.2f}%'
            out[f'{w}min_thresh_{int(th*10000)/100:.2f}%_rate'] = float(df_metrics[col].sum())/total if total>0 else None
    # by hour
    hour_groups = df_metrics.groupby('exit_hour_kst')
    hour_rates = {}
    for hour, g in hour_groups:
        hr = {}
        t = len(g)
        for w in (W1,W2):
            for th in (0.0,0.005,0.01):
                col = f'win_within_{w}min_thresh_{int(th*10000)/100:.2f}%'
                hr[f'{w}min_{int(th*10000)/100:.2f}%'] = float(g[col].sum())/t if t>0 else None
        hour_rates[int(hour) if hour is not None else 'na'] = hr
    out['by_hour'] = hour_rates
    return out

agg_15_all = aggregate_recovery(stops_metrics_15)
agg_15_top20 = aggregate_recovery(stops_metrics_15[stops_metrics_15['entry_time'].isin(top20_losses['entry_time'])]) if not top20_losses.empty else None

agg_30_all = aggregate_recovery(stops_metrics_30)
agg_30_top20 = aggregate_recovery(stops_metrics_30[stops_metrics_30['entry_time'].isin(top20_losses['entry_time'])]) if not top20_losses.empty else None

agg_60_all = aggregate_recovery(stops_metrics_60)
agg_60_top20 = aggregate_recovery(stops_metrics_60[stops_metrics_60['entry_time'].isin(top20_losses['entry_time'])]) if not top20_losses.empty else None

# TP-extension analysis
tp_trades = trades_df[trades_df['reason']=='take_profit'].copy()
tp_trades['entry_price'] = tp_trades['entry_price'].astype(float)

# for each tp trade compute within windows (45/90/180) whether max high reaches thresholds +0.5/+1/+2 relative to entry
delta_pts = [0.005, 0.01, 0.02]
windows = [W1, W2, W3]

def tp_extension_metrics(tp_subset, ohlcv_df):
    rows=[]
    for _, r in tp_subset.iterrows():
        entry_price = float(r['entry_price'])
        exit_time = r['exit_time_dt']
        if exit_time is None:
            continue
        res={'entry_time': r['entry_time'], 'exit_time': r['exit_time'], 'entry_price': entry_price, 'exit_dt': exit_time}
        for w in windows:
            bars = bars_within(ohlcv_df, exit_time, w)
            highs = bars['high'].values if not bars.empty else np.array([])
            max_high = float(np.max(highs)) if highs.size>0 else None
            # additional percent beyond TP (TP is entry_price*(1+TP_PCT))
            if max_high is not None:
                additional_pct = (max_high - entry_price*(1+TP_PCT)) / entry_price
            else:
                additional_pct = None
            res[f'max_additional_pct_within_{w}min'] = additional_pct
            for d in delta_pts:
                target = entry_price * (1+TP_PCT + d)
                hit = bool((highs >= target).any())
                res[f'hit_plus_{int(d*100)}bps_within_{w}min'] = hit
        rows.append(res)
    return pd.DataFrame(rows)

print('TP extension on 15m')
tp_ext_15 = tp_extension_metrics(tp_trades, ohlcv15)
print('TP extension on 30m')
tp_ext_30 = tp_extension_metrics(tp_trades, ohlcv30)
print('TP extension on 60m')
tp_ext_60 = tp_extension_metrics(tp_trades, ohlcv60)

# aggregate tp extension
def aggregate_tp_ext(df_ext):
    out={}
    total = len(df_ext)
    out['count']=total
    for w in windows:
        vals = df_ext[f'max_additional_pct_within_{w}min'].dropna()
        out[f'within_{w}min_mean_extra_pct'] = float(vals.mean()) if not vals.empty else None
        out[f'within_{w}min_median_extra_pct'] = float(vals.median()) if not vals.empty else None
        out[f'within_{w}min_90pct_extra_pct'] = float(vals.quantile(0.9)) if not vals.empty else None
        for d in delta_pts:
            col = f'hit_plus_{int(d*100)}bps_within_{w}min'
            out[f'within_{w}min_plus_{int(d*100)}bps_rate'] = float(df_ext[col].sum())/total if total>0 else None
    return out

agg_tp_15 = aggregate_tp_ext(tp_ext_15)
agg_tp_30 = aggregate_tp_ext(tp_ext_30)
agg_tp_60 = aggregate_tp_ext(tp_ext_60)

# Same-bar TP/SL conflict analysis
# For each trade that had both conditions true in the exit bar, count
# Build mapping of exit events: need entry_price, amount, exit_time
conflicts = []
fee_pct = float(bt['result'].get('fee_pct', 0.0005))
slippage_pct = float(bt['result'].get('slippage_pct', 0.001))

for _, r in trades_df.iterrows():
    if r['exit_time'] is None:
        continue
    entry_price = float(r['entry_price'])
    amount = float(r['amount']) if r['amount'] else None
    exit_time = parse_dt(r['exit_time'])
    # compute sl and tp
    sl_price = entry_price * (1.0 - SL_PCT)
    tp_price = entry_price * (1.0 + TP_PCT)
    # find the bar row corresponding to exit_time in ohlcv15
    try:
        bar = ohlcv15.loc[exit_time]
    except Exception:
        # try to find exact match or nearest
        bars = ohlcv15[ohlcv15.index == exit_time]
        if bars.empty:
            # find bar where index <= exit_time and > exit_time - 1min
            cand = ohlcv15.index.get_indexer([exit_time], method='nearest')
            try:
                bar = ohlcv15.iloc[cand[0]]
            except Exception:
                bar = None
    if bar is None or (not hasattr(bar,'high')):
        continue
    bar_high = float(bar['high'])
    bar_low = float(bar['low'])
    sl_hit = bar_low <= sl_price
    tp_hit = bar_high >= tp_price
    if sl_hit and tp_hit:
        # record conflict
        # actual pnl stored in dataframe row 'pnl'
        conflicts.append({'entry_time': r['entry_time'], 'exit_time': r['exit_time'], 'entry_price': entry_price, 'amount': amount, 'actual_pnl': float(r['pnl']) if not pd.isna(r['pnl']) else None, 'bar_high': bar_high, 'bar_low': bar_low, 'sl_price': sl_price, 'tp_price': tp_price})

num_conflicts = len(conflicts)
# compute average pnl of those trades
avg_conflict_pnl = float(np.mean([c['actual_pnl'] for c in conflicts])) if conflicts else None

# estimate PF if conflicts were TP-first instead of stop-first
# Build baseline gross profit/loss
wins_df = trades_df[trades_df['pnl']>0]
losses_df = trades_df[trades_df['pnl']<=0]
base_gp = float(wins_df['pnl'].sum())
base_gl = float(-losses_df['pnl'].sum())
# compute alt gp/gl by replacing conflict trades pnl with tp-first formula
alt_gp = base_gp
alt_gl = base_gl
for c in conflicts:
    # find trade in trades_df
    row = trades_df[(trades_df['entry_time']==c['entry_time']) & (trades_df['exit_time']==c['exit_time'])]
    if row.empty:
        continue
    row = row.iloc[0]
    # compute alt exit price as tp_price*(1 - slippage_pct)
    tp_exit_price = c['tp_price'] * (1.0 - slippage_pct)
    alt_exit_fee = tp_exit_price * c['amount'] * fee_pct
    alt_pnl = (tp_exit_price - c['entry_price']) * c['amount'] - alt_exit_fee
    # remove actual pnl contribution
    actual_pnl = float(row['pnl'])
    if actual_pnl > 0:
        alt_gp -= actual_pnl
    else:
        alt_gl -= -actual_pnl
    # add alt
    if alt_pnl > 0:
        alt_gp += alt_pnl
    else:
        alt_gl += -alt_pnl

alt_pf = (alt_gp/alt_gl) if alt_gl>0 else None

# HTF mismatch analysis for top20 vs all
# compute HTF indicators: ADX and EMA on ohlcv1h and ohlcv4h using ta
import ta

def compute_htf_features(ohlcv_df):
    df = ohlcv_df.copy()
    df['ema9'] = df['close'].ewm(span=9,adjust=False).mean()
    df['ema50'] = df['close'].ewm(span=50,adjust=False).mean()
    # ADX using ta
    try:
        from ta.trend import ADXIndicator
        adx = ADXIndicator(high=df['high'], low=df['low'], close=df['close'], window=14)
        df['adx14'] = adx.adx()
    except Exception:
        df['adx14'] = np.nan
    return df

htf1 = compute_htf_features(ohlcv1h)
htf4 = compute_htf_features(ohlcv4h)

# function to map HTF features to trade entry
def map_htf(trade_row):
    et = trade_row['entry_time_dt']
    if et is None:
        return {'htf1_ema9_gt_50': None, 'htf1_adx': None, 'htf4_ema9_gt_50': None, 'htf4_adx': None, 'vol_ratio_15': None}
    # find last available 1h row <= et
    try:
        row1 = htf1.loc[htf1.index <= et].iloc[-1]
    except Exception:
        row1 = None
    try:
        row4 = htf4.loc[htf4.index <= et].iloc[-1]
    except Exception:
        row4 = None
    # volume ratio from 15m features: last volume / vol_ma20
    try:
        vol_last = ohlcv15.loc[et]['volume']
    except Exception:
        # take last bar <= et
        try:
            vol_last = ohlcv15.loc[ohlcv15.index <= et].iloc[-1]['volume']
        except Exception:
            vol_last = None
    # compute vol_ma20 via rolling on 15min
    try:
        vol_ma20 = ohlcv15['volume'].rolling(window=20).mean().loc[ohlcv15.index <= et].iloc[-1]
    except Exception:
        vol_ma20 = None
    vol_ratio = float(vol_last/vol_ma20) if vol_last is not None and vol_ma20 and vol_ma20>0 else None
    return {
        'htf1_ema9_gt_50': bool(row1['ema9'] > row1['ema50']) if row1 is not None else None,
        'htf1_adx': float(row1['adx14']) if row1 is not None and not pd.isna(row1['adx14']) else None,
        'htf4_ema9_gt_50': bool(row4['ema9'] > row4['ema50']) if row4 is not None else None,
        'htf4_adx': float(row4['adx14']) if row4 is not None and not pd.isna(row4['adx14']) else None,
        'vol_ratio_15': vol_ratio
    }

# attach HTF features for all losses and top20
losses_df = losses.copy()
losses_df['htf1_ema9_gt_50']=None
losses_df['htf1_adx']=None
losses_df['htf4_ema9_gt_50']=None
losses_df['htf4_adx']=None
losses_df['vol_ratio_15']=None

for i,r in losses_df.iterrows():
    feats = map_htf(r)
    for k,v in feats.items():
        losses_df.at[i,k]=v

# top20_by_trades selection
# define top20_by_trades earlier based on total trades count
sel_top20_idx = top20_by_trades['entry_time'].values if not top20_by_trades.empty else []

# aggregate comparison
def aggregate_top_vs_all(losses_df, top_idx_times):
    out={}
    out['all_count']=len(losses_df)
    out['top_count']=len(top_idx_times)
    # HTF mismatch: where htf1_ema9_gt_50 is False (i.e., not supporting long)
    out['all_htf1_mismatch_rate']=float((losses_df['htf1_ema9_gt_50']==False).sum())/len(losses_df) if len(losses_df)>0 else None
    out['top_htf1_mismatch_rate']=float((losses_df[losses_df['entry_time'].isin(top_idx_times)]['htf1_ema9_gt_50']==False).sum())/len(top_idx_times) if len(top_idx_times)>0 else None
    # low volume rate (vol_ratio < 1)
    out['all_low_vol_rate']=float((losses_df['vol_ratio_15']<1.0).sum())/len(losses_df) if len(losses_df)>0 else None
    out['top_low_vol_rate']=float((losses_df[losses_df['entry_time'].isin(top_idx_times)]['vol_ratio_15']<1.0).sum())/len(top_idx_times) if len(top_idx_times)>0 else None
    # ADX buckets
    out['all_htf4_adx_mean']=float(losses_df['htf4_adx'].dropna().mean()) if not losses_df['htf4_adx'].dropna().empty else None
    out['top_htf4_adx_mean']=float(losses_df[losses_df['entry_time'].isin(top_idx_times)]['htf4_adx'].dropna().mean()) if len(top_idx_times)>0 else None
    # hour distribution for top
    out['top_hours']=losses_df[losses_df['entry_time'].isin(top_idx_times)]['entry_time'].apply(parse_dt).dropna().apply(lambda x: x.hour).value_counts().to_dict()
    out['all_hours']=losses_df['entry_time'].apply(parse_dt).dropna().apply(lambda x: x.hour).value_counts().to_dict()
    return out

agg_top_vs_all = aggregate_top_vs_all(losses_df, sel_top20_idx)

# prepare output JSON
output = {
    'recovery': {
        '15min_all': agg_15_all,
        '15min_top20_losses': agg_15_top20,
        '30min_all': agg_30_all,
        '30min_top20_losses': agg_30_top20,
        '60min_all': agg_60_all,
        '60min_top20_losses': agg_60_top20
    },
    'tp_extension': {
        '15min': agg_tp_15,
        '30min': agg_tp_30,
        '60min': agg_tp_60
    },
    'same_bar_conflicts': {
        'count': num_conflicts,
        'avg_conflict_pnl': avg_conflict_pnl,
        'alt_pf_if_tp_first': alt_pf
    },
    'top20_vs_all_htf': agg_top_vs_all
}

OUTPATH = LOG_DIR / 'poststop_tp_report.json'
with open(OUTPATH,'w',encoding='utf-8') as f:
    json.dump(output, f, indent=2, ensure_ascii=False, default=str)

print('Report written to', OUTPATH)
print(json.dumps(output, indent=2, ensure_ascii=False))
