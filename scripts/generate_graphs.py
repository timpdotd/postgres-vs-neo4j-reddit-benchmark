import json, math
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd
import seaborn as sns

# ── Catppuccin Mocha dark theme ──────────────────────────────────────────────
BG      = '#1e1e2e'
SURFACE = '#313244'
TEXT    = '#cdd6f4'
GRID    = '#45475a'
PG_C    = '#89b4fa'  # blue
NEO_C   = '#a6e3a1'  # green
RED_C   = '#f38ba8'
YELLOW  = '#f9e2af'
LAVENDER= '#b4befe'

plt.rcParams.update({
    'figure.facecolor':  BG,
    'axes.facecolor':    BG,
    'axes.edgecolor':    GRID,
    'axes.labelcolor':   TEXT,
    'text.color':        TEXT,
    'xtick.color':       TEXT,
    'ytick.color':       TEXT,
    'grid.color':        GRID,
    'grid.alpha':        0.4,
    'legend.facecolor':  SURFACE,
    'legend.edgecolor':  GRID,
    'font.family':       'DejaVu Sans',
    'figure.dpi':        120,
})


OUT_DIR = Path('../graphs')
OUT_DIR.mkdir(exist_ok=True)

RESULTS_PATH = Path('..') / 'data' / 'benchmark_results.json'
print(f'Loading: {RESULTS_PATH.resolve()}')
# ── Load & normalise results ─────────────────────────────────────────────────
with open(RESULTS_PATH) as f:
    raw = json.load(f)

df = pd.DataFrame(raw)
# Ensure optional columns exist to prevent KeyErrors when a DB is skipped
for col in ['tier', 'median_execution_ms', 'cold_ms', 'stdev_execution_ms', 'cv_pct', 'buffer_hit_ratio', 'total_buffer_hits', 'total_buffer_reads', 'median_planning_ms', 'median_available_ms', 'median_consumed_ms', 'db_hits']:
    if col not in df.columns:
        df[col] = pd.NA


# Convenience columns
df['db_label']        = df['db'].map({'postgresql': 'PostgreSQL', 'neo4j': 'Neo4j'})
df['median_ms']       = df['median_execution_ms'].astype(float)
df['cold_ms_f']       = df['cold_ms'].astype(float)
df['stdev_ms']        = df['stdev_execution_ms'].fillna(0).astype(float)
df['cv_pct_f']        = df['cv_pct'].fillna(0).astype(float)
df['has_error']       = df.get('error', pd.Series([None]*len(df))).notna()

QUERY_IDS = sorted(df['query_id'].unique())
print(f'{len(df)} result rows | {df["db"].nunique()} databases | {len(QUERY_IDS)} queries')
df[['query_id','tier','db_label','median_ms','cold_ms_f','cv_pct_f','result_count']].to_string(index=False)
def _get(df, qid, db, col):
    sub = df[(df.query_id == qid) & (df.db == db)]
    return float(sub[col].values[0]) if len(sub) and pd.notna(sub[col].values[0]) else 0

pg_med  = [_get(df, q, 'postgresql', 'median_ms') for q in QUERY_IDS]
neo_med = [_get(df, q, 'neo4j',      'median_ms') for q in QUERY_IDS]
pg_err  = [_get(df, q, 'postgresql', 'stdev_ms')  for q in QUERY_IDS]
neo_err = [_get(df, q, 'neo4j',      'stdev_ms')  for q in QUERY_IDS]

x = np.arange(len(QUERY_IDS))
w = 0.38

fig, ax = plt.subplots(figsize=(13, 5))
b1 = ax.bar(x - w/2, pg_med,  w, yerr=pg_err,  label='PostgreSQL', color=PG_C,  alpha=0.9, capsize=4, error_kw={'ecolor': TEXT, 'alpha':0.6})
b2 = ax.bar(x + w/2, neo_med, w, yerr=neo_err, label='Neo4j',      color=NEO_C, alpha=0.9, capsize=4, error_kw={'ecolor': TEXT, 'alpha':0.6})

for bar, val in [(b, v) for bars, vals in [(b1, pg_med),(b2, neo_med)] for b, v in zip(bars, vals)]:
    if val > 0:
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(pg_med+neo_med)*0.01,
                f'{val:.0f}', ha='center', va='bottom', fontsize=7.5, color=TEXT)

ax.set_xticks(x); ax.set_xticklabels(QUERY_IDS)
ax.set_ylabel('Median execution time (ms)')
ax.set_title('Chart 1 — Median Execution Time per Query  [error bars = ±1 stdev]', fontweight='bold', pad=12)
ax.legend(); ax.set_axisbelow(True); ax.yaxis.grid(True)
ax.set_xlabel('Query ID  (T1=trivial → T5=very hard)')
plt.tight_layout(); plt.savefig(OUT_DIR / f'chart_1.png', dpi=300, bbox_inches='tight') 
tier_df = df.groupby(['tier','db'])['median_ms'].median().reset_index()

fig, ax = plt.subplots(figsize=(9, 4))
for db, color, label in [('postgresql', PG_C, 'PostgreSQL'), ('neo4j', NEO_C, 'Neo4j')]:
    sub = tier_df[tier_df.db == db].sort_values('tier')
    if sub.empty: continue
    ax.plot(sub['tier'], sub['median_ms'], marker='o', lw=2.2, color=color, label=label)
    for _, row in sub.iterrows():
        ax.annotate(f"{row['median_ms']:.0f}ms", (row['tier'], row['median_ms']),
                    xytext=(0,9), textcoords='offset points', ha='center', fontsize=8, color=color)

ax.set_xlabel('Tier  (1 = trivial → 5 = very hard)')
ax.set_ylabel('Median time within tier (ms)')
ax.set_xticks([1,2,3,4,5])
ax.set_xticklabels(['T1\nTrivial','T2\nEasy','T3\nMedium','T4\nHard','T5\nVery Hard'])
ax.set_title('Chart 2 — Scaling: Execution Time vs. Query Complexity Tier', fontweight='bold', pad=12)
ax.legend(); ax.set_axisbelow(True); ax.yaxis.grid(True)
plt.tight_layout(); plt.savefig(OUT_DIR / f'chart_2.png', dpi=300, bbox_inches='tight') 
fig, axes = plt.subplots(1, 2, figsize=(14, 4.5), sharey=False)

for ax, db, color, label in [
    (axes[0], 'postgresql', PG_C,  'PostgreSQL'),
    (axes[1], 'neo4j',      NEO_C, 'Neo4j'),
]:
    sub = df[df.db == db].sort_values('query_id')
    if sub.empty: continue
    ids   = sub['query_id'].tolist()
    cold  = sub['cold_ms_f'].tolist()
    warm  = sub['median_ms'].tolist()
    x = np.arange(len(ids))
    ax.bar(x - 0.2, cold, 0.38, label='Cold (1st run)', color=RED_C,  alpha=0.8)
    ax.bar(x + 0.2, warm, 0.38, label='Warm (median)', color=color, alpha=0.9)
    ax.set_xticks(x); ax.set_xticklabels(ids, fontsize=9)
    ax.set_ylabel('Time (ms)'); ax.set_title(f'{label} — Cold vs Warm', fontweight='bold')
    ax.legend(); ax.set_axisbelow(True); ax.yaxis.grid(True)

fig.suptitle('Chart 3 — Cold Cache vs. Warm Cache (1st run vs median of 5 warm runs)', fontweight='bold', y=1.02)
plt.tight_layout(); plt.savefig(OUT_DIR / f'chart_3.png', dpi=300, bbox_inches='tight') 
# Explode per-run times
records = []
for _, row in df.iterrows():
    times = row.get('warm_execution_ms') or []
    for t in (times if isinstance(times, list) else []):
        if isinstance(t, (int, float)) and not math.isnan(t):
            records.append({'query_id': row['query_id'], 'db': row['db_label'], 'time_ms': t})
df_runs = pd.DataFrame(records)

fig, axes = plt.subplots(2, 5, figsize=(16, 7))
axes = axes.flatten()
for i, qid in enumerate(QUERY_IDS):
    ax = axes[i]
    sub = df_runs[df_runs.query_id == qid]
    pg_t  = sub[sub.db == 'PostgreSQL']['time_ms'].tolist()
    neo_t = sub[sub.db == 'Neo4j']['time_ms'].tolist()
    bp = ax.boxplot([pg_t or [0], neo_t or [0]], labels=['PG', 'Neo4j'],
                    patch_artist=True, widths=0.5,
                    medianprops={'color': RED_C, 'linewidth': 2})
    for patch, color in zip(bp['boxes'], [PG_C, NEO_C]):
        patch.set_facecolor(color); patch.set_alpha(0.8)
    ax.set_title(qid, fontweight='bold', fontsize=9)
    ax.set_ylabel('ms' if i % 5 == 0 else '')
    ax.yaxis.grid(True); ax.set_axisbelow(True)

for j in range(len(QUERY_IDS), len(axes)):
    axes[j].set_visible(False)

fig.suptitle('Chart 4 — Timing Variability (box plots, 5 warm runs each)', fontweight='bold', y=1.01)
plt.tight_layout(); plt.savefig(OUT_DIR / f'chart_4.png', dpi=300, bbox_inches='tight') 
ratios, ids = [], []
for qid in QUERY_IDS:
    pg_row  = df[(df.query_id == qid) & (df.db == 'postgresql')]
    neo_row = df[(df.query_id == qid) & (df.db == 'neo4j')]
    if pg_row.empty or neo_row.empty: continue
    pg_t  = pg_row['median_ms'].values[0]
    neo_t = neo_row['median_ms'].values[0]
    if pd.notna(pg_t) and pd.notna(neo_t) and neo_t > 0:
        ratios.append(pg_t / neo_t)
        ids.append(qid)

colors = [NEO_C if r > 1 else PG_C for r in ratios]
fig, ax = plt.subplots(figsize=(9, 4.5))
bars = ax.barh(ids, ratios, color=colors, alpha=0.9, height=0.6)
ax.axvline(1.0, color=RED_C, lw=1.8, ls='--', label='Parity (1.0×)')
for bar, r in zip(bars, ratios):
    ax.text(r + 0.03, bar.get_y() + bar.get_height()/2,
            f'{r:.2f}×', va='center', fontsize=9, color=TEXT)
ax.set_xlabel('Ratio = PG exec / Neo4j consumed   (>1 → Neo4j faster,  <1 → PG faster)')
ax.set_title('Chart 5 — Speedup Ratio per Query', fontweight='bold', pad=12)
pg_patch  = mpatches.Patch(color=PG_C,  label='PostgreSQL faster')
neo_patch = mpatches.Patch(color=NEO_C, label='Neo4j faster')
ax.legend(handles=[pg_patch, neo_patch, plt.Line2D([],[],color=RED_C, ls='--', label='Parity')])
ax.set_axisbelow(True); ax.xaxis.grid(True)
plt.tight_layout(); plt.savefig(OUT_DIR / f'chart_5.png', dpi=300, bbox_inches='tight') 
pg_df = df[df.db == 'postgresql'].copy().sort_values('query_id')
has_buf = pg_df['buffer_hit_ratio'].notna()

if has_buf.any():
    sub = pg_df[has_buf]
    hits   = sub['total_buffer_hits'].fillna(0).astype(float).tolist()
    reads  = sub['total_buffer_reads'].fillna(0).astype(float).tolist()
    ratios = sub['buffer_hit_ratio'].astype(float).tolist()
    ids_pg = sub['query_id'].tolist()
    x = np.arange(len(ids_pg))

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 4.5))

    # Stacked bar: hits vs reads
    ax1.bar(x, hits,  0.55, label='Cache Hits (shared_buffers)',  color=PG_C,   alpha=0.9)
    ax1.bar(x, reads, 0.55, bottom=hits, label='Disk Reads (cache miss)', color=RED_C, alpha=0.8)
    ax1.set_xticks(x); ax1.set_xticklabels(ids_pg)
    ax1.set_ylabel('Total buffer blocks (all 5 runs combined)')
    ax1.set_title('Buffer Blocks: Hits vs Disk Reads', fontweight='bold')
    ax1.legend(); ax1.yaxis.grid(True); ax1.set_axisbelow(True)

    # Hit ratio line
    colors_r = [PG_C if r >= 0.95 else YELLOW if r >= 0.70 else RED_C for r in ratios]
    ax2.bar(x, [r*100 for r in ratios], 0.55, color=colors_r, alpha=0.9)
    ax2.axhline(100, color=GRID, lw=1, ls='--')
    ax2.axhline(95,  color=YELLOW, lw=1, ls=':', label='95% threshold')
    for xi, r in zip(x, ratios):
        ax2.text(xi, r*100 + 0.5, f'{r*100:.1f}%', ha='center', va='bottom', fontsize=8, color=TEXT)
    ax2.set_xticks(x); ax2.set_xticklabels(ids_pg)
    ax2.set_ylim(0, 105)
    ax2.set_ylabel('Cache hit rate (%)')
    ax2.set_title('Buffer Cache Hit Rate per Query', fontweight='bold')
    ax2.legend(); ax2.yaxis.grid(True); ax2.set_axisbelow(True)

    fig.suptitle('Chart 6 — PostgreSQL Buffer Statistics (shared_buffers, summed over 5 warm runs)',
                 fontweight='bold', y=1.01)
    plt.tight_layout(); plt.savefig(OUT_DIR / f'chart_6.png', dpi=300, bbox_inches='tight') 
else:
    print('No buffer stats available (run with PostgreSQL enabled).')
pg_df2 = df[df.db == 'postgresql'].copy().sort_values('query_id')
has_plan = pg_df2['median_planning_ms'].notna()

if has_plan.any():
    sub      = pg_df2[has_plan]
    exec_t   = sub['median_ms'].tolist()
    plan_t   = sub['median_planning_ms'].astype(float).tolist()
    ids_pg2  = sub['query_id'].tolist()
    x = np.arange(len(ids_pg2))

    fig, ax = plt.subplots(figsize=(11, 4.5))
    b_exec = ax.bar(x, exec_t, 0.55, label='Execution', color=PG_C,   alpha=0.9)
    b_plan = ax.bar(x, plan_t, 0.55, bottom=exec_t, label='Planning', color=LAVENDER, alpha=0.85)

    for xi, (e, p) in enumerate(zip(exec_t, plan_t)):
        total = e + p
        pct = p / total * 100 if total > 0 else 0
        ax.text(xi, total + max(exec_t)*0.01, f'{pct:.0f}% plan',
                ha='center', va='bottom', fontsize=7.5, color=LAVENDER)

    ax.set_xticks(x); ax.set_xticklabels(ids_pg2)
    ax.set_ylabel('Time (ms)')
    ax.set_title('Chart 7 — PostgreSQL: Planning vs Execution Time (median warm runs)',
                 fontweight='bold', pad=12)
    ax.legend(); ax.yaxis.grid(True); ax.set_axisbelow(True)
    plt.tight_layout(); plt.savefig(OUT_DIR / f'chart_7.png', dpi=300, bbox_inches='tight') 
else:
    print('No planning time data available.')
neo_df = df[df.db == 'neo4j'].copy().sort_values('query_id')
has_avail = neo_df['median_available_ms'].notna()

if has_avail.any():
    sub      = neo_df[has_avail]
    consumed = sub['median_consumed_ms'].astype(float).tolist()
    available= sub['median_available_ms'].astype(float).tolist()
    transfer = [max(c - a, 0) for c, a in zip(consumed, available)]
    ids_neo  = sub['query_id'].tolist()
    x = np.arange(len(ids_neo))

    fig, ax = plt.subplots(figsize=(11, 4.5))
    ax.bar(x, available, 0.55, label='Server exec (available_after)',  color=NEO_C,  alpha=0.9)
    ax.bar(x, transfer,  0.55, bottom=available, label='Network transfer (consumed − available)', color=YELLOW, alpha=0.85)

    for xi, (a, tr) in enumerate(zip(available, transfer)):
        total = a + tr
        if total > 0:
            pct = tr / total * 100
            ax.text(xi, total + max(consumed)*0.01, f'{pct:.0f}% net',
                    ha='center', va='bottom', fontsize=7.5, color=YELLOW)

    ax.set_xticks(x); ax.set_xticklabels(ids_neo)
    ax.set_ylabel('Time (ms)')
    ax.set_title('Chart 8 — Neo4j: Server Execution vs Network Transfer Overhead',
                 fontweight='bold', pad=12)
    ax.legend(); ax.yaxis.grid(True); ax.set_axisbelow(True)
    plt.tight_layout(); plt.savefig(OUT_DIR / f'chart_8.png', dpi=300, bbox_inches='tight') 
else:
    print('No available_after data (requires Neo4j benchmarks).')
# Note: db_hits (Neo4j) ≠ buffer reads (PG) — different units, but both measure
# internal storage access cost. Shown together for comparison of access patterns.

pg_hits_df  = df[df.db == 'postgresql'][['query_id','total_buffer_hits','total_buffer_reads']].copy()
neo_hits_df = df[df.db == 'neo4j'][['query_id','db_hits']].copy()

if neo_hits_df['db_hits'].notna().any():
    merged = pg_hits_df.merge(neo_hits_df, on='query_id', how='outer').sort_values('query_id')
    ids_m  = merged['query_id'].tolist()
    x = np.arange(len(ids_m))
    w = 0.38

    pg_buf = merged['total_buffer_hits'].fillna(0).astype(float).tolist()
    pg_rd  = merged['total_buffer_reads'].fillna(0).astype(float).tolist()
    neo_db = merged['db_hits'].fillna(0).astype(float).tolist()

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 4.5))

    # PG: total buffer accesses (hits + reads)
    total_pg = [h + r for h, r in zip(pg_buf, pg_rd)]
    ax1.bar(x - w/2, total_pg, w*1.8, color=PG_C,  alpha=0.9, label='PG total buffer blocks')
    ax1.bar(x + w/2, neo_db,   w*1.8, color=NEO_C, alpha=0.9, label='Neo4j db_hits (PROFILE)')
    ax1.set_xticks(x); ax1.set_xticklabels(ids_m, fontsize=9)
    ax1.set_ylabel('Block / hit count')
    ax1.set_title('PG Buffer Blocks vs Neo4j DB Hits', fontweight='bold')
    ax1.legend(); ax1.yaxis.grid(True); ax1.set_axisbelow(True)

    # Coefficient of variation (timing stability)
    pg_cv  = [_get(df, q, 'postgresql', 'cv_pct_f') for q in ids_m]
    neo_cv = [_get(df, q, 'neo4j',      'cv_pct_f') for q in ids_m]
    ax2.plot(ids_m, pg_cv,  marker='o', lw=2, color=PG_C,  label='PostgreSQL CV%')
    ax2.plot(ids_m, neo_cv, marker='s', lw=2, color=NEO_C, label='Neo4j CV%')
    ax2.axhline(10, color=YELLOW, lw=1, ls=':', label='10% threshold')
    ax2.set_ylabel('Coefficient of Variation (%)')
    ax2.set_title('Timing Stability (CV% over 5 warm runs)', fontweight='bold')
    ax2.legend(); ax2.yaxis.grid(True); ax2.set_axisbelow(True)

    fig.suptitle('Chart 9 — Storage Access Patterns & Timing Stability', fontweight='bold', y=1.01)
    plt.tight_layout(); plt.savefig(OUT_DIR / f'chart_9.png', dpi=300, bbox_inches='tight') 
else:
    print('No db_hits data (PROFILE run may have failed).')
rows = []
for qid in QUERY_IDS:
    for db in ['postgresql', 'neo4j']:
        sub = df[(df.query_id == qid) & (df.db == db)]
        if sub.empty: continue
        r = sub.iloc[0]
        rows.append({
            'Query':       qid,
            'Tier':        r.get('tier','?'),
            'DB':          r.get('db_label', db),
            'Cold (ms)':   f"{r['cold_ms_f']:.1f}" if pd.notna(r.get('cold_ms_f')) else '—',
            'Warm med (ms)':f"{r['median_ms']:.1f}" if pd.notna(r.get('median_ms')) else '—',
            'Stdev (ms)':  f"{r['stdev_ms']:.2f}"  if pd.notna(r.get('stdev_ms')) else '—',
            'CV%':         f"{r['cv_pct_f']:.1f}"  if pd.notna(r.get('cv_pct_f')) else '—',
            'Cold-Warm Δ': f"{r.get('cold_warm_delta_ms',0):.1f}" if pd.notna(r.get('cold_warm_delta_ms')) else '—',
            'Rows':        str(int(r['result_count'])) if pd.notna(r.get('result_count')) else '—',
            'Buf hit%':    f"{r['buffer_hit_ratio']*100:.1f}%" if pd.notna(r.get('buffer_hit_ratio')) else '—',
            'Plan (ms)':   f"{r['median_planning_ms']:.2f}" if pd.notna(r.get('median_planning_ms')) else '—',
            'DB hits':     str(int(r['db_hits'])) if pd.notna(r.get('db_hits')) and r.get('db_hits',-1) >= 0 else '—',
        })

summary = pd.DataFrame(rows)
print(summary.to_string(index=False))
summary.to_csv(OUT_DIR / 'summary.csv', index=False)
print('All graphs and summary saved to /graphs directory!')
