import markdown
from pathlib import Path

ROOT = Path(__file__).parent.parent
REPORT_MD = ROOT / "report.md"
REPORT_HTML = ROOT / "report.html"

CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
body { font-family: 'Inter', sans-serif; font-size: 11pt; line-height: 1.65; color: #1a1a2e; max-width: 860px; margin: 0 auto; padding: 40px; }
h1 { font-size: 20pt; font-weight: 700; color: #0f3460; border-bottom: 3px solid #4a9eff; padding-bottom: 8px; }
h2 { font-size: 14pt; font-weight: 700; color: #16213e; border-bottom: 1.5px solid #c8d8e8; margin-top: 2.2em; padding-bottom: 4px; }
h3 { font-size: 12pt; color: #0f3460; }
code { font-family: 'JetBrains Mono', monospace; font-size: 9pt; background: #f0f4f8; padding: 2px 5px; border-radius: 3px; }
pre { background: #1a1a2e; color: #cdd6f4; padding: 14px; border-radius: 6px; border-left: 4px solid #4a9eff; overflow-x: auto; }
pre code { background: none; padding: 0; color: inherit; }
table { border-collapse: collapse; width: 100%; margin: 1.2em 0; font-size: 9.5pt; }
th { background: #0f3460; color: white; padding: 8px 12px; text-align: left; }
td { padding: 6px 12px; border-bottom: 1px solid #dde4ee; }
tr:nth-child(even) { background: #f5f8fc; }
blockquote { border-left: 4px solid #4a9eff; padding: 0.6em 1.2em; background: #f0f6ff; color: #333; }

/* Print & Pagination specific styles */
@media print {
  body { padding: 0; margin: 0; max-width: 100%; }
  h1 { break-before: page; }
  h1:first-of-type { break-before: auto; }
  h2, h3 { break-after: avoid; }
  pre, table, tr, img, blockquote { break-inside: avoid; }
  a { text-decoration: none; color: black; }
}
</style>
"""

md_text = REPORT_MD.read_text(encoding="utf-8")
html_body = markdown.markdown(md_text, extensions=["tables", "fenced_code"])
html = f"<!DOCTYPE html><html><head><meta charset='utf-8'>{CSS}</head><body>{html_body}</body></html>"
REPORT_HTML.write_text(html, encoding="utf-8")
print(f"Created {REPORT_HTML}")
