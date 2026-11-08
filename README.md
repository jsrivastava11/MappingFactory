# MappingFactory
Mapping Factory – A scalable data transformation framework for defining, governing, and executing source-to-target mappings with data lineage, auditability, and reusable business rules.

<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Data Engineer L1 Interview Prep — Databricks &amp; PySpark</title>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/styles/atom-one-dark.min.css">
<script src="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/highlight.min.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:system-ui,-apple-system,'Segoe UI',sans-serif;background:#f1f5f9;color:#0f172a;line-height:1.65}
.hdr{background:#fff;border-bottom:1px solid #e2e8f0;padding:13px 24px;display:flex;align-items:center;gap:12px;position:sticky;top:0;z-index:200;box-shadow:0 1px 4px rgba(0,0,0,.06)}
.hdr h1{font-size:16px;font-weight:700;color:#0f172a}
.hdr-pill{background:#2563eb;color:#fff;font-size:10px;padding:3px 9px;border-radius:20px;font-weight:700}
.hdr-sep{color:#cbd5e1}
.hdr-sub{color:#64748b;font-size:12px}
.layout{display:flex;min-height:calc(100vh - 50px)}
.sidebar{width:235px;flex-shrink:0;background:#fff;border-right:1px solid #e2e8f0;position:sticky;top:50px;height:calc(100vh - 50px);overflow-y:auto;padding:8px 0}
.nav-grp{padding:10px 16px 3px;font-size:10px;font-weight:700;color:#94a3b8;text-transform:uppercase;letter-spacing:.8px}
.nav-item{display:flex;align-items:center;gap:9px;padding:7px 16px;font-size:13px;color:#64748b;cursor:pointer;border-left:3px solid transparent;transition:all .15s;user-select:none}
.nav-item:hover{color:#0f172a;background:#f8fafc}
.nav-item.active{color:#2563eb;background:#eff6ff;border-left-color:#2563eb;font-weight:600}
.nav-cnt{margin-left:auto;font-size:10px;background:#f1f5f9;color:#94a3b8;padding:1px 7px;border-radius:10px;min-width:22px;text-align:center}
.nav-item.active .nav-cnt{background:#dbeafe;color:#2563eb}
.nav-icon{font-size:14px;width:18px;text-align:center;flex-shrink:0}
.main{flex:1;padding:24px 28px;min-width:0}
.sec{display:none;max-width:900px}
.sec.active{display:block}
.sec-hdr{margin-bottom:18px}
.sec-hdr h2{font-size:20px;font-weight:700;margin-bottom:4px}
.sec-hdr p{color:#64748b;font-size:13px}
.sec-controls{display:flex;gap:8px;margin-bottom:14px}
.btn{font-size:12px;padding:5px 12px;border-radius:6px;cursor:pointer;border:1px solid #e2e8f0;background:#fff;color:#64748b;transition:all .15s}
.btn:hover{background:#f1f5f9;color:#0f172a}
.card{background:#fff;border:1px solid #e2e8f0;border-radius:10px;margin-bottom:9px;overflow:hidden;box-shadow:0 1px 2px rgba(0,0,0,.04);transition:box-shadow .2s,border-color .2s}
.card:hover{box-shadow:0 2px 8px rgba(0,0,0,.08)}
.card.open{border-color:#93c5fd;box-shadow:0 2px 10px rgba(37,99,235,.1)}
.card-q{padding:13px 16px;cursor:pointer;display:flex;align-items:flex-start;gap:10px;user-select:none}
.card-q:hover{background:#f8fafc}
.card-q-body{flex:1;min-width:0}
.card-badges{display:flex;gap:5px;margin-bottom:6px;flex-wrap:wrap}
.badge{font-size:10px;padding:2px 7px;border-radius:4px;font-weight:700;text-transform:uppercase;letter-spacing:.4px}
.b-pyspark{background:#e0f2fe;color:#0369a1}
.b-delta{background:#f3e8ff;color:#7c3aed}
.b-databricks{background:#dcfce7;color:#166534}
.b-sql{background:#fef9c3;color:#854d0e}
.b-python{background:#fff7ed;color:#9a3412}
.b-coding{background:#fff7ed;color:#c2410c}
.b-advanced{background:#fee2e2;color:#991b1b}
.b-modern{background:#e0f7fa;color:#0e7490}
.b-project{background:#f0fdf4;color:#166534}
.b-easy{background:#f0fdf4;color:#15803d}
.b-medium{background:#fefce8;color:#a16207}
.b-hard{background:#fff1f2;color:#be123c}
.card-q-text{font-size:14px;color:#0f172a;font-weight:500;line-height:1.5}
.card-arrow{color:#94a3b8;font-size:11px;flex-shrink:0;margin-top:3px;transition:transform .2s}
.card.open .card-arrow{transform:rotate(180deg);color:#2563eb}
.card-ans{display:none;padding:4px 16px 16px;border-top:1px solid #f1f5f9}
.card.open .card-ans{display:block}
.ans-body{font-size:13px;color:#334155;line-height:1.75;margin-top:11px}
.ans-body p{margin-bottom:10px}
.ans-body p:last-child{margin-bottom:0}
.ans-body ul,.ans-body ol{padding-left:20px;margin-bottom:10px}
.ans-body li{margin-bottom:4px}
.ans-body strong{color:#0f172a}
.ans-body h4{font-size:11px;font-weight:700;margin:14px 0 6px;text-transform:uppercase;letter-spacing:.5px;color:#2563eb}
.ans-body code{background:#f1f5f9;padding:1px 5px;border-radius:4px;font-family:'Cascadia Code','Fira Code',Consolas,monospace;font-size:12px;color:#be185d;border:1px solid #e2e8f0}
.code-wrap{position:relative;margin:12px 0;border-radius:8px;overflow:hidden}
.code-lang{position:absolute;top:8px;right:8px;font-size:9px;color:#94a3b8;background:rgba(255,255,255,.12);padding:2px 7px;border-radius:3px;z-index:2;pointer-events:none}
.copy-btn{position:absolute;top:6px;right:58px;font-size:10px;color:#94a3b8;background:rgba(255,255,255,.1);border:1px solid rgba(255,255,255,.18);padding:2px 8px;border-radius:4px;cursor:pointer;z-index:2;transition:all .15s}
.copy-btn:hover{color:#fff;background:rgba(255,255,255,.22)}
.code-wrap pre{margin:0!important;border-radius:8px!important}
.code-wrap code{font-size:12.5px!important;font-family:'Cascadia Code','Fira Code',Consolas,monospace!important}
.hljs{background:#1e293b!important;padding:14px 16px!important}
.callout{padding:11px 15px;border-radius:6px;margin:10px 0;font-size:13px;border-left:3px solid}
.callout-blue{background:#eff6ff;border-color:#2563eb;color:#1e40af}
.callout-yellow{background:#fefce8;border-color:#ca8a04;color:#854d0e}
.callout-green{background:#f0fdf4;border-color:#16a34a;color:#166534}
.callout-red{background:#fff1f2;border-color:#e11d48;color:#9f1239}
.ref-table{width:100%;border-collapse:collapse;font-size:12.5px;margin:10px 0}
.ref-table th{background:#f8fafc;padding:7px 12px;text-align:left;color:#64748b;font-size:10px;text-transform:uppercase;letter-spacing:.5px;border-bottom:2px solid #e2e8f0}
.ref-table td{padding:7px 12px;border-bottom:1px solid #f1f5f9;vertical-align:top}
.ref-table tr:hover td{background:#f8fafc}
.ref-table code{background:#f1f5f9;padding:1px 5px;border-radius:3px;font-size:11.5px;color:#be185d;border:1px solid #e2e8f0;font-family:'Cascadia Code',Consolas,monospace}
.qr-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin:14px 0}
.qr-card{background:#fff;border:1px solid #e2e8f0;border-radius:10px;padding:16px;box-shadow:0 1px 2px rgba(0,0,0,.04)}
.qr-card h3{font-size:12px;font-weight:700;color:#0f172a;margin-bottom:10px;padding-bottom:8px;border-bottom:1px solid #f1f5f9;text-transform:uppercase;letter-spacing:.5px}
.ov-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(140px,1fr));gap:10px;margin:12px 0 20px}
.ov-card{background:#fff;border:1px solid #e2e8f0;border-radius:10px;padding:14px;cursor:pointer;transition:all .2s;box-shadow:0 1px 2px rgba(0,0,0,.04)}
.ov-card:hover{border-color:#93c5fd;transform:translateY(-1px);box-shadow:0 4px 12px rgba(0,0,0,.08)}
.ov-card h3{font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:.5px;font-weight:700}
.ov-card .num{font-size:26px;font-weight:800;margin:5px 0 2px}
.ov-card p{font-size:11px;color:#94a3b8}
.top-alert{background:#fefce8;border:1px solid #fde68a;border-radius:8px;padding:12px 16px;margin-bottom:18px;font-size:13px;color:#854d0e;display:flex;gap:10px}
.pipeline{display:flex;align-items:stretch;gap:0;margin:18px 0;overflow-x:auto}
.pstep{background:#fff;border:1px solid #e2e8f0;border-radius:8px;padding:12px 14px;flex:1;min-width:95px;text-align:center;position:relative;box-shadow:0 1px 3px rgba(0,0,0,.04)}
.pstep:not(:last-child)::after{content:'→';position:absolute;right:-16px;top:50%;transform:translateY(-50%);color:#94a3b8;font-size:18px;z-index:1}
.pstep .ps-icon{font-size:20px;margin-bottom:4px}
.pstep h4{font-size:11px;font-weight:700;color:#0f172a;margin-bottom:2px}
.pstep p{font-size:10px;color:#64748b;line-height:1.4}
.pstep-gap{width:28px;flex-shrink:0}
.pstep.bronze{border-color:#c2855a;background:#fff8f4}
.pstep.silver{border-color:#94a3b8;background:#f8fafc}
.pstep.gold{border-color:#d97706;background:#fffbeb}
.checklist{list-style:none;padding:0}
.checklist li{display:flex;align-items:center;gap:10px;padding:8px 12px;border-radius:6px;font-size:13px;border:1px solid #e2e8f0;margin-bottom:6px;background:#fff;cursor:pointer;transition:all .15s;user-select:none}
.checklist li:hover{background:#f8fafc}
.checklist li.done{color:#94a3b8}
.chk-box{width:16px;height:16px;border:2px solid #cbd5e1;border-radius:3px;flex-shrink:0;display:flex;align-items:center;justify-content:center;transition:all .15s;font-size:10px}
.checklist li.done .chk-box{background:#16a34a;border-color:#16a34a;color:#fff}
.two-col{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin:10px 0}
@media(max-width:768px){.sidebar{display:none}.main{padding:16px}.qr-grid,.two-col{grid-template-columns:1fr}}
</style>
</head>
<body>

<div class="hdr">
  <span>🏗️</span>
  <h1>Data Engineer Interview Prep</h1>
  <span class="hdr-sep">|</span>
  <span class="hdr-pill">L1</span>
  <span class="hdr-sep">|</span>
  <span class="hdr-sub">Databricks · PySpark · Delta Lake · Python · SQL</span>
</div>

<div class="layout">
<nav class="sidebar">
  <div class="nav-grp">Start Here</div>
  <div class="nav-item active" onclick="show('overview')"><span class="nav-icon">🗺️</span>Overview</div>
  <div class="nav-item" onclick="show('quickrev')"><span class="nav-icon">📋</span>Quick Revision<span class="nav-cnt">ref</span></div>

  <div class="nav-grp">Python &amp; Core</div>
  <div class="nav-item" onclick="show('python')"><span class="nav-icon">🐍</span>Python Refresher<span class="nav-cnt">12</span></div>
  <div class="nav-item" onclick="show('pyspark')"><span class="nav-icon">⚡</span>PySpark<span class="nav-cnt">18</span></div>

  <div class="nav-grp">Delta &amp; Databricks</div>
  <div class="nav-item" onclick="show('delta')"><span class="nav-icon">🔺</span>Delta Lake<span class="nav-cnt">13</span></div>
  <div class="nav-item" onclick="show('databricks')"><span class="nav-icon">🧱</span>Databricks Essentials<span class="nav-cnt">12</span></div>
  <div class="nav-item" onclick="show('advanced')"><span class="nav-icon">🚀</span>Databricks Advanced<span class="nav-cnt">9</span></div>
  <div class="nav-item" onclick="show('modern')"><span class="nav-icon">✨</span>Modern Features<span class="nav-cnt">7</span></div>

  <div class="nav-grp">Interview Prep</div>
  <div class="nav-item" onclick="show('sql')"><span class="nav-icon">🗄️</span>SQL<span class="nav-cnt">12</span></div>
  <div class="nav-item" onclick="show('project')"><span class="nav-icon">💼</span>Project Q&amp;A<span class="nav-cnt">6</span></div>
  <div class="nav-item" onclick="show('checklist')"><span class="nav-icon">✅</span>Prep Checklist</div>
</nav>

<main class="main">

<!-- ══ OVERVIEW ══ -->
<div id="sec-overview" class="sec active">
  <div class="sec-hdr"><h2>🗺️ Interview Overview</h2><p>What matters most — based on real interviewer feedback across multiple candidates.</p></div>
  <div class="top-alert"><span>⚠️</span><div><strong>Critical:</strong> Delta Lake hands-on (MERGE, VACUUM, Z-ORDER, Medallion Architecture) has the highest weight. PySpark problem-solving must be clean and simple — interviewers have rejected candidates who over-complicated basic problems.</div></div>
  <div class="ov-grid">
    <div class="ov-card" onclick="show('python')"><h3>Python</h3><div class="num" style="color:#ea580c">12</div><p>OOP, generators, decorators, exceptions, JSON, APIs</p></div>
    <div class="ov-card" onclick="show('pyspark')"><h3>PySpark</h3><div class="num" style="color:#0284c7">18</div><p>Concepts, coding, file I/O, optimization</p></div>
    <div class="ov-card" onclick="show('delta')"><h3>Delta Lake ★</h3><div class="num" style="color:#7c3aed">13</div><p>Highest weight — hands-on critical</p></div>
    <div class="ov-card" onclick="show('databricks')"><h3>Databricks</h3><div class="num" style="color:#059669">12</div><p>Platform, Unity Catalog, pipeline, commands</p></div>
    <div class="ov-card" onclick="show('advanced')"><h3>Advanced</h3><div class="num" style="color:#dc2626">9</div><p>Service Principal, DABs, CLI, Job creation</p></div>
    <div class="ov-card" onclick="show('modern')"><h3>Modern</h3><div class="num" style="color:#0891b2">7</div><p>Genie, Serverless, Lakeflow, Liquid Clustering</p></div>
    <div class="ov-card" onclick="show('sql')"><h3>SQL</h3><div class="num" style="color:#d97706">12</div><p>Intermediate queries, write correctly first try</p></div>
    <div class="ov-card" onclick="show('project')"><h3>Project Q&A</h3><div class="num" style="color:#16a34a">6</div><p>STAR format, explain your work</p></div>
  </div>
  <table class="ref-table">
    <thead><tr><th>Topic</th><th>What Interviewers Test</th><th>Weight</th></tr></thead>
    <tbody>
      <tr><td><strong>Delta Lake</strong></td><td>MERGE, VACUUM, Z-ORDER, Time Travel, hands-on delta tables, medallion architecture</td><td style="color:#dc2626">●●●●</td></tr>
      <tr><td><strong>PySpark Coding</strong></td><td>Clean simple solutions — groupBy, window functions, joins. No over-engineering.</td><td style="color:#d97706">●●●○</td></tr>
      <tr><td><strong>Databricks Platform</strong></td><td>Architecture, cluster types, Unity Catalog, Workflows</td><td style="color:#d97706">●●●○</td></tr>
      <tr><td><strong>SQL</strong></td><td>Intermediate queries written correctly without errors</td><td style="color:#d97706">●●●○</td></tr>
      <tr><td><strong>Python Basics</strong></td><td>OOP, error handling, working with JSON/APIs</td><td style="color:#16a34a">●●○○</td></tr>
      <tr><td><strong>Testing &amp; Deployment</strong></td><td>Explain how you test before prod, CI/CD basics</td><td style="color:#16a34a">●●○○</td></tr>
    </tbody>
  </table>
</div>

<!-- ══ QUICK REVISION ══ -->
<div id="sec-quickrev" class="sec">
  <div class="sec-hdr"><h2>📋 Quick Revision</h2><p>Last-minute cheat sheet — read this 30 minutes before the interview.</p></div>

  <div class="qr-grid">
    <div class="qr-card">
      <h3>⚡ One-Line Answers</h3>
      <table class="ref-table">
        <thead><tr><th>Question</th><th>Answer</th></tr></thead>
        <tbody>
          <tr><td>What is Delta Lake?</td><td>ACID transactions + Time Travel on top of Parquet files in cloud storage</td></tr>
          <tr><td>What is the transaction log?</td><td><code>_delta_log/</code> folder — JSON file per commit recording adds/removes</td></tr>
          <tr><td>What is MERGE INTO?</td><td>Atomic upsert — update matching rows, insert new ones</td></tr>
          <tr><td>What is VACUUM?</td><td>Deletes old Parquet files; default 7-day retention; breaks time travel</td></tr>
          <tr><td>What is Z-ORDER?</td><td>Co-locates related data in same files for faster filter queries</td></tr>
          <tr><td>What is Liquid Clustering?</td><td>Modern replacement for partitioning+Z-ORDER; dynamic, easier to change</td></tr>
          <tr><td>What is Unity Catalog?</td><td>Centralized governance: 3-level namespace, fine-grained access, lineage</td></tr>
          <tr><td>What is a shuffle?</td><td>Data moved across executors — expensive; caused by groupBy/join/orderBy</td></tr>
          <tr><td>cache() vs persist()?</td><td>cache=memory+disk default; persist=choose storage level</td></tr>
          <tr><td>Broadcast join?</td><td>Send small table to every executor; avoids shuffle; use for &lt;10MB tables</td></tr>
          <tr><td>repartition vs coalesce?</td><td>repartition=full shuffle+evenly distributed; coalesce=cheap reduce only</td></tr>
          <tr><td>Transformation vs Action?</td><td>Transform=lazy (builds DAG); Action=triggers execution</td></tr>
          <tr><td>What is a Service Principal?</td><td>Non-human identity for automated jobs/CI-CD; not a user account</td></tr>
          <tr><td>What are DABs?</td><td>Databricks Asset Bundles — deploy jobs/resources as code via YAML</td></tr>
          <tr><td>What is Genie?</td><td>AI tool — natural language → SQL → answer; no SQL knowledge needed</td></tr>
        </tbody>
      </table>
    </div>
    <div class="qr-card">
      <h3>🔺 Must-Know Delta Commands</h3>
      <div class="code-wrap"><span class="code-lang">SQL</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-sql">-- View history of all changes
DESCRIBE HISTORY my_table;

-- Time travel by version
SELECT * FROM my_table VERSION AS OF 5;

-- Time travel by timestamp
SELECT * FROM my_table TIMESTAMP AS OF '2024-01-01';

-- Restore table to old version
RESTORE TABLE my_table TO VERSION AS OF 3;

-- Upsert (CDC pattern)
MERGE INTO silver.customers t
USING bronze.cdc s ON t.id = s.id
WHEN MATCHED     THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Compact + data skipping
OPTIMIZE my_table ZORDER BY (customer_id, date);

-- Clean up old files (keep 7 days)
VACUUM my_table RETAIN 168 HOURS;

-- Schema evolution
df.write.format("delta")
  .option("mergeSchema","true")
  .mode("append").saveAsTable("t")</code></pre></div>
    </div>
  </div>

  <div class="qr-grid">
    <div class="qr-card">
      <h3>⚡ Must-Know PySpark</h3>
      <div class="code-wrap"><span class="code-lang">Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python">from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Broadcast join (small table)
result = big.join(broadcast(small), "id")

# Window function (rank per group)
w = Window.partitionBy("dept").orderBy(F.desc("salary"))
df.withColumn("rank", F.dense_rank().over(w))

# Top N per group
df.withColumn("rn", F.row_number().over(w)) \
  .filter(F.col("rn") <= 3)

# Running total
w2 = Window.partitionBy("cust").orderBy("date") \
           .rowsBetween(Window.unboundedPreceding, 0)
df.withColumn("running", F.sum("amt").over(w2))

# Null handling
df.fillna({"col":"default"})
df.withColumn("x", F.coalesce("x", F.lit("NA")))
df.filter(F.col("id").isNotNull())

# Write modes
df.write.format("delta").mode("append").saveAsTable("t")
df.write.format("delta").mode("overwrite").saveAsTable("t")</code></pre></div>
    </div>
    <div class="qr-card">
      <h3>🔑 30 Key Terms</h3>
      <table class="ref-table">
        <thead><tr><th>Term</th><th>Definition</th></tr></thead>
        <tbody>
          <tr><td><strong>RDD</strong></td><td>Low-level distributed dataset, untyped, rarely used now</td></tr>
          <tr><td><strong>DataFrame</strong></td><td>Distributed table with schema; use this in PySpark</td></tr>
          <tr><td><strong>DAG</strong></td><td>Directed Acyclic Graph — Spark's lazy execution plan</td></tr>
          <tr><td><strong>Shuffle</strong></td><td>Network data movement between executors; expensive</td></tr>
          <tr><td><strong>Partition</strong></td><td>Chunk of data processed by one executor task</td></tr>
          <tr><td><strong>AQE</strong></td><td>Adaptive Query Execution — auto-tunes joins/partitions</td></tr>
          <tr><td><strong>Delta Lake</strong></td><td>ACID storage layer on top of Parquet files</td></tr>
          <tr><td><strong>_delta_log</strong></td><td>Transaction log folder storing commit JSON files</td></tr>
          <tr><td><strong>ACID</strong></td><td>Atomicity, Consistency, Isolation, Durability</td></tr>
          <tr><td><strong>Time Travel</strong></td><td>Query previous Delta table versions</td></tr>
          <tr><td><strong>CDF</strong></td><td>Change Data Feed — track row-level inserts/updates/deletes</td></tr>
          <tr><td><strong>Auto Loader</strong></td><td>Incremental file ingestion with checkpointing</td></tr>
          <tr><td><strong>Medallion</strong></td><td>Bronze (raw) → Silver (clean) → Gold (aggregated)</td></tr>
          <tr><td><strong>Unity Catalog</strong></td><td>3-level namespace + governance across workspaces</td></tr>
          <tr><td><strong>Service Principal</strong></td><td>App identity for automated/non-human access</td></tr>
          <tr><td><strong>DABs</strong></td><td>Databricks Asset Bundles — IaC for Databricks jobs</td></tr>
          <tr><td><strong>Job Cluster</strong></td><td>Ephemeral cluster for one job; terminates after</td></tr>
          <tr><td><strong>Liquid Clustering</strong></td><td>Dynamic data layout replacing partitioning+Z-ORDER</td></tr>
          <tr><td><strong>Genie</strong></td><td>NL→SQL AI interface in Databricks</td></tr>
          <tr><td><strong>Serverless</strong></td><td>Instant-start clusters, no config, pay-per-second</td></tr>
          <tr><td><strong>Lakeflow</strong></td><td>Declarative ETL framework (Delta Live Tables)</td></tr>
          <tr><td><strong>Schema enforcement</strong></td><td>Reject writes not matching Delta table schema</td></tr>
          <tr><td><strong>Schema evolution</strong></td><td>Allow new columns via mergeSchema option</td></tr>
          <tr><td><strong>Broadcast join</strong></td><td>Send small table to all executors; no shuffle</td></tr>
          <tr><td><strong>coalesce</strong></td><td>Reduce partitions cheaply; no full shuffle</td></tr>
          <tr><td><strong>repartition</strong></td><td>Increase or decrease partitions; full shuffle</td></tr>
          <tr><td><strong>UDF</strong></td><td>User-Defined Function; slow in Python; use built-ins</td></tr>
          <tr><td><strong>PAT</strong></td><td>Personal Access Token — authenticate to Databricks API</td></tr>
          <tr><td><strong>Control Plane</strong></td><td>Databricks-managed UI/scheduler/metadata</td></tr>
          <tr><td><strong>Data Plane</strong></td><td>Customer's cloud — where clusters and data live</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</div>

<!-- ══ PYTHON REFRESHER ══ -->
<div id="sec-python" class="sec">
  <div class="sec-hdr"><h2>🐍 Python Refresher</h2><p>OOP concepts, generators, decorators, exception handling, JSON, and APIs.</p></div>
  <div class="sec-controls"><button class="btn" onclick="expandAll('python')">Expand All</button><button class="btn" onclick="collapseAll('python')">Collapse All</button></div>

  <div class="card" data-sec="python">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-python">Python</span><span class="badge b-easy">Easy</span></div><div class="card-q-text">What is OOP? Explain classes, objects, and the __init__ method.</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p><strong>OOP (Object-Oriented Programming)</strong> organises code into classes (blueprints) and objects (instances). The four pillars are: Encapsulation, Inheritance, Polymorphism, Abstraction.</p>
      <div class="code-wrap"><span class="code-lang">Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python">class DataPipeline:
    # Class variable — shared by all instances
    pipeline_count = 0

    def __init__(self, name, source, target):   # constructor
        self.name   = name       # instance variables
        self.source = source
        self.target = target
        DataPipeline.pipeline_count += 1

    def run(self):
        print(f"Running {self.name}: {self.source} → {self.target}")

    def __repr__(self):   # string representation
        return f"DataPipeline(name={self.name})"

# Create objects (instances)
etl1 = DataPipeline("orders_etl", "bronze.orders", "silver.orders")
etl2 = DataPipeline("customers_etl", "bronze.customers", "silver.customers")

etl1.run()
print(DataPipeline.pipeline_count)  # 2</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="python">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-python">Python</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">What is inheritance in Python? Show single and multi-level inheritance with <code>super()</code>.</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p><strong>Inheritance</strong> lets a child class reuse and extend a parent class. Use <code>super()</code> to call the parent's methods.</p>
      <div class="code-wrap"><span class="code-lang">Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python">class Pipeline:
    def __init__(self, name):
        self.name = name

    def run(self):
        print(f"Pipeline {self.name} running")

    def validate(self):
        print("Base validation")

# Single inheritance
class DeltaPipeline(Pipeline):
    def __init__(self, name, table):
        super().__init__(name)   # call parent __init__
        self.table = table

    def run(self):                # override parent method
        print(f"Delta pipeline writing to {self.table}")
        super().run()             # also call parent version

# Multi-level
class StreamingDeltaPipeline(DeltaPipeline):
    def run(self):
        print("Streaming mode: ")
        super().run()

p = StreamingDeltaPipeline("stream_etl", "silver.orders")
p.run()
print(isinstance(p, Pipeline))   # True — p IS-A Pipeline</code></pre></div>
      <div class="callout callout-blue"><strong>Key terms:</strong> <code>isinstance(obj, Class)</code> — checks if object is instance of class. <code>issubclass(Child, Parent)</code> — checks class hierarchy.</div>
    </div></div>
  </div>

  <div class="card" data-sec="python">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-python">Python</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">What is encapsulation? Explain public, protected, and private attributes.</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p><strong>Encapsulation</strong> bundles data and methods together and controls access. Python uses naming conventions (not strict enforcement):</p>
      <div class="code-wrap"><span class="code-lang">Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python">class DatabaseConnection:
    def __init__(self, host, password):
        self.host      = host          # public — accessible anywhere
        self._host     = host          # protected — by convention, internal use
        self.__password = password     # private — name-mangled to _DatabaseConnection__password

    @property                          # getter
    def password(self):
        return "***hidden***"          # never expose real password

    @password.setter                   # setter with validation
    def password(self, value):
        if len(value) < 8:
            raise ValueError("Password too short")
        self.__password = value

    def connect(self):
        # Uses __password internally
        print(f"Connecting to {self.host}")

db = DatabaseConnection("localhost", "securepass123")
print(db.password)       # "***hidden***"
db.password = "newpass123"  # calls setter</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="python">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-python">Python</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">What are generators in Python? How are they different from regular functions and lists?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p>A <strong>generator</strong> uses <code>yield</code> instead of <code>return</code>. It produces values one at a time (<em>lazy evaluation</em>) — only when asked. This is memory-efficient for large datasets.</p>
      <div class="code-wrap"><span class="code-lang">Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python"># Regular function — builds full list in memory at once
def get_records_list(n):
    return [i * 2 for i in range(n)]   # all n items in memory

# Generator — produces one value at a time
def get_records_gen(n):
    for i in range(n):
        yield i * 2    # pauses here, resumes on next()

# Usage
gen = get_records_gen(1_000_000)
print(next(gen))   # 0
print(next(gen))   # 2

# Iterate
for val in get_records_gen(5):
    print(val)   # 0, 2, 4, 6, 8

# Generator expression (like list comprehension but lazy)
squares_gen = (x**2 for x in range(100))   # no memory used yet
total = sum(squares_gen)                    # evaluated on demand

# Real use case: read large file line by line
def read_large_file(filepath):
    with open(filepath, 'r') as f:
        for line in f:
            yield line.strip()   # one line at a time, not whole file</code></pre></div>
      <div class="callout callout-blue">Generators are great for big data processing — they never load everything into memory at once.</div>
    </div></div>
  </div>

  <div class="card" data-sec="python">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-python">Python</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">What are decorators in Python? Show a practical example.</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p>A <strong>decorator</strong> wraps a function to add behaviour before/after it runs — without modifying the original function. Used via <code>@decorator_name</code> syntax.</p>
      <div class="code-wrap"><span class="code-lang">Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python">import time
from functools import wraps

# Define decorator
def timer(func):
    @wraps(func)              # preserves original function metadata
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)   # call original function
        end   = time.time()
        print(f"{func.__name__} took {end-start:.3f}s")
        return result
    return wrapper

def retry(max_attempts=3):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts - 1:
                        raise
                    print(f"Retry {attempt+1}/{max_attempts}")
        return wrapper
    return decorator

# Usage
@timer
@retry(max_attempts=3)
def run_pipeline(name):
    print(f"Running {name}")
    # simulate failure on first try...

run_pipeline("orders_etl")

# Built-in decorators
class Config:
    @staticmethod              # no self/cls — utility function in class
    def get_env():
        return "prod"

    @classmethod               # gets class, not instance
    def create(cls):
        return cls()

    @property                  # access method like attribute
    def version(self):
        return "1.0"</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="python">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-python">Python</span><span class="badge b-easy">Easy</span></div><div class="card-q-text">How does exception handling work in Python? Show <code>try/except/else/finally</code> and custom exceptions.</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <div class="code-wrap"><span class="code-lang">Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python"># Full try/except/else/finally
try:
    result = 10 / 0
except ZeroDivisionError as e:
    print(f"Math error: {e}")
except (ValueError, TypeError) as e:   # catch multiple types
    print(f"Value/Type error: {e}")
except Exception as e:                  # catch-all
    print(f"Unexpected: {e}")
else:
    print("No exception — runs only if try succeeded")
finally:
    print("Always runs — cleanup code here")

# Custom exception
class PipelineError(Exception):
    def __init__(self, message, table_name):
        super().__init__(message)
        self.table_name = table_name

class DataQualityError(PipelineError):
    pass

# Raise custom exception
def validate_data(df, table_name):
    count = df.count()
    if count == 0:
        raise DataQualityError(f"Empty result", table_name)
    return count

# Handle it
try:
    validate_data(empty_df, "silver.orders")
except DataQualityError as e:
    print(f"Quality issue on {e.table_name}: {e}")

# Context manager (auto-cleanup)
with open("data.json", "r") as f:    # file auto-closed even if error
    data = f.read()</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="python">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-python">Python</span><span class="badge b-easy">Easy</span></div><div class="card-q-text">How do you work with JSON in Python? What is JSON and why is it important for data engineering?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p><strong>JSON (JavaScript Object Notation)</strong> is a lightweight data format for exchanging data between systems. Almost every API returns JSON. It maps directly to Python dicts and lists.</p>
      <div class="code-wrap"><span class="code-lang">Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python">import json

# Parse JSON string → Python dict
json_str = '{"customer_id": 1, "name": "Alice", "orders": [101, 102]}'
data = json.loads(json_str)      # loads = load from string
print(data["name"])              # Alice
print(data["orders"][0])         # 101

# Python dict → JSON string
python_dict = {"id": 1, "status": "active", "score": 9.5}
json_output = json.dumps(python_dict, indent=2)   # dumps = dump to string
print(json_output)

# Read JSON file
with open("config.json", "r") as f:
    config = json.load(f)        # load = from file object

# Write JSON file
with open("output.json", "w") as f:
    json.dump(python_dict, f, indent=2)   # dump = to file object

# Common options
json.dumps(data, indent=4)        # pretty print
json.dumps(data, sort_keys=True)  # sorted keys
json.dumps(data, default=str)     # convert non-serializable types (dates) to str

# Nested JSON
response = {
    "status": "ok",
    "data": {
        "users": [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"}
        ]
    }
}
users = response["data"]["users"]
names = [u["name"] for u in users]  # ["Alice", "Bob"]</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="python">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-python">Python</span><span class="badge b-pyspark">PySpark</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">How do you handle nested/complex JSON in PySpark? Show <code>from_json</code>, <code>get_json_object</code>, and <code>explode</code>.</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <div class="code-wrap"><span class="code-lang">Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python">from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# Sample: JSON stored as a string in a column
df = spark.createDataFrame([
    (1, '{"name":"Alice","orders":[{"id":101,"amount":50},{"id":102,"amount":75}]}'),
    (2, '{"name":"Bob",  "orders":[{"id":103,"amount":30}]}'),
], ["customer_id", "json_col"])

# Method 1: get_json_object — extract specific field
df.withColumn("name", F.get_json_object("json_col", "$.name")) \
  .withColumn("first_order", F.get_json_object("json_col", "$.orders[0].id"))

# Method 2: from_json — parse full JSON column into struct
schema = StructType([
    StructField("name", StringType()),
    StructField("orders", ArrayType(StructType([
        StructField("id", IntegerType()),
        StructField("amount", IntegerType())
    ])))
])
parsed = df.withColumn("data", F.from_json("json_col", schema))
parsed.select("customer_id", "data.name", "data.orders")

# Method 3: explode — flatten array into rows
exploded = parsed.select(
    "customer_id",
    "data.name",
    F.explode("data.orders").alias("order")
)
exploded.select("customer_id", "name", "order.id", "order.amount").show()
# Each order becomes its own row

# Read JSON files directly
df = spark.read.format("json").option("multiLine","true").load("/path/data.json")
df = spark.read.json("/path/data.json")  # shorthand</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="python">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-python">Python</span><span class="badge b-easy">Easy</span></div><div class="card-q-text">How do you call an external API in Python? Show GET and POST requests with authentication.</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p>Use the <code>requests</code> library — the standard for HTTP calls in Python.</p>
      <div class="code-wrap"><span class="code-lang">Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python">import requests

# ── GET request ───────────────────────────────────────────────
response = requests.get(
    url="https://api.example.com/customers",
    params={"status": "active", "limit": 100},   # query string
    headers={"Authorization": "Bearer my_token",
             "Content-Type": "application/json"},
    timeout=30   # always set timeout
)

# Check response
print(response.status_code)   # 200 = OK, 404 = not found, 500 = server error
data = response.json()        # parse JSON response
print(data["customers"])

# ── POST request ──────────────────────────────────────────────
payload = {"name": "Alice", "email": "alice@example.com"}
response = requests.post(
    url="https://api.example.com/customers",
    json=payload,              # auto-serializes dict to JSON + sets Content-Type
    headers={"Authorization": "Bearer my_token"},
    timeout=30
)
new_customer = response.json()

# ── Error handling ────────────────────────────────────────────
try:
    response = requests.get("https://api.example.com/data", timeout=10)
    response.raise_for_status()   # raises HTTPError for 4xx/5xx
    return response.json()
except requests.exceptions.Timeout:
    print("Request timed out")
except requests.exceptions.HTTPError as e:
    print(f"HTTP Error: {e.response.status_code} — {e}")
except requests.exceptions.ConnectionError:
    print("Connection failed")

# ── Call Databricks REST API ──────────────────────────────────
WORKSPACE_URL = "https://adb-1234567890.12.azuredatabricks.net"
TOKEN = dbutils.secrets.get("my-scope", "databricks-pat")

headers = {"Authorization": f"Bearer {TOKEN}"}
jobs = requests.get(f"{WORKSPACE_URL}/api/2.1/jobs/list", headers=headers)
print(jobs.json())</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="python">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-python">Python</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">How do you create your own REST API in Python? Show a FastAPI example.</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p><strong>FastAPI</strong> is the modern Python framework for building REST APIs — fast, auto-generates docs, uses type hints.</p>
      <div class="code-wrap"><span class="code-lang">Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python"># pip install fastapi uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Data Pipeline API")

# Data model (request/response schema)
class Customer(BaseModel):
    customer_id: int
    name: str
    email: str
    status: str = "active"   # default value

# Fake database
customers_db = {}

# GET — retrieve a resource
@app.get("/customers/{customer_id}")
def get_customer(customer_id: int):
    if customer_id not in customers_db:
        raise HTTPException(status_code=404, detail="Customer not found")
    return customers_db[customer_id]

# POST — create a resource
@app.post("/customers", status_code=201)
def create_customer(customer: Customer):
    customers_db[customer.customer_id] = customer.dict()
    return {"message": "Created", "id": customer.customer_id}

# GET with query params
@app.get("/customers")
def list_customers(status: str = None, limit: int = 100):
    all_c = list(customers_db.values())
    if status:
        all_c = [c for c in all_c if c["status"] == status]
    return all_c[:limit]

# Run: uvicorn main:app --reload
# Docs auto-generated at: http://localhost:8000/docs</code></pre></div>
      <table class="ref-table"><thead><tr><th>HTTP Method</th><th>Use Case</th><th>Status Code</th></tr></thead>
      <tbody>
        <tr><td>GET</td><td>Retrieve data</td><td>200 OK</td></tr>
        <tr><td>POST</td><td>Create new resource</td><td>201 Created</td></tr>
        <tr><td>PUT/PATCH</td><td>Update existing resource</td><td>200 OK</td></tr>
        <tr><td>DELETE</td><td>Delete resource</td><td>204 No Content</td></tr>
      </tbody></table>
    </div></div>
  </div>

  <div class="card" data-sec="python">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-python">Python</span><span class="badge b-easy">Easy</span></div><div class="card-q-text">What are list comprehensions, lambda functions, and common built-ins to know?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <div class="code-wrap"><span class="code-lang">Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python"># List comprehension
squares   = [x**2 for x in range(10)]
evens     = [x for x in range(20) if x % 2 == 0]
flat      = [item for sublist in [[1,2],[3,4]] for item in sublist]

# Dict comprehension
word_len  = {word: len(word) for word in ["apple","bat","cherry"]}

# Lambda
double    = lambda x: x * 2
add       = lambda x, y: x + y

# map / filter / sorted
doubled   = list(map(lambda x: x*2, [1,2,3]))
positives = list(filter(lambda x: x>0, [-1,2,-3,4]))
sorted_by = sorted(customers, key=lambda c: c["name"])

# zip / enumerate
for i, (a, b) in enumerate(zip(list1, list2)):
    print(i, a, b)

# Any / all
all_valid = all(x > 0 for x in amounts)
any_error = any(e is not None for e in errors)

# String methods (useful for data cleaning)
"  hello  ".strip()          # "hello"
"Alice,Bob".split(",")       # ["Alice","Bob"]
",".join(["Alice","Bob"])    # "Alice,Bob"
"Hello World".lower()        # "hello world"
f"Hello {name}".format()     # f-string formatting</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="python">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-python">Python</span><span class="badge b-easy">Easy</span></div><div class="card-q-text">What is polymorphism in Python? Show method overriding.</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p><strong>Polymorphism</strong> means different classes can share the same method name but each implements it differently. This lets you write code that works on any object that has the right interface.</p>
      <div class="code-wrap"><span class="code-lang">Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python">class DataSource:
    def read(self):
        raise NotImplementedError   # abstract-like

class CSVSource(DataSource):
    def read(self):
        return "Reading CSV file"

class DeltaSource(DataSource):
    def read(self):
        return "Reading Delta table"

class APISource(DataSource):
    def read(self):
        return "Calling REST API"

# Polymorphic usage — same code works for any source
def ingest(source: DataSource):
    data = source.read()   # calls the right version automatically
    print(data)

for source in [CSVSource(), DeltaSource(), APISource()]:
    ingest(source)   # each calls its own read()</code></pre></div>
    </div></div>
  </div>
</div><!-- /python -->


<!-- ══ PYSPARK ══ -->
<div id="sec-pyspark" class="sec">
  <div class="sec-hdr"><h2>⚡ PySpark</h2><p>Concepts, coding problems, file I/O, and optimization. Keep solutions simple and clean.</p></div>
  <div class="sec-controls"><button class="btn" onclick="expandAll('pyspark')">Expand All</button><button class="btn" onclick="collapseAll('pyspark')">Collapse All</button></div>

  <div class="card" data-sec="pyspark">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-pyspark">PySpark</span><span class="badge b-easy">Easy</span></div><div class="card-q-text">What is the difference between Transformation and Action? Give examples.</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p><strong>Transformations</strong> are lazy — they build a plan (DAG) without executing. <strong>Actions</strong> trigger actual execution.</p>
      <table class="ref-table"><thead><tr><th>Transformations (Lazy)</th><th>Actions (Trigger Execution)</th></tr></thead><tbody>
        <tr><td><code>filter()</code>, <code>select()</code>, <code>withColumn()</code></td><td><code>show()</code>, <code>count()</code>, <code>collect()</code></td></tr>
        <tr><td><code>groupBy()</code>, <code>join()</code>, <code>distinct()</code></td><td><code>write()</code>, <code>take(n)</code>, <code>first()</code></td></tr>
      </tbody></table>
      <div class="callout callout-yellow">Never use <code>collect()</code> on large DataFrames — it pulls everything to the driver and can cause OOM. Use <code>show()</code> for inspection.</div>
    </div></div>
  </div>

  <div class="card" data-sec="pyspark">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-pyspark">PySpark</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">What is a shuffle? Why is it expensive? How do you minimize it?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p>A <strong>shuffle</strong> moves data across the cluster so related records land on the same executor. Triggered by: <code>groupBy</code>, <code>join</code>, <code>distinct</code>, <code>orderBy</code>.</p>
      <p><strong>Why expensive:</strong> data is serialized → written to disk → sent over network → deserialized. Can cause OOM and spill.</p>
      <p><strong>How to reduce:</strong> broadcast small tables, filter early to reduce data size, use <code>coalesce()</code> not <code>repartition()</code> when reducing, pre-partition on join keys.</p>
      <div class="code-wrap"><span class="code-lang">Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python">from pyspark.sql.functions import broadcast
# Broadcast join: send small_df to every executor — no shuffle
result = large_df.join(broadcast(small_df), on="id", how="left")

# Enable AQE — Spark auto-picks best join/partition strategy
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="pyspark">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-pyspark">PySpark</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">Explain window functions. Write rank, running total, and LAG examples.</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <div class="code-wrap"><span class="code-lang">Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python">from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Rank within department
w_rank = Window.partitionBy("department").orderBy(F.desc("salary"))
df.withColumn("rank", F.dense_rank().over(w_rank))

# Running total per customer
w_run = Window.partitionBy("customer_id").orderBy("order_date") \
              .rowsBetween(Window.unboundedPreceding, Window.currentRow)
df.withColumn("running_total", F.sum("amount").over(w_run))

# LAG: compare current row to previous
w_ord = Window.partitionBy("customer_id").orderBy("order_date")
df.withColumn("prev_amount", F.lag("amount", 1).over(w_ord)) \
  .withColumn("change", F.col("amount") - F.col("prev_amount"))</code></pre></div>
      <div class="callout callout-blue">Window functions do NOT reduce row count. Use <code>groupBy().agg()</code> when you need a summary. Use window functions when you need per-row calculations with group context.</div>
    </div></div>
  </div>

  <div class="card" data-sec="pyspark">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-pyspark">PySpark</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">What is the difference between <code>repartition()</code> and <code>coalesce()</code>?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <table class="ref-table"><thead><tr><th></th><th>repartition(n)</th><th>coalesce(n)</th></tr></thead><tbody>
        <tr><td>Direction</td><td>Increase or decrease</td><td>Only decrease</td></tr>
        <tr><td>Shuffle</td><td>Full shuffle — expensive</td><td>No full shuffle — cheap</td></tr>
        <tr><td>Distribution</td><td>Evenly balanced</td><td>May be uneven</td></tr>
        <tr><td>Use case</td><td>Fix skew, increase parallelism, partition by column</td><td>Reduce small files before writing</td></tr>
      </tbody></table>
      <div class="code-wrap"><span class="code-lang">Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python">df.repartition(10, "region")       # shuffle + balance by column
df.coalesce(1).write.csv("/out/")  # merge to 1 file before saving</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="pyspark">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-coding">Coding</span><span class="badge b-pyspark">PySpark</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">🖥 Find top 3 customers by total spend in the last 30 days.</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <div class="code-wrap"><span class="code-lang">Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python">from pyspark.sql import functions as F
from datetime import datetime, timedelta

cutoff = datetime.now() - timedelta(days=30)
result = (
    transactions_df
    .filter(F.col("date") >= F.lit(cutoff))
    .groupBy("customer_id")
    .agg(F.sum("amount").alias("total_spend"))
    .orderBy(F.desc("total_spend"))
    .limit(3)
)
result.show()</code></pre></div>
      <div class="callout callout-green">Keep it simple: filter → groupBy → agg → orderBy → limit. No RDDs, no UDFs needed.</div>
    </div></div>
  </div>

  <div class="card" data-sec="pyspark">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-coding">Coding</span><span class="badge b-pyspark">PySpark</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">🖥 Rank employees by salary per department. Return rank ≤ 2.</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <div class="code-wrap"><span class="code-lang">Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python">from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("department").orderBy(F.desc("salary"))
(employees_df
 .withColumn("rank", F.dense_rank().over(w))
 .filter(F.col("rank") <= 2)
 .orderBy("department", "rank")
 .show())</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="pyspark">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-coding">Coding</span><span class="badge b-pyspark">PySpark</span><span class="badge b-easy">Easy</span></div><div class="card-q-text">🖥 Reading files in PySpark — show CSV, JSON, Parquet, Delta with options.</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <div class="code-wrap"><span class="code-lang">Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python">from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# ── CSV ───────────────────────────────────────────────────────
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true")   \
    .option("delimiter", ",") \
    .option("nullValue", "NULL") \
    .load("/mnt/landing/orders/*.csv")

# Define schema explicitly (better for prod — avoids full scan)
schema = StructType([
    StructField("order_id",    IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("amount",      DoubleType()),
    StructField("status",      StringType())
])
df = spark.read.schema(schema).csv("/mnt/landing/orders/")

# ── JSON ──────────────────────────────────────────────────────
df = spark.read.json("/path/data.json")
df = spark.read.format("json").option("multiLine","true").load("/path/")

# ── Parquet ───────────────────────────────────────────────────
df = spark.read.parquet("/path/data.parquet")
df = spark.read.format("parquet").load("/path/")

# ── Delta ─────────────────────────────────────────────────────
df = spark.read.format("delta").load("/mnt/silver/orders")
df = spark.read.table("catalog.silver.orders")
df = spark.sql("SELECT * FROM silver.orders WHERE status='active'")

# ── S3 ────────────────────────────────────────────────────────
# Configure credentials first (or use IAM role / Unity Catalog)
spark.conf.set("fs.s3a.access.key", dbutils.secrets.get("scope","s3-key"))
spark.conf.set("fs.s3a.secret.key", dbutils.secrets.get("scope","s3-secret"))
df = spark.read.csv("s3a://my-bucket/data/orders.csv", header=True)

# With Unity Catalog external location (preferred — no keys in code)
df = spark.read.csv("s3://my-bucket/data/", header=True)  # UC handles auth</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="pyspark">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-coding">Coding</span><span class="badge b-pyspark">PySpark</span><span class="badge b-easy">Easy</span></div><div class="card-q-text">🖥 Writing files in PySpark — all formats, modes, and partitioned writes.</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <div class="code-wrap"><span class="code-lang">Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python"># ── Write modes ───────────────────────────────────────────────
# append   = add rows, keep existing data
# overwrite = replace all data
# error    = fail if already exists (default)
# ignore   = skip write if already exists

# ── Delta (most common) ───────────────────────────────────────
df.write.format("delta").mode("append").saveAsTable("silver.orders")
df.write.format("delta").mode("overwrite").saveAsTable("gold.summary")

# Schema evolution — allow new columns
df.write.format("delta").mode("append") \
    .option("mergeSchema", "true").saveAsTable("silver.orders")

# Overwrite specific partition only (not whole table)
df.write.format("delta").mode("overwrite") \
    .option("replaceWhere", "order_date = '2024-01-15'") \
    .saveAsTable("silver.orders")

# ── Partitioned write ─────────────────────────────────────────
df.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("region", "year") \
    .saveAsTable("silver.orders_partitioned")

# ── Parquet ───────────────────────────────────────────────────
df.write.format("parquet").mode("overwrite") \
    .partitionBy("year", "month").save("/output/orders/")

# ── CSV (single file) ─────────────────────────────────────────
df.coalesce(1).write.format("csv") \
    .option("header", "true").mode("overwrite").save("/output/report/")

# ── JSON ──────────────────────────────────────────────────────
df.write.format("json").mode("overwrite").save("/output/data/")

# ── Write to path (external table) ───────────────────────────
df.write.format("delta").mode("overwrite").save("/mnt/silver/orders")</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="pyspark">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-pyspark">PySpark</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">What is AQE (Adaptive Query Execution)? What does it optimize automatically?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p><strong>AQE</strong> (enabled by default in Spark 3.x) re-optimizes the execution plan at runtime based on actual data statistics collected during the job — not just estimates.</p>
      <p>AQE automatically handles:</p>
      <ul>
        <li><strong>Skew join optimization</strong> — splits large skewed partitions into smaller ones</li>
        <li><strong>Coalescing shuffle partitions</strong> — merges small post-shuffle partitions to reduce overhead</li>
        <li><strong>Dynamic join strategy switching</strong> — downgrades sort-merge join to broadcast join if one side turns out small</li>
      </ul>
      <div class="code-wrap"><span class="code-lang">Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python"># Check if enabled (default: true in Spark 3.x)
spark.conf.get("spark.sql.adaptive.enabled")   # "true"

# Enable explicitly (older clusters)
spark.conf.set("spark.sql.adaptive.enabled",         "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled","true")

# View query plan to see what Spark is doing
df.explain("formatted")   # shows physical plan with AQE decisions</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="pyspark">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-pyspark">PySpark</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">How do you debug a slow PySpark job? What is <code>df.explain()</code>?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <div class="code-wrap"><span class="code-lang">Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python"># See what Spark plans to do (before execution)
df.explain()           # basic physical plan
df.explain("formatted") # detailed plan with stage info
df.explain("extended")  # logical + physical plan

# Common things to look for in the plan:
# BroadcastHashJoin   = broadcast join ✅ (good)
# SortMergeJoin       = shuffle join (expensive for large tables)
# Exchange hashpartitioning = shuffle happening here
# FileScan + PushedFilters = predicate pushdown ✅ (Spark reading less data)

# Spark UI (Databricks: Cluster → Spark UI tab)
# - Stages: find slowest stage, check shuffle read/write bytes
# - Tasks: look for skew (1 task = 10x longer than others)
# - SQL tab: see plan visually

# Quick diagnostics
df.printSchema()         # check types are correct (string vs int)
df.describe().show()     # basic statistics
df.count()               # check row count
df.filter(F.col("key").isNull()).count()  # check nulls on join key</code></pre></div>
      <p><strong>Common fixes:</strong> broadcast small table → remove shuffle, filter early → less data shuffled, OPTIMIZE Delta table → fewer small files, check for null join keys → data skew.</p>
    </div></div>
  </div>

  <div class="card" data-sec="pyspark">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-coding">Coding</span><span class="badge b-pyspark">PySpark</span><span class="badge b-easy">Easy</span></div><div class="card-q-text">🖥 Common data cleaning patterns — nulls, casting, dedup, string cleaning.</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <div class="code-wrap"><span class="code-lang">Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python">from pyspark.sql import functions as F

clean = (df
  # Cast types
  .withColumn("id",     F.col("id").cast("int"))
  .withColumn("amount", F.col("amount").cast("double"))
  .withColumn("date",   F.to_date("date_str", "yyyy-MM-dd"))

  # Handle nulls
  .filter(F.col("id").isNotNull())
  .fillna({"status": "unknown", "region": "UNKNOWN"})
  .withColumn("email", F.coalesce("email", F.lit("no-email@na.com")))

  # String cleaning
  .withColumn("name",  F.trim(F.col("name")))
  .withColumn("email", F.lower(F.col("email")))

  # Filter invalid data
  .filter(F.col("amount") > 0)
  .filter(F.col("status").isin(["active","inactive","pending"]))

  # Dedup
  .dropDuplicates(["id", "date"])

  # Derived columns
  .withColumn("year",  F.year("date"))
  .withColumn("flag",  F.when(F.col("amount") > 1000, True).otherwise(False))

  # Select final columns
  .select("id","name","email","amount","date","status","flag")
)</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="pyspark">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-pyspark">PySpark</span><span class="badge b-easy">Easy</span></div><div class="card-q-text">What are Pandas UDFs and when would you use them over regular UDFs?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p>Python UDFs are slow (serialize/deserialize row-by-row to Python). <strong>Pandas UDFs</strong> (vectorized) process whole columns as Pandas Series using Apache Arrow — much faster.</p>
      <div class="code-wrap"><span class="code-lang">Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python">from pyspark.sql.functions import pandas_udf
import pandas as pd

# Pandas UDF — vectorized, processes whole column at once
@pandas_udf("double")
def apply_tax(amount: pd.Series) -> pd.Series:
    return amount * 1.2

df.withColumn("amount_with_tax", apply_tax("amount"))

# Rule of thumb:
# Built-in Spark functions (F.upper, F.when, F.regexp_replace) → ALWAYS use first
# Pandas UDF → when no built-in exists and need Python logic on a column
# Python UDF → last resort (slowest)</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="pyspark">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-pyspark">PySpark</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">What is Structured Streaming in Spark? Show a basic streaming pipeline.</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p><strong>Structured Streaming</strong> processes unbounded (continuous) data as micro-batches using the same DataFrame API as batch. Use when data arrives continuously (Kafka, Auto Loader).</p>
      <div class="code-wrap"><span class="code-lang">Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python"># Read stream (Auto Loader — incremental file ingest)
stream_df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/checkpoints/schema")
    .load("/mnt/landing/orders/")
)

# Transform (same API as batch)
clean_df = stream_df.withColumn("date", F.to_date("date_str"))

# Write stream to Delta table
query = (
    clean_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/checkpoints/orders")
    .table("bronze.orders")
)
query.start()     # starts the continuous stream
# query.stop()   # stop it

# Batch vs Streaming
# spark.read      = batch (bounded — reads all files now)
# spark.readStream = streaming (unbounded — reads new files as they arrive)</code></pre></div>
    </div></div>
  </div>
</div><!-- /pyspark -->

<!-- ══ DELTA LAKE ══ -->
<div id="sec-delta" class="sec">
  <div class="sec-hdr"><h2>🔺 Delta Lake</h2><p>Highest-weight topic. Know MERGE, VACUUM, Z-ORDER, Time Travel, and Medallion Architecture inside out.</p></div>
  <div class="sec-controls"><button class="btn" onclick="expandAll('delta')">Expand All</button><button class="btn" onclick="collapseAll('delta')">Collapse All</button></div>
  <div class="top-alert"><span>⚠️</span><div>Candidates have been rejected for being unclear on Delta Lake and lacking hands-on knowledge of delta tables.</div></div>

  <div class="card" data-sec="delta">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-delta">Delta Lake</span><span class="badge b-easy">Easy</span></div><div class="card-q-text">What is Delta Lake? How is it different from Parquet, a Data Lake, and a Data Warehouse?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <table class="ref-table"><thead><tr><th></th><th>Data Warehouse<br><small>Snowflake, Redshift</small></th><th>Data Lake<br><small>Raw files in S3/ADLS</small></th><th>Delta Lake<br><small>Lakehouse</small></th></tr></thead><tbody>
        <tr><td><strong>ACID</strong></td><td>✅ Yes</td><td>❌ No</td><td>✅ Yes</td></tr>
        <tr><td><strong>Schema</strong></td><td>Schema-on-write</td><td>Schema-on-read</td><td>Schema enforcement + evolution</td></tr>
        <tr><td><strong>Data types</strong></td><td>Structured only</td><td>Any format</td><td>Any format, structured best</td></tr>
        <tr><td><strong>Cost</strong></td><td>Expensive</td><td>Cheap</td><td>Cheap (object storage)</td></tr>
        <tr><td><strong>Time Travel</strong></td><td>Some support</td><td>❌ No</td><td>✅ Yes</td></tr>
        <tr><td><strong>Updates/Deletes</strong></td><td>✅ Yes</td><td>❌ Painful</td><td>✅ Yes (MERGE/UPDATE/DELETE)</td></tr>
      </tbody></table>
      <div class="callout callout-blue"><strong>Delta Lake = Lakehouse.</strong> Best of both worlds — cheap object storage with data warehouse features (ACID, schema, updates, time travel).</div>
    </div></div>
  </div>

  <div class="card" data-sec="delta">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-delta">Delta Lake</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">How does the Delta transaction log work? What is in <code>_delta_log</code>?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p>The <code>_delta_log/</code> folder stores every operation as a numbered JSON commit file. Each file records which Parquet files were <strong>added</strong> and which were <strong>removed</strong>.</p>
      <ul>
        <li>Every write creates a new commit: <code>00000000000000000001.json</code></li>
        <li>Every 10 commits, a Parquet <strong>checkpoint</strong> is created for fast recovery</li>
        <li>Readers replay commits from last checkpoint to get current state</li>
        <li>If a write fails mid-way, the commit file is never created → table stays consistent</li>
      </ul>
      <div class="code-wrap"><span class="code-lang">SQL</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-sql">DESCRIBE HISTORY silver.orders;
-- version | timestamp | operation | operationParameters | numOutputRows</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="delta">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-delta">Delta Lake</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">What is Time Travel? How do you query a previous version?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <div class="code-wrap"><span class="code-lang">Python / SQL</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python"># By version number
df = spark.read.format("delta").option("versionAsOf", 5).load("/path/")
# By timestamp
df = spark.read.format("delta").option("timestampAsOf","2024-01-01").load("/path/")

# SQL syntax
spark.sql("SELECT * FROM silver.orders VERSION AS OF 5")
spark.sql("SELECT * FROM silver.orders TIMESTAMP AS OF '2024-01-15'")

# Restore table to previous version (overwrites current)
spark.sql("RESTORE TABLE silver.orders TO VERSION AS OF 3")</code></pre></div>
      <div class="callout callout-yellow">Time Travel only works while old Parquet files exist. Running VACUUM deletes them and breaks time travel for those versions.</div>
    </div></div>
  </div>

  <div class="card" data-sec="delta">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-delta">Delta Lake</span><span class="badge b-hard">Hard</span></div><div class="card-q-text">Write a full MERGE INTO (upsert) for a CDC use case.</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <div class="code-wrap"><span class="code-lang">Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python">from delta.tables import DeltaTable

delta_target = DeltaTable.forName(spark, "silver.customers")

delta_target.alias("t").merge(
    source_df.alias("s"),
    "t.customer_id = s.customer_id"
).whenMatchedUpdate(
    condition="s.operation = 'UPDATE'",
    set={"name": "s.name", "email": "s.email", "updated_at": "s.updated_at"}
).whenMatchedDelete(
    condition="s.operation = 'DELETE'"
).whenNotMatchedInsert(
    condition="s.operation != 'DELETE'",
    values={"customer_id":"s.customer_id","name":"s.name","email":"s.email"}
).execute()</code></pre></div>
      <div class="code-wrap"><span class="code-lang">SQL</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-sql">-- Simple upsert
MERGE INTO silver.customers AS t
USING bronze.customers_cdc AS s ON t.customer_id = s.customer_id
WHEN MATCHED     THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="delta">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-delta">Delta Lake</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">What is VACUUM? What is the risk? What is OPTIMIZE + Z-ORDER?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p><strong>VACUUM</strong> removes old Parquet files no longer in the transaction log. Default retention: 7 days (168 hours). Saves storage cost.</p>
      <p class="callout callout-red"><strong>Risk:</strong> After VACUUM, Time Travel to deleted versions is impossible.</p>
      <p><strong>OPTIMIZE</strong> compacts many small Parquet files into fewer larger ones — faster reads. <strong>Z-ORDER</strong> co-locates related data in same files so Spark can skip entire files when filtering.</p>
      <div class="code-wrap"><span class="code-lang">SQL</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-sql">VACUUM silver.orders RETAIN 168 HOURS;   -- default; dry run first
VACUUM silver.orders DRY RUN;            -- preview what would be deleted

OPTIMIZE silver.orders;                               -- compact files
OPTIMIZE silver.orders ZORDER BY (customer_id, date); -- compact + cluster</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="delta">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-delta">Delta Lake</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">What is schema enforcement vs schema evolution?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p><strong>Schema Enforcement</strong> (default) — Delta rejects writes that don't match the existing table schema. Prevents accidental corruption.</p>
      <p><strong>Schema Evolution</strong> — allow controlled changes (new columns) using options:</p>
      <div class="code-wrap"><span class="code-lang">Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python"># Allow new columns to be added
df.write.format("delta").option("mergeSchema","true").mode("append").saveAsTable("t")

# Replace entire schema (breaking change — column type change)
df.write.format("delta").option("overwriteSchema","true").mode("overwrite").saveAsTable("t")</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="delta">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-delta">Delta Lake</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">What is the Medallion Architecture? Explain Bronze, Silver, Gold with an example.</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <div class="pipeline">
        <div class="pstep bronze"><div class="ps-icon">🥉</div><h4>Bronze</h4><p>Raw ingest<br>Append-only<br>No transforms</p></div>
        <div class="pstep-gap"></div>
        <div class="pstep silver"><div class="ps-icon">🥈</div><h4>Silver</h4><p>Cleaned &amp; joined<br>Dedup + MERGE<br>Schema enforced</p></div>
        <div class="pstep-gap"></div>
        <div class="pstep gold"><div class="ps-icon">🥇</div><h4>Gold</h4><p>Aggregated<br>Business KPIs<br>Report-ready</p></div>
      </div>
      <table class="ref-table"><thead><tr><th>Layer</th><th>Purpose</th><th>Operations</th></tr></thead><tbody>
        <tr><td><strong>Bronze</strong></td><td>Raw data, source of truth</td><td>Append-only write, Auto Loader, no transformation</td></tr>
        <tr><td><strong>Silver</strong></td><td>Cleaned, conformed</td><td>Dedup, type casting, null fill, MERGE for CDC</td></tr>
        <tr><td><strong>Gold</strong></td><td>Business aggregates</td><td>groupBy, window functions, overwrite</td></tr>
      </tbody></table>
      <div class="callout callout-green"><strong>How to explain it:</strong> "Bronze stores raw data exactly as it arrives — no transformation. Silver cleans and deduplicates using MERGE so we track updates. Gold aggregates it into summaries that business users and dashboards consume."</div>
    </div></div>
  </div>

  <div class="card" data-sec="delta">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-coding">Coding</span><span class="badge b-delta">Delta Lake</span><span class="badge b-easy">Easy</span></div><div class="card-q-text">🖥 Create a Delta table, insert, update, and query with time travel — full DDL.</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <div class="code-wrap"><span class="code-lang">SQL</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-sql">-- Create managed Delta table
CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id  INT           NOT NULL,
    name         STRING,
    email        STRING,
    status       STRING        DEFAULT 'active',
    updated_at   TIMESTAMP
) USING DELTA
COMMENT 'Cleaned customer records'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- Insert data (creates version 1)
INSERT INTO silver.customers VALUES
  (1,'Alice','alice@example.com','active', current_timestamp()),
  (2,'Bob',  'bob@example.com',  'active', current_timestamp());

-- Update (creates version 2)
UPDATE silver.customers SET status='inactive', updated_at=current_timestamp()
WHERE customer_id = 2;

-- View history
DESCRIBE HISTORY silver.customers;

-- Time travel
SELECT * FROM silver.customers VERSION AS OF 1;  -- before update

-- Optimize
OPTIMIZE silver.customers ZORDER BY (customer_id);
VACUUM silver.customers;</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="delta">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-delta">Delta Lake</span><span class="badge b-modern">Modern</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">What is Liquid Clustering? How does it differ from partitioning + Z-ORDER?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <table class="ref-table"><thead><tr><th></th><th>Partitioning + Z-ORDER</th><th>Liquid Clustering</th></tr></thead><tbody>
        <tr><td>Setup</td><td>Decide at table creation, hard to change</td><td>Can change cluster columns anytime</td></tr>
        <tr><td>Small files</td><td>Can create too many small files</td><td>Handles automatically</td></tr>
        <tr><td>Re-layout</td><td>Full table rewrite needed to change</td><td>Incremental — only new data re-clustered</td></tr>
        <tr><td>High cardinality</td><td>Partitioning breaks for high-cardinality cols</td><td>Works well for high-cardinality</td></tr>
      </tbody></table>
      <div class="code-wrap"><span class="code-lang">SQL</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-sql">-- Create table with Liquid Clustering
CREATE TABLE silver.orders (order_id INT, customer_id INT, amount DOUBLE, date DATE)
USING DELTA CLUSTER BY (customer_id, date);

-- Incrementally re-cluster (only new/changed files)
OPTIMIZE silver.orders;

-- Change cluster columns without full rewrite
ALTER TABLE silver.orders CLUSTER BY (region, date);</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="delta">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-delta">Delta Lake</span><span class="badge b-easy">Easy</span></div><div class="card-q-text">What is Auto Loader? How does it work for Bronze ingestion?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p>Auto Loader (<code>cloudFiles</code> format) incrementally processes new files in cloud storage as they arrive. It tracks processed files via checkpointing — never reprocesses old files.</p>
      <div class="code-wrap"><span class="code-lang">Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python">df = (spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", "/chk/schema")
      .load("/mnt/landing/orders/"))

(df.writeStream.format("delta")
   .outputMode("append")
   .option("checkpointLocation", "/chk/bronze_orders")
   .table("bronze.orders")).start()</code></pre></div>
    </div></div>
  </div>
</div><!-- /delta -->

<!-- ══ DATABRICKS ESSENTIALS ══ -->
<div id="sec-databricks" class="sec">
  <div class="sec-hdr"><h2>🧱 Databricks Essentials</h2><p>Platform architecture, Unity Catalog, ETL pipeline flow, and day-to-day commands.</p></div>
  <div class="sec-controls"><button class="btn" onclick="expandAll('databricks')">Expand All</button><button class="btn" onclick="collapseAll('databricks')">Collapse All</button></div>

  <div class="card" data-sec="databricks">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-databricks">Databricks</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">Walk me through the Databricks architecture. What is the control plane vs data plane?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <table class="ref-table"><thead><tr><th>Control Plane (Databricks-managed)</th><th>Data Plane (Your cloud account)</th></tr></thead><tbody>
        <tr><td>Web UI, notebooks, job scheduler</td><td>Spark cluster VMs (compute)</td></tr>
        <tr><td>Cluster manager, billing, metadata</td><td>Data storage (ADLS, S3, GCS)</td></tr>
        <tr><td>Unity Catalog metastore</td><td>DBFS, Delta tables, MLflow artifacts</td></tr>
      </tbody></table>
      <div class="callout callout-blue">Your data never leaves your cloud account. Only metadata and job instructions flow through Databricks' control plane. This is a key security point.</div>
    </div></div>
  </div>

  <div class="card" data-sec="databricks">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-databricks">Databricks</span><span class="badge b-easy">Easy</span></div><div class="card-q-text">What is the difference between an All-Purpose Cluster and a Job Cluster?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <table class="ref-table"><thead><tr><th></th><th>All-Purpose Cluster</th><th>Job Cluster</th></tr></thead><tbody>
        <tr><td>Use case</td><td>Interactive notebooks (dev)</td><td>Production job runs</td></tr>
        <tr><td>Lifecycle</td><td>Runs until manually stopped</td><td>Created for one job, terminates after</td></tr>
        <tr><td>Cost</td><td>Expensive — always on</td><td>Cheaper — ephemeral</td></tr>
        <tr><td>Isolation</td><td>Shared by multiple users</td><td>Isolated per job run</td></tr>
      </tbody></table>
      <div class="callout callout-green">Always use Job Clusters in production. All-Purpose Clusters are for development only.</div>
    </div></div>
  </div>

  <div class="card" data-sec="databricks">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-databricks">Databricks</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">What is Unity Catalog? How is data organized? Show key commands.</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p>Unity Catalog is centralized governance — 3-level namespace <code>catalog.schema.table</code>, fine-grained access control, cross-workspace, full lineage and audit logs.</p>
      <div class="code-wrap"><span class="code-lang">SQL</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-sql">SHOW CATALOGS;
CREATE CATALOG IF NOT EXISTS finance_catalog;
USE CATALOG finance_catalog;
CREATE SCHEMA IF NOT EXISTS finance_catalog.silver;
SHOW TABLES IN finance_catalog.silver;

-- Grant access
GRANT SELECT ON TABLE finance_catalog.silver.customers TO `analyst_group`;
GRANT MODIFY ON SCHEMA finance_catalog.silver TO `engineer_group`;
SHOW GRANTS ON TABLE finance_catalog.silver.customers;</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="databricks">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-databricks">Databricks</span><span class="badge b-easy">Easy</span></div><div class="card-q-text">How does a typical Databricks ETL pipeline look end-to-end?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <div class="pipeline">
        <div class="pstep"><div class="ps-icon">📂</div><h4>Source</h4><p>Files land in ADLS/S3</p></div>
        <div class="pstep-gap"></div>
        <div class="pstep bronze"><div class="ps-icon">🥉</div><h4>Bronze</h4><p>Auto Loader<br>Raw append</p></div>
        <div class="pstep-gap"></div>
        <div class="pstep silver"><div class="ps-icon">🥈</div><h4>Silver</h4><p>PySpark + MERGE<br>Clean &amp; dedup</p></div>
        <div class="pstep-gap"></div>
        <div class="pstep gold"><div class="ps-icon">🥇</div><h4>Gold</h4><p>Aggregations<br>Overwrite</p></div>
        <div class="pstep-gap"></div>
        <div class="pstep"><div class="ps-icon">📊</div><h4>Output</h4><p>BI / ML / API</p></div>
      </div>
      <p>Each step runs as a task in a <strong>Databricks Workflow</strong> with Bronze→Silver→Gold dependencies. Scheduled on a Job Cluster (cron or file-arrival trigger). Failures send email alerts and retry automatically.</p>
    </div></div>
  </div>

  <div class="card" data-sec="databricks">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-coding">Coding</span><span class="badge b-databricks">Databricks</span><span class="badge b-easy">Easy</span></div><div class="card-q-text">🖥 Day-to-day SQL commands in Databricks — navigation, inspection, insert, update, delete.</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <div class="code-wrap"><span class="code-lang">SQL</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-sql">-- Navigate
SHOW CATALOGS;  SHOW SCHEMAS IN my_catalog;  SHOW TABLES IN my_catalog.silver;
USE CATALOG my_catalog;  USE SCHEMA silver;

-- Inspect
DESCRIBE TABLE customers;                  -- columns, types
DESCRIBE TABLE EXTENDED customers;         -- + location, properties
DESCRIBE HISTORY customers;               -- all versions
DESCRIBE DETAIL customers;                -- file count, size

-- Query
SELECT * FROM customers LIMIT 20;
SELECT COUNT(DISTINCT customer_id) FROM customers;

-- Insert
INSERT INTO silver.customers VALUES (101,'Alice','a@x.com','active',current_timestamp());
INSERT INTO silver.customers SELECT * FROM bronze.raw WHERE id=1;

-- Update / Delete
UPDATE silver.customers SET status='inactive' WHERE last_login < '2023-01-01';
DELETE FROM silver.customers WHERE status='deleted';

-- Table management
ALTER TABLE silver.customers ADD COLUMNS (phone STRING);
DROP TABLE IF EXISTS temp_customers;

-- Maintenance
OPTIMIZE silver.customers ZORDER BY (customer_id);
VACUUM silver.customers;
RESTORE TABLE silver.customers TO VERSION AS OF 2;</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="databricks">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-coding">Coding</span><span class="badge b-databricks">Databricks</span><span class="badge b-easy">Easy</span></div><div class="card-q-text">🖥 How do you use <code>%sql</code>, <code>%python</code>, and <code>dbutils</code> in a Databricks notebook?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <div class="code-wrap"><span class="code-lang">Notebook</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python"># Default Python cell
df = spark.read.table("silver.customers")
display(df)   # renders table/chart in Databricks UI

# %sql magic — run SQL in a Python notebook
# %sql
# SELECT region, COUNT(*) FROM silver.orders GROUP BY region

# dbutils — file system
dbutils.fs.ls("/mnt/landing/")
dbutils.fs.cp("/mnt/file.csv", "/mnt/processed/file.csv")

# dbutils — secrets (never hardcode passwords)
password = dbutils.secrets.get(scope="my-scope", key="db-password")

# dbutils — widgets (parameters passed from a job)
dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")   # "dev" or "prod"

# dbutils — run another notebook
dbutils.notebook.run("/pipelines/transform", timeout_seconds=600,
                     arguments={"env": "prod"})</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="databricks">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-databricks">Databricks</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">How do you test Databricks code before deploying to production?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <ol>
        <li><strong>Dev workspace:</strong> run against sample data, use <code>dbutils.widgets</code> for env switching</li>
        <li><strong>Data quality checks:</strong> assert row counts, check nulls on key columns after each layer</li>
        <li><strong>Code review:</strong> push to Git via Databricks Repos, raise a PR</li>
        <li><strong>Staging run:</strong> deploy to staging workspace with full dataset, validate outputs</li>
        <li><strong>Deploy to prod:</strong> via DABs (<code>databricks bundle deploy --target prod</code>) or CI/CD pipeline</li>
      </ol>
      <div class="code-wrap"><span class="code-lang">Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python">def assert_quality(df, table_name):
    assert df.count() > 0,                              f"{table_name}: empty"
    assert df.filter(F.col("id").isNull()).count() == 0, f"{table_name}: null IDs"

silver_df = transform(bronze_df)
assert_quality(silver_df, "silver.orders")
silver_df.write.format("delta").mode("append").saveAsTable("silver.orders")</code></pre></div>
    </div></div>
  </div>
</div><!-- /databricks -->

<!-- ══ DATABRICKS ADVANCED ══ -->
<div id="sec-advanced" class="sec">
  <div class="sec-hdr"><h2>🚀 Databricks Advanced</h2><p>Service Principals, DABs, CLI, Job creation, workspace login, cloud account linking, and secrets.</p></div>
  <div class="sec-controls"><button class="btn" onclick="expandAll('advanced')">Expand All</button><button class="btn" onclick="collapseAll('advanced')">Collapse All</button></div>

  <div class="card" data-sec="advanced">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-advanced">Advanced</span><span class="badge b-easy">Easy</span></div><div class="card-q-text">How do you log into Databricks? What is a workspace ID, URL, and Personal Access Token (PAT)?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p>A Databricks <strong>workspace</strong> is your team's isolated environment inside the platform — with its own notebooks, clusters, jobs, and Unity Catalog.</p>
      <table class="ref-table"><thead><tr><th>Term</th><th>What it is</th><th>Example</th></tr></thead><tbody>
        <tr><td><strong>Workspace URL</strong></td><td>The web address of your workspace</td><td><code>https://adb-1234567890.12.azuredatabricks.net</code></td></tr>
        <tr><td><strong>Workspace ID</strong></td><td>Numeric ID in the URL</td><td><code>1234567890</code></td></tr>
        <tr><td><strong>PAT</strong></td><td>Personal Access Token — authenticate API/CLI calls</td><td><code>dapi12abc34def...</code></td></tr>
      </tbody></table>
      <div class="code-wrap"><span class="code-lang">Steps</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-bash"># Generate a PAT:
# Settings → Developer → Access Tokens → Generate New Token

# Use PAT in CLI
databricks configure --token
# Enter: host = https://adb-xxxx.azuredatabricks.net
#        token = dapi...

# Use PAT in REST API calls
curl -H "Authorization: Bearer dapi..." \
     https://adb-xxxx.azuredatabricks.net/api/2.1/jobs/list

# Use PAT in Python
import requests
headers = {"Authorization": f"Bearer {pat_token}"}
response = requests.get(f"{workspace_url}/api/2.1/clusters/list", headers=headers)</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="advanced">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-advanced">Advanced</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">What is a Service Principal and why use it instead of a user account for production?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p>A <strong>Service Principal (SP)</strong> is a non-human identity (application identity) used for automated processes — CI/CD pipelines, scheduled jobs, API calls. Never run production jobs under a personal user account.</p>
      <p><strong>Why:</strong> User accounts are tied to a person — if they leave the company, the job breaks. SPs have their own credentials, scoped permissions, and don't expire when someone leaves.</p>
      <ul>
        <li>In Azure: Azure AD App Registration → assigned to Databricks workspace</li>
        <li>Authenticates via <strong>Client ID + Client Secret</strong> (not username/password)</li>
        <li>Assign only the permissions it needs (least privilege)</li>
        <li>Rotate secrets regularly; store in Azure Key Vault</li>
      </ul>
      <div class="code-wrap"><span class="code-lang">Bash / Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-bash"># Add SP to Databricks workspace (admin action)
databricks service-principals create --display-name "prod-etl-sp"

# Grant SP access to Unity Catalog
# GRANT USE CATALOG ON CATALOG prod_catalog TO `app_sp_id`;

# In Azure: assign "Storage Blob Data Contributor" on ADLS to SP
# → SP can read/write your data storage without keys in code</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="advanced">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-advanced">Advanced</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">What are Databricks Asset Bundles (DABs)? Why use them?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p><strong>DABs</strong> is Infrastructure-as-Code for Databricks — define jobs, pipelines, clusters, and permissions as a YAML file. Deploy the same code to dev/staging/prod without manual UI clicks.</p>
      <div class="code-wrap"><span class="code-lang">YAML — databricks.yml</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-yaml">bundle:
  name: orders_pipeline

targets:
  dev:
    mode: development
    workspace:
      host: https://adb-dev.azuredatabricks.net
  prod:
    mode: production
    workspace:
      host: https://adb-prod.azuredatabricks.net

resources:
  jobs:
    daily_etl:
      name: "Daily Orders ETL"
      schedule:
        quartz_cron_expression: "0 0 2 * * ?"  # 2 AM daily
        timezone_id: "UTC"
      tasks:
        - task_key: bronze_ingest
          notebook_task:
            notebook_path: ./notebooks/01_bronze
        - task_key: silver_transform
          depends_on: [{task_key: bronze_ingest}]
          notebook_task:
            notebook_path: ./notebooks/02_silver</code></pre></div>
      <div class="code-wrap"><span class="code-lang">Bash</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-bash">databricks bundle validate          # check YAML syntax
databricks bundle deploy --target dev   # deploy to dev workspace
databricks bundle run daily_etl --target prod  # trigger a run</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="advanced">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-advanced">Advanced</span><span class="badge b-easy">Easy</span></div><div class="card-q-text">What is the Databricks CLI? Show the most common commands.</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <div class="code-wrap"><span class="code-lang">Bash</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-bash"># Install
pip install databricks-cli

# Configure (enter workspace URL + PAT when prompted)
databricks configure --token

# ── Clusters ──────────────────────────────────────────────────
databricks clusters list
databricks clusters start --cluster-id abc123
databricks clusters delete --cluster-id abc123

# ── Jobs ──────────────────────────────────────────────────────
databricks jobs list
databricks jobs run-now --job-id 12345
databricks runs get --run-id 67890

# ── File System (DBFS) ────────────────────────────────────────
databricks fs ls dbfs:/mnt/
databricks fs cp localfile.csv dbfs:/mnt/landing/file.csv
databricks fs rm dbfs:/mnt/old_file.csv

# ── Secrets ───────────────────────────────────────────────────
databricks secrets create-scope --scope my-scope
databricks secrets put --scope my-scope --key db-password
databricks secrets list --scope my-scope

# ── Workspace / Notebooks ─────────────────────────────────────
databricks workspace list /Users/
databricks workspace export /path/notebook --format SOURCE --output local.py
databricks workspace import local.py /path/notebook --format SOURCE

# ── DABs ──────────────────────────────────────────────────────
databricks bundle validate
databricks bundle deploy --target dev
databricks bundle run job_name --target prod</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="advanced">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-advanced">Advanced</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">How do you create a Databricks Job step by step?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <ol>
        <li><strong>Workflows → Create Job</strong></li>
        <li>Name the job (e.g., <code>daily_orders_pipeline</code>)</li>
        <li>Add Task 1: choose type (Notebook / Python / SQL), set path, pick cluster</li>
        <li>Configure cluster: runtime version, node type, min/max workers (autoscaling), auto-termination 30 min</li>
        <li>Add Task 2: set dependency → "Depends on Task 1" (creates DAG)</li>
        <li>Set schedule: cron expression <code>0 0 2 * * ?</code> = 2 AM daily UTC</li>
        <li>Set alerts: email on failure, retry 2 times with 5-min interval</li>
        <li>Add parameters via widgets: <code>env=prod</code>, <code>table=orders</code></li>
        <li>Save → Run Now to test → enable schedule for production</li>
      </ol>
      <div class="callout callout-green">In production, prefer creating jobs via <strong>DABs YAML</strong> (version-controlled) rather than the UI (manual, hard to audit).</div>
    </div></div>
  </div>

  <div class="card" data-sec="advanced">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-advanced">Advanced</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">How is a Databricks workspace linked to an Azure cloud account?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <div class="code-wrap"><span class="code-lang">Architecture</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-bash">Azure Subscription
├── Your Resource Group
│   ├── ADLS Gen2 Storage (your data)
│   ├── Azure Key Vault (secrets)
│   └── Access Connector (Managed Identity → auth to storage)
│
└── Databricks Managed Resource Group (auto-created on workspace setup)
    ├── VMs (Spark cluster nodes)
    ├── Virtual Network (VNet)
    └── DBFS Storage Account (internal)</code></pre></div>
      <ul>
        <li>When you create a Databricks workspace, it creates a <strong>Managed Resource Group</strong> in your Azure subscription with the compute infrastructure</li>
        <li>Your data stays in <strong>your own</strong> ADLS Gen2 storage — Databricks never owns it</li>
        <li>Unity Catalog uses an <strong>Access Connector</strong> (Azure Managed Identity) with "Storage Blob Data Contributor" role to access ADLS without keys</li>
        <li>No data leaves your Azure subscription — only job metadata goes to Databricks control plane</li>
      </ul>
    </div></div>
  </div>

  <div class="card" data-sec="advanced">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-advanced">Advanced</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">How do you manage secrets in Databricks? What is Azure Key Vault backing?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p>Never hardcode passwords. Store them in Databricks Secrets and retrieve at runtime.</p>
      <div class="code-wrap"><span class="code-lang">Python / Bash</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-bash"># Create scope backed by Azure Key Vault (best practice)
databricks secrets create-scope \
  --scope akv-scope \
  --scope-backend-type AZURE_KEYVAULT \
  --resource-id "/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.KeyVault/vaults/{kv}" \
  --dns-name "https://my-keyvault.vault.azure.net/"</code></pre></div>
      <div class="code-wrap"><span class="code-lang">Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python"># In notebook — secret value is redacted if printed
password = dbutils.secrets.get(scope="akv-scope", key="db-password")

# Use in JDBC connection
df = spark.read.format("jdbc") \
    .option("url", "jdbc:sqlserver://server:1433;database=mydb") \
    .option("user", "admin") \
    .option("password", password) \
    .option("dbtable", "customers") \
    .load()</code></pre></div>
    </div></div>
  </div>
</div><!-- /advanced -->

<!-- ══ MODERN FEATURES ══ -->
<div id="sec-modern" class="sec">
  <div class="sec-hdr"><h2>✨ Modern Databricks Features</h2><p>Genie, Serverless Compute, Lakeflow, Liquid Clustering, Delta Sharing, AI/BI Dashboards.</p></div>
  <div class="sec-controls"><button class="btn" onclick="expandAll('modern')">Expand All</button><button class="btn" onclick="collapseAll('modern')">Collapse All</button></div>

  <div class="card" data-sec="modern">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-modern">Modern</span><span class="badge b-easy">Easy</span></div><div class="card-q-text">What is Genie? How does it work?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p><strong>Genie</strong> is Databricks' AI-powered natural language interface. Business users type questions in plain English → Genie writes SQL → runs it → returns results as a chart or table. No SQL knowledge needed.</p>
      <ul>
        <li>Data teams create <strong>Genie Spaces</strong> — curated collections of Delta tables + context (business definitions, example questions)</li>
        <li>End users (analysts, managers) ask: "What was last quarter's revenue by region?" → Genie writes and runs the SQL</li>
        <li>Powered by SQL Warehouse under the hood</li>
        <li>Lives in the AI/BI section of the Databricks workspace</li>
      </ul>
      <div class="callout callout-blue">Genie democratises data access — non-technical stakeholders can query data without knowing SQL or needing a data engineer for every question.</div>
    </div></div>
  </div>

  <div class="card" data-sec="modern">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-modern">Modern</span><span class="badge b-easy">Easy</span></div><div class="card-q-text">What is Serverless Compute in Databricks? What are the benefits?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p>Serverless means Databricks manages cluster infrastructure entirely — no cluster configuration needed, instant start, pay-per-second.</p>
      <table class="ref-table"><thead><tr><th></th><th>Classic Cluster</th><th>Serverless</th></tr></thead><tbody>
        <tr><td>Start time</td><td>5-10 minutes</td><td>Seconds</td></tr>
        <tr><td>Config</td><td>You pick node type, workers, etc.</td><td>No config — just run</td></tr>
        <tr><td>Cost</td><td>Pay even when idle</td><td>Pay per query/second</td></tr>
        <tr><td>Scaling</td><td>Manual or autoscaling (slow)</td><td>Instant auto-scale</td></tr>
      </tbody></table>
      <p>Two types: <strong>Serverless SQL Warehouse</strong> (SQL analytics) and <strong>Serverless Jobs</strong> (notebook/Python runs). Best for sporadic or low-latency workloads.</p>
    </div></div>
  </div>

  <div class="card" data-sec="modern">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-modern">Modern</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">What is Lakeflow (Delta Live Tables)? How does it differ from regular notebooks?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p><strong>Lakeflow Pipelines</strong> (formerly Delta Live Tables / DLT) is a declarative ETL framework — you define WHAT the data should look like, Databricks handles orchestration, retries, and data quality automatically.</p>
      <div class="code-wrap"><span class="code-lang">Python — Lakeflow/DLT</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python">import dlt
from pyspark.sql import functions as F

@dlt.table(comment="Raw orders from landing zone")
def bronze_orders():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format","json").load("/mnt/landing/orders/")

@dlt.table(comment="Cleaned orders")
@dlt.expect_or_drop("valid_amount", "amount > 0")   # auto data quality
@dlt.expect("order_id_not_null", "order_id IS NOT NULL")
def silver_orders():
    return dlt.read_stream("bronze_orders") \
              .withColumn("date", F.to_date("date_str"))

@dlt.table(comment="Monthly revenue")
def gold_revenue():
    return dlt.read("silver_orders") \
              .groupBy(F.date_trunc("month","date").alias("month")) \
              .agg(F.sum("amount").alias("revenue"))</code></pre></div>
      <p>Databricks auto-handles: ordering (runs Bronze before Silver), retries, lineage tracking, and data quality metrics — no manual wiring needed.</p>
    </div></div>
  </div>

  <div class="card" data-sec="modern">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-modern">Modern</span><span class="badge b-easy">Easy</span></div><div class="card-q-text">What is AI/BI Dashboards in Databricks?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p>AI/BI Dashboards is Databricks' native dashboarding tool (replaced the old Redash-based dashboards). Build drag-and-drop charts on top of SQL queries against Delta tables.</p>
      <ul>
        <li>Real-time data via SQL Warehouse — always shows current data</li>
        <li>No data export needed — charts query Delta tables directly</li>
        <li>Can embed AI insights (natural language summaries) alongside charts</li>
        <li>Shareable with business users — no Databricks account needed to view</li>
        <li>Governed by Unity Catalog — users only see data they have access to</li>
      </ul>
    </div></div>
  </div>

  <div class="card" data-sec="modern">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-modern">Modern</span><span class="badge b-easy">Easy</span></div><div class="card-q-text">What is Predictive Optimization?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p>Predictive Optimization automatically runs <code>OPTIMIZE</code> and <code>VACUUM</code> on Delta tables without you scheduling it manually. Databricks analyses table access patterns and decides when to compact.</p>
      <ul>
        <li>Only available for Unity Catalog managed tables</li>
        <li>No ops overhead — tables are always in good shape</li>
        <li>Can enable at catalog/schema/table level</li>
      </ul>
      <div class="code-wrap"><span class="code-lang">SQL</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-sql">ALTER TABLE silver.orders
SET TBLPROPERTIES ('delta.enablePredictiveOptimization' = 'true');</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="modern">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-modern">Modern</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">What is Delta Sharing?</div></div><span class="card-answer">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p>Delta Sharing is an open protocol for sharing Delta tables with external organizations — without copying data. The recipient reads live data directly from your storage via signed URLs or bearer tokens. They don't need Databricks.</p>
      <div class="code-wrap"><span class="code-lang">SQL / Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-sql">-- Provider: create a share
CREATE SHARE partner_share;
ALTER SHARE partner_share ADD TABLE gold.monthly_revenue;
CREATE RECIPIENT external_partner;
-- → generates activation link for recipient

-- Recipient (no Databricks needed):
-- pip install delta-sharing
-- import delta_sharing
-- df = delta_sharing.load_as_pandas("profile.share#partner_share.gold.monthly_revenue")</code></pre></div>
    </div></div>
  </div>
</div><!-- /modern -->

<!-- ══ SQL ══ -->
<div id="sec-sql" class="sec">
  <div class="sec-hdr"><h2>🗄️ SQL</h2><p>Intermediate queries — write them correctly on the first try. Errors here have failed candidates.</p></div>
  <div class="sec-controls"><button class="btn" onclick="expandAll('sql')">Expand All</button><button class="btn" onclick="collapseAll('sql')">Collapse All</button></div>

  <div class="card" data-sec="sql">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-sql">SQL</span><span class="badge b-easy">Easy</span></div><div class="card-q-text">What is the difference between ROW_NUMBER, RANK, and DENSE_RANK?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <table class="ref-table"><thead><tr><th>Function</th><th>Ties</th><th>Skips after tie?</th><th>Example (90,90,80)</th></tr></thead><tbody>
        <tr><td><code>ROW_NUMBER()</code></td><td>Different numbers</td><td>N/A</td><td>1, 2, 3</td></tr>
        <tr><td><code>RANK()</code></td><td>Same rank</td><td>Yes</td><td>1, 1, 3</td></tr>
        <tr><td><code>DENSE_RANK()</code></td><td>Same rank</td><td>No</td><td>1, 1, 2</td></tr>
      </tbody></table>
      <div class="code-wrap"><span class="code-lang">SQL</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-sql">SELECT name, department, salary,
  ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS row_num,
  RANK()       OVER (PARTITION BY department ORDER BY salary DESC) AS rnk,
  DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dense_rnk
FROM employees;</code></pre></div>
      <div class="callout callout-blue">Use <strong>DENSE_RANK</strong> for "2nd highest" — RANK would skip rank 2 if two employees share rank 1.</div>
    </div></div>
  </div>

  <div class="card" data-sec="sql">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-sql">SQL</span><span class="badge b-easy">Easy</span></div><div class="card-q-text">What is the difference between WHERE and HAVING?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p><strong>WHERE</strong> filters rows before aggregation. <strong>HAVING</strong> filters groups after aggregation. You cannot use aggregate functions in WHERE.</p>
      <div class="code-wrap"><span class="code-lang">SQL</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-sql">SELECT department, COUNT(*) AS cnt, AVG(salary) AS avg_sal
FROM employees
WHERE hire_date >= '2020-01-01'   -- filter rows first
GROUP BY department
HAVING COUNT(*) > 5               -- filter groups after agg
   AND AVG(salary) > 60000;</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="sql">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-sql">SQL</span><span class="badge b-easy">Easy</span></div><div class="card-q-text">What is a CTE and when would you use it over a subquery?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <div class="code-wrap"><span class="code-lang">SQL</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-sql">WITH active_customers AS (
    SELECT customer_id, name, region FROM customers WHERE status = 'active'
),
customer_totals AS (
    SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id
)
SELECT ac.name, ac.region, ct.total
FROM active_customers ac
JOIN customer_totals ct ON ac.customer_id = ct.customer_id
ORDER BY ct.total DESC;</code></pre></div>
      <p>CTEs improve readability, can be referenced multiple times in the same query, and make complex logic easier to debug.</p>
    </div></div>
  </div>

  <div class="card" data-sec="sql">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-coding">Coding</span><span class="badge b-sql">SQL</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">🖥 Find the employee with the second highest salary per department.</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <div class="code-wrap"><span class="code-lang">SQL</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-sql">WITH ranked AS (
    SELECT employee_id, name, department, salary,
           DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rnk
    FROM employees
)
SELECT employee_id, name, department, salary
FROM ranked
WHERE rnk = 2
ORDER BY department;</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="sql">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-coding">Coding</span><span class="badge b-sql">SQL</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">🖥 Calculate the running total of sales per customer ordered by date.</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <div class="code-wrap"><span class="code-lang">SQL</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-sql">SELECT customer_id, order_date, amount,
    SUM(amount) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM orders
ORDER BY customer_id, order_date;</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="sql">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-coding">Coding</span><span class="badge b-sql">SQL</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">🖥 Find customers who bought product A but never bought product B.</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <div class="code-wrap"><span class="code-lang">SQL</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-sql">-- Method 1: NOT EXISTS
SELECT DISTINCT customer_id FROM orders o1
WHERE product = 'A'
  AND NOT EXISTS (SELECT 1 FROM orders o2
                  WHERE o2.customer_id = o1.customer_id AND o2.product = 'B');

-- Method 2: Conditional aggregation (clean)
SELECT customer_id FROM orders
GROUP BY customer_id
HAVING MAX(CASE WHEN product='A' THEN 1 ELSE 0 END) = 1
   AND MAX(CASE WHEN product='B' THEN 1 ELSE 0 END) = 0;</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="sql">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-coding">Coding</span><span class="badge b-sql">SQL</span><span class="badge b-hard">Hard</span></div><div class="card-q-text">🖥 Calculate month-over-month revenue growth percentage.</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <div class="code-wrap"><span class="code-lang">SQL</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-sql">WITH monthly AS (
    SELECT DATE_TRUNC('month', order_date) AS month, SUM(amount) AS revenue
    FROM orders GROUP BY 1
),
with_prev AS (
    SELECT month, revenue,
           LAG(revenue,1) OVER (ORDER BY month) AS prev_revenue
    FROM monthly
)
SELECT month, revenue, prev_revenue,
       ROUND((revenue - prev_revenue)/prev_revenue * 100.0, 2) AS mom_pct
FROM with_prev
WHERE prev_revenue IS NOT NULL
ORDER BY month;</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="sql">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-coding">Coding</span><span class="badge b-sql">SQL</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">🖥 Find employees earning above their department average salary.</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <div class="code-wrap"><span class="code-lang">SQL</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-sql">WITH dept_avgs AS (
    SELECT employee_id, name, department, salary,
           AVG(salary) OVER (PARTITION BY department) AS dept_avg
    FROM employees
)
SELECT employee_id, name, department, salary, ROUND(dept_avg,2) AS dept_avg
FROM dept_avgs
WHERE salary > dept_avg
ORDER BY department, salary DESC;</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="sql">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-coding">Coding</span><span class="badge b-sql">SQL</span><span class="badge b-medium">Medium</span></div><div class="card-q-text">🖥 Show a self-join: find employees who share the same manager.</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <div class="code-wrap"><span class="code-lang">SQL</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-sql">-- Pairs of employees with same manager
SELECT e1.name AS emp1, e2.name AS emp2, e1.manager_id
FROM employees e1 JOIN employees e2
  ON e1.manager_id = e2.manager_id
WHERE e1.employee_id < e2.employee_id;  -- avoid (A,B) and (B,A)

-- Employee with their manager name
SELECT e.name AS employee, m.name AS manager
FROM employees e LEFT JOIN employees m ON e.manager_id = m.employee_id;</code></pre></div>
    </div></div>
  </div>
</div><!-- /sql -->

<!-- ══ PROJECT Q&A ══ -->
<div id="sec-project" class="sec">
  <div class="sec-hdr"><h2>💼 Project Q&amp;A</h2><p>How to describe your work and experience clearly. Use STAR format: Situation → Task → Action → Result.</p></div>
  <div class="sec-controls"><button class="btn" onclick="expandAll('project')">Expand All</button><button class="btn" onclick="collapseAll('project')">Collapse All</button></div>

  <div class="card" data-sec="project">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-project">Project</span></div><div class="card-q-text">"Tell me about a data pipeline you've worked on." — How do you structure this answer?</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p>Use <strong>STAR format</strong>. Keep it to 2 minutes. Always include: data volume, tools, your role, and the outcome.</p>
      <table class="ref-table"><thead><tr><th>Part</th><th>What to say</th></tr></thead><tbody>
        <tr><td><strong>Situation</strong></td><td>"We were migrating on-premises data processing to a cloud-based platform."</td></tr>
        <tr><td><strong>Task</strong></td><td>"My responsibility was to design and build the ingestion and transformation pipelines."</td></tr>
        <tr><td><strong>Action</strong></td><td>"I built Bronze/Silver/Gold Delta tables using the Medallion Architecture. Used PySpark to clean and transform raw files. Implemented MERGE INTO for incremental CDC updates. Scheduled via Databricks Workflows."</td></tr>
        <tr><td><strong>Result</strong></td><td>"The pipeline ran daily, processed X GB of data, and reduced reporting refresh time from X hours to X minutes."</td></tr>
      </tbody></table>
      <div class="callout callout-green">Always say what technology you used and what problem it solved — not just "I built a pipeline."</div>
    </div></div>
  </div>

  <div class="card" data-sec="project">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-project">Project</span></div><div class="card-q-text">"What challenges have you faced in data engineering and how did you solve them?"</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p>Name the problem → describe impact → diagnosis → fix → outcome. Good example topics:</p>
      <ul>
        <li><strong>Slow pipeline:</strong> "Found data skew in Spark UI — one task processing 80% of data. Fixed by salting the join key. Runtime dropped from 6 hours to 45 minutes."</li>
        <li><strong>Data quality issue:</strong> "Downstream reports had wrong totals. Added row count assertions and null checks after each transformation layer. Caught issues before they reached Gold."</li>
        <li><strong>Schema mismatch:</strong> "Source team added a new column without telling us. Delta's schema enforcement rejected the write. Added <code>mergeSchema=true</code> and set up a schema drift alert."</li>
      </ul>
    </div></div>
  </div>

  <div class="card" data-sec="project">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-project">Project</span></div><div class="card-q-text">"How do you ensure data quality in your pipelines?"</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p>Cover three layers:</p>
      <ol>
        <li><strong>Schema enforcement:</strong> Delta Lake rejects writes that don't match schema</li>
        <li><strong>Row-level validation:</strong> Assert no nulls on key columns, check row counts, validate value ranges after each transformation</li>
        <li><strong>Monitoring:</strong> Track row counts per run, alert on drops &gt;10%, use DLT expectations if available</li>
      </ol>
      <div class="code-wrap"><span class="code-lang">Python</span><button class="copy-btn" onclick="copyCode(this)">Copy</button><pre><code class="language-python">bronze_count = spark.read.table("bronze.orders").count()
silver_df    = transform(spark.read.table("bronze.orders"))

# Quality gate before writing
assert silver_df.count() >= bronze_count * 0.95, "Data loss > 5%"
assert silver_df.filter(F.col("order_id").isNull()).count() == 0, "Null order IDs"

silver_df.write.format("delta").mode("append").saveAsTable("silver.orders")</code></pre></div>
    </div></div>
  </div>

  <div class="card" data-sec="project">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-project">Project</span></div><div class="card-q-text">"Describe your dev-to-production deployment workflow."</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <ol>
        <li><strong>Dev workspace:</strong> Develop notebooks against sample data. Use <code>dbutils.widgets</code> to switch dev/prod paths.</li>
        <li><strong>Code review:</strong> Push to Git via Databricks Repos, raise a PR, peer review.</li>
        <li><strong>Staging:</strong> Deploy to staging workspace with real data, validate outputs against expected results.</li>
        <li><strong>Deploy to prod:</strong> <code>databricks bundle deploy --target prod</code> (DABs) or via CI/CD (Azure DevOps / GitHub Actions).</li>
        <li><strong>Monitor:</strong> Databricks Workflows dashboard, email alerts on failure, row count checks in notebook.</li>
      </ol>
    </div></div>
  </div>

  <div class="card" data-sec="project">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-project">Project</span></div><div class="card-q-text">"What is the difference between batch and streaming? Which have you used?"</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <table class="ref-table"><thead><tr><th></th><th>Batch</th><th>Streaming</th></tr></thead><tbody>
        <tr><td>Data</td><td>Bounded (all files at once)</td><td>Unbounded (arrives continuously)</td></tr>
        <tr><td>Latency</td><td>Minutes to hours</td><td>Seconds to minutes</td></tr>
        <tr><td>Use case</td><td>Daily ETL, reports</td><td>Real-time dashboards, event processing</td></tr>
        <tr><td>Spark API</td><td><code>spark.read</code></td><td><code>spark.readStream</code></td></tr>
        <tr><td>Auto Loader</td><td>Processes all files now</td><td>Processes new files as they arrive</td></tr>
      </tbody></table>
    </div></div>
  </div>

  <div class="card" data-sec="project">
    <div class="card-q" onclick="toggle(this)"><div class="card-q-body"><div class="card-badges"><span class="badge b-project">Project</span></div><div class="card-q-text">"What tools and technologies have you used in data engineering?"</div></div><span class="card-arrow">▼</span></div>
    <div class="card-ans"><div class="ans-body">
      <p>Mention tools in context — not just a list:</p>
      <div class="callout callout-green">"I've worked with <strong>Databricks</strong> for building ETL pipelines on Delta Lake tables following the Medallion Architecture. For data transformation I use <strong>PySpark</strong> — specifically DataFrames, window functions, and MERGE for CDC patterns. I write <strong>Python</strong> for utility scripts, API integrations, and data quality checks. For scheduling and orchestration I use <strong>Databricks Workflows</strong>. For governance, <strong>Unity Catalog</strong>. For ad-hoc analysis I use <strong>SQL</strong> directly in Databricks notebooks."</div>
    </div></div>
  </div>
</div><!-- /project -->

<!-- ══ PREP CHECKLIST ══ -->
<div id="sec-checklist" class="sec">
  <div class="sec-hdr"><h2>✅ Prep Checklist</h2><p>Click to mark done. Go through this the day before.</p></div>

  <h3 style="font-size:12px;color:#64748b;text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px">🔺 Delta Lake (Critical)</h3>
  <ul class="checklist">
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Can explain the transaction log and how it gives ACID guarantees</li>
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Can write a MERGE INTO for a CDC use case from memory</li>
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Know VACUUM — 7-day default, risk to time travel</li>
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Know OPTIMIZE and Z-ORDER — when to use each</li>
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Can write Time Travel queries (by version and timestamp)</li>
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Can explain Bronze/Silver/Gold with a clear one-paragraph description</li>
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Know mergeSchema vs overwriteSchema</li>
  </ul>

  <h3 style="font-size:12px;color:#64748b;text-transform:uppercase;letter-spacing:.5px;margin:14px 0 8px">⚡ PySpark</h3>
  <ul class="checklist">
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Can explain Transformation vs Action with 3 examples each</li>
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Can explain shuffle and how to minimize (broadcast, early filter)</li>
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Can write window function (rank, running total) from memory</li>
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Can solve "Top N per group" cleanly in 5 lines</li>
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Know all write modes: append, overwrite, replaceWhere, mergeSchema</li>
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Know how to read CSV/JSON/Parquet/Delta with options</li>
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Can read from S3 — knows two auth methods</li>
  </ul>

  <h3 style="font-size:12px;color:#64748b;text-transform:uppercase;letter-spacing:.5px;margin:14px 0 8px">🧱 Databricks</h3>
  <ul class="checklist">
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Can explain Control Plane vs Data Plane in one paragraph</li>
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Know Job Cluster vs All-Purpose Cluster</li>
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Can explain Unity Catalog: 3-level namespace, grants</li>
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Know workspace URL, workspace ID, and PAT</li>
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Can describe dev → staging → prod workflow</li>
  </ul>

  <h3 style="font-size:12px;color:#64748b;text-transform:uppercase;letter-spacing:.5px;margin:14px 0 8px">🚀 Advanced &amp; Modern</h3>
  <ul class="checklist">
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Can explain Service Principal and why it's used instead of a user account</li>
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Know DABs: YAML-based job deployment, databricks bundle deploy</li>
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Can name 5 Databricks CLI commands</li>
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Can explain Liquid Clustering vs partitioning + Z-ORDER in one sentence</li>
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Can explain Genie (NL→SQL AI tool for business users)</li>
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Can explain Serverless Compute: instant start, no config, pay-per-second</li>
  </ul>

  <h3 style="font-size:12px;color:#64748b;text-transform:uppercase;letter-spacing:.5px;margin:14px 0 8px">🐍 Python &amp; SQL</h3>
  <ul class="checklist">
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Can explain OOP: class, __init__, inheritance, super()</li>
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Know generators (yield), decorators (@), exception handling (try/except)</li>
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Can use json.loads/dumps and explain JSON structure</li>
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Know how to call an API with requests library</li>
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Can write 2nd highest salary per department correctly, first try</li>
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Know ROW_NUMBER vs RANK vs DENSE_RANK by heart</li>
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Can write running total and MoM growth queries</li>
  </ul>

  <h3 style="font-size:12px;color:#64748b;text-transform:uppercase;letter-spacing:.5px;margin:14px 0 8px">💼 Communication</h3>
  <ul class="checklist">
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Have a 2-minute STAR answer ready for "tell me about a pipeline you built"</li>
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Can explain Databricks vs Data Warehouse vs Delta Lake clearly</li>
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Start every answer with a one-line definition before going deep</li>
    <li onclick="toggleCheck(this)"><span class="chk-box"></span>Structure answers: "There are two parts: first X, then Y..."</li>
  </ul>
</div><!-- /checklist -->

</main>
</div>

<script>
function show(id){
  document.querySelectorAll('.sec').forEach(s=>s.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(n=>n.classList.remove('active'));
  var el=document.getElementById('sec-'+id);
  if(el) el.classList.add('active');
  document.querySelectorAll('.nav-item').forEach(n=>{
    if(n.getAttribute('onclick')&&n.getAttribute('onclick').includes("'"+id+"'")) n.classList.add('active');
  });
  window.scrollTo(0,0);
}
function toggle(btn){btn.closest('.card').classList.toggle('open')}
function expandAll(sec){document.querySelectorAll('[data-sec="'+sec+'"]').forEach(c=>c.classList.add('open'))}
function collapseAll(sec){document.querySelectorAll('[data-sec="'+sec+'"]').forEach(c=>c.classList.remove('open'))}
function copyCode(btn){
  var code=btn.closest('.code-wrap').querySelector('code');
  navigator.clipboard.writeText(code.innerText).then(()=>{
    var o=btn.textContent;btn.textContent='Copied!';btn.style.color='#4ade80';
    setTimeout(()=>{btn.textContent=o;btn.style.color=''},1500);
  });
}
function toggleCheck(li){
  li.classList.toggle('done');
  li.querySelector('.chk-box').textContent=li.classList.contains('done')?'✓':'';
}
document.addEventListener('DOMContentLoaded',()=>{
  document.querySelectorAll('pre code').forEach(el=>hljs.highlightElement(el));
  document.querySelectorAll('.card-q').forEach(q=>{
    q.addEventListener('click',()=>{
      setTimeout(()=>{q.closest('.card').querySelectorAll('pre code').forEach(el=>{if(!el.classList.contains('hljs'))hljs.highlightElement(el)})},50);
    });
  });
});
</script>
</body>
</html>
