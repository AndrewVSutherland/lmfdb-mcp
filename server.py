"""
LMFDB MCP Server
================
A remote MCP server that exposes the LMFDB PostgreSQL mirror as tools
for Claude (or any MCP client).

Deployment:
  - Google Cloud Run, Fly.io, Railway, or any container host
  - Users add the public URL as a Custom Connector in Claude settings

Connection defaults point to the public read-only LMFDB mirror at
devmirror.lmfdb.xyz (user: lmfdb, password: lmfdb, port: 5432).
Override via environment variables if using a different mirror.
"""

import json
import logging
import os
import re
import textwrap
import threading

import psycopg2
import psycopg2.extras
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("lmfdb-mcp")

# ---------------------------------------------------------------------------
# Configuration (override with environment variables)
# ---------------------------------------------------------------------------
DB_HOST = os.environ.get("LMFDB_HOST", "devmirror.lmfdb.xyz")
DB_PORT = int(os.environ.get("LMFDB_PORT", "5432"))
DB_NAME = os.environ.get("LMFDB_DBNAME", "lmfdb")
DB_USER = os.environ.get("LMFDB_USER", "lmfdb")
DB_PASS = os.environ.get("LMFDB_PASSWORD", "lmfdb")

MAX_ROWS = int(os.environ.get("LMFDB_MAX_ROWS", "100000"))
DEFAULT_LIMIT = int(os.environ.get("LMFDB_DEFAULT_LIMIT", "100"))

# Export / bulk download config. Exports stream results to a download URL
# instead of returning them through the MCP tool response, so the row cap
# can be much higher than MAX_ROWS (which is bounded by what fits in an
# LLM's context window).
EXPORT_MAX_ROWS = int(os.environ.get("LMFDB_EXPORT_MAX_ROWS", "1000000"))
EXPORT_DEFAULT_ROWS = int(os.environ.get("LMFDB_EXPORT_DEFAULT_ROWS", "100000"))
EXPORT_TOKEN_TTL = int(os.environ.get("LMFDB_EXPORT_TOKEN_TTL", "600"))  # seconds
EXPORT_MAX_TOKENS = int(os.environ.get("LMFDB_EXPORT_MAX_TOKENS", "1000"))

# Public base URL for download links. In production this should be set to
# the externally-visible URL of this server (e.g. https://mcp.lmfdb.org).
# If unset, we fall back to a value derived from the request host at
# download time, which works for local testing but not behind load balancers
# without X-Forwarded-* handling.
MCP_BASE_URL = os.environ.get("MCP_BASE_URL", "").rstrip("/")

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_connection():
    """Get a database connection, reusing if possible."""
    global _connection
    try:
        if _connection is not None and _connection.closed == 0:
            _connection.isolation_level
            return _connection
    except Exception:
        pass
    _connection = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        connect_timeout=30,
        options="-c statement_timeout=120000",  # 120s query timeout
    )
    _connection.autocommit = True
    return _connection

_connection = None


# Regex to detect and capture a top-level LIMIT clause (with optional OFFSET)
_LIMIT_RE = re.compile(r"\bLIMIT\s+(\d+)(\s+OFFSET\s+\d+)?\s*$", re.IGNORECASE)

# Concurrency limit: at most 5 queries running at once
_query_semaphore = threading.Semaphore(5)

# Patterns that are blocked to prevent resource exhaustion on devmirror.
# These are case-insensitive substring/regex checks on the SQL text.
_BLOCKED_PATTERNS = [
    (re.compile(r"\bpg_sleep\b", re.IGNORECASE),
     "pg_sleep is not allowed."),
    (re.compile(r"\bWITH\s+RECURSIVE\b", re.IGNORECASE),
     "Recursive CTEs are not allowed (risk of unbounded execution)."),
    (re.compile(r"\bCROSS\s+JOIN\b", re.IGNORECASE),
     "CROSS JOIN is not allowed (risk of cartesian product explosion)."),
    (re.compile(r"\bgenerate_series\b", re.IGNORECASE),
     "generate_series is not allowed (risk of memory exhaustion)."),
    (re.compile(r"\bpg_authid\b", re.IGNORECASE),
     "Access to pg_authid is not allowed."),
    (re.compile(r"\bpg_shadow\b", re.IGNORECASE),
     "Access to pg_shadow is not allowed."),
    (re.compile(r"\bpg_roles\b", re.IGNORECASE),
     "Access to pg_roles is not allowed."),
    (re.compile(r"\bCOPY\b", re.IGNORECASE),
     "COPY is not allowed."),
    (re.compile(r"\bIMPORT\b", re.IGNORECASE),
     "IMPORT is not allowed."),
]


def _check_blocked(sql: str) -> str | None:
    """Return an error message if the SQL matches a blocked pattern, else None."""
    for pattern, message in _BLOCKED_PATTERNS:
        if pattern.search(sql):
            return message
    # Block multiple statements (semicolon followed by more SQL)
    # Strip trailing whitespace/semicolons, then check for internal semicolons
    core = sql.strip().rstrip(";").strip()
    if ";" in core:
        return "Multiple SQL statements are not allowed."
    return None


def run_query(sql: str, params: list | None = None, limit: int | None = None) -> dict:
    """
    Execute a read-only SQL query and return results as a dict with
    'columns' and 'rows' keys.  Enforces SELECT-only and row limits.
    """
    # Basic safety: only allow SELECT / WITH / EXPLAIN
    stripped = sql.strip()
    keyword = stripped.split()[0].upper() if stripped else ""
    if keyword not in ("SELECT", "WITH", "EXPLAIN"):
        return {"error": "Only SELECT queries are allowed on the read-only mirror."}

    # Check for blocked patterns
    blocked = _check_blocked(stripped)
    if blocked:
        return {"error": blocked}

    effective_limit = min(limit or DEFAULT_LIMIT, MAX_ROWS)

    # If query has a LIMIT, cap it at MAX_ROWS; otherwise append one
    clean = stripped.rstrip(";").rstrip()
    match = _LIMIT_RE.search(clean)
    if match:
        user_limit = int(match.group(1))
        capped = min(user_limit, MAX_ROWS)
        offset_part = match.group(2) or ""
        sql = clean[:match.start()] + f"LIMIT {capped}{offset_part}"
    else:
        sql = clean + f" LIMIT {effective_limit}"

    # Limit concurrent queries to protect devmirror
    acquired = _query_semaphore.acquire(timeout=30)
    if not acquired:
        return {"error": "Too many concurrent queries. Please try again shortly."}

    try:
        conn = get_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Pass None (not []) when there are no parameters, so psycopg2
            # skips %-substitution and bare % characters (e.g. in LIKE
            # patterns) are treated literally.
            cur.execute(sql, params if params else None)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description] if cur.description else []
            return {
                "columns": columns,
                "row_count": len(rows),
                "rows": [dict(r) for r in rows],
            }
    except Exception as e:
        global _connection
        _connection = None
        return {"error": str(e)}
    finally:
        _query_semaphore.release()


def _validate_identifier(name: str) -> bool:
    """Check that a name is a valid SQL identifier (alphanumeric + underscores)."""
    return bool(name) and name.replace("_", "").isalnum()


def _log_tool(name: str, **kwargs):
    """Log a tool invocation with its arguments."""
    args = ", ".join(f"{k}={v!r}" for k, v in kwargs.items() if v)
    log.info("tool=%s %s", name, args)


# All tools are read-only; this tells Claude to auto-approve them.
_READ_ONLY = {"readOnlyHint": True, "destructiveHint": False}


# ---------------------------------------------------------------------------
# MCP Server
# ---------------------------------------------------------------------------

mcp = FastMCP(
    name="LMFDB",
    stateless_http=True,
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=False,
    ),
    instructions=textwrap.dedent("""\
        You are connected to the LMFDB (L-functions and Modular Forms Database).
        Use the tools below to explore tables, inspect schemas, and run SQL
        queries against the read-only PostgreSQL mirror of the LMFDB.

        Orientation:
        - Call overview() first to see a curated map of the production tables
          grouped by mathematical section, with a hub table identified per
          section and notes on grain and joins. This is almost always the
          fastest path from a question to the right table.
        - Table names use underscores: ec_curvedata, nf_fields, g2c_curves, etc.
          The prefix indicates the mathematical area (ec = elliptic curves,
          nf = number fields, g2c = genus 2 curves, gps = groups, mf = modular
          forms, lfunc = L-functions, etc.)

        Typical workflow:
        1. overview() — which section covers what you need, and which table
           is the hub for that section.
        2. search_knowls(keywords) — if overview() doesn't obviously answer
           "which column stores X", search documentation for the column
           (e.g. "frobenius traces", "conductor", "sato-tate group").
        3. describe_table(name) — confirm columns, types, and column-level
           descriptions before writing SQL.
        4. run_sql(sql) — prefer single queries with GROUP BY / JOIN over
           multiple count_rows calls.

        For bulk downloads destined for client-side analysis (pandas,
        numpy, plotting, ML), use export_query(sql, format) INSTEAD of
        run_sql. It returns a short-lived download URL, keeping the data
        out of the conversation entirely — so it can return far more rows
        than run_sql's 100,000 cap. Typical pattern:
            result = export_query("SELECT ... FROM ec_curvedata WHERE ...")
            # in a sandbox / code-execution environment:
            import pandas as pd
            df = pd.read_csv(result["url"])

        Canonical joins — elliptic curves over Q:
          ec_curvedata <-> ec_mwbsd / ec_galrep / ec_iwasawa : ON lmfdb_label (1:1)
          ec_curvedata <-> ec_localdata / ec_torsion_growth  : ON lmfdb_label (1:N)
          ec_curvedata <-> ec_classdata                      : ON lmfdb_iso  (N:1)
        ec_classdata is keyed by isogeny class (lmfdb_iso) and has no
        lmfdb_label column — always route curve-level joins through
        ec_curvedata. Rank is in ec_curvedata.rank (Mordell-Weil rank).

        Canonical joins — genus 2 curves over Q:
          g2c_curves <-> g2c_endomorphisms / g2c_ratpts / g2c_tamagawa : ON label
          g2c_curves <-> g2c_galrep                                    : ON lmfdb_label
        Note the quirk: g2c_galrep uses lmfdb_label while the other g2c_*
        tables use label for the same identifier.

        Other notes:
        - Data about one mathematical object is often spread across multiple
          tables; see overview() for the per-section breakdown.
        - Results are limited to 100,000 rows by default. For very large
          datasets, use WHERE clauses and aggregations.
        - Some tables have tens or hundreds of millions of rows. Queries
          that scan full large tables (e.g. ORDER BY on unindexed columns,
          or aggregations without WHERE clauses) may be slow. Use targeted
          WHERE clauses when possible.
    """),
)


# ---------------------------------------------------------------------------
# lmfdb_tables metadata cache
# ---------------------------------------------------------------------------
# Curated metadata about LMFDB tables lives in the lmfdb_tables table
# (name, section, status, mcp_visible, notes). We cache it in memory for
# _METADATA_TTL seconds so list_tables() and overview() don't do a DB
# roundtrip on every call, but so curation updates propagate promptly.

_METADATA_TTL = 300  # seconds
_metadata_cache = {"rows": None, "fetched_at": 0.0}
_metadata_lock = threading.Lock()


def _fetch_metadata() -> list:
    """Return a list of dicts from lmfdb_tables, using a short-TTL cache."""
    import time
    now = time.time()
    with _metadata_lock:
        if (
            _metadata_cache["rows"] is not None
            and now - _metadata_cache["fetched_at"] < _METADATA_TTL
        ):
            return _metadata_cache["rows"]
    # Fetch outside the lock (DB call); last-write-wins on concurrent refresh.
    result = run_query(
        "SELECT id, name, section, status, mcp_visible, notes "
        "FROM lmfdb_tables ORDER BY id",
        limit=1000,
    )
    rows = result.get("rows", []) if "error" not in result else []
    with _metadata_lock:
        _metadata_cache["rows"] = rows
        _metadata_cache["fetched_at"] = now
    return rows


# ---------------------------------------------------------------------------
# Export token store
# ---------------------------------------------------------------------------
# export_query() registers a validated SELECT along with a format, returning
# a short-lived opaque token. The /download/<token> endpoint later looks up
# the SQL by token, streams the result set as CSV or JSONL, and discards
# the token on expiry. Tokens are stateful and process-local; with Cloud Run
# min-instances=1 and our expected traffic, cross-instance routing hasn't
# been an issue. A restart loses pending tokens; clients should retry.

import secrets
import time as _time

_export_tokens: dict = {}
_export_tokens_lock = threading.Lock()


def _prune_expired_tokens(now: float | None = None) -> None:
    """Drop any tokens whose expires_at is in the past. Caller holds the lock."""
    if now is None:
        now = _time.time()
    expired = [t for t, rec in _export_tokens.items() if rec["expires_at"] <= now]
    for t in expired:
        del _export_tokens[t]


def _register_export(sql: str, fmt: str, max_rows: int) -> dict:
    """
    Register a validated SELECT query for later streaming. Returns a dict
    with token, expires_at, and other metadata. Raises RuntimeError if
    the token store is full.
    """
    now = _time.time()
    token = secrets.token_urlsafe(24)
    record = {
        "sql": sql,
        "format": fmt,
        "max_rows": max_rows,
        "created_at": now,
        "expires_at": now + EXPORT_TOKEN_TTL,
    }
    with _export_tokens_lock:
        _prune_expired_tokens(now)
        if len(_export_tokens) >= EXPORT_MAX_TOKENS:
            raise RuntimeError(
                "Export token store is full; try again in a few minutes."
            )
        _export_tokens[token] = record
    return {"token": token, **record}


def _lookup_export(token: str) -> dict | None:
    """Return the SQL/format record for a token, or None if missing/expired."""
    now = _time.time()
    with _export_tokens_lock:
        rec = _export_tokens.get(token)
        if rec is None:
            return None
        if rec["expires_at"] <= now:
            del _export_tokens[token]
            return None
        return dict(rec)  # return a copy


# LIMIT-handling helper specifically for export queries. The regular
# run_query() caps LIMITs at MAX_ROWS; for exports we use EXPORT_MAX_ROWS
# instead. Same shape as the logic in run_query().
def _apply_export_limit(sql: str, requested_limit: int) -> str:
    """Return sql with a LIMIT clause that respects EXPORT_MAX_ROWS."""
    clean = sql.strip().rstrip(";").rstrip()
    effective = min(max(1, requested_limit), EXPORT_MAX_ROWS)
    match = _LIMIT_RE.search(clean)
    if match:
        user_limit = int(match.group(1))
        capped = min(user_limit, EXPORT_MAX_ROWS)
        offset_part = match.group(2) or ""
        return clean[: match.start()] + f"LIMIT {capped}{offset_part}"
    return clean + f" LIMIT {effective}"


def _estimate_row_count(sql: str) -> int | None:
    """
    Run EXPLAIN (FORMAT JSON) <sql> and return the top-level row estimate.
    Returns None on failure; the caller can proceed without an estimate.
    """
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(f"EXPLAIN (FORMAT JSON) {sql}")
            plan = cur.fetchone()[0]
            return int(plan[0]["Plan"]["Plan Rows"])
    except Exception as e:
        log.info("EXPLAIN failed for export row estimate: %s", e)
        return None


@mcp.tool(annotations=_READ_ONLY)
def overview() -> str:
    """
    Curated map of the LMFDB (L-functions and Modular Forms Database)
    by mathematical area — ALWAYS CALL THIS FIRST for any task
    involving number theory data.

    Covers these mathematical areas: elliptic curves over Q and over
    number fields, classical modular forms, Maass forms, Hilbert
    modular forms, Bianchi modular forms, genus 2 curves, higher
    genus families, L-functions, number fields, p-adic fields,
    Dirichlet characters, Artin representations, Galois groups,
    Sato-Tate groups, abstract groups, abelian varieties over finite
    fields, Belyi maps.

    Returns a sectioned JSON map of production tables grouped by
    mathematical area, with for each table:
      - the hub table of each section (the table to start joins from)
      - grain of each table (per-curve, per-isogeny-class, etc.)
      - canonical join columns (often the key to avoiding wrong joins)
      - format quirks where present

    Use this BEFORE search_knowls / describe_table / list_tables — it
    usually shortcuts the whole discovery workflow. Follow up with:
      - search_knowls(keywords): find the column storing a specific
        mathematical quantity when overview doesn't already tell you
      - describe_table(name): see the full schema of a specific table
      - run_sql(query): execute a SELECT query
      - count_rows, sample_rows, table_stats: convenience wrappers
        for common operations without writing SQL

    Takes no arguments.

    Returns:
        JSON object with shape:
          {
            "sections": [
              {
                "section": "Elliptic curves over Q",
                "tables": [
                  {"name": "ec_curvedata", "notes": "Hub table ..."},
                  ...
                ]
              },
              ...
            ]
          }
    """
    _log_tool("overview")
    rows = _fetch_metadata()
    # Keep only production + mcp_visible rows for the overview output.
    visible = [
        r for r in rows
        if r.get("status") == "production" and r.get("mcp_visible")
    ]
    sections = {}
    section_order = []  # preserve first-seen order (which matches id order)
    for r in visible:
        sec = r["section"]
        if sec not in sections:
            sections[sec] = []
            section_order.append(sec)
        sections[sec].append({"name": r["name"], "notes": r["notes"]})
    output = {
        "sections": [
            {"section": sec, "tables": sections[sec]}
            for sec in section_order
        ]
    }
    return json.dumps(output, default=str)


@mcp.tool(annotations=_READ_ONLY)
def list_tables(prefix: str = "", status: str = "production") -> str:
    """
    List LMFDB tables with metadata (section, notes) when available.

    If you're starting a new task, prefer overview() — it gives the same
    metadata grouped by mathematical area and is usually more useful.
    Use list_tables when you need a flat list or when filtering by a
    prefix you already know.

    By default returns only production tables flagged visible through the
    MCP (status='production', mcp_visible=true in the lmfdb_tables
    metadata). Pass status='all' to also include tables that are not
    curated (test tables, beta tables, obsolete variants, etc.) — useful
    for developers and researchers who want access to work-in-progress
    data.

    Auxiliary tables ending in _counts or _stats (psycodict machinery)
    are always filtered out.

    Args:
        prefix: Optional table-name prefix. Common prefixes:
          ec (elliptic curves over Q), ec_nf (elliptic curves over
          number fields), nf (number fields), lf (p-adic fields),
          mf (classical modular forms), hmf (Hilbert modular forms),
          bmf (Bianchi modular forms), maass (Maass forms),
          g2c (genus 2 curves), hgcwa (higher genus families),
          av_fq (abelian varieties over Fq), belyi (Belyi maps),
          lfunc (L-functions), char (Dirichlet characters),
          artin (Artin representations), gps (groups — abstract,
          Galois, Sato-Tate).
        status: 'production' (default) returns only production-visible
                tables. 'all' returns every matching table including
                uncurated ones. Other values ('beta', 'alpha', 'obsolete')
                filter by that exact status value in lmfdb_tables.

    Returns:
        JSON array of {table_name, section, row_estimate, notes} objects.
        section and notes are null for tables not present in lmfdb_tables.
    """
    _log_tool("list_tables", prefix=prefix, status=status)

    # Pull physical tables from pg_class (authoritative for existence and
    # row estimates).
    sql = """
        SELECT c.relname AS table_name,
               c.reltuples::bigint AS row_estimate
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relkind = 'r'
          AND c.relname NOT LIKE %s
          AND c.relname NOT LIKE %s
    """
    params = ["%_counts", "%_stats"]
    if prefix:
        sql += " AND c.relname LIKE %s"
        params.append(f"{prefix}%")
    sql += " ORDER BY c.relname"

    result = run_query(sql, params, limit=1000)
    if "error" in result:
        return json.dumps(result)

    # Enrich with metadata from lmfdb_tables and apply status filter.
    metadata = {r["name"]: r for r in _fetch_metadata()}
    output = []
    for row in result["rows"]:
        meta = metadata.get(row["table_name"])
        if status == "all":
            # No filter; include everything, curated or not.
            pass
        elif status == "production":
            # Default: require curated + production-visible.
            if not meta or meta.get("status") != "production" \
                    or not meta.get("mcp_visible"):
                continue
        else:
            # Exact-match filter on a specific status value.
            if not meta or meta.get("status") != status:
                continue
        output.append({
            "table_name": row["table_name"],
            "row_estimate": row["row_estimate"],
            "section": meta["section"] if meta else None,
            "notes": meta["notes"] if meta else None,
        })
    return json.dumps(output, default=str)


@mcp.tool(annotations=_READ_ONLY)
def search_knowls(query: str, limit: int = 20) -> str:
    """
    Search LMFDB documentation for tables and columns matching the given
    keywords. Use this to discover which table or column contains a
    mathematical quantity you're looking for.

    This is typically the first tool to call when you know what
    mathematical data you want but don't know where it's stored. For
    example:
      - search_knowls("frobenius traces") finds columns storing a_p values
      - search_knowls("isogeny class") finds tables about isogeny classes
      - search_knowls("rational points") finds columns with rational point data
      - search_knowls("analytic rank") finds the relevant column(s)

    Args:
        query: One or more keywords (e.g. "frobenius traces",
               "conductor elliptic curve", "sato-tate").
        limit: Maximum results to return (default 20, max 100).
    Returns:
        JSON array of {id, title, content, match_count} objects, ranked
        by how many query keywords match. IDs have the form
        "tables.<table_name>" or "columns.<table_name>.<column_name>".
    """
    _log_tool("search_knowls", query=query, limit=limit)

    # Split query into lowercase keywords; ignore very short tokens
    keywords = [w.lower() for w in re.findall(r"\w+", query) if len(w) >= 2]
    if not keywords:
        return json.dumps({"error": "No searchable keywords in query."})

    limit = min(max(1, limit), 100)

    # Find matching knowls, sorted by number of matching keywords descending.
    # The && operator checks array overlap (fast); the subquery counts matches.
    # kwl_knowls keeps a full revision history per id (like wikipedia), so we
    # use DISTINCT ON + ORDER BY timestamp DESC in a CTE to restrict to the
    # latest revision of each knowl before scoring and ranking.
    sql = """
        WITH latest AS (
            SELECT DISTINCT ON (id) id, title, content, _keywords
            FROM kwl_knowls
            WHERE _keywords && %s::text[]
              AND (id LIKE 'tables.%%' OR id LIKE 'columns.%%')
            ORDER BY id, timestamp DESC
        )
        SELECT
            id,
            title,
            content,
            (SELECT COUNT(*) FROM unnest(%s::text[]) kw
             WHERE kw = ANY(_keywords)) AS match_count
        FROM latest
        ORDER BY match_count DESC, id
    """
    result = run_query(sql, [keywords, keywords], limit=limit)
    if "error" in result:
        return json.dumps(result)

    return json.dumps(result["rows"], default=str)


@mcp.tool(annotations=_READ_ONLY)
def describe_table(table_name: str) -> str:
    """
    Show the column names, types, and (where available) documentation
    for a given LMFDB table.

    The output includes:
      - table_name
      - description: the contents of the knowl "tables.<table_name>", if any,
        describing what the table represents
      - columns: a list of {column_name, data_type, is_nullable, description,
        array_length} objects. "description" comes from the knowl
        "columns.<table_name>.<column_name>", if any. "array_length" is
        present for array-typed columns and reports the typical length
        sampled from a few rows (useful for columns that store a fixed
        number of values, e.g. aplist has length 25 for a_p with p < 100).

    Knowl content may contain LaTeX, markdown, and nested knowl references.

    Args:
        table_name: The table to describe (e.g. "ec_curvedata", "g2c_curves").
    Returns:
        JSON object with keys: table_name, description, columns.
    """
    _log_tool("describe_table", table_name=table_name)
    if not _validate_identifier(table_name):
        return json.dumps({"error": "Invalid table name."})

    # Columns from pg_catalog
    cols_sql = """
        SELECT a.attname AS column_name,
               pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
               CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relname = %s
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY a.attnum
    """
    cols_result = run_query(cols_sql, [table_name], limit=500)
    if "error" in cols_result:
        return json.dumps(cols_result)
    if not cols_result["rows"]:
        return json.dumps({"error": f"Table '{table_name}' not found."})

    # Knowls: table-level and all column-level in one query.
    # kwl_knowls keeps a full revision history per id (like wikipedia),
    # so DISTINCT ON + ORDER BY timestamp DESC selects the latest revision.
    knowl_sql = """
        SELECT DISTINCT ON (id) id, content FROM kwl_knowls
        WHERE id = %s OR id LIKE %s
        ORDER BY id, timestamp DESC
    """
    table_knowl_id = f"tables.{table_name}"
    column_knowl_prefix = f"columns.{table_name}.%"
    knowl_result = run_query(
        knowl_sql, [table_knowl_id, column_knowl_prefix], limit=500
    )
    knowls = {}
    if "error" not in knowl_result:
        knowls = {row["id"]: row["content"] for row in knowl_result["rows"]}

    # For array columns, sample a few rows to report typical length.
    # This surfaces information that is often critical (e.g. "aplist"
    # contains 25 values — a_p for the primes < 100).
    array_cols = [
        c["column_name"] for c in cols_result["rows"]
        if c["data_type"].endswith("[]")
    ]
    array_lengths = {}
    if array_cols:
        length_exprs = ", ".join(
            f'array_length("{c}", 1) AS "{c}"' for c in array_cols
        )
        sample_sql = f'SELECT {length_exprs} FROM "{table_name}" LIMIT 5'
        sample_result = run_query(sample_sql, limit=5)
        if "error" not in sample_result and sample_result["rows"]:
            for col in array_cols:
                values = [r[col] for r in sample_result["rows"] if r[col] is not None]
                if not values:
                    array_lengths[col] = None
                elif all(v == values[0] for v in values):
                    array_lengths[col] = f"length: {values[0]}"
                else:
                    array_lengths[col] = (
                        f"length: variable (sample: {min(values)}–{max(values)})"
                    )

    # Attach descriptions and array-length hints to columns
    columns = []
    for col in cols_result["rows"]:
        col["description"] = knowls.get(
            f"columns.{table_name}.{col['column_name']}"
        )
        if col["column_name"] in array_lengths:
            col["array_length"] = array_lengths[col["column_name"]]
        columns.append(col)

    output = {
        "table_name": table_name,
        "description": knowls.get(table_knowl_id),
        "columns": columns,
    }
    return json.dumps(output, default=str)


@mcp.tool(annotations=_READ_ONLY)
def sample_rows(table_name: str, n: int = 5) -> str:
    """
    Return a random sample of rows from a table to understand its contents.

    Args:
        table_name: The table to sample (e.g. "ec_curvedata").
        n: Number of rows to return (default 5, max 50).
    Returns:
        JSON with columns and sample rows.
    """
    _log_tool("sample_rows", table_name=table_name, n=n)
    if not _validate_identifier(table_name):
        return json.dumps({"error": "Invalid table name."})
    n = min(max(1, n), 50)

    # Use TABLESAMPLE for large tables (fast, truly random);
    # fall back to ORDER BY random() for small tables where
    # TABLESAMPLE might return 0 rows.
    sql = f"""
        SELECT * FROM "{table_name}" TABLESAMPLE SYSTEM (1)
        LIMIT {n}
    """
    result = run_query(sql, limit=n)
    if "error" not in result and result["row_count"] > 0:
        return json.dumps(result, default=str)

    # Fallback for small tables
    sql = f'SELECT * FROM "{table_name}" ORDER BY random() LIMIT {n}'
    result = run_query(sql, limit=n)
    return json.dumps(result, default=str)


@mcp.tool(annotations=_READ_ONLY)
def run_sql(sql: str, limit: int = 100) -> str:
    """
    Run a read-only SQL SELECT query against the LMFDB database.

    This is the most flexible tool — use it for filtering, joining,
    aggregating, or computing statistics across LMFDB tables.

    Args:
        sql: A SELECT query. Only SELECT/WITH/EXPLAIN statements are allowed.
             Examples:
               - SELECT label, rank, conductor FROM ec_curvedata WHERE rank >= 3 LIMIT 20
               - SELECT st_group, COUNT(*) FROM g2c_curves GROUP BY st_group
               - SELECT AVG(rank) FROM ec_curvedata WHERE torsion_order = 5
        limit: Maximum rows to return (default 100, max 100000).
    Returns:
        JSON with 'columns', 'row_count', and 'rows'.
    """
    _log_tool("run_sql", sql=sql[:1000], limit=limit)
    result = run_query(sql, limit=limit)
    return json.dumps(result, default=str)


@mcp.tool(annotations=_READ_ONLY)
def count_rows(table_name: str, where: str = "") -> str:
    """
    Count rows in a table, optionally with a WHERE clause.

    Args:
        table_name: The table to count (e.g. "ec_curvedata").
        where: Optional WHERE clause without the WHERE keyword
               (e.g. "rank >= 2 AND conductor < 1000").
    Returns:
        JSON with the count.
    """
    _log_tool("count_rows", table_name=table_name, where=where)
    if not _validate_identifier(table_name):
        return json.dumps({"error": "Invalid table name."})

    sql = f'SELECT COUNT(*) AS count FROM "{table_name}"'
    if where:
        sql += f" WHERE {where}"

    result = run_query(sql, limit=1)
    if "error" in result:
        return json.dumps(result)
    return json.dumps(result["rows"][0] if result["rows"] else {"count": 0}, default=str)


@mcp.tool(annotations=_READ_ONLY)
def table_stats(table_name: str, column: str, where: str = "") -> str:
    """
    Compute basic statistics for a numeric column in a table.
    For non-numeric columns, returns count and distinct count only.

    Args:
        table_name: The table (e.g. "ec_curvedata").
        column: The column to analyze (e.g. "rank", "conductor").
        where: Optional WHERE clause without the keyword.
    Returns:
        JSON with count, distinct_count, and (for numeric columns)
        min, max, avg, stddev.
    """
    _log_tool("table_stats", table_name=table_name, column=column, where=where)
    if not _validate_identifier(table_name):
        return json.dumps({"error": "Invalid table name."})
    if not _validate_identifier(column):
        return json.dumps({"error": "Invalid column name."})

    where_clause = f" WHERE {where}" if where else ""

    # Try the full numeric version first
    sql = f"""
        SELECT
            COUNT(*) AS count,
            COUNT(DISTINCT "{column}") AS distinct_count,
            MIN("{column}") AS min,
            MAX("{column}") AS max,
            AVG("{column}"::numeric) AS avg,
            STDDEV("{column}"::numeric) AS stddev
        FROM "{table_name}"{where_clause}
    """
    result = run_query(sql, limit=1)

    if "error" in result and "numeric" in result["error"].lower():
        # Column isn't numeric; fall back to count/distinct only
        sql = f"""
            SELECT
                COUNT(*) AS count,
                COUNT(DISTINCT "{column}") AS distinct_count,
                MIN("{column}"::text) AS min,
                MAX("{column}"::text) AS max
            FROM "{table_name}"{where_clause}
        """
        result = run_query(sql, limit=1)
        if "error" in result:
            return json.dumps(result)
        row = result["rows"][0] if result["rows"] else {}
        row["note"] = f"Column '{column}' is not numeric; avg and stddev are not available."
        return json.dumps(row, default=str)

    if "error" in result:
        return json.dumps(result)
    return json.dumps(result["rows"][0] if result["rows"] else {}, default=str)


@mcp.tool(annotations=_READ_ONLY)
def export_query(
    sql: str,
    format: str = "csv",
    max_rows: int = EXPORT_DEFAULT_ROWS,
) -> str:
    """
    Prepare a bulk download of a SELECT query's results. Returns a short-
    lived URL that streams the full result set as CSV or JSONL — the data
    does NOT go through the MCP conversation, so this is the right tool
    for anything destined for client-side analysis (pandas, numpy, plots,
    machine learning, etc.) rather than direct inspection.

    Typical use: ask the LLM to write a query joining several tables and
    selecting the columns you want, call export_query, then in the LLM's
    code-execution sandbox:

        import pandas as pd
        df = pd.read_csv("<returned url>")
        # or for JSONL:
        # df = pd.read_json("<returned url>", lines=True)
        ... analysis ...

    Compared to run_sql:
      - run_sql returns rows inline (capped at 100,000, meant for direct
        inspection; everything flows through the LLM's context window).
      - export_query returns a URL (default cap 100,000 rows, hard cap
        1,000,000; data bypasses the LLM's context entirely).

    The download URL is valid for about 10 minutes and can be fetched
    multiple times within that window. Only SELECT / WITH / EXPLAIN
    queries are allowed (same safety rules as run_sql).

    Args:
        sql: A SELECT query. Same rules as run_sql.
        format: 'csv' (default, universal, types coerced to strings) or
                'jsonl' (JSON Lines; preserves types; pandas reads via
                pd.read_json(url, lines=True)).
        max_rows: Maximum rows to export (default 100,000, hard cap
                  1,000,000). If the query has its own LIMIT, the smaller
                  of the two is used.

    Returns:
        JSON object:
          {
            "url": "https://.../download/<token>",
            "format": "csv",
            "estimated_rows": 84321,       # from EXPLAIN; may be null
            "max_rows": 100000,            # cap actually applied
            "expires_in_seconds": 600,
            "sandbox_hint": "import pandas as pd; df = pd.read_csv(url)"
          }
        Or {"error": "..."} on failure.
    """
    _log_tool("export_query", sql=sql[:1000], format=format, max_rows=max_rows)

    # Format validation
    fmt = format.lower()
    if fmt not in ("csv", "jsonl"):
        return json.dumps({
            "error": "format must be 'csv' or 'jsonl'.",
        })

    # SQL validation — mirror run_query's checks (SELECT-only, blocklist,
    # single statement).
    stripped = sql.strip()
    keyword = stripped.split()[0].upper() if stripped else ""
    if keyword not in ("SELECT", "WITH", "EXPLAIN"):
        return json.dumps({
            "error": "Only SELECT / WITH / EXPLAIN queries can be exported.",
        })
    blocked = _check_blocked(stripped)
    if blocked:
        return json.dumps({"error": blocked})

    # Apply the export-specific LIMIT cap.
    try:
        requested = int(max_rows)
    except (TypeError, ValueError):
        return json.dumps({"error": "max_rows must be an integer."})
    if requested < 1:
        return json.dumps({"error": "max_rows must be >= 1."})
    bounded_sql = _apply_export_limit(stripped, requested)

    # Cheap planner-based row estimate (not exact — could be off by an
    # order of magnitude for complex joins, but useful for orientation).
    estimated = _estimate_row_count(bounded_sql)

    # Register the token.
    try:
        rec = _register_export(bounded_sql, fmt, requested)
    except RuntimeError as e:
        return json.dumps({"error": str(e)})

    # Build the public URL. MCP_BASE_URL is preferred; otherwise the
    # caller must use a relative path (unusual for an LLM-facing tool).
    base = MCP_BASE_URL or "https://mcp.lmfdb.org"
    url = f"{base}/download/{rec['token']}"

    hint = (
        'import pandas as pd; df = pd.read_csv(url)'
        if fmt == "csv"
        else 'import pandas as pd; df = pd.read_json(url, lines=True)'
    )

    return json.dumps({
        "url": url,
        "format": fmt,
        "estimated_rows": estimated,
        "max_rows": min(requested, EXPORT_MAX_ROWS),
        "expires_in_seconds": EXPORT_TOKEN_TTL,
        "sandbox_hint": hint,
    })


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import urllib.request
    import uvicorn
    from starlette.responses import HTMLResponse, Response, StreamingResponse
    from starlette.routing import Route

    port = int(os.environ.get("PORT", "8080"))
    mcp_app = mcp.streamable_http_app()

    # Fetch the LMFDB favicon at startup so we can serve it from memory.
    # Fall back gracefully if the fetch fails.
    FAVICON_BYTES = b""
    try:
        with urllib.request.urlopen(
            "https://www.lmfdb.org/favicon.ico", timeout=10
        ) as resp:
            FAVICON_BYTES = resp.read()
        log.info("Loaded LMFDB favicon (%d bytes)", len(FAVICON_BYTES))
    except Exception as e:
        log.warning("Could not fetch LMFDB favicon: %s", e)

    LANDING_HTML = """\
<!DOCTYPE html>
<html>
<head>
<title>LMFDB MCP Server</title>
<link rel="icon" href="/favicon.ico">
</head>
<body>
<h1>LMFDB MCP Server</h1>
<p>This is a <a href="https://modelcontextprotocol.io/">Model Context Protocol</a>
server providing read-only access to the
<a href="https://www.lmfdb.org">LMFDB</a> database.</p>
<h2>Setup</h2>
<ol>
<li>In <a href="https://claude.ai">Claude</a>, go to
<strong>Settings &rarr; Connectors &rarr; Add custom connector</strong></li>
<li>Enter the URL: <code>https://mcp.lmfdb.org/mcp</code></li>
<li>Enable the LMFDB connector in your conversation</li>
</ol>
<p>Source code: <a href="https://github.com/AndrewVSutherland/lmfdb-mcp">
github.com/AndrewVSutherland/lmfdb-mcp</a></p>
</body>
</html>"""

    async def landing(request):
        return HTMLResponse(LANDING_HTML)

    async def favicon(request):
        if FAVICON_BYTES:
            return Response(
                FAVICON_BYTES,
                media_type="image/x-icon",
                headers={"Cache-Control": "public, max-age=86400"},
            )
        return Response(status_code=404)

    # ----- /download/<token> -----------------------------------------------
    # Streams the result set of a registered export query as CSV or JSONL.
    # The query is run on a fresh connection with a server-side named cursor
    # so that very large result sets don't materialize in server memory.
    def _stream_rows(sql: str, fmt: str, token: str):
        """Sync generator yielding encoded chunks. Run by Starlette's
        threadpool (StreamingResponse handles sync iterables)."""
        import csv
        import io

        # Fresh connection for streaming — distinct from the shared one in
        # get_connection(), because server-side cursors require a transaction
        # (we can't use autocommit) and we don't want to disturb interactive
        # queries that share the cached connection.
        conn = None
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASS,
                connect_timeout=30,
                options="-c statement_timeout=600000",  # 10 min for exports
            )
            # Named cursor -> PostgreSQL server-side cursor (streamed fetch).
            cursor_name = f"mcp_export_{token[:16]}"
            with conn.cursor(name=cursor_name) as cur:
                cur.itersize = 10000  # rows per network roundtrip
                cur.execute(sql)

                # psycopg2 named cursors: execute() issues DECLARE, which
                # returns no row description. cur.description is None until
                # the first FETCH, so we pull the first batch before reading
                # it. We then stream that batch followed by the remainder.
                first_batch = cur.fetchmany(cur.itersize)
                columns = (
                    [desc[0] for desc in cur.description]
                    if cur.description else []
                )

                def _all_rows():
                    yield from first_batch
                    yield from cur

                # Accumulate output in a buffer and only yield when it
                # crosses FLUSH_BYTES. Yielding one tiny chunk per row
                # is ~100x slower end-to-end: Starlette's threadpool hop
                # per next() plus Cloud Run's front-end buffering of
                # small chunks dominates actual throughput (observed
                # ~200 ms per yield for ~20-byte chunks). Batching keeps
                # the stream behaviour (large exports still flow) while
                # amortising per-yield overhead.
                FLUSH_BYTES = 64 * 1024
                buf = io.StringIO()

                if fmt == "csv":
                    writer = csv.writer(buf)
                    writer.writerow(columns)
                    for row in _all_rows():
                        # csv.writer coerces everything via str(); None
                        # becomes empty string, which is the CSV convention.
                        writer.writerow(
                            ["" if v is None else v for v in row]
                        )
                        if buf.tell() >= FLUSH_BYTES:
                            yield buf.getvalue().encode("utf-8")
                            buf.seek(0)
                            buf.truncate()
                else:  # jsonl
                    for row in _all_rows():
                        obj = {col: row[i] for i, col in enumerate(columns)}
                        buf.write(json.dumps(obj, default=str))
                        buf.write("\n")
                        if buf.tell() >= FLUSH_BYTES:
                            yield buf.getvalue().encode("utf-8")
                            buf.seek(0)
                            buf.truncate()

                # Flush any trailing bytes.
                if buf.tell() > 0:
                    yield buf.getvalue().encode("utf-8")
        except Exception as e:
            log.error("export stream failed for token=%s: %s", token[:8], e)
            # We've likely already started sending the response; we can't
            # cleanly propagate an error. Emit a comment-ish trailing line
            # so anyone reading the file at least sees something went wrong.
            if fmt == "csv":
                yield f"\n# export failed mid-stream: {e}\n".encode("utf-8")
            else:
                yield (json.dumps({"_error": str(e)}) + "\n").encode("utf-8")
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    async def download(request):
        token = request.path_params.get("token", "")
        rec = _lookup_export(token)
        if rec is None:
            return Response(
                "Download token not found or expired.",
                status_code=404,
                media_type="text/plain",
            )

        fmt = rec["format"]
        sql = rec["sql"]
        log.info(
            "tool=download token=%s... fmt=%s",
            token[:8], fmt,
        )

        if fmt == "csv":
            media_type = "text/csv"
            filename = f"lmfdb-export-{token[:8]}.csv"
        else:
            media_type = "application/x-ndjson"
            filename = f"lmfdb-export-{token[:8]}.jsonl"

        return StreamingResponse(
            _stream_rows(sql, fmt, token),
            media_type=media_type,
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "Cache-Control": "no-store",
            },
        )

    # Add the landing page, favicon, and download routes to the MCP app.
    mcp_app.routes.append(Route("/", landing))
    mcp_app.routes.append(Route("/favicon.ico", favicon))
    mcp_app.routes.append(Route("/download/{token}", download))
    mcp_app.middleware_stack = None  # force rebuild to pick up new routes

    uvicorn.run(mcp_app, host="0.0.0.0", port=port)
