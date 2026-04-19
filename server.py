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

        Key conventions:
        - Table names use underscores: ec_curvedata, nf_fields, g2c_curves, etc.
        - The prefix indicates the mathematical area (ec = elliptic curves,
          nf = number fields, g2c = genus 2 curves, gps = groups, mf = modular
          forms, etc.)

        Typical workflow for finding data:
        1. If you know what mathematical quantity you want but not where
           it lives, start with search_knowls (e.g. "frobenius traces",
           "conductor", "sato-tate group"). It returns table and column
           documentation ranked by relevance.
        2. Use describe_table to see the schema and descriptions for a
           specific table.
        3. Use run_sql to execute SELECT queries. Prefer single queries
           with GROUP BY / JOIN over multiple count_rows calls.

        Other notes:
        - Data about one mathematical object is often spread across
          multiple tables (e.g. elliptic curves over Q use ec_curvedata,
          ec_classdata, ec_localdata, ec_mwbsd).
        - Results are limited to 100,000 rows by default. For very large
          datasets, use WHERE clauses and aggregations.
        - Some tables have tens or hundreds of millions of rows. Queries
          that scan full large tables (e.g. ORDER BY on unindexed columns,
          or aggregations without WHERE clauses) may be slow. Use targeted
          WHERE clauses when possible.
    """),
)


@mcp.tool(annotations=_READ_ONLY)
def list_tables(prefix: str = "") -> str:
    """
    List all available tables in the LMFDB database.

    Args:
        prefix: Optional prefix to filter tables (e.g. "ec" for elliptic
                curves, "nf" for number fields, "g2c" for genus 2 curves,
                "gps" for groups, "mf" for modular forms).
    Returns:
        JSON array of {table_name, row_estimate} objects.
    """
    _log_tool("list_tables", prefix=prefix)
    sql = """
        SELECT c.relname AS table_name,
               c.reltuples::bigint AS row_estimate
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relkind = 'r'
    """
    params = []
    if prefix:
        sql += " AND c.relname LIKE %s"
        params.append(f"{prefix}%")
    sql += " ORDER BY c.relname"

    result = run_query(sql, params, limit=1000)
    if "error" in result:
        return json.dumps(result)
    return json.dumps(result["rows"], default=str)


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
    sql = """
        SELECT
            k.id,
            k.title,
            k.content,
            (SELECT COUNT(*) FROM unnest(%s::text[]) kw
             WHERE kw = ANY(k._keywords)) AS match_count
        FROM kwl_knowls k
        WHERE k._keywords && %s::text[]
          AND (k.id LIKE 'tables.%%' OR k.id LIKE 'columns.%%')
        ORDER BY match_count DESC, k.id
    """
    result = run_query(sql, [keywords, keywords], limit=limit * 3)
    if "error" in result:
        return json.dumps(result)

    # Dedupe by id, preserving order (some tables appear as duplicate rows
    # in pg_class on the read-only mirror)
    seen = set()
    deduped = []
    for row in result["rows"]:
        if row["id"] not in seen:
            seen.add(row["id"])
            deduped.append(row)
    return json.dumps(deduped[:limit], default=str)


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

    # Knowls: table-level and all column-level in one query
    knowl_sql = """
        SELECT id, content FROM kwl_knowls
        WHERE id = %s OR id LIKE %s
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


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import urllib.request
    import uvicorn
    from starlette.responses import HTMLResponse, Response
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

    # Add the landing page and favicon routes to the MCP app
    mcp_app.routes.append(Route("/", landing))
    mcp_app.routes.append(Route("/favicon.ico", favicon))
    mcp_app.middleware_stack = None  # force rebuild to pick up new routes

    uvicorn.run(mcp_app, host="0.0.0.0", port=port)
