# LMFDB MCP Server

A remote [MCP](https://modelcontextprotocol.io/) server that gives Claude
(and other MCP clients) direct read-only access to the
[LMFDB](https://www.lmfdb.org) PostgreSQL mirror.

Once deployed and connected, users can make natural-language requests like:

> "Create a scatter plot of number of rational points vs analytic rank for
> genus 2 curves in the LMFDB with Sato-Tate group USp(4)."

…and Claude will query the database, analyze results, and produce plots —
all within the chat interface, no code to run.

The code in this repository was written entirely by Claude Opus 4.6.

## Architecture

```
┌──────────────┐     HTTPS      ┌──────────────────┐    PostgreSQL  ┌──────────────────┐
│  Claude.ai   │ ──────────────►│  lmfdb-mcp       │ ──────────────►│ devmirror        │
│  (any user)  │   MCP protocol │  (Cloud Run)     │   read-only    │ .lmfdb.xyz:5432  │
└──────────────┘                └──────────────────┘                └──────────────────┘
```

## Available Tools

| Tool            | Description                                           |
|:----------------|:------------------------------------------------------|
| `overview`      | Overview of LMFDB sections and tables                 |
| `list_tables`   | Curated list of tables, optionally filtered by prefix |
| `describe_table`| Show column names and types for a table               |
| `search_knowls` | Search table/column knowls using keywords             |
| `sample_rows`   | Return a small sample from a table                    |
| `run_sql`       | Run an arbitrary SELECT query (max 100,000 rows)      |
| `count_rows`    | Count rows with optional WHERE clause                 |
| `table_stats`   | Compute min/max/avg/stddev for a numeric column       |
| `export_query`  | Bulk download (CSV or JSONL) via short-lived URL |

## Connecting Claude to the MCP server

All Claude models (Haiku/Sonnet/Opus) can use tools via MCP servers.

### For Individual Users (Pro / Max / Free)

1. Go to [claude.ai](https://claude.ai) → **Settings** → **Connectors**
2. Click **Add custom connector**
3. Enter:
   - **Name:** `LMFDB`
   - **URL:** `https://mcp.lmfdb.org/mcp`
4. Click **Add**
5. Set Read-only tools to **Always allow** (optional, lets Claude use the tool without asking permission). 
6. In any conversation, click **+** → **Connectors** → enable **LMFDB**

### For Teams / Enterprise

An organization Owner adds the connector once in **Organization Settings →
Connectors**, and all members can then enable it per-conversation.

## Enabling sandbox access for `export_query` (Claude.ai)

The `export_query` tool returns a URL on `https://mcp.lmfdb.org/` instead
of streaming rows through the MCP conversation. For Claude to actually
fetch that URL inside its code-execution sandbox — where pandas, numpy,
and plotting happen — the domain needs to be on the sandbox's
network-egress allowlist. The other eight tools return data directly
through the MCP conversation and are unaffected; this setup is only
needed if you (or your users) want to use `export_query`.

### Setup

1. Go to [claude.ai](https://claude.ai) → **Settings** → **Capabilities**.
2. Under **Code execution and file creation**, make sure the feature is
   enabled and turn on **Allow network egress**.
3. Set the domain allowlist to **Package managers and specific domains**
   and add `mcp.lmfdb.org` to **Additional allowed domains**.

### Settings apply to new conversations only

Allowlist changes are copied into the sandbox when a conversation
starts. Toggling settings mid-conversation has no effect on the current
chat — start a fresh conversation after updating.

## Connecting ChatGPT to the MCP server

In ChatGPT MCP connectors are treated as "apps".  Custom apps (including the LMFDB MCP server) are not available on the Free/Go plan, as you need to enable **Developer mode** in order to install them.

[Apps cannot be used in Pro models](https://help.openai.com/en/articles/11487775-connectors-in-chatgpt) (but they can be used in all other models available under the Pro plan).

### For Individual Users (Pro / Plus)

1. Go to [chatgpt.com](https://chatgpt.com) **→ Settings → Apps → Advanced settings**
2. Enable **Developer mode**.
3. Go to **Settings → Apps** and click **Create app**.
4. Enter the app details:
   - **Name:** `LMFDB`
   - **MCP server URL:** `https://mcp.lmfdb.org/mcp`
   - **Authentication:** `NoAuth`
   - **I understand and want to continue:** tick the box (OpenAI has not reviewed this MCP server).
5. Click **Create**.
6. In any chat, use **+ → More** to add the **LMFDB** app.

### For Business / Enterprise / Education

An administrator needs to enable developer mode and create a new app for
the LMFDB connector, all members can then enable it per-conversation.

## Deployment to Google Cloud Run

The instructions below assume you are an administrator of the lmfdb-mirror project on Google Cloud.

### Steps

```bash
# 1. Set your project
gcloud config set project YOUR_PROJECT_ID

# 2. Build and push the container
gcloud builds submit --tag us-central1-docker.pkg.dev/lmfdbmirror/lmfdb-mcp/lmfdb-mcp

# 3. Deploy to Cloud Run
gcloud run deploy lmfdb-mcp \
  --image us-central1-docker.pkg.dev/lmfdbmirror/lmfdb-mcp/lmfdb-mcp \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --port 8080 \
  --memory 512Mi \
  --timeout 300 \
  --min-instances=1
```

## Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run locally
python server.py

# Test with MCP Inspector
npx @modelcontextprotocol/inspector http://localhost:8000/mcp
```

## Environment Variables (optional)

| Variable             | Default               | Description                    |
|:---------------------|:----------------------|:-------------------------------|
| `LMFDB_HOST`         | `devmirror.lmfdb.xyz` | PostgreSQL host                |
| `LMFDB_PORT`         | `5432`                | PostgreSQL port                |
| `LMFDB_DBNAME`       | `lmfdb`               | Database name                  |
| `LMFDB_USER`         | `lmfdb`               | Database user                  |
| `LMFDB_PASSWORD`     | `lmfdb`               | Database password              |
| `LMFDB_MAX_ROWS`     | `100000`              | Hard limit on returned rows    |
| `LMFDB_DEFAULT_LIMIT`| `100`                 | Default LIMIT if not specified |
| `PORT`               | `8000`                | HTTP port (Cloud Run sets this)|


## Security Notes

- The server only allows `SELECT`, `WITH`, and `EXPLAIN` queries.
- A 120-second statement timeout prevents runaway queries.
- Row results are capped at 100,000.
- The underlying LMFDB mirror is read-only; writes will fail at the
  database level regardless of what SQL is sent.
- The default config is auth-less, which is appropriate since
  the LMFDB mirror credentials are public.

## License

GPL-3.0 (matching lmfdb-lite)
