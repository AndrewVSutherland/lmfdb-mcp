# LMFDB MCP Server

A remote [MCP](https://modelcontextprotocol.io/) server that gives Claude
(and other MCP clients) direct read-only access to the
[LMFDB](https://www.lmfdb.org) PostgreSQL mirror.

Once deployed and connected, users can make natural-language requests like:

> "Download all genus 2 curves with Sato-Tate group USp(4), create a scatter
> plot of number of rational points vs analytic rank, and compute the
> correlation."

…and Claude will query the database, analyze the results, and produce plots —
all within the chat interface, with no code to run.

## Architecture

```
┌──────────────┐     HTTPS      ┌──────────────────┐    PostgreSQL   ┌──────────────────┐
│  Claude.ai   │ ──────────────►│  lmfdb-mcp       │ ──────────────►│ devmirror        │
│  (any user)  │   MCP protocol │  (Cloud Run)     │   read-only    │ .lmfdb.xyz:5432  │
└──────────────┘                └──────────────────┘                └──────────────────┘
```

## Available Tools

| Tool            | Description                                           |
|:----------------|:------------------------------------------------------|
| `list_tables`   | List all tables, optionally filtered by prefix        |
| `describe_table`| Show column names and types for a table               |
| `sample_rows`   | Return a small sample from a table                    |
| `query`         | Run an arbitrary SELECT query (max 10,000 rows)       |
| `count_rows`    | Count rows with optional WHERE clause                 |
| `table_stats`   | Compute min/max/avg/stddev for a numeric column       |

## Deployment to Google Cloud Run

### Prerequisites

- [Google Cloud CLI](https://cloud.google.com/sdk/docs/install) (`gcloud`)
- A GCP project with billing enabled
- Docker (or use Cloud Build)

### Steps

```bash
# 1. Set your project
gcloud config set project YOUR_PROJECT_ID

# 2. Build and push the container
gcloud builds submit --tag gcr.io/YOUR_PROJECT_ID/lmfdb-mcp

# 3. Deploy to Cloud Run
gcloud run deploy lmfdb-mcp \
  --image gcr.io/YOUR_PROJECT_ID/lmfdb-mcp \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --port 8080 \
  --memory 512Mi \
  --timeout 300
```

This gives you a URL like `https://lmfdb-mcp-XXXXX-uc.a.run.app`.

### Environment Variables (optional)

| Variable             | Default               | Description                    |
|:---------------------|:----------------------|:-------------------------------|
| `LMFDB_HOST`         | `devmirror.lmfdb.xyz` | PostgreSQL host                |
| `LMFDB_PORT`         | `5432`                | PostgreSQL port                |
| `LMFDB_DBNAME`       | `lmfdb`               | Database name                  |
| `LMFDB_USER`         | `lmfdb`               | Database user                  |
| `LMFDB_PASSWORD`     | `lmfdb`               | Database password              |
| `LMFDB_MAX_ROWS`     | `10000`               | Hard limit on returned rows    |
| `LMFDB_DEFAULT_LIMIT`| `100`                 | Default LIMIT if not specified |
| `PORT`               | `8000`                | HTTP port (Cloud Run sets this)|

## Connecting Claude to the Server

### For Individual Users (Pro / Max / Free)

1. Go to [claude.ai](https://claude.ai) → **Settings** → **Connectors**
2. Click **Add custom connector**
3. Enter:
   - **Name:** `LMFDB`
   - **URL:** `https://YOUR-CLOUD-RUN-URL/mcp`
4. Click **Add**
5. In any conversation, click **+** → **Connectors** → enable **LMFDB**

### For Teams / Enterprise

An organization Owner adds the connector once in **Organization Settings →
Connectors**, and all members can then enable it per-conversation.

## Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run locally
python server.py

# Test with MCP Inspector
npx @modelcontextprotocol/inspector http://localhost:8000/mcp
```

## Security Notes

- The server only allows `SELECT`, `WITH`, and `EXPLAIN` queries.
- A 120-second statement timeout prevents runaway queries.
- Row results are capped at 10,000.
- The underlying LMFDB mirror is read-only; writes will fail at the
  database level regardless of what SQL is sent.
- For production, consider adding authentication (OAuth or API key)
  if you want to restrict access beyond what the LMFDB mirror itself
  allows.  The default config is auth-less, which is appropriate since
  the LMFDB mirror credentials are public.

## License

GPL-3.0 (matching lmfdb-lite)
