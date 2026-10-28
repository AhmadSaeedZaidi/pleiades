# Pleiades MCP Server

An [MCP](https://modelcontextprotocol.io) server that exposes Pleiades' YouTube
intelligence capabilities to LLM clients. Instead of querying the Pleiades
database, it serves **on-demand** media for any YouTube video a client names:

| Tool | What it does |
|------|--------------|
| `search_youtube` | Search YouTube (Hunter pipeline, key-rotated Data API) and return candidate videos. |
| `get_video_metadata` | Resolve title, channel, duration, view/like stats for a video. |
| `get_transcript` | Fetch the video's transcript/captions (free caption cascade). Returns segments, plain text, or a saved file. |
| `summarize_transcript` | Fetch captions and create a Mistral summary + saved artifact. Provider billing applies. |
| `get_keyframes` | Extract keyframe images (ffmpeg surgical sampling) and return downloadable image URIs. |
| `get_audio` | Download the speech-optimized audio track (opus) and optionally transcribe it via Mistral Voxtral. |
| `list_artifacts` | List artifacts cached by this server. |

## Design

Each tool is self-contained and produces **downloadable artifacts** (written to a
local cache and returned as `file://` URIs, or `http://` URIs when serving over
HTTP). This keeps large binaries (audio, images) out of the JSON-RPC channel.
The server reuses the existing `atlas` + `maia` libraries — the same
`StealthVideoStreamer`, `TranscriptLoader`, Painter frame logic, and Mistral
clients the agent fleet uses — so it inherits their rate-limit/pacing behaviour.

## Requirements

- Python 3.12, a YouTube Data API key pool (`YOUTUBE_API_KEY_POOL_JSON`), and a
  `MISTRAL_API_KEY` for summaries/audio transcription.
- System tools the fleet needs: **ffmpeg**, **yt-dlp**, and **Deno** (for
  YouTube's PoToken challenge). Same as the Maia agents.

## Install

```bash
cd mcp
../.venv/bin/pip install -e .        # installs mcp SDK + editable atlas/maia
```

## Run

```bash
# Stdio transport (default — for local LLM clients)
pleiades-mcp --transport stdio

# Streamable HTTP transport. Keep it on loopback unless an authenticated
# reverse proxy protects both MCP and artifact downloads.
pleiades-mcp --transport streamable-http --host 127.0.0.1 --port 8000
```

Artifacts are cached under `PLEIADES_MCP_ARTIFACT_DIR` (default
`/tmp/pleiades_mcp/artifacts`). Over HTTP, they are served from a sidecar static
server (default `<port+1>`), so a client fetches `http://host:<port+1>/<video_id>/...`.
The included public launcher does not add authentication; do not expose it
directly to the Internet.

## Configuration

The server reads the same environment variables as Atlas/Maia (see
`atlas/ENV.example` and `maia/ENV.example`). Minimum needed:

```bash
export DATABASE_URL=...            # required by atlas.config (no DB calls made)
export YOUTUBE_API_KEY_POOL_JSON='["your-key"]'
export MISTRAL_API_KEY=...
export VAULT_PROVIDER=huggingface
export HF_DATASET_ID=...
export HF_TOKEN=...
```

> The database/vault are not queried by the MCP tools, but `atlas.config` still
> validates `DATABASE_URL` at import. A valid DSN is required, but these tools do not connect.

## Testing

```bash
cd mcp && ../.venv/bin/python -m pytest tests/ -q
```
