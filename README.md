# bigquery-builder-ts

Orchestrates your data pipeline (model building). Lightweight and powerful.

Can only be used with BigQuery. No CLI provided, it's a TypeScript library so your interface is code.
Build your own CLI on top if you like!

Very much a work in progress right now!

Implemented:
- "Full Refresh" and "External" model types
- Ability to specify table clustering and partitioning

Planned:
- Cost tracking (log run information to BigQuery table and ability to cap spend)

Considering:
- "Incremental" model type

## Setup

```bash
asdf install
npm install
```

## Running tests

Start up a bigquery emulator like so:
```
docker run -p 9050:9050 --platform linux/amd64 -it ghcr.io/goccy/bigquery-emulator:latest --project=local-test-project
```
