# BigQuery Builder TS

Inspired by [DBT](https://docs.getdbt.com/docs/introduction), but exclusively for BigQuery and 100% in TypeScript.
Materialises BigQuery tables from models defined in code.

## How to use

```ts
import { BigQueryBuilder, fullRefreshModel, incrementalModel, externalModel } from 'bigquery-builder-ts'
import { BigQuery } from '@google-cloud/bigquery'

// Construct a BigQuery instance, pointing to your project.
const bigQuery = new BigQuery({ projectId: 'my-project-id' })

// Construct a Builder, which will be used to materialise our models.
const builder = new BigQueryBuilder(bigQuery, { defaultDataset: 'marts' })

// There are 3 basic types of model: Full Refresh, Incremental and External.

// Use external models when you want to reference existing tables that are defined and built elsewhere.
const clientsTable = externalModel({ dataset: 'staging', table: 'clients' })
const ordersTable = externalModel({ dataset: 'staging', table: 'orders' })

// Use incremental models when you need to optimise cost (by not processing the same data multiple times)
const clientDailyOrdersTable = incrementalModel({
    name: { table: 'client_daily_orders' },
    sql: ({ model, self }, incremental) => `
        SELECT c.client_id, DATE(o.order_time) as order_date, SUM(o.order_total) as total
        ${incremental ? `WHERE DATE(o.order_time) >= SELECT(MAX(order_date))` : ''}
        FROM ${model(clientsTable)} c
        JOIN ${model(ordersTable)} o ON c.client_id = o.client_id
        GROUP BY c.client_id, DATE(o.order_time)
    `,
    // The unique key to determine whether rows are updated or inserted in the merge.
    mergeKey: ['client_id', 'order_date'],
    // Both incremental and full refresh models allow for partitioning and clustering:
    clusterBy: ['client_id'],
    timePartitioning: {
        field: 'order_date',
    },
})

// Use full refresh models for simplicity, where cost is not a problem.
const dailyOrdersTable = fullRefreshModel({
    name: { table: 'daily_orders' },
    sql: ({ model }) => `
        SELECT order_date, SUM(total) as total
        FROM ${model(clientDailyOrdersTable)}
        GROUP BY order_date
    `,
})

// Building will automatically materialise tables in the correct order.
// clientDailyOrdersTable will be built first, as it's a dependency of dailyOrdersTable.
await builder.build(dailyOrdersTable)
```

## Dev Setup

```bash
asdf install
npm install
```

### Running tests

The local tests are run against a BigQuery emulator, which can be started with Docker:

```
docker run -p 9050:9050 --platform linux/amd64 -it ghcr.io/goccy/bigquery-emulator:latest --project=local-test-project
```
