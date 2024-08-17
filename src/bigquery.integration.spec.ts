import { describe, expect, it } from '@jest/globals'
import { BigQuery, BigQueryDate } from '@google-cloud/bigquery'
import {
    ExternalBigQueryModel,
    FullRefreshBigQueryModel,
    IncrementalBigQueryModel,
    ModelType,
    NameResolver,
} from './types'
import { BigQueryModelBuilder } from './builder'
import { Big } from 'big.js'

describe('BigQuery Model Builder (using real BigQuery)', () => {
    it('should create table with clustering', async () => {
        const table = bq.dataset(dataset).table('daily_temps')

        await table.delete().catch(() => {})

        const model: FullRefreshBigQueryModel = {
            name: { table: 'daily_temps' },
            type: ModelType.FullRefresh,
            sql: () =>
                `select date('2024-01-01') as record_date, 'Brisbane' as city, 30 as temp_c`,
            clusterBy: ['record_date'],
        }

        await builder.build(model)

        const [metadata] = await table.getMetadata()

        expect(metadata.clustering).toEqual({ fields: ['record_date'] })
    })
    it('should create table with time partitioning', async () => {
        const table = bq.dataset(dataset).table('daily_temps')

        await table.delete().catch(() => {})
        const model: FullRefreshBigQueryModel = {
            name: { table: 'daily_temps' },
            type: ModelType.FullRefresh,
            sql: () =>
                `select date('2024-01-01') as record_date, 'Brisbane' as city, 30 as temp_c`,
            timePartitioning: {
                field: 'record_date',
            },
        }

        await builder.build(model)

        const [metadata] = await table.getMetadata()

        expect(metadata.timePartitioning).toEqual({
            field: 'record_date',
            type: 'DAY',
        })
    })
    it('should error when clustering/partitioning differs from existing table', async () => {
        const table = bq.dataset(dataset).table('daily_temps')

        await table.delete().catch(() => {})

        const model: FullRefreshBigQueryModel = {
            name: { table: 'daily_temps' },
            type: ModelType.FullRefresh,
            sql: () =>
                `select date('2024-01-01') as record_date, 'Brisbane' as city, 30 as temp_c`,
            clusterBy: ['record_date', 'city'],
        }

        await builder.build(model)

        // try to build the table again with a different cluster key
        model.clusterBy = ['city', 'record_date']

        // throws with "Incompatible table partitioning specification"
        await expect(builder.build(model)).rejects.toThrowError()
    })
    it('should support incremental builds', async () => {
        const sourceTableName = 'orders-' + Date.now()
        const sourceTable = bq.dataset(dataset).table(sourceTableName)
        await sourceTable.create({
            schema: {
                fields: [
                    { name: 'order_id', type: 'STRING' },
                    { name: 'client_id', type: 'STRING' },
                    { name: 'placed_on', type: 'DATE' },
                    { name: 'amount', type: 'NUMERIC' },
                ],
            },
        })
        await sourceTable.insert([
            {
                order_id: '1',
                client_id: '1',
                placed_on: '2024-01-01',
                amount: 100,
            },
            {
                order_id: '2',
                client_id: '1',
                placed_on: '2024-01-01',
                amount: 200,
            },
            {
                order_id: '3',
                client_id: '2',
                placed_on: '2024-01-02',
                amount: 300,
            },
        ])
        const sourceModel: ExternalBigQueryModel = {
            name: { table: sourceTableName },
            type: ModelType.External,
        }

        const destTableName = 'daily_order_totals-' + Date.now()
        const destTable = bq.dataset(dataset).table(destTableName)

        const getMaxPartition = ({ self }: NameResolver) =>
            `DECLARE max_partition DATE; SET max_partition = (SELECT MAX(order_placed_on) FROM ${self});`
        const sql = ({ model }: NameResolver, whereClause: string) => `
            select client_id, placed_on as order_placed_on, sum(amount) as total_order_amount,
            from ${model(sourceModel)} ${whereClause}
            group by client_id, placed_on
        `
        const model: IncrementalBigQueryModel = {
            name: { table: destTableName },
            type: ModelType.Incremental,
            timePartitioning: {
                field: 'order_placed_on',
            },
            sqlFull: (ref) => sql(ref, ''),
            sqlIncremental: (ref, columns) => `
                ${getMaxPartition(ref)}
                MERGE INTO ${ref.self} as dest USING (
                ${sql(ref, 'WHERE placed_on >= max_partition')}
                ) as src
                ON dest.client_id = src.client_id AND dest.order_placed_on = src.order_placed_on
                WHEN MATCHED THEN UPDATE SET ${columns.map((c) => `dest.${c} = src.${c}`).join(', ')}
                WHEN NOT MATCHED THEN INSERT ROW
            `,
        }

        await builder.build(model)

        const [rows] = await destTable.getRows()
        expect(rows.length).toBe(2)
        expect(rows).toEqual(
            expect.arrayContaining([
                {
                    client_id: '1',
                    order_placed_on: new BigQueryDate('2024-01-01'),
                    total_order_amount: Big('300'),
                },
                {
                    client_id: '2',
                    order_placed_on: new BigQueryDate('2024-01-02'),
                    total_order_amount: Big('300'),
                },
            ]),
        )

        await sourceTable.insert([
            {
                order_id: '4',
                client_id: '1',
                placed_on: '2024-01-03',
                amount: 100,
            },
            {
                order_id: '5',
                client_id: '1',
                placed_on: '2024-01-01',
                amount: 100,
            },
            {
                order_id: '6',
                client_id: '2',
                placed_on: '2024-01-02',
                amount: 100,
            },
        ])

        await builder.build(model)

        const [rowsAfter] = await destTable.getRows()
        expect(rowsAfter.length).toBe(3)
        expect(rowsAfter).toEqual(
            expect.arrayContaining([
                {
                    client_id: '1',
                    order_placed_on: new BigQueryDate('2024-01-01'),
                    total_order_amount: Big('300'),
                },
                {
                    client_id: '2',
                    order_placed_on: new BigQueryDate('2024-01-02'),
                    total_order_amount: Big('400'),
                },
                {
                    client_id: '1',
                    order_placed_on: new BigQueryDate('2024-01-03'),
                    total_order_amount: Big('100'),
                },
            ]),
        )

        await sourceTable.delete()
        await destTable.delete()
    })
})

const project = 'predictive-fx-418804'
const dataset = 'test_dataset'
const bq = new BigQuery({ projectId: project })
const builder = new BigQueryModelBuilder(bq, {
    nameTransform: (partialName) => ({ project, dataset, ...partialName }),
})
