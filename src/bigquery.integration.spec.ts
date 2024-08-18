import { describe, expect, it } from '@jest/globals'
import { BigQuery, BigQueryDate } from '@google-cloud/bigquery'
import { BigQueryModelBuilder } from './builder'
import { Big } from 'big.js'
import {
    externalModel,
    fullRefreshModel,
    incrementalModel,
} from './model-helpers'

describe('BigQuery Model Builder (using real BigQuery)', () => {
    it('should create table with clustering', async () => {
        const table = bq.dataset(dataset).table('daily_temps')
        await table.delete().catch(() => {})

        await builder.build(
            fullRefreshModel({
                name: { table: 'daily_temps' },
                sql: () =>
                    `select date('2024-01-01') as record_date, 'Brisbane' as city, 30 as temp_c`,
                clusterBy: ['record_date'],
            }),
        )

        const [metadata] = await table.getMetadata()
        expect(metadata.clustering).toEqual({ fields: ['record_date'] })
    })
    it('should create table with time partitioning', async () => {
        const table = bq.dataset(dataset).table('daily_temps')
        await table.delete().catch(() => {})

        await builder.build(
            fullRefreshModel({
                name: { table: 'daily_temps' },
                sql: () =>
                    `select date('2024-01-01') as record_date, 'Brisbane' as city, 30 as temp_c`,
                timePartitioning: {
                    field: 'record_date',
                },
            }),
        )

        const [metadata] = await table.getMetadata()
        expect(metadata.timePartitioning).toEqual({
            field: 'record_date',
            type: 'DAY',
        })
    })
    it('should error when clustering/partitioning differs from existing table', async () => {
        const table = bq.dataset(dataset).table('daily_temps')
        await table.delete().catch(() => {})

        const model = fullRefreshModel({
            name: { table: 'daily_temps' },
            sql: () =>
                `select date('2024-01-01') as record_date, 'Brisbane' as city, 30 as temp_c`,
            clusterBy: ['record_date', 'city'],
        })
        await builder.build(model)

        // when trying to build again with a different cluster key
        model.clusterBy = ['city', 'record_date']
        // should throw with "Incompatible table partitioning specification"
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
        const sourceModel = externalModel({ table: sourceTableName })

        const destTableName = 'daily_order_totals-' + Date.now()
        const destTable = bq.dataset(dataset).table(destTableName)
        const model = incrementalModel({
            name: { table: destTableName },
            timePartitioning: {
                field: 'order_placed_on',
            },
            mergeKey: ['client_id', 'order_placed_on'],
            sql: ({ self, model }, incremental) => `
                SELECT client_id, placed_on as order_placed_on, sum(amount) as total_order_amount,
                FROM ${model(sourceModel)} ${incremental ? `WHERE placed_on >= (SELECT MAX(order_placed_on) FROM ${self})` : ''}
                GROUP BY client_id, placed_on
            `,
        })
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
const builder = new BigQueryModelBuilder(bq, { defaultDataset: dataset })
