import { describe, expect, it } from '@jest/globals'
import { BigQuery } from '@google-cloud/bigquery'
import { FullRefreshBigQueryModel, ModelType } from './types'
import { BigQueryModelBuilder } from './builder'

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
})

const project = 'predictive-fx-418804'
const dataset = 'test_dataset'
const bq = new BigQuery({ projectId: project })
const builder = new BigQueryModelBuilder(bq, {
    nameTransform: (partialName) => ({ project, dataset, ...partialName }),
})
