import { BigQueryModel } from './types'
import { describe, it, expect, beforeEach } from '@jest/globals'
import { BigQueryModelBuilder } from './builder'
import { BigQueryDate } from '@google-cloud/bigquery'
import { localBigQuery, localBigQueryProject } from './test-utils'
import { externalModel, fullRefreshModel } from './model-helpers'

describe('BigQuery Model Builder (local/emulator tests)', () => {
    beforeEach(async () => {
        builder = newBuilder()
        const dataset = bq.dataset(datasetName)
        const [exists] = await dataset.exists()
        if (exists) {
            const [tables] = await dataset.getTables()
            for (const table of tables) {
                await table.delete()
            }
            await dataset.delete()
        }
    })
    it('should build a simple model with no dependencies', async () => {
        await builder.build(
            fullRefreshModel({
                name: { table: 'daily_temps' },
                sql: () => `
                    select date('2024-01-01') as record_date, 'Brisbane' as city, 30 as temp_c
                `,
            }),
        )

        expect(await tableRows('daily_temps')).toEqual([
            {
                record_date: new BigQueryDate('2024-01-01'),
                city: 'Brisbane',
                temp_c: 30,
            },
        ])
    })
    it('should build model dependencies before building model', async () => {
        const dependency = fullRefreshModel({
            name: { table: 'daily_temps' },
            sql: () => `
                select date('2024-01-01') as record_date, 'Brisbane' as city, 30 as temp_c union all
                select date('2024-01-02') as record_date, 'Brisbane' as city, 31 as temp_c
            `,
        })

        await builder.build(
            fullRefreshModel({
                name: { table: 'filtered_daily_temps' },
                sql: ({ model }) => `
                    select *
                    from ${model(dependency)}
                    WHERE temp_c > 30
                `,
            }),
        )

        expect(await tableRows('filtered_daily_temps')).toEqual([
            {
                record_date: new BigQueryDate('2024-01-02'),
                city: 'Brisbane',
                temp_c: 31,
            },
        ])
    })
    it('should build the same model only once (in the same run)', async () => {
        let builtTimes = 0
        const dependency = fullRefreshModel({
            name: { table: 'daily_temps' },
            sql: () => {
                builtTimes++
                return `select date('2024-01-01') as record_date, 'Brisbane' as city, 30 as temp_c`
            },
        })

        const model = fullRefreshModel({
            name: { table: 'combined_daily_temps' },
            sql: ({ model }) => `
                select *
                from ${model(dependency)}
                union all
                select *
                from ${model(dependency)}
            `,
        })
        await builder.build(model)

        // we expect the sql method to be invoked twice, once for the dry run
        // and once for the real run
        // todo: find better way to test this
        expect(builtTimes).toBe(2)
    })
    it('should build the same model only once (in separate runs)', async () => {
        // todo: should handle this with optional run ID/key instead, which would rely on DB-stored properties
    })
    it('should not allow different models to have the same name', async () => {
        const dependencySql = () => `
            select date('2024-01-01') as record_date, 'Brisbane' as city, 30 as temp_c union all
            select date('2024-01-02') as record_date, 'Brisbane' as city, 31 as temp_c
        `
        const dependencyA = fullRefreshModel({
            name: { table: 'daily_temps' },
            sql: dependencySql,
        })
        const dependencyB = fullRefreshModel({
            name: { table: 'daily_temps' },
            sql: dependencySql,
        })

        const model = fullRefreshModel({
            name: { table: 'combined_daily_temps' },
            sql: ({ model }) => `
                select *
                from ${model(dependencyA)}
                union all
                select *
                from ${model(dependencyB)}
            `,
        })

        await expect(builder.build(model)).rejects.toThrowError(
            `Different models can't use the same name: \`${localBigQueryProject}.${datasetName}.daily_temps\`.`,
        )

        expect(await tableRows('daily_temps')).toEqual([])
        expect(await tableRows('combined_daily_temps')).toEqual([])
    })
    it('should not allow circular dependencies', async () => {
        let depA: BigQueryModel
        let depB: BigQueryModel
        let depC: BigQueryModel

        depA = fullRefreshModel({
            name: { table: 'a' },
            sql: ({ model }) => `select *
                                 from ${model(depB)}`,
        })
        depB = fullRefreshModel({
            name: { table: 'b' },
            sql: ({ model }) => `select *
                                 from ${model(depC)}`,
        })
        depC = fullRefreshModel({
            name: { table: 'c' },
            sql: ({ model }) => `select *
                                 from ${model(depA)}`,
        })

        await expect(builder.build(depA)).rejects.toThrowError(
            'Circular dependency detected.',
        )
    })
    it('should not allow parallel builds on the same builder', async () => {
        // disallowing as it's not worth implementing this.
        // as it is, this would have unpredictable results.

        const model = fullRefreshModel({
            name: { table: 'daily_temps' },
            sql: () => `
                select date('2024-01-01') as record_date, 'Brisbane' as city, 30 as temp_c
            `,
        })

        const firstBuild = builder.build(model)

        await expect(builder.build(model)).rejects.toThrowError(
            `Can't build until previous build has finished.`,
        )
        await firstBuild
    })
    it('should support external tables', async () => {
        await givenTableHasData(
            'daily_temps',
            `select date('2024-01-01') as record_date, 'Brisbane' as city, 30 as temp_c`,
        )

        const dependency = externalModel({ table: 'daily_temps' })

        await builder.build(
            fullRefreshModel({
                name: { table: 'table_with_dependency_on_daily_temps' },
                sql: ({ model }) => `select *
                                     from ${model(dependency)}`,
            }),
        )

        expect(await tableRows('table_with_dependency_on_daily_temps')).toEqual(
            [
                {
                    record_date: new BigQueryDate('2024-01-01'),
                    city: 'Brisbane',
                    temp_c: 30,
                },
            ],
        )
    })
})

let builder: BigQueryModelBuilder

const datasetName = 'test_dataset'

const bq = localBigQuery()

async function givenTableHasData(tableName: string, sql: string) {
    const model = fullRefreshModel({
        name: { table: tableName },
        sql: () => sql,
    })
    await newBuilder().build(model)
}

async function tableRows(tableName: string) {
    // check if exists
    const [exists] = await bq.dataset(datasetName).table(tableName).exists()
    if (!exists) {
        return []
    }
    const [rows] = await bq.query(`SELECT *
                                   FROM ${datasetName}.${tableName}`)
    return rows
}

function newBuilder() {
    return new BigQueryModelBuilder(bq, { defaultDataset: datasetName })
}
