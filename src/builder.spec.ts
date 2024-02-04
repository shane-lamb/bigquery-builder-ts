import { BigQueryModel, FullRefreshBigQueryModel, ModelType } from './types'
import { describe, it, expect, beforeEach } from '@jest/globals'
import { BigqueryModelBuilder } from './builder'
import { BigQueryDate } from '@google-cloud/bigquery'
import {
    getLocalBigQuery,
    localBigQueryProject,
    localNameTransform,
} from './local-bigquery'

describe('BigQuery Model Builder', () => {
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
            fullRefreshModel(
                dailyTempsTable,
                () => `
                    select date('2024-01-01') as record_date, 'Brisbane' as city, 30 as temp_c
                `,
            ),
        )

        expect(await tableRows(dailyTempsTable)).toEqual([
            {
                record_date: new BigQueryDate('2024-01-01'),
                city: 'Brisbane',
                temp_c: 30,
            },
        ])
    })
    it('should build model dependencies before building model', async () => {
        const dependency = fullRefreshModel(
            dailyTempsTable,
            () => `
                select date('2024-01-01') as record_date, 'Brisbane' as city, 30 as temp_c union all
                select date('2024-01-02') as record_date, 'Brisbane' as city, 31 as temp_c
            `,
        )

        await builder.build(
            fullRefreshModel(
                dailyTempsTable2,
                ({ model }) => `select *
                        from ${model(dependency)}
                        WHERE temp_c > 30`,
            ),
        )

        expect(await tableRows(dailyTempsTable2)).toEqual([
            {
                record_date: new BigQueryDate('2024-01-02'),
                city: 'Brisbane',
                temp_c: 31,
            },
        ])
    })
    it('should build the same model only once (in the same run)', async () => {
        let builtTimes = 0
        const dependency = fullRefreshModel(dailyTempsTable, () => {
            builtTimes++
            return `select date('2024-01-01') as record_date, 'Brisbane' as city, 30 as temp_c`
        })

        const model = fullRefreshModel(
            dailyTempsTable2,
            ({ model }) => `
          select *
          from ${model(dependency)}
          union all
          select *
          from ${model(dependency)}
      `,
        )
        await builder.build(model)

        // we expect the sql method to be invoked twice, once for the dry run
        // and once for the real run
        // todo: find better way to test this
        expect(builtTimes).toBe(2)
    })
    it('should build the same model only once (in separate runs)', async () => {
        // todo
    })
    it('should not allow different models to have the same name', async () => {
        const dependencySql = () => `
            select date('2024-01-01') as record_date, 'Brisbane' as city, 30 as temp_c union all
            select date('2024-01-02') as record_date, 'Brisbane' as city, 31 as temp_c
        `
        const dependencyA = fullRefreshModel(dailyTempsTable, dependencySql)
        const dependencyB = fullRefreshModel(dailyTempsTable, dependencySql)

        const model = fullRefreshModel(
            dailyTempsTable2,
            ({ model }) => `
          select *
          from ${model(dependencyA)}
          union all
          select *
          from ${model(dependencyB)}
      `,
        )

        await expect(builder.build(model)).rejects.toThrowError(
            `Different models can't use the same name: '${localBigQueryProject}.${datasetName}.${dailyTempsTable}'.`,
        )

        expect(await tableRows(dailyTempsTable)).toEqual([])
        expect(await tableRows(dailyTempsTable2)).toEqual([])
    })
    it('should not allow circular dependencies', async () => {
        let depA: BigQueryModel
        let depB: BigQueryModel
        let depC: BigQueryModel

        depA = fullRefreshModel(
            'a',
            ({ model }) => `select *
                      from ${model(depB)}`,
        )
        depB = fullRefreshModel(
            'b',
            ({ model }) => `select *
                      from ${model(depC)}`,
        )
        depC = fullRefreshModel(
            'c',
            ({ model }) => `select *
                      from ${model(depA)}`,
        )

        await expect(builder.build(depA)).rejects.toThrowError(
            'Circular dependency detected.',
        )
    })
    it('should not allow parallel builds on the same builder', async () => {
        // disallowing as it's not worth implementing this.
        // as it is, this would have unpredictable results.

        const model = fullRefreshModel(
            dailyTempsTable,
            () => `
                select date('2024-01-01') as record_date, 'Brisbane' as city, 30 as temp_c
            `,
        )

        const firstBuild = builder.build(model)

        await expect(builder.build(model)).rejects.toThrowError(
            `Can't build until previous build has finished.`,
        )
        await firstBuild
    })
    it('should support external tables', async () => {
        await givenTableHasData(
            dailyTempsTable,
            `select date('2024-01-01') as record_date, 'Brisbane' as city, 30 as temp_c`,
        )

        const dependency = externalModel(dailyTempsTable)

        await builder.build(
            fullRefreshModel(
                dailyTempsTable2,
                ({ model }) => `
                    select *
                    from ${model(dependency)}
                `,
            ),
        )

        expect(await tableRows(dailyTempsTable2)).toEqual([
            {
                record_date: new BigQueryDate('2024-01-01'),
                city: 'Brisbane',
                temp_c: 30,
            },
        ])
    })
})

let builder: BigqueryModelBuilder

const datasetName = 'test_dataset'
const dailyTempsTable = 'daily_temps'
const dailyTempsTable2 = 'daily_temps_2'
const dailyTempsSchema = [
    { name: 'record_date', type: 'DATE' },
    { name: 'city', type: 'STRING' },
    { name: 'temp_c', type: 'FLOAT' },
]

const bq = getLocalBigQuery({
    [dailyTempsTable]: dailyTempsSchema,
    [dailyTempsTable2]: dailyTempsSchema,
})

function fullRefreshModel(
    tableName: string,
    sql: FullRefreshBigQueryModel['sql'],
): BigQueryModel {
    return {
        name: { table: tableName },
        type: ModelType.FullRefresh,
        sql,
    }
}

function externalModel(tableName: string): BigQueryModel {
    return {
        name: { table: tableName },
        type: ModelType.External,
    }
}

async function givenTableHasData(tableName: string, sql: string) {
    const model = fullRefreshModel(tableName, () => sql)
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
    return new BigqueryModelBuilder(bq, {
        nameTransform: localNameTransform(datasetName),
    })
}
