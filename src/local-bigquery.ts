import { BigQuery, Query, Table } from '@google-cloud/bigquery'
import { TablePartialName } from './types'

export type TableSchema = { name: string; type: string }[]

export const localBigQueryProject = 'local-test-project'

let _localBigQuery: BigQuery

function localBigQueryUnwrapped() {
    return (_localBigQuery =
        _localBigQuery ??
        new BigQuery({
            projectId: localBigQueryProject,
            apiEndpoint: 'http://0.0.0.0:9050',
        }))
}

export function localBigQuery(tableSchemaMap: {
    [tableName: string]: TableSchema
}) {
    const bq = localBigQueryUnwrapped()
    return new Proxy(bq, {
        get: (target, prop, receiver) => {
            const value = Reflect.get(target, prop, receiver)
            if (typeof value === 'function') {
                if (value === bq.createQueryJob) {
                    return async (options: Query) => {
                        const table = options.destination as Table
                        const schema = tableSchemaMap[table.id!]
                        if (options.writeDisposition === 'WRITE_TRUNCATE') {
                            const [exists] = await table.exists()
                            if (exists) {
                                await table.delete()
                            }
                        }
                        await table.create({ schema })
                        return value.apply(target, [options])
                    }
                }
                return (...args: any[]) => {
                    return value.apply(target, args)
                }
            }
            return value
        },
    })
}

export function localNameTransform(datasetName: string) {
    return (partialName: TablePartialName) => ({
        project: localBigQueryProject,
        dataset: datasetName,
        ...partialName,
    })
}
