import { BigQuery } from '@google-cloud/bigquery'
import { TablePartialName } from './types'

export const localBigQueryProject = 'local-test-project'

let _localBigQuery: BigQuery

export function localBigQuery() {
    return (_localBigQuery =
        _localBigQuery ??
        new BigQuery({
            projectId: localBigQueryProject,
            apiEndpoint: 'http://0.0.0.0:9050',
        }))
}

export function localNameTransform(datasetName: string) {
    return (partialName: TablePartialName) => ({
        project: localBigQueryProject,
        dataset: datasetName,
        ...partialName,
    })
}
