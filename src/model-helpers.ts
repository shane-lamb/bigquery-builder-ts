import {
    ExternalBigQueryModel,
    FullRefreshBigQueryModel,
    IncrementalBigQueryModel,
    ModelType,
    NameResolver,
    NonEmptyArray,
    TablePartialName,
} from './types'
import bigquery from '@google-cloud/bigquery/build/src/types'
import ITimePartitioning = bigquery.ITimePartitioning

export function incrementalModel(props: {
    name: TablePartialName
    clusterBy?: NonEmptyArray<string>
    timePartitioning?: ITimePartitioning
    sql: (ref: NameResolver, incremental: boolean) => string
    mergeKey: NonEmptyArray<string>
}): IncrementalBigQueryModel {
    return {
        name: props.name,
        type: ModelType.Incremental,
        clusterBy: props.clusterBy,
        timePartitioning: props.timePartitioning,
        sqlFull: (ref) => props.sql(ref, false),
        sqlIncremental: (ref, columns) => `
MERGE INTO ${ref.self} AS MERGE_DEST USING (
${props.sql(ref, true)}
) AS MERGE_SOURCE
ON ${props.mergeKey.map((c) => `MERGE_DEST.\`${c}\` = MERGE_SOURCE.\`${c}\``).join(' AND ')}
WHEN MATCHED THEN UPDATE SET ${columns.map((c) => `MERGE_DEST.\`${c}\` = MERGE_SOURCE.\`${c}\``).join(', ')}
WHEN NOT MATCHED THEN INSERT ROW
`,
    }
}

export function externalModel(name: TablePartialName): ExternalBigQueryModel {
    return {
        name,
        type: ModelType.External,
    }
}

export function fullRefreshModel(props: {
    name: TablePartialName
    clusterBy?: NonEmptyArray<string>
    timePartitioning?: ITimePartitioning
    sql: (ref: NameResolver) => string
}): FullRefreshBigQueryModel {
    return {
        name: props.name,
        type: ModelType.FullRefresh,
        clusterBy: props.clusterBy,
        timePartitioning: props.timePartitioning,
        sql: props.sql,
    }
}
