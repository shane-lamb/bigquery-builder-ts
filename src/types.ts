import bigquery from '@google-cloud/bigquery/build/src/types'
import ITimePartitioning = bigquery.ITimePartitioning

export interface TableFullName {
    project: string
    dataset: string
    table: string
}

export interface TablePartialName {
    project?: string
    dataset?: string
    table: string
}

export interface NameResolver {
    self: TableFullName
    model: (model: BigQueryModel) => TableFullName
}

export type NonEmptyArray<T> = [T, ...T[]]

export enum ModelType {
    Incremental,
    FullRefresh,
    External,
}

interface BaseBigQueryModel {
    name: TablePartialName
    type: ModelType
    clusterBy?: NonEmptyArray<string>
    timePartitioning?: ITimePartitioning
}

interface IncrementalBigQueryModel extends BaseBigQueryModel {
    type: ModelType.Incremental
    uniqueKey: NonEmptyArray<string>
    getSql: (ref: NameResolver, incremental: boolean) => string
}

export interface FullRefreshBigQueryModel extends BaseBigQueryModel {
    type: ModelType.FullRefresh
    sql: (ref: NameResolver) => string
}

interface ExternalBigQueryModel extends BaseBigQueryModel {
    type: ModelType.External
}

export type BigQueryModel =
    | IncrementalBigQueryModel
    | FullRefreshBigQueryModel
    | ExternalBigQueryModel
