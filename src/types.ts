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
}

export interface BaseBuildableBigQueryModel extends BaseBigQueryModel {
    clusterBy?: NonEmptyArray<string>
    timePartitioning?: ITimePartitioning
}

export interface IncrementalBigQueryModel extends BaseBuildableBigQueryModel {
    type: ModelType.Incremental
    sqlIncremental: (ref: NameResolver, columns: string[]) => string
    sqlFull: (ref: NameResolver) => string
}

export interface FullRefreshBigQueryModel extends BaseBuildableBigQueryModel {
    type: ModelType.FullRefresh
    sql: (ref: NameResolver) => string
}

export interface ExternalBigQueryModel extends BaseBigQueryModel {
    type: ModelType.External
}

export type BigQueryModel =
    | IncrementalBigQueryModel
    | FullRefreshBigQueryModel
    | ExternalBigQueryModel
