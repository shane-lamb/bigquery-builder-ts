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

type NonEmptyArray<T> = [T, ...T[]]

export enum ModelType {
    Incremental,
    FullRefresh,
    External,
}

interface BaseBigQueryModel {
    name: TablePartialName
    type: ModelType
    clusterBy?: NonEmptyArray<string>
    partitionBy?: unknown // todo
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
