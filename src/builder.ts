import { BigQuery, Table, TableMetadata } from '@google-cloud/bigquery'

import {
    BigQueryModel,
    TableFullName,
    ModelType,
    NameResolver,
    BaseBuildableBigQueryModel,
    TablePartialName,
} from './types'

export interface BigQueryModelBuilderConfig {
    defaultDataset?: string
    labels?: { [key: string]: string }
    nameTransform?: (partialName: TablePartialName) => TablePartialName
}

interface Logger {
    info: (message: string, props?: any) => void
    debug: (message: string, props?: any) => void
}

export class BigQueryModelBuilder {
    private isBuilding = false

    constructor(
        private bigquery: BigQuery,
        private config?: BigQueryModelBuilderConfig,
        private log: Logger = console,
    ) {}

    async build(model: BigQueryModel) {
        if (this.isBuilding) {
            throw new Error(`Can't build until previous build has finished.`)
        }
        this.isBuilding = true
        try {
            await this.buildStep(model, true, {})
            await this.buildStep(model, false, {})
        } finally {
            this.isBuilding = false
        }
    }

    private async buildStep(
        model: BigQueryModel,
        dryRun: boolean,
        usedInRun: {
            [name: string]: BigQueryModel
        },
        dependencyChain: BigQueryModel[] = [],
    ) {
        if (model.type === ModelType.External) {
            return
        }

        const name = this.getFullName(model)

        if (this.alreadyBuilt(model, name.toString(), usedInRun)) {
            return
        }

        this.log.info(`Started building '${name}'.`)

        const dependencies: BigQueryModel[] = []
        const resolver: NameResolver = {
            self: name,
            model: (dependency) => {
                dependencies.push(dependency)
                return this.getFullName(dependency)
            },
        }
        const buildDependencies = async () => {
            for (const dep of dependencies) {
                if (dependencyChain.includes(dep)) {
                    throw new Error('Circular dependency detected.')
                }
                await this.buildStep(dep, dryRun, usedInRun, [
                    ...dependencyChain,
                    model,
                ])
            }
        }

        const table = this.bigquery.dataset(name.dataset).table(name.table)
        const metadata = await this.getMetadata(table)

        const modelIsFullRefresh = model.type === ModelType.FullRefresh
        const doFullRefresh = modelIsFullRefresh || !metadata

        const sql = doFullRefresh
            ? modelIsFullRefresh
                ? model.sql(resolver)
                : model.sqlFull(resolver)
            : model.sqlIncremental(
                  resolver,
                  metadata.schema?.fields?.map((f) => f.name) as string[],
              )

        await buildDependencies()

        if (!dryRun) {
            await this.createDatasetIfNotExists(table)

            if (doFullRefresh) {
                await this.fullRefreshJob(name, table, sql, model)
            } else {
                await this.incrementalJob(name, sql)
            }
        }

        this.log.info(`Finished building '${name}'.`)
    }

    private async incrementalJob(name: TableFullName, sql: string) {
        this.log.info(`Starting job to incrementally update table '${name}'.`)
        this.log.debug(sql)
        const [job] = await this.bigquery.createQueryJob({
            query: sql,
            labels: { ...this.config?.labels },
        })
        this.log.debug(`Job results for table '${name}'`, job.metadata)
        await job.getQueryResults()
        this.log.info(`Finished job to incrementally update table '${name}'.`)
    }

    private async fullRefreshJob(
        name: TableFullName,
        table: Table,
        sql: string,
        model: BaseBuildableBigQueryModel,
    ) {
        this.log.info(`Starting job to fully refresh table '${name}'.`)
        this.log.debug(sql)
        const [job] = await this.bigquery.createQueryJob({
            query: sql,
            destination: table,
            writeDisposition: 'WRITE_TRUNCATE',
            clustering: model.clusterBy
                ? {
                      fields: model.clusterBy,
                  }
                : undefined,
            timePartitioning: model.timePartitioning,
            labels: { ...this.config?.labels },
        })
        this.log.debug(`Job results for table '${name}'`, job.metadata)
        await job.getQueryResults()
        this.log.info(`Finished job to (re)create table '${name}'.`)
    }

    getFullName(model: BigQueryModel): TableFullName {
        const { nameTransform, defaultDataset } = this.config ?? {}
        const name = nameTransform ? nameTransform(model.name) : model.name
        const table = name.table
        const dataset = name.dataset ?? defaultDataset
        const project = name.project ?? this.bigquery.projectId
        if (!dataset) {
            throw new Error(`No dataset specified for table '${table}'.`)
        }
        return {
            project,
            dataset,
            table,
            toString: () => `\`${project}.${dataset}.${table}\``,
        }
    }

    private alreadyBuilt(
        model: BigQueryModel,
        name: string,
        usedInRun: { [name: string]: BigQueryModel },
    ) {
        const modelWithSameName = usedInRun[name]
        if (modelWithSameName) {
            if (modelWithSameName === model) {
                return true
            }
            throw new Error(
                `Different models can't use the same name: ${name}.`,
            )
        }
        usedInRun[name] = model
        return false
    }

    private async createDatasetIfNotExists(table: Table) {
        const dataset = table.dataset
        const [datasetExists] = await dataset.exists()
        if (datasetExists) {
            this.log.debug(`Dataset '${dataset.id}' already exists.`)
        } else {
            this.log.debug(
                `Dataset '${dataset.id}' doesn't already exist. Creating it.`,
            )
            await dataset.create()
        }
    }

    private async getMetadata(table: Table): Promise<null | TableMetadata> {
        const [datasetExists] = await table.dataset.exists()
        if (!datasetExists) {
            this.log.debug(
                `Dataset '${table.dataset.id}' doesn't already exist.`,
            )
            return null
        }
        this.log.debug(`Dataset '${table.dataset.id}' already exists.`)

        const [tableExists] = await table.exists()
        if (!tableExists) {
            this.log.debug(`Table '${table.id}' doesn't already exist.`)
            return null
        }
        this.log.debug(`Table '${table.id}' already exists.`)

        const [metadata] = await table.getMetadata()
        return metadata
    }
}
