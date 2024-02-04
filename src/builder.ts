import { BigQuery } from '@google-cloud/bigquery'

import {
    BigQueryModel,
    TableFullName,
    ModelType,
    NameResolver,
    TablePartialName,
} from './types'

export interface BigQueryModelBuilderConfig {
    nameTransform: (partialName: TablePartialName) => TableFullName
}

export interface Logger {
    info: (message: string, props?: any) => void
    debug: (message: string, props?: any) => void
}

export class BigQueryModelBuilder {
    private isBuilding = false

    constructor(
        private bigquery: BigQuery,
        private config: BigQueryModelBuilderConfig,
        private log: Logger = console,
    ) {}

    async build(model: BigQueryModel) {
        if (this.isBuilding) {
            throw new Error(`Can't build until previous build has finished.`)
        }
        this.isBuilding = true
        await this.buildStep(model, true, {})
        await this.buildStep(model, false, {})
        this.isBuilding = false
    }

    private async buildStep(
        model: BigQueryModel,
        dryRun: boolean,
        usedInRun: {
            [name: string]: BigQueryModel
        },
        dependencyChain: BigQueryModel[] = [],
    ) {
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

        // todo: if already exists, check partitioning and clustering are equal to config

        if (model.type === ModelType.FullRefresh) {
            const sql = model.sql(resolver)
            for (const dep of dependencies) {
                if (dependencyChain.includes(dep)) {
                    throw new Error('Circular dependency detected.')
                }
                await this.buildStep(dep, dryRun, usedInRun, [
                    ...dependencyChain,
                    model,
                ])
            }
            if (!dryRun) {
                this.log.info(`Starting job to (re)create table '${name}'.`)
                this.log.debug(sql)
                const [job] = await this.bigquery.createQueryJob({
                    query: sql,
                    destination: await this.tableRef(name),
                    writeDisposition: 'WRITE_TRUNCATE',
                })
                this.log.debug(
                  `Job results for table '${name}'`,
                  job.metadata,
                )
                await job.getQueryResults()
                this.log.info(`Finished job to (re)create table '${name}'.`)
            }
        }

        this.log.info(`Finished building '${name}'.`)
    }

    getFullName(model: BigQueryModel) {
        const name = this.config.nameTransform(model.name)
        return {
            ...name,
            toString: () => `${name.project}.${name.dataset}.${name.table}`,
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
                `Different models can't use the same name: '${name}'.`,
            )
        }
        usedInRun[name] = model
        return false
    }

    private async tableRef(name: TableFullName) {
        this.log.debug(`Checking if dataset '${name.dataset}' exists.`)
        const dataset = this.bigquery.dataset(name.dataset)
        const [datasetExists] = await dataset.exists()
        if (datasetExists) {
            this.log.debug(`Dataset '${name.dataset}' already exists.`)
        } else {
            this.log.debug(
                `Dataset '${name.dataset}' doesn't already exist. Creating it.`,
            )
            await dataset.create()
        }

        this.log.debug(`Checking if table '${name.table}' exists.`)
        const table = dataset.table(name.table)
        const [tableExists] = await table.exists()
        if (tableExists) {
            this.log.debug(`Table '${name.table}' already exists.`)
        } else {
            this.log.debug(`Table '${name.table}' doesn't already exist.`)
        }

        return table
    }
}
