import { connect, Connection } from '@planetscale/database'
import Debug from '@prisma/debug'
import { DMMF } from '@prisma/generator-helper'
import EventEmitter from 'events'

import type { EngineConfig, EngineEventType, GetConfigResult, InlineDatasource } from '../common/Engine'
import { Engine } from '../common/Engine'
import { EngineMetricsOptions, Metrics, MetricsOptionsJson, MetricsOptionsPrometheus } from '../common/types/Metrics'

const debug = Debug('prisma:client:planetscaledriverEngine')

export class PlanetScaleDriverEngine extends Engine {
  private inlineSchema: string
  private inlineSchemaHash: string
  private inlineDatasources: Record<string, InlineDatasource>
  private config: EngineConfig
  private logEmitter: EventEmitter
  private env: { [k in string]?: string }

  private clientVersion: string
  private host: string

  private conn: Connection

  constructor(config: EngineConfig) {
    super()

    this.config = config
    this.env = { ...this.config.env, ...process.env }
    this.inlineSchema = config.inlineSchema ?? ''
    this.inlineDatasources = config.inlineDatasources ?? {}
    this.inlineSchemaHash = config.inlineSchemaHash ?? ''
    this.clientVersion = config.clientVersion ?? 'unknown'

    this.logEmitter = new EventEmitter()
    this.logEmitter.on('error', () => {})

    const connConfig = this.extractConnConfig()

    this.host = connConfig.host

    this.conn = connect(connConfig)

    debug('host', this.host)
  }

  version() {
    // QE is remote, we don't need to know the exact commit SHA
    return 'unknown'
  }

  async start() {}
  async stop() {}
  on(event: EngineEventType, listener: (args?: any) => any): void {
    if (event === 'beforeExit') {
      // TODO: hook into the process
      throw new Error('beforeExit event is not yet supported')
    } else {
      this.logEmitter.on(event, listener)
    }
  }

  // TODO: looks like activeProvider is the only thing
  // used externally; verify that
  async getConfig() {
    return Promise.resolve({
      datasources: [
        {
          activeProvider: this.config.activeProvider,
        },
      ],
    } as GetConfigResult)
  }

  getDmmf(): Promise<DMMF.Document> {
    // This code path should not be reachable, as it is handled upstream in `getPrismaClient`.
    throw new Error('getDmmf is not yet supported')
  }

  async request(query: string, headers: Record<string, string>, attempt = 0) {
    this.logEmitter.emit('query', { query })

    const response = await this.requestInternal(query)

    return response.rows as any
  }

  async requestBatch(queries: string[], headers: Record<string, string>, isTransaction = false, attempt = 0) {
    if (isTransaction) {
      const results = await this.conn.transaction(async (tx) => {
        return Promise.all(queries.map((query) => tx.execute(query)))
      })

      return results.map((result) => result.rows)
    }

    const all = await Promise.all(
      queries.map((query) => {
        return this.requestInternal(query)
      }),
    )

    return all.map((res) => res.rows) as any
  }

  private async requestInternal(query: string) {
    return this.conn.execute(query)
  }

  // TODO: figure out how to support transactions
  transaction(): Promise<any> {
    throw new Error('Interactive transactions are not yet supported')
  }

  private extractConnConfig() {
    const datasources = this.mergeOverriddenDatasources()
    const mainDatasourceName = Object.keys(datasources)[0]
    const mainDatasource = datasources[mainDatasourceName]
    const pscaleURL = this.resolveDatasourceURL(mainDatasourceName, mainDatasource)

    if (!pscaleURL.startsWith('mysql')) {
      throw new Error('Datasource URL must use mysql:// protocol when --planetscale-driver is used')
    }

    // pscaleURL Format: mysql://<username>:<password>@<host>/<database>?sslaccept=strict
    // Extract host, username and password
    const [_, username, password, host] = pscaleURL.match(/mysql:\/\/([^:]+):([^@]+)@([^/]+)\/([^?]+)/) ?? []

    return {
      host,
      username,
      password,
    }
  }

  private mergeOverriddenDatasources(): Record<string, InlineDatasource> {
    if (this.config.datasources === undefined) {
      return this.inlineDatasources
    }

    const finalDatasources = { ...this.inlineDatasources }

    for (const override of this.config.datasources) {
      if (!this.inlineDatasources[override.name]) {
        throw new Error(`Unknown datasource: ${override.name}`)
      }

      finalDatasources[override.name] = {
        url: {
          fromEnvVar: null,
          value: override.url,
        },
      }
    }

    return finalDatasources
  }

  private resolveDatasourceURL(name: string, datasource: InlineDatasource): string {
    if (datasource.url.value) {
      return datasource.url.value
    }

    if (datasource.url.fromEnvVar) {
      const envVar = datasource.url.fromEnvVar
      const loadedEnvURL = this.env[envVar]

      if (loadedEnvURL === undefined) {
        throw new Error(`Datasource "${name}" references an environment variable "${envVar}" that is not set`)
      }

      return loadedEnvURL
    }

    throw new Error(`Datasource "${name}" specification is invalid: both value and fromEnvVar are null`)
  }

  metrics(options: MetricsOptionsJson): Promise<Metrics>
  metrics(options: MetricsOptionsPrometheus): Promise<string>
  metrics(options: EngineMetricsOptions): Promise<Metrics> | Promise<string> {
    throw new Error('Metric are not yet supported for Data Proxy')
  }
}
