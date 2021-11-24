import { ERedisIndex } from '../enums/ResultCode';
import { EntityRepository, Repository, TransactionManager, EntityManager, DeleteResult, getConnection, UpdateResult, DeepPartial, EntityTarget, SaveOptions } from 'typeorm';
import { currentUTCTime } from "../utils/clock";
import { logger } from '../utils/Logger';
import { Inject } from 'typedi';
import e from 'express';
import { Redis } from '../Redis/Redis';
import { QueryDeepPartialEntity } from 'typeorm/query-builder/QueryPartialEntity';

enum EDB_TYPE {
    DBMS = 0,
    REDIS,
}

export enum EDBMS_COMMIT_TYPE {
    SAVE = 0,
    UPDATE,
    DELETE,
    INCREASE,
    DECREASE
}

class Task<Entity, T extends DeepPartial<Entity>> {
    dbType: EDB_TYPE
    order: number

    // REDIS
    redisIndex: ERedisIndex
    redisFunc: any
    redisFuncArgs: any[]

    // DBMS
    processingType: EDBMS_COMMIT_TYPE
    dbmsEntityTarget: EntityTarget<Entity>
    dbmsEntities: T[]
    dbmsWhere: any
    dbmsPartialEntity?: QueryDeepPartialEntity<Entity>
    dbmsOptions?: SaveOptions
    dbmsColName: string
    dbmsSize: number
}

export class DBJob {

    @Inject("logger")
    private logger
    private m_redis: Redis

    private m_jobList: any[] = []
    private m_jobSize: number = 0

    private m_dbmsJobAdded: boolean = false
    private m_redisJobAdded: boolean = false

    constructor() {
        this.Init()
    }

    private Init() {

    }

    public AddJob_REDIS(redisIndex: ERedisIndex, redisFunc: any, ...args: any) {
        this.m_redisJobAdded = true

        let job = new Task()
        job.dbType = EDB_TYPE.REDIS
        job.order = this.m_jobSize
        job.redisIndex = redisIndex
        job.redisFunc = redisFunc
        job.redisFuncArgs = args

        this.m_jobList.push(job)
        this.m_jobSize += 1
    }

    public AddJob_DBMS<Entity, T extends DeepPartial<Entity>>(processingType: EDBMS_COMMIT_TYPE, targetOrEntity: EntityTarget<Entity>, entities: T[], options?: SaveOptions): void
    public AddJob_DBMS<Entity, T extends DeepPartial<Entity>>(processingType: EDBMS_COMMIT_TYPE, targetOrEntity: EntityTarget<Entity>, where: any, partialEntity?: QueryDeepPartialEntity<Entity>): void
    public AddJob_DBMS<Entity, T extends DeepPartial<Entity>>(processingType: EDBMS_COMMIT_TYPE, targetOrEntity: EntityTarget<Entity>, where: any, columnName: string, size: number): void

    public AddJob_DBMS<Entity, T extends DeepPartial<Entity>>(processingType: EDBMS_COMMIT_TYPE, targetOrEntity: EntityTarget<Entity>, param3: any, param4?: any, param5?: number) {
        this.m_dbmsJobAdded = true

        let job = new Task()
        job.dbType = EDB_TYPE.DBMS
        job.order = this.m_jobSize
        job.dbmsEntityTarget = targetOrEntity
        job.processingType = processingType

        switch (processingType) {
            case EDBMS_COMMIT_TYPE.SAVE:
                job.dbmsEntities = param3
                job.dbmsOptions = param4
                break
            case EDBMS_COMMIT_TYPE.UPDATE:
            case EDBMS_COMMIT_TYPE.DELETE:
                job.dbmsWhere = param3
                job.dbmsPartialEntity = param4
                break
            case EDBMS_COMMIT_TYPE.INCREASE:
            case EDBMS_COMMIT_TYPE.DECREASE:
                job.dbmsWhere = param3
                job.dbmsColName = param4
                job.dbmsSize = param5
                break
            default:
                break
        }

        this.m_jobList.push(job)
        this.m_jobSize += 1
    }

    private async Execute_RedisJob(job: any) {
        let res = await this.m_redis.SelectAsync(job.redisIndex)
        if (res != "OK") {
            logger.error(`[ DBJob ERROR ] Redis job error - ( ${job.redisIndex} redis select failed : No.${job.order} = ${job.redisFunc.name} ) `)
            return false
        }

        res = await job.redisFunc(...job.redisFuncArgs)
        if (undefined == res) {
            logger.error(`[ DBJob ERROR ] Redis job error - ( job excute failed : No.${job.order} = ${job.redisFunc.name} ) `)
            return false
        }

        return true
    }

    public async Run(): Promise<boolean> {

        if (true == this.m_redisJobAdded) {
            this.m_redis = Redis.Inst()
        }

        if (true == this.m_dbmsJobAdded) {
            // Included DMBS job
            const queryRunner = await getConnection().createQueryRunner()
            await queryRunner.startTransaction()
            try {
                await Promise.all(
                    this.m_jobList.map(async e => {
                        switch (e.dbType) {
                            case EDB_TYPE.DBMS:

                                let ret = undefined
                                switch (e.processingType) {
                                    case EDBMS_COMMIT_TYPE.SAVE:
                                        ret = await queryRunner.manager.save(e.dbmsEntityTarget, e.dbmsEntities, e.dbmsOptions)
                                        break
                                    case EDBMS_COMMIT_TYPE.UPDATE:
                                        if (undefined == e.dbmsPartialEntity) { throw new Error("[ DBJob ERROR ] undefined partial entity when update DBMS in DBJob") }
                                        ret = await queryRunner.manager.update(e.dbmsEntityTarget, e.dbmsWhere, e.dbmsPartialEntity)
                                        break
                                    case EDBMS_COMMIT_TYPE.DELETE:
                                        ret = await queryRunner.manager.delete(e.dbmsEntityTarget, e.dbmsWhere)
                                        break
                                    case EDBMS_COMMIT_TYPE.INCREASE:
                                        if (undefined == e.dbmsColName) { throw new Error("[ DBJob ERROR ] undefined dbmsColName when increase DBMS in DBJob") }
                                        if (undefined == e.dbmsSize) { throw new Error("[ DBJob ERROR ] undefined dbmsSize when increase DBMS in DBJob") }
                                        ret = await queryRunner.manager.increment(e.dbmsEntityTarget, e.dbmsWhere, e.dbmsColName, e.dbmsSize)
                                        break
                                    case EDBMS_COMMIT_TYPE.DECREASE:
                                        if (undefined == e.dbmsColName) { throw new Error("[ DBJob ERROR ] undefined dbmsColName when decrease DBMS in DBJob") }
                                        if (undefined == e.dbmsSize) { throw new Error("[ DBJob ERROR ] undefined dbmsSize when decrease DBMS in DBJob") }
                                        ret = await queryRunner.manager.decrement(e.dbmsEntityTarget, e.dbmsWhere, e.dbmsColName, e.dbmsSize)
                                        break
                                    default:
                                        // 기타 함수 정의 https://github.com/typeorm/typeorm/blob/master/docs/entity-manager-api.md 
                                        throw new Error("[ DBJob ERROR ] invalid processing type")
                                }

                                if (undefined == ret) {
                                    logger.error(`[ DBJob ERROR ] DBMS job error - ( job excute failed : No.${e.order} = ${e.dbmsEntityTarget.name} ) `)
                                    throw new Error("[ DBJob ERROR ] DBMS job failed")
                                }
                                break
                            case EDB_TYPE.REDIS:
                                if (false == await this.Execute_RedisJob(e)) {
                                    throw new Error("[ DBJob ERROR ] Redis job failed")
                                }
                                break
                            default:
                                throw new Error("[ DBJob ERROR ] Invalid DBType")
                        }
                    })
                )

                await queryRunner.commitTransaction();
            } catch (error) {
                await queryRunner.rollbackTransaction();
                logger.error(error)
                return false
            } finally {
                await queryRunner.release();
            }
        }
        else {
            this.m_jobList.forEach(async e => {
                if (false == await this.Execute_RedisJob(e)) {
                    return false
                }
            })
        }

        return true
    }
}