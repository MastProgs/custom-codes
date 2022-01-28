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
    dbmsDefaultEntity: T
}

export class DBJob {

    @Inject("logger")
    private logger

    private m_jobList: any[] = []
    private m_jobSize: number = 0

    private m_dbmsJobAdded: boolean = false
    private m_redisJobAdded: boolean = false

    private m_isFinished: boolean = false
    private m_errorInterupted: boolean = false
    private m_errorIndex: number = 0

    private m_resultMap = new Map<number, any>()

    constructor() {
        this.Init()
    }

    private Init() {

    }

    public AddJob_REDIS(redisFunc: any, ...args: any): number {
        this.m_redisJobAdded = true

        let job = new Task()
        job.dbType = EDB_TYPE.REDIS
        job.order = this.m_jobSize
        job.redisFunc = redisFunc
        job.redisFuncArgs = args

        this.m_jobList.push(job)
        this.m_jobSize += 1

        return job.order
    }

    public AddJob_DBMS<Entity, T extends DeepPartial<Entity>>(processingType: EDBMS_COMMIT_TYPE, targetOrEntity: EntityTarget<Entity>, entity: T, options?: SaveOptions): number
    public AddJob_DBMS<Entity, T extends DeepPartial<Entity>>(processingType: EDBMS_COMMIT_TYPE, targetOrEntity: EntityTarget<Entity>, entities: T[], options?: SaveOptions): number
    public AddJob_DBMS<Entity, T extends DeepPartial<Entity>>(processingType: EDBMS_COMMIT_TYPE, targetOrEntity: EntityTarget<Entity>, where: any, partialEntity?: QueryDeepPartialEntity<Entity>): number
    public AddJob_DBMS<Entity, T extends DeepPartial<Entity>>(processingType: EDBMS_COMMIT_TYPE, targetOrEntity: EntityTarget<Entity>, where: any, columnName: string, size: number, defaultEntity?: T): number

    public AddJob_DBMS<Entity, T extends DeepPartial<Entity>>(processingType: EDBMS_COMMIT_TYPE, targetOrEntity: EntityTarget<Entity>, param3: any, param4?: any, param5?: number, param6?: T): number {
        this.m_dbmsJobAdded = true

        let job = new Task()
        job.dbType = EDB_TYPE.DBMS
        job.order = this.m_jobSize
        job.dbmsEntityTarget = targetOrEntity
        job.processingType = processingType

        switch (processingType) {
            case EDBMS_COMMIT_TYPE.SAVE:
                if (false == Array.isArray(param3)) { job.dbmsEntities = [param3] }
                else { job.dbmsEntities = param3 }
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
                job.dbmsDefaultEntity = param6
                break
            default:
                break
        }

        this.m_jobList.push(job)
        this.m_jobSize += 1

        return job.order
    }

    private async Execute_RedisJob(job: any) {
        const res = await job.redisFunc(...job.redisFuncArgs)
        if (undefined == res) {
            logger.error(`[ DBJob ERROR ] Redis job error - ( job excute failed : No.${job.order} = ${job.redisFunc.name} ) `)
            return false
        }

        return true
    }

    public async SingleRun(): Promise<any> {

        if (0 >= this.m_jobSize) {
            logger.error(`[ DBJob ERROR ] No job listed - m_jobSize : ${this.m_jobSize}`)
            return undefined
        }

        if (1 < this.m_jobSize) {
            logger.error(`[ DBJob ERROR ] Too Many Job in SingleRun - m_jobSize : ${this.m_jobSize}`)
            return undefined
        }

        if (true == this.m_dbmsJobAdded) {
            // Included DMBS job
            const queryRunner = getConnection().createQueryRunner()
            await queryRunner.startTransaction()
            try {
                const e = this.m_jobList[0]
                let rtn;
                switch (e.dbType) {
                    case EDB_TYPE.DBMS:
                        switch (e.processingType) {
                            case EDBMS_COMMIT_TYPE.SAVE:
                                rtn = await queryRunner.manager.save(e.dbmsEntityTarget, e.dbmsEntities, e.dbmsOptions)
                                await queryRunner.commitTransaction();
                                return rtn
                            case EDBMS_COMMIT_TYPE.UPDATE:
                                if (undefined == e.dbmsPartialEntity) { throw new Error("[ DBJob ERROR ] undefined partial entity when update DBMS in DBJob") }
                                rtn = await queryRunner.manager.update(e.dbmsEntityTarget, e.dbmsWhere, e.dbmsPartialEntity)
                                await queryRunner.commitTransaction();
                                return rtn
                            case EDBMS_COMMIT_TYPE.DELETE:
                                rtn = await queryRunner.manager.delete(e.dbmsEntityTarget, e.dbmsWhere)
                                await queryRunner.commitTransaction();
                                return rtn
                            case EDBMS_COMMIT_TYPE.INCREASE:
                                if (undefined == e.dbmsColName) { throw new Error("[ DBJob ERROR ] undefined dbmsColName when increase DBMS in DBJob") }
                                if (undefined == e.dbmsSize) { throw new Error("[ DBJob ERROR ] undefined dbmsSize when increase DBMS in DBJob") }
                                rtn = await queryRunner.manager.increment(e.dbmsEntityTarget, e.dbmsWhere, e.dbmsColName, e.dbmsSize)
                                await queryRunner.commitTransaction();
                                return rtn
                            case EDBMS_COMMIT_TYPE.DECREASE:
                                if (undefined == e.dbmsColName) { throw new Error("[ DBJob ERROR ] undefined dbmsColName when decrease DBMS in DBJob") }
                                if (undefined == e.dbmsSize) { throw new Error("[ DBJob ERROR ] undefined dbmsSize when decrease DBMS in DBJob") }
                                rtn = await queryRunner.manager.decrement(e.dbmsEntityTarget, e.dbmsWhere, e.dbmsColName, e.dbmsSize)
                                await queryRunner.commitTransaction();
                                return rtn
                            default:
                                // 기타 함수 정의 https://github.com/typeorm/typeorm/blob/master/docs/entity-manager-api.md 
                                throw new Error("[ DBJob ERROR ] invalid processing type")
                        }
                    case EDB_TYPE.REDIS:
                        return await this.Execute_RedisJob(e)
                    default:
                        throw new Error("[ DBJob ERROR ] Invalid DBType")
                }
            } catch (error) {
                await queryRunner.rollbackTransaction();
                this.m_errorInterupted = true
                logger.error(error)
                return undefined
            } finally {
                // await queryRunner.commitTransaction();
                await queryRunner.release();
            }
        }
        else {
            const job = this.m_jobList[0]
            return await job.redisFunc(...job.redisFuncArgs)
        }
    }

    public async Run(): Promise<boolean> {

        if (0 >= this.m_jobSize) {
            logger.error(`[ DBJob ERROR ] No job listed - m_jobSize : ${this.m_jobSize}`)
            return false
        }

        if (10 <= this.m_jobSize) {
            logger.error(`[ DBJob ERROR ] Job size overed 10 - m_jobSize : ${this.m_jobSize}`)
            return false
        }

        if (true == this.m_errorInterupted) {
            return false
        }

        if (true == this.m_dbmsJobAdded) {
            // Included DMBS job
            const queryRunner = getConnection().createQueryRunner()
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
                                        this.m_resultMap.set(e.order, ret)
                                        break
                                    case EDBMS_COMMIT_TYPE.UPDATE:
                                        if (undefined == e.dbmsPartialEntity) { throw new Error("[ DBJob ERROR ] undefined partial entity when update DBMS in DBJob") }
                                        ret = await queryRunner.manager.update(e.dbmsEntityTarget, e.dbmsWhere, e.dbmsPartialEntity)
                                        this.m_resultMap.set(e.order, ret)
                                        break
                                    case EDBMS_COMMIT_TYPE.DELETE:
                                        ret = await queryRunner.manager.delete(e.dbmsEntityTarget, e.dbmsWhere)
                                        this.m_resultMap.set(e.order, ret)
                                        break
                                    case EDBMS_COMMIT_TYPE.INCREASE:
                                        if (undefined == e.dbmsColName) { throw new Error("[ DBJob ERROR ] undefined dbmsColName when increase DBMS in DBJob") }
                                        if (undefined == e.dbmsSize) { throw new Error("[ DBJob ERROR ] undefined dbmsSize when increase DBMS in DBJob") }
                                        ret = await queryRunner.manager.increment(e.dbmsEntityTarget, e.dbmsWhere, e.dbmsColName, e.dbmsSize)
                                        if (0 == ret.affected && undefined != e.dbmsDefaultEntity) {
                                            const semiRet = await queryRunner.manager.save(e.dbmsEntityTarget, e.dbmsDefaultEntity)
                                            if (undefined != semiRet) {
                                                ret = semiRet//await queryRunner.manager.increment(e.dbmsEntityTarget, e.dbmsWhere, e.dbmsColName, e.dbmsSize)
                                            }
                                        }

                                        this.m_resultMap.set(e.order, ret)
                                        break
                                    case EDBMS_COMMIT_TYPE.DECREASE:
                                        if (undefined == e.dbmsColName) { throw new Error("[ DBJob ERROR ] undefined dbmsColName when decrease DBMS in DBJob") }
                                        if (undefined == e.dbmsSize) { throw new Error("[ DBJob ERROR ] undefined dbmsSize when decrease DBMS in DBJob") }
                                        ret = await queryRunner.manager.decrement(e.dbmsEntityTarget, e.dbmsWhere, e.dbmsColName, e.dbmsSize)
                                        if (0 == ret.affected && undefined != e.dbmsDefaultEntity) {
                                            const semiRet = await queryRunner.manager.save(e.dbmsEntityTarget, e.dbmsDefaultEntity)
                                            if (undefined != semiRet) {
                                                ret = semiRet//await queryRunner.manager.decrement(e.dbmsEntityTarget, e.dbmsWhere, e.dbmsColName, e.dbmsSize)
                                            }
                                        }

                                        this.m_resultMap.set(e.order, ret)
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
                this.m_errorInterupted = true
                logger.error(error)
                return false
            } finally {
                await queryRunner.release();
            }
        }
        else {
            try {
                this.m_jobList.forEach(async e => {
                    if (false == await this.Execute_RedisJob(e)) {
                        throw new Error("[ DBJob ERROR ] Redis job failed")
                    }
                })
            } catch (error) {
                this.m_errorInterupted = true
                logger.error(error)
                return false
            }
        }

        this.m_isFinished = true
        return true
    }
}