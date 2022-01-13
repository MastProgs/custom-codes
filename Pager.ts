import { EResultCode } from '../enums/ResultCode';
import { ObjectType, SelectQueryBuilder } from 'typeorm';
export declare type Order = 'ASC' | 'DESC';

import { buildPaginator } from 'typeorm-cursor-pagination';

export class PagerResult<Entity> {

    public resultCode: EResultCode
    public data: Entity[]
    public bookmark: string
    public isEnd: boolean

    constructor(data: Entity[], bookmark: string | null | undefined, isEnd: boolean) {
        this.resultCode = data == undefined ? EResultCode.ERROR_DBMS_RESPONSE_FAIL : EResultCode.SUCCESS
        this.data = data
        this.bookmark = (bookmark == null) || (bookmark == undefined) ? "" : bookmark
        this.isEnd = isEnd
    }

    public isSuccess() { return this.resultCode == EResultCode.SUCCESS ? true : false }
    public isFailed() { return !this.isSuccess() }

    public GetData() { return this.isSuccess() ? this.data : [] }
    public GetBookmark() { return this.bookmark }
}

export class Pager<Entity> {

    private pagenator: any
    constructor(entity: ObjectType<Entity>, orderColums: string[] | undefined = undefined, bookmark: string | null | undefined = undefined, limit: number = 10, order: Order = "DESC") {
        bookmark = (bookmark == null) || (bookmark == undefined) ? "" : bookmark
        this.pagenator = buildPaginator({
            entity: entity,
            paginationKeys: orderColums as Extract<keyof Entity, string>[],
            query: {
                limit: limit,
                order: order,
                afterCursor: bookmark
            }
        })
    }

    private builder: SelectQueryBuilder<Entity>
    public SetQuery(builder: SelectQueryBuilder<Entity>) {
        this.builder = builder
    }

    public async GetResult(): Promise<PagerResult<Entity>> {
        const { data, cursor } = await this.pagenator.paginate(this.builder)
        const hash = cursor.afterCursor
        const isEnd = hash == null ? true : false

        return new PagerResult(data, hash, isEnd)
    }
}