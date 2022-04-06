# custom-codes
> 필요에 의해서 직접 구현했던 몇몇 커스텀 코드 기록보관소

# 각 파일 설명

## [DBJob.ts](https://github.com/MastProgs/custom-codes/blob/main/DBJob.ts)
> 각 DB 쿼리 및 Redis 요청을 하나의 job 으로 잡고, 여러 다수의 job 처리들을 하나의 트랜젝션 같이 runtime 서버 상에서 코드적으로 보장하는 핸들러 클래스.
> AddJob_DBMS 로 SQL Job 을 추가하고, AddJob_REDIS 로 Redis 요청을 추가하여, Run() 을 통해 각 요청들을 한 번에 편히 처리 가능.
### 주의점
1. typeORM 이 필요하다
2. Run() 함수에서 await Promise.all() 함수는 최대 10개만 처리 가능하므로, 더 많은 job 을 처리해야하는 경우에는 커스텀이 필요하다. (하지만 10개의 job 이 넘어간다면 코드 구조가 잘못된거 아닐까?)
### 사용예시
```typescript
let dbjob = new DBJob()
{
    dbjob.AddJob_DBMS(EDBMS_COMMIT_TYPE.UPDATE, Profile, {
        platformID: userProfileInfo.platformID
    }, {
        grade: userProfileInfo.grade + 1,
        followings: userProfileInfo.followings + 1
    })

    dbjob.AddJob_DBMS(EDBMS_COMMIT_TYPE.UPDATE, Profile, {
        platformID: targetUserProfileInfo.platformID
    }, {
        followers: targetUserProfileInfo.followers + 1
    })

    // rank dbms update
    dbjob.AddJob_DBMS(EDBMS_COMMIT_TYPE.UPDATE, RankProfile, { platformID: targetUserProfileInfo.platformID }, { follow: targetUserProfileInfo.followers + 1 })

    // #Notification
    const notificationEntity = notificationInfo.toEntity()
    dbjob.AddJob_DBMS(EDBMS_COMMIT_TYPE.SAVE, Notification, notificationEntity)

    dbjob.AddJob_REDIS(Redis.Inst(ERedisIndex.FOLLOW).ZAddAsync, this.followingsRedis.GetRedisKey_Followings() + platformID, Number(currentUTCTime().format('YYYYMMDDHHMMSS')), targetPlatformID)
    dbjob.AddJob_REDIS(Redis.Inst(ERedisIndex.FOLLOW).ZAddAsync, this.followingsRedis.GetRedisKey_Followers() + targetPlatformID, Number(currentUTCTime().format('YYYYMMDDHHMMSS')), platformID)

    // Ranking redis follow
    dbjob.AddJob_REDIS(Redis.Inst(ERedisIndex.RANK).ZAddAsync, this.rankService.GetRedisKey_Follow(), "INCR", 1, targetUserProfileInfo.platformID)
}
const res = await dbjob.Run()
```

## [Pager.ts](https://github.com/MastProgs/custom-codes/blob/main/Pager.ts)
> 페이지네이션 기법을 조금 더 쉽게 활용할 수 있도록, 한 번 더 랩핑한 핸들러 클래스.
> 필요한 쿼리를 넣으면, hash pointer index 와 그에 대응대는 필요한 value 들을 반환 받을 수 있음.

### 주의점
1. typeORM 이 필요하다.
2. pagination 이 필요하다.

### 사용예시
```typescript
public async GetNextMyNFTList(walletAddress: WALLET_ADDR[], afterCursor: string) {
    let owner = this.create()
    owner.nftSN = 0

    let condition = []
    walletAddress.forEach(e => {
        condition.push({
            walletAddress: e
        })
    })

    if (1 > condition.length) { return new PagerResult<NFTOwner>(undefined, "", true) }

    let pager = new Pager(NFTOwner, Object.getOwnPropertyNames(owner), afterCursor, 50)
    pager.SetQuery(this.createQueryBuilder(NFTOwner.name).where(condition))
    return await pager.GetResult()
}
```

## [Redis.ts](https://github.com/MastProgs/custom-codes/blob/main/Redis.ts)
> Redis 를 따로 클러스터링을 사용하지 않고, 각 redis index number DB 형태로 접근하여 사용하는 경우, 편하게 index 번호만 입력하여 redis 로 요청을 날릴 수 있는 핸들러 클래스.
