import { env } from '../env'

const redis = require('redis');
const { promisify } = require("util");

export class Redis {
    public RedisClient: any;

    public SelectAsync: any;
    public HMGetAsync: any;
    public HMSetAsync: any;
    public ZAddAsync: any;
    public ZRangeAsync: any;
    public ZRangeByScoreAsync: any
    public ZRevRangeAsync: any;
    public ZRevRankAsync: any;
    public ZRemAsync: any;
    public SAddAsync: any;
    public SRemAsync: any
    public SMembers: any
    public ZScoreAsync: any;
    public ZInterStoreAsync: any;
    public KeysAsync: any;
    public ZCardAsync: any;
    public ZScanAsync: any;
    public ZUnionstoreAsync: any;
    public ZMscoreAsync: any

    // redis 6.2.0
    public ZDiffstoreAsync: any

    private static redisMap: Map<number, Redis> = new Map<number, Redis>()

    public static Inst(index: number) {
        return this.redisMap.get(index)
    }

    private constructor() {
        this.Init();
    }

    private async Init() {
        //console.log('Begin RedisManager Init()');
        this.RedisClient = redis.createClient(
            env.isDevelopment ? env.Redis_local.local_port : env.Redis_dev.dev_port,
            env.isDevelopment ? env.Redis_local.local_host : env.Redis_dev.dev_host);

        this.RedisClient.on("error", function (error) {
            console.error(error);
        });

        this.SelectAsync = promisify(this.RedisClient.select).bind(this.RedisClient)
        this.HMGetAsync = promisify(this.RedisClient.hmget).bind(this.RedisClient);
        this.HMSetAsync = promisify(this.RedisClient.hmset).bind(this.RedisClient);
        this.SAddAsync = promisify(this.RedisClient.sadd).bind(this.RedisClient);
        this.SRemAsync = promisify(this.RedisClient.srem).bind(this.RedisClient)
        this.SMembers = promisify(this.RedisClient.smembers).bind(this.RedisClient)
        this.ZAddAsync = promisify(this.RedisClient.zadd).bind(this.RedisClient);
        this.ZRangeAsync = promisify(this.RedisClient.zrange).bind(this.RedisClient);
        this.ZRangeByScoreAsync = promisify(this.RedisClient.zrangebyscore).bind(this.RedisClient)
        this.ZRevRangeAsync = promisify(this.RedisClient.zrevrange).bind(this.RedisClient);
        this.ZRevRankAsync = promisify(this.RedisClient.zrevrank).bind(this.RedisClient);
        this.ZRemAsync = promisify(this.RedisClient.zrem).bind(this.RedisClient);
        this.ZScoreAsync = promisify(this.RedisClient.zscore).bind(this.RedisClient);
        this.ZInterStoreAsync = promisify(this.RedisClient.zinterstore).bind(this.RedisClient);
        this.KeysAsync = promisify(this.RedisClient.keys).bind(this.RedisClient);
        this.ZCardAsync = promisify(this.RedisClient.zcard).bind(this.RedisClient);
        this.ZScanAsync = promisify(this.RedisClient.zscan).bind(this.RedisClient);
        this.ZUnionstoreAsync = promisify(this.RedisClient.zunionstore).bind(this.RedisClient);

        // redis 6.2.0
        this.ZDiffstoreAsync = promisify(this.RedisClient.zdiffstore).bind(this.RedisClient)
        this.ZMscoreAsync = promisify(this.RedisClient.zmscore).bind(this.RedisClient)

        //console.log('End RedisManager Init()');
    }

    public static async MakeAllConnection() {
        for (let index = 0; index < 16; index++) {
            let red = new this()
            this.redisMap.set(index, red)
            await red.SelectAsync(index)
        }
    }
}