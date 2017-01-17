package com.serli.oracle.of.bacon.repository;

import redis.clients.jedis.Jedis;

import java.util.List;

public class RedisRepository {

    private Jedis jedis = new Jedis("localhost");

    public List<String> getLastTenSearches() {
        return jedis.lrange("lastTenSearches", 0, -1);
    }

    public void putSearch(String value) {
        if (this.getLastTenSearches().size() >= 10){
            jedis.rpop("lastTenSearches");
        }
        jedis.lpush("lastTenSearches", value);
    }
}
