<?php

declare(strict_types=1);

namespace gyDb\DB;

use RedisException;

class BaseRedis
{
    protected $pool;

    protected $connection;

    public function __construct($config = null)
    {
        if (! empty($config)) {
            $this->pool = \gyDb\DB\Redis::getInstance($config);
        } else {
            $this->pool = \gyDb\DB\Redis::getInstance();
        }
    }

    /**
     * @param $name
     * @param $arguments
     * @return mixed
     * @throws RedisException
     */
    public function __call($name, $arguments)
    {
        $this->connection = $this->pool->getConnection();

        try {
            $data = $this->connection->{$name}(...$arguments);
        } catch (RedisException $e) {
            $this->pool->close(null);
            throw $e;
        }

        $this->pool->close($this->connection);

        return $data;
    }

    /**
     * @param $keys
     * @param $timeout
     * @return array
     * @throws RedisException
     */
    public function brPop($keys, $timeout)
    {
        $this->connection = $this->pool->getConnection();

        if ($timeout === 0) {
            // TODO Need to optimize...
            $timeout = 99999999999;
        }

        $this->connection->setOption(3, (string) $timeout);

        $data = [];
        $start = time();
        try {
            $data = $this->connection->brPop($keys, $timeout);
        } catch (RedisException $e) {
            $end = time();
            if ($end - $start < $timeout) {
                $this->pool->close(null);
                throw $e;
            }
        }

        $this->connection->setOption(3, (string) $this->pool->getConfig()['time_out']);

        $this->pool->close($this->connection);

        return $data;
    }

    /**
     * @param $keys
     * @param $timeout
     * @return array
     * @throws RedisException
     */
    public function blPop($keys, $timeout)
    {
        $this->connection = $this->pool->getConnection();

        if ($timeout === 0) {
            // TODO Need to optimize...
            $timeout = 99999999999;
        }

        $this->connection->setOption(3, (string) $timeout);

        $data = [];
        $start = time();
        try {
            $data = $this->connection->blPop($keys, $timeout);
        } catch (RedisException $e) {
            $end = time();
            if ($end - $start < $timeout) {
                $this->pool->close(null);
                throw $e;
            }
        }

        $this->connection->setOption(3, (string) $this->pool->getConfig()['time_out']);

        $this->pool->close($this->connection);

        return $data;
    }

    /**
     * @param $channels
     * @param $callback
     * @return mixed|null
     * @throws RedisException
     */
    public function subscribe($channels, $callback)
    {
        $this->connection = $this->pool->getConnection();

        $this->connection->setOption(3, '-1');

        try {
            $data = $this->connection->subscribe($channels, $callback);
        } catch (RedisException $e) {
            $this->pool->close(null);
            throw $e;
        }

        $this->connection->setOption(3, (string) $this->pool->getConfig()['time_out']);

        $this->pool->close($this->connection);

        return $data;
    }

    /**
     * @param $srcKey
     * @param $dstKey
     * @param $timeout
     * @return bool|mixed|string
     * @throws RedisException
     */
    public function brpoplpush($srcKey, $dstKey, $timeout)
    {
        $this->connection = $this->pool->getConnection();

        $this->connection->setOption(3, (string) $timeout);
        $start = time();
        try {
            $data = $this->connection->brpoplpush($srcKey, $dstKey, $timeout);
        } catch (RedisException $e) {
            $end = time();
            if ($end - $start < $timeout) {
                throw $e;
            }
            $data = false;
        }

        $this->connection->setOption(3, (string) $this->pool->getConfig()['time_out']);

        $this->pool->close($this->connection);

        return $data;
    }
}
