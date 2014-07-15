<?php

namespace Dflydev\EventStore\Doctrine\Dbal\Sqlite;

use Dflydev\EventStore\Doctrine\Dbal\DbalFollowStore;
use Doctrine\DBAL\Connection;

class SqliteFollowStore extends DbalFollowStore
{
    public function transactional(Connection $connection, $followStoreId, $tableName, $callback)
    {
        $connection->exec('BEGIN EXCLUSIVE TRANSACTION');

        try {
            $callback($connection, $followStoreId, $tableName);
        } catch (\Exception $e) {
            $connection->exec('ROLLBACK');
            throw $e;
        }

        $connection->exec('COMMIT');
    }
}
