<?php

namespace Dflydev\EventStore\Doctrine\Dbal\Sqlite;

use Dflydev\EventStore\Doctrine\Dbal\DbalFollowStore;
use Doctrine\DBAL\Connection;

class SqliteFollowStore extends DbalFollowStore
{
    public function transactional(Connection $connection, $tableName, $followStoreId, $callback)
    {
        $connection->exec('BEGIN EXCLUSIVE TRANSACTION');

        try {
            $callback($connection, $tableName, $followStoreId);
        } catch (\Exception $e) {
            $connection->exec('ROLLBACK');
            throw $e;
        }

        $connection->exec('COMMIT');
    }
}
