<?php

namespace Dflydev\EventStore\Doctrine\Dbal\Sqlite;

use Dflydev\EventStore\Doctrine\Dbal\DbalFollowStore;
use Doctrine\DBAL\Connection;

class SqliteFollowStore extends DbalFollowStore
{
    public function transactional(Connection $connection, $tableName, $callback)
    {
        $connection->exec('BEGIN EXCLUSIVE TRANSACTION');

        $caughtException = null;

        try {
            $callback($connection, $tableName);
        } catch (\Exception $e) {
            $caughtException = $e;
        }

        if ($caughtException) {
            $connection->exec('ROLLBACK');
            throw $caughtException;
        }

        $connection->exec('COMMIT');
    }
}
