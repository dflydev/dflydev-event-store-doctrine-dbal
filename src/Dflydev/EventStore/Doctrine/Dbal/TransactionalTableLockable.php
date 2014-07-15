<?php

namespace Dflydev\EventStore\Doctrine\Dbal;

use Doctrine\DBAL\Connection;

trait TransactionalTableLockable
{
    public function transactional(Connection $connection, $followStoreId, $tableName, $callback)
    {
        $connection->transactional(function ($connection) use ($followStoreId, $tableName, $callback) {
            $connection->exec('LOCK TABLES '.$tableName.' WRITE');

            $caughtException = null;

            try {
                $callback($connection, $followStoreId, $tableName);
            } catch (\Exception $e) {
                $caughtException = $e;
            }

            $connection->exec('UNLOCK TABLES');

            if ($caughtException) {
                throw $caughtException;
            }
        });
    }
}
