<?php

namespace Dflydev\EventStore\Doctrine\Dbal;

use Doctrine\DBAL\Connection;

trait TransactionalTableLockable
{
    public function transactional(Connection $connection, $tableName, $followStoreId, $callback)
    {
        $connection->transactional(function ($connection) use ($tableName, $followStoreId, $callback) {
            $connection->exec('LOCK TABLES '.$tableName.' WRITE');

            $caughtException = null;

            try {
                $callback($connection, $tableName, $followStoreId);
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
