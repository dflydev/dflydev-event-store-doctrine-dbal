<?php

namespace Dflydev\EventStore\Doctrine\Dbal\Schema;

use Doctrine\Dbal\Connection;
use Doctrine\DBAL\Schema\Comparator;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Types\Type;

class DbalFollowStoreSchemaUtil
{
    public static function updateSchema(Connection $connection, $tableName = 'dflydev_fs_last_event')
    {
        $schemaManager = $connection->getSchemaManager();

        $fromSchema = $schemaManager->createSchema();
        $toSchema = static::getSchema($tableName);

        $comparator = new Comparator();
        $diff = $comparator->compare($fromSchema, $toSchema);

        if ($sqlList = $diff->toSaveSql($connection->getDatabasePlatform())) {
            foreach ($sqlList as $sql) {
                $connection->exec($sql);
            }
        }
    }

    protected static function getSchema($tableName)
    {
        $schema = new Schema();

        $dispatcherLastEvent = $schema->createTable($tableName);
        $dispatcherLastEvent->addColumn('event_id', 'integer');
        $dispatcherLastEvent->addColumn('follow_store_id', 'string');
        $dispatcherLastEvent->setPrimaryKey(['event_id', 'follow_store_id']);

        return $schema;
    }
}
