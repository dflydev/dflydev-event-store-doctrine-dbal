<?php

namespace Dflydev\EventStore\Doctrine\Dbal\Schema;

use Doctrine\Dbal\Connection;
use Doctrine\DBAL\Schema\Comparator;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Types\Type;

class DbalFollowStoreSchemaUtil
{
    public static function updateSchema(Connection $connection)
    {
        $schemaManager = $connection->getSchemaManager();

        $fromSchema = $schemaManager->createSchema();
        $toSchema = static::getSchema();

        $comparator = new Comparator();
        $diff = $comparator->compare($fromSchema, $toSchema);

        if ($sqlList = $diff->toSaveSql($connection->getDatabasePlatform())) {
            foreach ($sqlList as $sql) {
                $connection->exec($sql);
            }
        }
    }

    protected static function getSchema()
    {
        $schema = new Schema();

        $dispatcherLastEvent = $schema->createTable('dflydev_fs_last_event');
        $dispatcherLastEvent->addColumn('event_id', 'integer');
        $dispatcherLastEvent->setPrimaryKey(['event_id']);

        return $schema;
    }
}
