<?php

namespace Dflydev\EventStore\Doctrine\Dbal\Schema;

use Doctrine\Dbal\Connection;
use Doctrine\DBAL\Schema\Comparator;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Types\Type;

class DbalEventStoreSchemaUtil
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

        $esEventStore = $schema->createTable('dflydev_es_event_store');
        $esEventStore->addColumn('event_id', 'integer', ['autoincrement' => true,]);
        $esEventStore->addColumn('event_body', 'blob');
        $esEventStore->addColumn('event_type', 'string');
        $esEventStore->addColumn('stream_name', 'string');
        $esEventStore->addColumn('stream_version', 'integer');
        $esEventStore->addIndex(['stream_name']);
        $esEventStore->addUniqueIndex(['stream_name', 'stream_version']);
        $esEventStore->setPrimaryKey(['event_id']);

        return $schema;
    }
}
