<?php

namespace Dflydev\EventStore\Doctrine\Dbal;

use Dflydev\EventStore\EventStore;
use Dflydev\EventStore\FollowStore;
use Dflydev\EventStore\FollowStoreDispatcher;
use Doctrine\DBAL\Connection;

class DbalFollowStore extends FollowStore
{
    private $eventStore;
    private $followStoreDispatcher;
    private $connection;
    private $tableName;
    private $followStoreId;

    public function __construct(
        EventStore $eventStore,
        FollowStoreDispatcher $followStoreDispatcher,
        Connection $connection,
        $followStoreId = null,
        $tableName = null
    ) {
        $this->eventStore = $eventStore;
        $this->followStoreDispatcher = $followStoreDispatcher;
        $this->connection = $connection;
        $this->followStoreId = $followStoreId ?: 'default';
        $this->tableName = $tableName ?: 'dflydev_fs_last_event';
    }

    public function notifyDispatchableEvents()
    {
        $this->transactional($this->connection, $this->followStoreId, $this->tableName, function (Connection $connection, $followStoreId, $tableName) {
            $this->findAndDispatchNewDispatchableEvents($connection, $followStoreId, $tableName);
        });
    }

    protected function transactional(Connection $connection, $followStoreId, $tableName, $callback)
    {
        $connection->transactional(function (Connection $connection) use ($followStoreId, $tableName, $callback) {
            $callback($connection, $followStoreId, $tableName);
        });
    }

    private function findAndDispatchNewDispatchableEvents(Connection $connection, $followStoreId, $tableName)
    {
        $currentLastDispatchedEventId = $this->queryLastDispatchedEventId($connection, $followStoreId, $tableName);

        $lastDispatchedEventId = $this->followStoreDispatcher->notifyEventDispatchers(
            $this->eventStore,
            $currentLastDispatchedEventId,
            $this->eventDispatchers()
        );

        if ($currentLastDispatchedEventId !== $lastDispatchedEventId) {
            $this->saveLastDispatchedEventId($connection, $followStoreId, $tableName, $lastDispatchedEventId);
        }
    }

    protected function saveLastDispatchedEventId(Connection $connection, $followStoreId, $tableName, $eventId)
    {
        $numberOfAffectedRows = $connection->executeUpdate(
            'UPDATE '.$tableName.' SET event_id = ? WHERE follow_store_id = ?',
            [$eventId, $followStoreId]
        );

        if ($numberOfAffectedRows < 1) {
            $numberOfAffectedRows = $connection->insert($tableName, [
                'event_id' => $eventId,
                'follow_store_id' => $followStoreId,
            ]);
        }

        if ($numberOfAffectedRows < 1) {
            throw new \RuntimeException("Could not save last dispatched event ID");
        }
    }

    protected function queryLastDispatchedEventId(Connection $connection, $followStoreId, $tableName)
    {
        try {
            if ($val = $connection->fetchColumn(
                'SELECT MAX(event_id) FROM '.$tableName. ' WHERE follow_store_id = ?',
                [$followStoreId]
            )) {
                return (int) $val;
            }
        } catch (\Exception $e)
        {
            throw new \RuntimeException("Could not find the last dispatched event ID");
        }

        return 0;
    }
}
