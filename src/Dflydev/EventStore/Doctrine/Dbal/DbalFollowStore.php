<?php

namespace Dflydev\EventStore\Doctrine\Dbal;

use Dflydev\EventStore\EventStore;
use Dflydev\EventStore\FollowStore;
use Dflydev\EventStore\FollowStoreDispatcher;
use Doctrine\DBAL\Connection;

abstract class DbalFollowStore extends FollowStore
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
        $tableName = null,
        $followStoreId = null
    ) {
        $this->eventStore = $eventStore;
        $this->followStoreDispatcher = $followStoreDispatcher;
        $this->connection = $connection;
        $this->tableName = $tableName ?: 'dflydev_fs_last_event';
        $this->followStoreId = $followStoreId ?: 'default';
    }

    public function notifyDispatchableEvents()
    {
        $this->transactional($this->connection, $this->tableName, $this->followStoreId, function (Connection $connection, $tableName, $followStoreId) {
            $this->findAndDispatchNewDispatchableEvents($connection, $tableName, $followStoreId);
        });
    }

    abstract protected function transactional(Connection $connection, $tableName, $followStoreId, $callback);

    private function findAndDispatchNewDispatchableEvents(Connection $connection, $tableName, $followStoreId)
    {
        $currentLastDispatchedEventId = $this->queryLastDispatchedEventId($connection, $tableName, $followStoreId);

        $lastDispatchedEventId = $this->followStoreDispatcher->notifyEventDispatchers(
            $this->eventStore,
            $currentLastDispatchedEventId,
            $this->eventDispatchers()
        );

        if ($currentLastDispatchedEventId !== $lastDispatchedEventId) {
            $this->saveLastDispatchedEventId($connection, $tableName, $followStoreId, $lastDispatchedEventId);
        }
    }

    protected function saveLastDispatchedEventId(Connection $connection, $tableName, $followStoreId, $eventId)
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

    protected function queryLastDispatchedEventId(Connection $connection, $tableName, $followStoreId)
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
