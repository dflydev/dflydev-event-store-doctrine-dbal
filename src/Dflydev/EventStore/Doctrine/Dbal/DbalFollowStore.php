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

    public function __construct(
        EventStore $eventStore,
        FollowStoreDispatcher $followStoreDispatcher,
        Connection $connection,
        $tableName = null,
        array $eventDispatchers = []
    ) {
        $this->eventStore = $eventStore;
        $this->followStoreDispatcher = $followStoreDispatcher;
        $this->connection = $connection;
        $this->tableName = $tableName ?: 'dflydev_fs_last_event';
        $this->registerEventDispatchers($eventDispatchers);
    }

    public function notifyDispatchableEvents()
    {
        $this->transactional($this->connection, $this->tableName, function (Connection $connection, $tableName) {
            $this->findAndDispatchNewDispatchableEvents($connection, $tableName);
        });
    }

    abstract protected function transactional(Connection $connection, $tableName, $callback);

    private function findAndDispatchNewDispatchableEvents(Connection $connection, $tableName)
    {
        $currentLastDispatchedEventId = $this->queryLastDispatchedEventId($connection, $tableName);

        $lastDispatchedEventId = $this->followStoreDispatcher->notifyEventDispatchers(
            $this->eventStore,
            $currentLastDispatchedEventId,
            $this->eventDispatchers()
        );

        if ($currentLastDispatchedEventId !== $lastDispatchedEventId) {
            $this->saveLastDispatchedEventId($connection, $tableName, $lastDispatchedEventId);
        }
    }

    protected function saveLastDispatchedEventId(Connection $connection, $tableName, $eventId)
    {
        $numberOfAffectedRows = $connection->executeUpdate('UPDATE '.$tableName.' SET event_id = ?', [$eventId]);

        if ($numberOfAffectedRows < 1) {
            $numberOfAffectedRows = $connection->insert($tableName, [
                'event_id' => $eventId,
            ]);
        }

        if ($numberOfAffectedRows < 1) {
            throw new \RuntimeException("Could not save last dispatched event ID");
        }
    }

    protected function queryLastDispatchedEventId(Connection $connection, $tableName)
    {
        try {
            if ($val = $connection->fetchColumn('SELECT MAX(event_id) FROM '.$tableName)) {
                return (int) $val;
            }
        } catch (\Exception $e)
        {
            throw new \RuntimeException("Could not find the last dispatched event ID");
        }

        return 0;
    }
}
