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
    private $lastDispatchedEventId;

    public function __construct(
        EventStore $eventStore,
        FollowStoreDispatcher $followStoreDispatcher,
        Connection $connection,
        array $eventDispatchers = []
    ) {
        $this->eventStore = $eventStore;
        $this->followStoreDispatcher = $followStoreDispatcher;
        $this->connection = $connection;
        $this->registerEventDispatchers($eventDispatchers);
        $this->lastDispatchedEventId = static::queryLastDispatchedEventId($connection);
    }

    public function notifyDispatchableEvents()
    {
        $lastDispatchedEventId = $this->followStoreDispatcher->notifyEventDispatchers(
            $this->eventStore,
            $this->lastDispatchedEventId,
            $this->eventDispatchers()
        );

        if ($this->lastDispatchedEventId !== $lastDispatchedEventId) {
            static::saveLastDispatchedEventId($this->connection, $lastDispatchedEventId);

            $this->lastDispatchedEventId = $lastDispatchedEventId;
        }
    }

    private static function saveLastDispatchedEventId(Connection $connection, $eventId)
    {
        $connection->transactional(function ($connection) use ($eventId) {
            $numberOfAffectedRows = $connection->insert('dflydev_fs_last_event', [
                'event_id' => $eventId,
            ]);

            if ($numberOfAffectedRows < 1) {
                $numberOfAffectedRows = $connection->update('dflydev_fs_last_event', [
                    'event_id' => $eventId,
                ]);
            }

            if ($numberOfAffectedRows < 1) {
                throw new \RuntimeException("Could not save last dispatched event ID");
            }
        });
    }

    private static function queryLastDispatchedEventId(Connection $connection)
    {
        try {
            if ($val = $connection->fetchColumn('SELECT MAX(event_id) FROM dflydev_fs_last_event')) {
                return (int) $val;
            }

            static::saveLastDispatchedEventId($connection, 0);
        } catch (\Exception $e)
        {
            // noop
        }

        return 0;
    }
}
