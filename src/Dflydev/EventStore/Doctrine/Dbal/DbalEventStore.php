<?php

namespace Dflydev\EventStore\Doctrine\Dbal;

use Dflydev\EventStore\DispatchableDomainEvent;
use Dflydev\EventStore\EventNotifiable;
use Dflydev\EventStore\EventSerializer;
use Dflydev\EventStore\EventStore;
use Dflydev\EventStore\EventStreamId;
use Dflydev\EventStore\DefaultEventStream;
use Dflydev\EventStore\NullEventNotifiable;
use Doctrine\DBAL\Connection;
use EventCentric\DomainEvents\DomainEvent;
use EventCentric\DomainEvents\DomainEvents;
use EventCentric\DomainEvents\DomainEventsArray;
use Verraes\ClassFunctions\ClassFunctions;

class DbalEventStore implements EventStore
{
    private $eventSerializer;
    private $connection;
    private $eventNotifiable;

    const EVENT_STREAM_SINCE_SQL = "
        SELECT stream_version,
               event_type,
               event_body
          FROM dflydev_es_event_store
         WHERE stream_name = ?
           AND stream_version >= ?
         ORDER BY stream_version
    ";

    const EVENTS_SINCE_SQL = "
        SELECT event_id,
               event_body,
               event_type
          FROM dflydev_es_event_store
         WHERE event_id > ?
         ORDER BY event_id
    ";

    public function __construct(
        EventSerializer $eventSerializer,
        Connection $connection,
        EventNotifiable $eventNotifiable = null
    ) {
        $this->eventSerializer = $eventSerializer;
        $this->connection = $connection;
        $this->eventNotifiable = $eventNotifiable ?: new NullEventNotifiable();
    }

    public function appendWith(EventStreamId $eventStreamId, DomainEvents $domainEvents)
    {
        try {
            $this->connection->transactional(function ($connection) use ($eventStreamId, $domainEvents) {
                $index = 0;

                foreach ($domainEvents as $domainEvent) {
                    $this->appendEventStore($connection, $eventStreamId, $index++, $domainEvent);
                }

                $this->eventNotifiable->notifyDispatchableEvents();
            });
        } catch (\Exception $e) {
            throw new \RuntimeException('Could not save domain events to this event stream likely due to a conflict.');
        }
    }

    public function eventStreamSince(EventStreamId $eventStreamId)
    {
        $result = $this->connection->fetchAll(
            self::EVENT_STREAM_SINCE_SQL,
            [$eventStreamId->streamName(), $eventStreamId->streamVersion()]
        );

        return $this->buildEventStream($result);
    }

    public function fullEventStreamFor(EventStreamId $eventStreamId)
    {
    }

    public function eventsSince($lastReceivedEventId)
    {
        $result = $this->connection->fetchAll(
            self::EVENTS_SINCE_SQL,
            [$lastReceivedEventId]
        );

        return $this->buildEventSequence($result);
    }

    private function appendEventStore(
        Connection $connection,
        EventSTreamId $identity,
        $index,
        DomainEvent $domainEvent
    ) {
        $connection->insert('dflydev_es_event_store', array(
            'event_body' => $this->eventSerializer->serialize($domainEvent),
            'event_type' => ClassFunctions::fqcn($domainEvent),
            'stream_name' => (string) $identity->streamName(),
            'stream_version' => $identity->streamVersion() + $index,
        ));
    }

    private function buildEventStream($result)
    {
        $events = [];
        $version = 0;

        foreach ($result as $storedEvent) {
            $version = $storedEvent['stream_version'];
            $className = $storedEvent['event_type'];
            $events[] = $className::jsonDeserialize(json_decode($storedEvent['event_body']));
        }

        return new DefaultEventStream(new DomainEventsArray($events), $version);
    }

    private function buildEventSequence($result)
    {
        $events = [];

        foreach ($result as $storedEvent) {
            $className = $storedEvent['event_type'];
            $events[] = new DispatchableDomainEvent(
                $storedEvent['event_id'],
                $this->eventSerializer->deserialize($storedEvent['event_body'], $className)
            );
        }

        return $events;
    }
}
