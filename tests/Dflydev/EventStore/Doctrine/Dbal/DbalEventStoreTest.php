<?php

namespace Dflydev\EventStore\Doctrine\Dbal;

use Dflydev\EventStore\EventNotifiable;
use Dflydev\EventStore\EventStreamId;
use Dflydev\EventStore\DefaultEventStream;
use Dflydev\EventStore\JsonEventSerializer;
use EventCentric\DomainEvents\DomainEvent;
use EventCentric\DomainEvents\DomainEvents;
use EventCentric\DomainEvents\DomainEventsArray;
use JsonSerializable;
use PHPUnit_Framework_TestCase;

class DbalEventStoreTest extends PHPUnit_Framework_TestCase
{
    private $configuration;
    private $connection;

    public function setUp()
    {
        $this->configuration = new \Doctrine\DBAL\Configuration();
        $this->connection = \Doctrine\DBAL\DriverManager::getConnection([
            'memory' => true,
            'driver' => 'pdo_sqlite',
        ], $this->configuration);

        \Dflydev\EventStore\Doctrine\Dbal\Schema\DbalEventStoreSchemaUtil::updateSchema($this->connection);

        $this->testEventSerializer = new TestJsonEventSerializer();
        $this->testEventNotifiable = new TestCountingEventNotifiable();
    }

    /** @test */
    public function shouldAppendEvents()
    {
        $eventStore = new DbalEventStore(
            $this->testEventSerializer,
            $this->connection,
            $this->testEventNotifiable
        );

        $eventStreamId = EventStreamId::create('test');

        $domainEvents = new DomainEventsArray([
            new TestDomainEvent('one'),
            new TestDomainEvent('two'),
            new TestDomainEvent('three'),
        ]);

        $eventStore->appendWith($eventStreamId, $domainEvents);

        $rows = $this->connection->fetchAll('SELECT * FROM dflydev_es_event_store');

        $this->assertEquals(3, count($rows));
        $this->assertEquals(1, $this->testEventNotifiable->count());
    }

    /** @test */
    public function shouldAppendAdditionalEvents()
    {
        $eventStore = new DbalEventStore(
            $this->testEventSerializer,
            $this->connection,
            $this->testEventNotifiable
        );

        $eventStreamId = EventStreamId::create('test');

        $domainEvents = new DomainEventsArray([
            new TestDomainEvent('one'),
            new TestDomainEvent('two'),
            new TestDomainEvent('three'),
        ]);

        $eventStore->appendWith($eventStreamId, $domainEvents);

        $eventStreamId = EventStreamId::create('test', 4);

        $eventStore->appendWith($eventStreamId, $domainEvents);

        $rows = $this->connection->fetchAll('SELECT * FROM dflydev_es_event_store');

        $this->assertEquals(6, count($rows));
        $this->assertEquals(2, $this->testEventNotifiable->count());
    }

    /** @test */
    public function shouldAppendEventsWithDifferentNamesButSameIds()
    {
        $eventStore = new DbalEventStore(
            $this->testEventSerializer,
            $this->connection,
            $this->testEventNotifiable
        );

        $eventStreamId = EventStreamId::create('test');

        $domainEvents = new DomainEventsArray([
            new TestDomainEvent('one'),
            new TestDomainEvent('two'),
            new TestDomainEvent('three'),
        ]);

        $eventStore->appendWith($eventStreamId, $domainEvents);

        $eventStreamId = EventStreamId::create('test2');

        $domainEvents = new DomainEventsArray([
            new TestDomainEvent('one'),
            new TestDomainEvent('two'),
            new TestDomainEvent('three'),
        ]);

        $eventStore->appendWith($eventStreamId, $domainEvents);

        $rows = $this->connection->fetchAll('SELECT * FROM dflydev_es_event_store');

        $this->assertEquals(6, count($rows));
        $this->assertEquals(2, $this->testEventNotifiable->count());
    }

    /**
     * @expectedException RuntimeException
     * @expectedExceptionMessage conflict
     * @test
     */
    public function shouldThrowExceptionWhenAppendingConflictingEvents()
    {
        $eventStore = new DbalEventStore(
            $this->testEventSerializer,
            $this->connection,
            $this->testEventNotifiable
        );

        $eventStreamId = EventStreamId::create('test');

        $domainEvents = new DomainEventsArray([
            new TestDomainEvent('one'),
            new TestDomainEvent('two'),
            new TestDomainEvent('three'),
        ]);

        $eventStore->appendWith($eventStreamId, $domainEvents);
        $eventStore->appendWith($eventStreamId, $domainEvents);

        $rows = $this->connection->fetchAll('SELECT * FROM dflydev_es_event_store');

        $this->assertEquals(3, count($rows));
        $this->assertEquals(1, $this->testEventNotifiable->count());
    }

    /** @test */
    public function shouldReturnEventStream()
    {
        $eventStore = new DbalEventStore(
            $this->testEventSerializer,
            $this->connection,
            $this->testEventNotifiable
        );

        $eventStreamId = EventStreamId::create('test');

        $domainEvents = new DomainEventsArray([
            new TestDomainEvent('one'),
            new TestDomainEvent('two'),
            new TestDomainEvent('three'),
        ]);

        $eventStore->appendWith($eventStreamId, $domainEvents);

        $eventStream = $eventStore->eventStreamSince($eventStreamId);

        $this->assertEquals(3, $eventStream->version());
        $domainEvents = $eventStream->domainEvents();
        $this->assertEquals('one', $domainEvents[0]->value());
        $this->assertEquals('two', $domainEvents[1]->value());
        $this->assertEquals('three', $domainEvents[2]->value());
    }

    /** @test */
    public function shouldReturnEventsSince()
    {
        $eventStore = new DbalEventStore(
            $this->testEventSerializer,
            $this->connection,
            $this->testEventNotifiable
        );

        $eventStreamId = EventStreamId::create('test');

        $domainEvents = new DomainEventsArray([
            new TestDomainEvent('one'),
            new TestDomainEvent('two'),
            new TestDomainEvent('three'),
        ]);

        $eventStore->appendWith($eventStreamId, $domainEvents);

        $domainEvents = $eventStore->eventsSince(0);

        $this->assertEquals(3, count($domainEvents));
        $this->assertEquals(1, $domainEvents[0]->eventId());
        $this->assertEquals('one', $domainEvents[0]->domainEvent()->value());
        $this->assertEquals(2, $domainEvents[1]->eventId());
        $this->assertEquals('two', $domainEvents[1]->domainEvent()->value());
        $this->assertEquals(3, $domainEvents[2]->eventId());
        $this->assertEquals('three', $domainEvents[2]->domainEvent()->value());

        $domainEvents = $eventStore->eventsSince(2);

        $this->assertEquals(1, count($domainEvents));
        $this->assertEquals(3, $domainEvents[0]->eventId());
        $this->assertEquals('three', $domainEvents[0]->domainEvent()->value());

        $domainEvents = $eventStore->eventsSince(3);

        $this->assertEquals(0, count($domainEvents));
    }
}

class TestJsonEventSerializer extends JsonEventSerializer
{
    public function deserialize($data, $className)
    {
        return $className::jsonDeserialize(json_decode($data, true));
    }
}

class TestCountingEventNotifiable implements EventNotifiable
{
    private $count = 0;

    public function notifyDispatchableEvents()
    {
        $this->count++;
    }

    public function count()
    {
        return $this->count;
    }
}
