<?php

namespace Dflydev\EventStore\Doctrine\Dbal;

use Dflydev\EventStore\EventNotifiable;
use Dflydev\EventStore\EventStreamId;
use Dflydev\EventStore\EventDispatcher;
use Dflydev\EventStore\DispatchableDomainEvent;
use Dflydev\EventStore\EventStore;
use Dflydev\EventStore\FollowStoreDispatcher;
use Dflydev\EventStore\Implementations\DefaultEventStream;
use Dflydev\EventStore\Implementations\JsonEventSerializer;
use EventCentric\DomainEvents\DomainEvent;
use EventCentric\DomainEvents\DomainEvents;
use EventCentric\DomainEvents\DomainEventsArray;
use JsonSerializable;
use PHPUnit_Framework_TestCase;

class DbalFollowStoreTest extends PHPUnit_Framework_TestCase
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

        \Dflydev\EventStore\Doctrine\Dbal\Schema\DbalFollowStoreSchemaUtil::updateSchema($this->connection);
    }

    /** @test */
    public function shouldNotifyDispatchableEvents()
    {
        $eventStore = new TestEventStore([
            [0, [
                new DispatchableDomainEvent(1, new TestDomainEvent('one')),
                new DispatchableDomainEvent(2, new TestDomainEvent('two')),
            ]],
            [2, [
                new DispatchableDomainEvent(3, new TestDomainEvent('three')),
            ]],
            [3, [
                new DispatchableDomainEvent(4, new TestDomainEvent('AAA')),
                new DispatchableDomainEvent(5, new TestDomainEvent('BBB')),
                new DispatchableDomainEvent(6, new TestDomainEvent('CCC')),
                new DispatchableDomainEvent(7, new TestDomainEvent('DDD')),
            ]],
            [7, []],
            [7, []],
            [7, [
                new DispatchableDomainEvent(8, new TestDomainEvent('eee')),
            ]],
        ]);

        $followStoreDispatcher = new FollowStoreDispatcher();

        $eventDispatcher = function () {
            $args = func_get_args();

            return new TestEventDispatcher($args);
        };

        $testEventDispatcher = $eventDispatcher('one', 'two', 'three', 'AAA', 'BBB', 'CCC', 'DDD', 'eee');

        $followStore = new DbalFollowStore(
            $eventStore,
            $followStoreDispatcher,
            $this->connection,
            [$testEventDispatcher]
        );

        $followStore->notifyDispatchableEvents();

        $this->assertEquals(2, $testEventDispatcher->numberOfDomainEventsDispatched());

        $followStore->notifyDispatchableEvents();

        $this->assertEquals(3, $testEventDispatcher->numberOfDomainEventsDispatched());

        $followStore->notifyDispatchableEvents();

        $this->assertEquals(7, $testEventDispatcher->numberOfDomainEventsDispatched());

        $followStore = new DbalFollowStore(
            $eventStore,
            $followStoreDispatcher,
            $this->connection,
            [$testEventDispatcher]
        );

        $followStore->notifyDispatchableEvents();

        $followStore->notifyDispatchableEvents();

        $followStore->notifyDispatchableEvents();

        $this->assertEquals(8, $testEventDispatcher->numberOfDomainEventsDispatched());
    }
}

class TestEventStore implements EventStore
{
    private $eventsSinceResponses = [];
    private $eventsSinceResponsesIndex = 0;

    public function __construct(array $eventsSinceResponses = [])
    {
        $this->eventsSinceResponses = $eventsSinceResponses;
    }
    public function appendWith(EventStreamId $eventStreamId, DomainEvents $domainEvents)
    {
        throw new \RuntimeException("Not implemented.");
    }

    public function eventStreamSince(EventStreamId $eventStreamId)
    {
        throw new \RuntimeException("Not implemented.");
    }

    public function fullEventStreamFor(EventStreamId $eventStreamId)
    {
        throw new \RuntimeException("Not implemented.");
    }

    public function eventsSince($lastReceivedEventId)
    {
        $expected = $this->eventsSinceResponses[$this->eventsSinceResponsesIndex++];

        if ($lastReceivedEventId !== $expected[0]) {
            throw new \RuntimeException("eventsSince received '".$lastReceivedEventId."' but expected '".$expected[0]."'");
        }

        return $expected[1];
    }
}

class TestEventDispatcher implements EventDispatcher
{
    private $expectedValues = [];
    private $expectedValuesIndex = 0;

    public function __construct(array $expectedValues = [])
    {
        $this->expectedValues = $expectedValues;
    }

    public function dispatch(DispatchableDomainEvent $dispatchableDomainEvent)
    {
        $expected = $this->expectedValues[$this->expectedValuesIndex++];
        $actual = $dispatchableDomainEvent->domainEvent()->value();
        if ($actual !== $expected) {
            throw new \RuntimeException("dispatch received '".$actual."' but expected '".$expected."'");
        }
    }

    public function numberOfDomainEventsDispatched()
    {
        return $this->expectedValuesIndex;
    }
}
