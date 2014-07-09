<?php

namespace Dflydev\EventStore\Doctrine\Dbal;

use EventCentric\DomainEvents\DomainEvent;
use JsonSerializable;


class TestDomainEvent implements DomainEvent, JsonSerializable
{
    private $value;

    public function __construct($value)
    {
        $this->value = $value;
    }

    public function value()
    {
        return $this->value;
    }

    public function jsonSerialize()
    {
        return ['contrived', $this->value];
    }

    public static function jsonDeserialize($data)
    {
        if ('contrived' !== $data[0]) {
            throw new \RuntimeException('Invalid JSON serialization format detected');
        }

        return new static($data[1]);
    }
}
