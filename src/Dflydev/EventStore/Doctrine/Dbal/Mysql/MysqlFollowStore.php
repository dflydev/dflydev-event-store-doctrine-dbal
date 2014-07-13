<?php

namespace Dflydev\EventStore\Doctrine\Dbal\Mysql;

use Dflydev\EventStore\Doctrine\Dbal\DbalFollowStore;
use Dflydev\EventStore\Doctrine\Dbal\TransactionalTableLockable;

class MysqlFollowStore extends DbalFollowStore
{
    use TransactionalTableLockable;
}
