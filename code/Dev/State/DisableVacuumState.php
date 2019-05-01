<?php

namespace SilverStripe\PostgreSQL\Dev\State;

use SilverStripe\Core\Config\Config;
use SilverStripe\Dev\SapphireTest;
use SilverStripe\Dev\State\TestState;
use SilverStripe\ORM\DB;
use SilverStripe\PostgreSQL\PostgreSQLConnector;
use SilverStripe\PostgreSQL\PostgreSQLSchemaManager;

class DisableVacuumState implements TestState
{
    /**
     * Called on setup
     *
     * @param SapphireTest $test
     */
    public function setUp(SapphireTest $test)
    {
        // TODO: Implement setUp() method.
    }

    /**
     * Called on tear down
     *
     * @param SapphireTest $test
     */
    public function tearDown(SapphireTest $test)
    {
        // TODO: Implement tearDown() method.
    }

    /**
     * Called once on setup
     *
     * @param string $class Class being setup
     */
    public function setUpOnce($class)
    {
        if (DB::get_conn()->getConnector() instanceof PostgreSQLConnector) {
            Config::modify()->set(PostgreSQLSchemaManager::class, 'check_and_repair_on_build', false);
        }
    }

    /**
     * Called once on tear down
     *
     * @param string $class Class being torn down
     */
    public function tearDownOnce($class)
    {
        // TODO: Implement tearDownOnce() method.
    }
}
