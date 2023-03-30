<?php

use SilverStripe\Dev\Install\DatabaseAdapterRegistry;
use SilverStripe\PostgreSQL\PostgreSQLDatabaseConfigurationHelper;

DatabaseAdapterRegistry::register(array(
    /** @skipUpgrade */
    'class' => 'PostgreSQLDatabase',
    'module' => 'postgresql',
    'title' => 'PostgreSQL 8.3+ (using pg_connect)',
    'helperPath' => __DIR__.'/code/PostgreSQLDatabaseConfigurationHelper.php',
    'helperClass' => PostgreSQLDatabaseConfigurationHelper::class,
    'supported' => function_exists('pg_connect'),
    'missingExtensionText' =>
        'The <a href="http://php.net/pgsql">pgsql</a> PHP extension is not
		available. Please install or enable it and refresh this page.'
));
