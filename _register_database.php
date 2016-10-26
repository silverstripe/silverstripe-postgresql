<?php

use SilverStripe\Dev\Install\DatabaseAdapterRegistry;
use SilverStripe\PostgreSQL\PostgreSQLDatabaseConfigurationHelper;

// PDO Postgre database
DatabaseAdapterRegistry::register(array(
    /** @skipUpgrade */
    'class' => 'PostgrePDODatabase',
    'module' => 'postgresql',
    'title' => 'PostgreSQL 8.3+ (using PDO)',
    'helperPath' => __DIR__.'/code/PostgreSQLDatabaseConfigurationHelper.php',
    'helperClass' => PostgreSQLDatabaseConfigurationHelper::class,
    'supported' => (class_exists('PDO') && in_array('postgresql', PDO::getAvailableDrivers())),
    'missingExtensionText' =>
        'Either the <a href="http://www.php.net/manual/en/book.pdo.php">PDO Extension</a> or 
		the <a href="http://www.php.net/manual/en/ref.pdo-sqlsrv.php">SQL Server PDO Driver</a> 
		are unavailable. Please install or enable these and refresh this page.'
));


// PDO Postgre database
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
