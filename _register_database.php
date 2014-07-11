<?php

// PDO Postgre database
DatabaseAdapterRegistry::register(array(
	'class' => 'PostgrePDODatabase',
	'title' => 'PostgreSQL 8.3+ (using PDO)',
	'helperPath' => dirname(__FILE__).'/code/PostgreSQLDatabaseConfigurationHelper.php',
	'supported' => (class_exists('PDO') && in_array('postgresql', PDO::getAvailableDrivers())),
	'missingExtensionText' =>
		'Either the <a href="http://www.php.net/manual/en/book.pdo.php">PDO Extension</a> or 
		the <a href="http://www.php.net/manual/en/ref.pdo-sqlsrv.php">SQL Server PDO Driver</a> 
		are unavailable. Please install or enable these and refresh this page.'
));


// PDO Postgre database
DatabaseAdapterRegistry::register(array(
	'class' => 'PostgreSQLDatabase',
	'title' => 'PostgreSQL 8.3+ (using pg_connect)',
	'helperPath' => dirname(__FILE__).'/code/PostgreSQLDatabaseConfigurationHelper.php',
	'supported' => function_exists('pg_connect'),
	'missingExtensionText' => 
		'The <a href="http://php.net/pgsql">pgsql</a> PHP extension is not
		available. Please install or enable it and refresh this page.'
));
