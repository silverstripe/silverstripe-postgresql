<?php
/**
 * This is a helper class for the SS installer.
 * 
 * It does all the specific checking for PostgreSQLDatabase
 * to ensure that the configuration is setup correctly.
 * 
 * @package postgresql
 */
class PostgreSQLDatabaseConfigurationHelper implements DatabaseConfigurationHelper {

	/**
	 * Ensure that the database function pg_connect
	 * is available. If it is, we assume the PHP module for this
	 * database has been setup correctly.
	 * 
	 * @param array $databaseConfig Associative array of database configuration, e.g. "server", "username" etc
	 * @return boolean
	 */
	public function requireDatabaseFunctions($databaseConfig) {
		return (function_exists('pg_connect')) ? true : false;
	}

	/**
	 * Ensure that the database server exists.
	 * @param array $databaseConfig Associative array of db configuration, e.g. "server", "username" etc
	 * @return array Result - e.g. array('success' => true, 'error' => 'details of error')
	 */
	public function requireDatabaseServer($databaseConfig) {
		$success = false;
		$error = '';
		$username = $databaseConfig['username'] ? $databaseConfig['username'] : '';
		$password = $databaseConfig['password'] ? $databaseConfig['password'] : '';
		$server = $databaseConfig['server'];
		$userPart = $username ? " user=$username" : '';
		$passwordPart = $password ? " password=$password" : '';
		$connstring = "host=$server port=5432 dbname=postgres {$userPart}{$passwordPart}";

		$conn = @pg_connect($connstring);
		if($conn) {
			$success = true;
		} else {
			$success = false;
			$error = 'PostgreSQL requires a valid username and password to determine if the server exists.';
		}
		
		return array(
			'success' => $success,
			'error' => $error
		);
	}

	/**
	 * Ensure a database connection is possible using credentials provided.
	 * @param array $databaseConfig Associative array of db configuration, e.g. "server", "username" etc
	 * @return array Result - e.g. array('success' => true, 'error' => 'details of error')
	 */
	public function requireDatabaseConnection($databaseConfig) {
		$success = false;
		$error = '';
		$username = $databaseConfig['username'] ? $databaseConfig['username'] : '';
		$password = $databaseConfig['password'] ? $databaseConfig['password'] : '';
		$server = $databaseConfig['server'];
		$userPart = $username ? " user=$username" : '';
		$passwordPart = $password ? " password=$password" : '';
		$connstring = "host=$server port=5432 dbname=postgres {$userPart}{$passwordPart}";
		
		$conn = @pg_connect($connstring);
		if($conn) {
			$success = true;
		} else {
			$success = false;
			$error = '';
		}
		
		return array(
			'success' => $success,
			'connection' => $conn,
			'error' => $error
		);
	}

	public function getDatabaseVersion($databaseConfig) {
		$version = 0;
		$username = $databaseConfig['username'] ? $databaseConfig['username'] : '';
		$password = $databaseConfig['password'] ? $databaseConfig['password'] : '';
		$server = $databaseConfig['server'];
		$userPart = $username ? " user=$username" : '';
		$passwordPart = $password ? " password=$password" : '';
		$connstring = "host=$server port=5432 dbname=postgres {$userPart}{$passwordPart}";
		$conn = @pg_connect($connstring);
		$info = @pg_version($conn);
		$version = ($info && isset($info['server'])) ? $info['server'] : null;
		if(!$version) {
			// fallback to using the version() function
			$result = @pg_query($conn, "SELECT version()");
			$row = @pg_fetch_array($result);

			if($row && isset($row[0])) {
				$parts = explode(' ', trim($row[0]));
				// ASSUMPTION version number is the second part e.g. "PostgreSQL 8.4.3"
				$version = trim($parts[1]);
			}
		}

		return $version;
	}

	/**
	 * Ensure that the PostgreSQL version is at least 8.3.
	 * @param array $databaseConfig Associative array of db configuration, e.g. "server", "username" etc
	 * @return array Result - e.g. array('success' => true, 'error' => 'details of error')
	 */
	public function requireDatabaseVersion($databaseConfig) {
		$success = false;
		$error = '';
		$version = $this->getDatabaseVersion($databaseConfig);

		if($version) {
			$success = version_compare($version, '8.3', '>=');
			if(!$success) {
				$error = "Your PostgreSQL version is $version. It's recommended you use at least 8.3.";
			}
		} else {
			$error = "Your PostgreSQL version could not be determined.";
		}

		return array(
			'success' => $success,
			'error' => $error
		);
	}

	/**
	 * Ensure that the database connection is able to use an existing database,
	 * or be able to create one if it doesn't exist.
	 * 
	 * @param array $databaseConfig Associative array of db configuration, e.g. "server", "username" etc
	 * @return array Result - e.g. array('success' => true, 'alreadyExists' => 'true')
	 */
	public function requireDatabaseOrCreatePermissions($databaseConfig) {
		$success = false;
		$alreadyExists = false;
		$check = $this->requireDatabaseConnection($databaseConfig);
		$conn = $check['connection'];
		
		$result = pg_query($conn, "SELECT datname FROM pg_database WHERE datname = '$databaseConfig[database]'");
		if(pg_fetch_array($result)) {
			$success = true;
			$alreadyExists = true;
		} else {
			if(@pg_query($conn, "CREATE DATABASE testing123")) {
				pg_query($conn, "DROP DATABASE testing123");
				$success = true;
				$alreadyExists = false;
			}
		}
		
		return array(
			'success' => $success,
			'alreadyExists' => $alreadyExists
		);
	}

}
