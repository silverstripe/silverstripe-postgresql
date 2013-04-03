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
	 * Create a connection of the appropriate type
	 * 
	 * @param array $databaseConfig
	 * @param string $error Error message passed by value
	 * @return mixed|null Either the connection object, or null if error
	 */
	protected function createConnection($databaseConfig, &$error) {
		$error = null;
		$username = empty($databaseConfig['username']) ? '' : $databaseConfig['username'];
		$password = empty($databaseConfig['password']) ? '' : $databaseConfig['password'];
		$server = $databaseConfig['server'];
		
		try {
			switch($databaseConfig['type']) {
				case 'PostgreSQLDatabase':
					$userPart = $username ? " user=$username" : '';
					$passwordPart = $password ? " password=$password" : '';
					$connstring = "host=$server port=5432 dbname=postgres{$userPart}{$passwordPart}";
					$conn = pg_connect($connstring);
					break;
				case 'PostgrePDODatabase':
					// May throw a PDOException if fails
					$conn = @new PDO('postgresql:host='.$server.';dbname=postgres;port=5432', $username, $password);
					break;
				default:
					$error = 'Invalid connection type';
					return null;
			}
		} catch(Exception $ex) {
			$error = $ex->getMessage();
			return null;
		}
		if($conn) {
			return $conn;
		} else {
			$error = 'PostgreSQL requires a valid username and password to determine if the server exists.';
			return null;
		}
	}

	public function requireDatabaseFunctions($databaseConfig) {
		$data = DatabaseAdapterRegistry::get_adapter($databaseConfig['type']);
		return !empty($data['supported']);
	}

	public function requireDatabaseServer($databaseConfig) {
		$conn = $this->createConnection($databaseConfig, $error);
		$success = !empty($conn);
		return array(
			'success' => $success,
			'error' => $error
		);
	}

	public function requireDatabaseConnection($databaseConfig) {
		$conn = $this->createConnection($databaseConfig, $error);
		$success = !empty($conn);
		return array(
			'success' => $success,
			'connection' => $conn,
			'error' => $error
		);
	}

	public function getDatabaseVersion($databaseConfig) {
		$conn = $this->createConnection($databaseConfig, $error);
		if(!$conn) {
			return false;
		} elseif($conn instanceof PDO) {
			return $conn->getAttribute(PDO::ATTR_SERVER_VERSION);
		} elseif(is_resource($conn)) {
			$info = pg_version($conn);
			return $info['server'];
		} else {
			return false;
		}
	}

	/**
	 * Ensure that the PostgreSQL version is at least 8.3.
	 * 
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
	 * Helper function to quote a string value
	 * 
	 * @param mixed $conn Connection object/resource
	 * @param string $value Value to quote
	 * @return string Quoted strieng
	 */
	protected function quote($conn, $value) {
		if($conn instanceof PDO) {
			return $conn->quote($value);
		} elseif(is_resource($conn)) {
			return "'".pg_escape_string($conn, $value)."'";
		} else {
			user_error('Invalid database connection', E_USER_ERROR);
		}
	}
	
	/**
	 * Helper function to execute a query
	 * 
	 * @param mixed $conn Connection object/resource
	 * @param string $sql SQL string to execute
	 * @return array List of first value from each resulting row
	 */
	protected function query($conn, $sql) {
		$items = array();
		if($conn instanceof PDO) {
			foreach($conn->query($sql) as $row) {
				$items[] = $row[0];
			}
		} elseif(is_resource($conn)) {
			$result =  pg_query($conn, $sql);
			while ($row = pg_fetch_row($result)) {
				$items[] = $row[0];
			}
		}
		return $items;
	}

	public function requireDatabaseOrCreatePermissions($databaseConfig) {
		$success = false;
		$alreadyExists = false;
		$conn = $this->createConnection($databaseConfig, $error);
		if($conn) {
			// Check if db already exists
			$existingDatabases = $this->query($conn, "SELECT datname FROM pg_database");
			$alreadyExists = in_array($databaseConfig['database'], $existingDatabases);
			if($alreadyExists) {
				$success = true;
			} else {
				// Check if this user has create privileges
				$allowedUsers = $this->query($conn, "select rolname from pg_authid where rolcreatedb = true;");
				$success = in_array($databaseConfig['username'], $allowedUsers);
			}
		}
		
		return array(
			'success' => $success,
			'alreadyExists' => $alreadyExists
		);
	}

	public function requireDatabaseAlterPermissions($databaseConfig) {
		$success = false;
		$conn = $this->createConnection($databaseConfig, $error);
		if($conn) {
			// Check if this user has create privileges on the default tablespace
			$sqlUsername = $this->quote($conn, $databaseConfig['username']);
			$permissions = $this->query($conn, "select * from has_tablespace_privilege($sqlUsername, 'pg_default', 'create')");
			$success = $permissions && (reset($permissions) == 't');
		}
		return array(
			'success' => $success,
			'applies' => true
		);
	}
}
