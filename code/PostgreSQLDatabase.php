<?php

/**
 * @package sapphire
 * @subpackage model
 */

/**
 * PostgreSQL connector class.
 * @package sapphire
 * @subpackage model
 */
class PostgreSQLDatabase extends SS_Database {
	
	/**
	 * Connection to the DBMS.
	 * @var resource
	 */
	private $dbConn;
	
	/**
	 * True if we are connected to a database.
	 * @var boolean
	 */
	private $active;
	
	/**
	 * The name of the database.
	 * @var string
	 */
	private $database;
	
	/*
	 * This holds the name of the original database
	 * So if you switch to another for unit tests, you
	 * can then switch back in order to drop the temp database 
	 */
	private $database_original;
	
	/** 
 	* The database schema name. 
 	* @var string 
 	*/ 
 	private $schema; 
 	
	/*
	 * This holds the parameters that the original connection was created with,
	 * so we can switch back to it if necessary (used for unit tests)
	 */
	private $parameters;
	
	/*
	 * These two values describe how T-search will work.
	 * You can use either GiST or GIN, and '@@' (gist) or '@@@' (gin)
	 * Combinations of these two will also work, so you'll need to pick
	 * one which works best for you
	 */
	public $default_fts_cluster_method='GIN';
	public $default_fts_search_method='@@@';
	
	private $supportsTransactions=true;
	
	/**
	 * Determines whether to check a database exists on the host by 
	 * querying the 'postgres' database and running createDatabase.
	 * 
	 * Some locked down systems prevent access to the 'postgres' table in
	 * which case you need to set this to false.
	 */
	public static $check_database_exists = true;
	
	/**
	 * This holds a copy of all the constraint results that are returned
	 * via the function constraintExists().  This is a bit faster than 
	 * repeatedly querying this column, and should allow the database
	 * to use it's built-in caching features for better queries.
	 *  
	 * @var array
	 */
	private static $cached_constraints=array();
	
	/**
	 * Override the language that tsearch uses.  By default it is 'english, but
	 * could be any of the supported languages that can be found in the
	 * pg_catalog.pg_ts_config table.
	 * 
	 * @var string
	 */
	private $search_language='english';
	
	/**
	 * Connect to a PostgreSQL database.
	 * @param array $parameters An map of parameters, which should include:
	 *  - server: The server, eg, localhost
	 *  - username: The username to log on with
	 *  - password: The password to log on with
	 *  - database: The database to connect to
	 */
	public function __construct($parameters) {
		
		//We will store these connection parameters for use elsewhere (ie, unit tests)
		$this->parameters=$parameters;
		$this->connectDatabase();
		
		$this->database_original=$this->database;
	}
	
	/*
	 * Uses whatever connection details are in the $parameters array to connect to a database of a given name
	 */
	function connectDatabase(){
		
		$parameters=$this->parameters;
		
		if(!$parameters)
			return false;
			
		($parameters['username']!='') ? $username=' user=' . $parameters['username'] : $username='';
		($parameters['password']!='') ? $password=' password=' . $parameters['password'] : $password='';
		
		if(!isset($this->database))
			$dbName=$parameters['database'];
		else $dbName=$this->database;
		
		$port = empty($parameters['port']) ? 5432 : $parameters['port'];
		
		// First, we need to check that this database exists.  To do this, we will connect to the 'postgres' database first
		// some setups prevent access to this database so set PostgreSQLDatabase::$check_database_exists = false
		if(self::$check_database_exists) {
			$this->dbConn = pg_connect('host=' . $parameters['server'] . ' port=' . $port . ' dbname=postgres' . $username . $password);
			
			if(!$this->databaseExists($dbName))
				$this->createDatabase($dbName);
		}
			
		//Now we can be sure that this database exists, so we can connect to it
		$this->dbConn = pg_connect('host=' . $parameters['server'] . ' port=' . $port . ' dbname=' . $dbName . $username . $password);
		
		//By virtue of getting here, the connection is active:
		$this->active=true;
		$this->database = $dbName;
				
		if(!$this->dbConn) {
			$this->databaseError("Couldn't connect to PostgreSQL database");
			return false;
		}
		
		// Set up the schema if required 
 		$schema = isset($parameters['schema']) ? $parameters['schema'] : $this->currentSchema(); 
 		// Edge-case - database with no schemas: 
 		if(!$schema) $schema = "public"; 
                 
 		if(!$this->schemaExists($schema)) 
 			 $this->createSchema($schema);
 			  
 		$this->setSchema($schema); 
 	
		return true;
	}
	/**
	 * Not implemented, needed for PDO
	 */
	public function getConnect($parameters) {
		return null;
	}
	
	/**
	 * Return the parameters used to construct this database connection
	 */
	public function getParameters() {
		return $this->parameters;
	}
	
	/**
	 * Returns true if this database supports collations
	 * TODO: get rid of this?
	 * @return boolean
	 */
	public function supportsCollations() {
		return true;
	}
	
	/**
	 * Get the version of PostgreSQL.
	 * @return string
	 */
	public function getVersion() {
		$version = pg_version($this->dbConn);
		if(isset($version['server'])) return $version['server'];
		else return false;
	}
	
	/**
	 * Get the database server, namely PostgreSQL.
	 * @return string
	 */
	public function getDatabaseServer() {
		return "postgresql";
	}
	
	public function query($sql, $errorLevel = E_USER_ERROR) {
		
		if(isset($_REQUEST['previewwrite']) && in_array(strtolower(substr($sql,0,strpos($sql,' '))), array('insert','update','delete','replace'))) {
			Debug::message("Will execute: $sql");
			return;
		}

		if(isset($_REQUEST['showqueries'])) { 
			$starttime = microtime(true);
		}

		$handle = pg_query($this->dbConn, $sql);
		
		if(isset($_REQUEST['showqueries'])) {
			$endtime = round(microtime(true) - $starttime,4);
			Debug::message("\n$sql\n{$endtime}ms\n", false);
		}
		
		DB::$lastQuery=$handle;
		
		if(!$handle && $errorLevel) $this->databaseError("Couldn't run query: $sql | " . pg_last_error($this->dbConn), $errorLevel);
				
		return new PostgreSQLQuery($this, $handle);
	}
	
	public function getGeneratedID($table) {
		$result=DB::query("SELECT last_value FROM \"{$table}_ID_seq\";");
		$row=$result->first(); 
 		return $row['last_value'];
	}
	
	/**
	 * OBSOLETE: Get the ID for the next new record for the table.
	 * 
	 * @var string $table The name od the table.
	 * @return int
	 */
	public function getNextID($table) {
		user_error('getNextID is OBSOLETE (and will no longer work properly)', E_USER_WARNING);
		$result = $this->query("SELECT MAX(ID)+1 FROM \"$table\"")->value();
		return $result ? $result : 1;
	}
	
	public function isActive() {
		return $this->active ? true : false;
	}
	
	/*
	 * You can create a database based either on a supplied name, or from whatever is in the $this->database value
	 */
	public function createDatabase($name=false) {
		if(!$name)
			$name=$this->database;
			
		$this->query("CREATE DATABASE \"$name\";");
		
		$this->connectDatabase();
		
	}

	/**
	 * Drop the database that this object is currently connected to.
	 * Use with caution.
	 */
	public function dropDatabase() {
		
		//First, we need to switch back to the original database so we can drop the current one
		$db_to_drop=$this->database;
		$this->selectDatabase($this->database_original);
		$this->connectDatabase();
		
		$this->query("DROP DATABASE $db_to_drop");
	}
	
	/**
	 * Drop the database that this object is currently connected to.
	 * Use with caution.
	 */
	public function dropDatabaseByName($dbName) {
		if($dbName!=$this->database)
			$this->query("DROP DATABASE \"$dbName\";");
	}
	
	/**
	 * Returns the name of the currently selected database
	 */
	public function currentDatabase() {
		return $this->database;
	}
	
	/**
	 * Switches to the given database.
	 * If the database doesn't exist, you should call createDatabase() after calling selectDatabase()
	 */
	public function selectDatabase($dbname) {
		$this->database=$dbname;
		
		$this->tableList = $this->fieldList = $this->indexList = null;
				
		return true;
	}

	
	/**
	 * Returns true if the named database exists.
	 */
	public function databaseExists($name) {
		// We have to use addslashes here, since there may not be a database connection to base the Convert::raw2sql 
		// function off.
		$SQL_name=addslashes($name);
		return $this->query("SELECT datname FROM pg_database WHERE datname='$SQL_name';")->first() ? true : false;
	}
	
	/**
	 * Returns a column 
	 */
	public function allDatabaseNames() {
		return $this->query("SELECT datname FROM pg_database WHERE datistemplate=false;")->column();
	}
	
	/** 
 	* Returns true if the schema exists in the current database 
 	* @param string $name 
 	* @return boolean 
 	*/ 
 	public function schemaExists($name) { 
 		$SQL_name = pg_escape_string($this->dbConn, $name); 
 		return $this->query("SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname = '{$SQL_name}';")->first() ? true : false; 
 	}
 	 
 	/** 
 	* Creates a schema in the current database 
 	* @param string $name 
 	*/ 
 	public function createSchema($name) { 
 		$SQL_name = pg_escape_string($this->dbConn, $name); 
 		$this->query("CREATE SCHEMA \"{$SQL_name}\";"); 
 	} 
 	
 	/** 
 	* Drops a schema from the database. Use carefully! 
 	* @param string $name 
 	*/ 
 	public function dropSchema($name) { 
 		$SQL_name = pg_escape_string($this->dbConn, $name); 
 		$this->query("DROP SCHEMA \"{$SQL_name}\" CASCADE;"); 
 	} 
 		         
 	/** 
 	* Returns the name of the current schema in use 
 	*/ 
 	public function currentSchema() { 
 		return $this->query('SELECT current_schema()')->value(); 
 	} 
 	
 	/** 
 	* Utility method to manually set the schema to an alternative 
 	* Check existance & sets search path to the supplied schema name 
 	* @param string $schema 
 	*/ 
 	public function setSchema($schema) { 
 		if(!$this->schemaExists($schema)) 
 			$this->databaseError("Schema $schema does not exist"); 
 		$this->setSchemaSearchPath($schema); 
 	 	$this->schema = $schema; 
 	} 
 		         
 	/** 
 	* Override the schema search path. Search using the arguments supplied. 
 	* NOTE: The search path is normally set through setSchema() and only 
 	* one schema is selected. The facility to add more than one schema to  
 	* the search path is provided as an advanced PostgreSQL feature for raw  
 	* SQL queries. Sapphire cannot search for datamodel tables in alternate  
 	* schemas, so be wary of using alternate schemas within the ORM environment. 
 	* @param string $arg1 First schema to use 
 	* @param string $arg2 Second schema to use 
 	* @param string $argN Nth schema to use 
 	*/ 
 	public function setSchemaSearchPath() { 
 		if(func_num_args() == 0) 
 			$this->databaseError('At least one Schema must be supplied to set a search path.'); 
	 	$args = array_values(func_get_args()); 
	 	foreach($args as $key => $schema) 
	 		$args[$key] = '"' . pg_escape_string($this->dbConn, $schema) . '"'; 
	 	$args_SQL =implode(",", $args); 
	 	$this->query("SET search_path TO {$args_SQL}"); 
 	} 
	
	public function createTable($tableName, $fields = null, $indexes = null, $options = null, $extensions = null) {
		
		$fieldSchemas = $indexSchemas = "";
		if($fields) foreach($fields as $k => $v) $fieldSchemas .= "\"$k\" $v,\n";
		if(isset($this->class)){
			$addOptions = (isset($options[$this->class])) ? $options[$this->class] : null;
		} else $addOptions=null;
		
		//First of all, does this table already exist
		$doesExist=$this->TableExists($tableName);
		if($doesExist) {
			// Table already exists, just return the name, in line with baseclass documentation.
			return $tableName;
		}
			
		//If we have a fulltext search request, then we need to create a special column
		//for GiST searches
		$fulltexts='';
		$triggers='';
		if($indexes){
			foreach($indexes as $name=>$this_index){
				if($this_index['type']=='fulltext'){
					$ts_details=$this->fulltext($this_index, $tableName, $name);
					$fulltexts.=$ts_details['fulltexts'] . ', ';
					$triggers.=$ts_details['triggers'];
				}
			}
		}
		if($indexes) foreach($indexes as $k => $v) $indexSchemas .= $this->getIndexSqlDefinition($tableName, $k, $v) . "\n";
		
		//Do we need to create a tablespace for this item?
		if($extensions && isset($extensions['tablespace'])){
			
			$this->createOrReplaceTablespace($extensions['tablespace']['name'], $extensions['tablespace']['location']);
			$tableSpace=' TABLESPACE ' . $extensions['tablespace']['name'];
		} else 
			$tableSpace='';
		
		$this->query("CREATE TABLE \"$tableName\" (
				$fieldSchemas
				$fulltexts
				primary key (\"ID\")
			)$tableSpace; $indexSchemas $addOptions");
				
		if($triggers!=''){
			$this->query($triggers);
		}
		
		//If we have a partitioning requirement, we do that here:
		if($extensions && isset($extensions['partitions'])){
			$this->createOrReplacePartition($tableName, $extensions['partitions'], $indexes, $extensions);
		}
		
		//Lastly, clustering goes here:
		if($extensions && isset($extensions['cluster'])){
			DB::query("CLUSTER \"$tableName\" USING \"{$extensions['cluster']}\";");
		}
				
		return $tableName;
	}

	/**
	 * Alter a table's schema.
	 * @param $table The name of the table to alter
	 * @param $newFields New fields, a map of field name => field schema
	 * @param $newIndexes New indexes, a map of index name => index type
	 * @param $alteredFields Updated fields, a map of field name => field schema
	 * @param $alteredIndexes Updated indexes, a map of index name => index type
	 */
	public function alterTable($tableName, $newFields = null, $newIndexes = null, $alteredFields = null, $alteredIndexes = null, $alteredOptions = null, $advancedOptions = null) {
		
		$fieldSchemas = $indexSchemas = "";
		$alterList = array();
		if($newFields) foreach($newFields as $k => $v) $alterList[] .= "ADD \"$k\" $v";
		
		if($alteredFields) {
			foreach($alteredFields as $k => $v) {
				
				$val=$this->alterTableAlterColumn($tableName, $k, $v);
				if($val!='')
					$alterList[] .= $val;
			}
		}
		
		//Do we need to do anything with the tablespaces?
		if($alteredOptions && isset($advancedOptions['tablespace'])){
			$this->createOrReplaceTablespace($advancedOptions['tablespace']['name'], $advancedOptions['tablespace']['location']);
			$this->query("ALTER TABLE \"$tableName\" SET TABLESPACE {$advancedOptions['tablespace']['name']};");
		}
			
		//DB ABSTRACTION: we need to change the constraints to be a separate 'add' command,
		//see http://www.postgresql.org/docs/8.1/static/sql-altertable.html
		$alterIndexList=Array();
		//Pick up the altered indexes here:
		$fieldList = $this->fieldList($tableName);
		$fulltexts=false;
		$drop_triggers=false;
		$triggers=false;
		if($alteredIndexes) foreach($alteredIndexes as $key=>$v) {
			//We are only going to delete indexes which exist
			$indexes=$this->indexList($tableName);
			
			if($v['type']=='fulltext'){
				//For full text indexes, we need to drop the trigger, drop the index, AND drop the column
				
				//Go and get the tsearch details:
				$ts_details=$this->fulltext($v, $tableName, $key);
				
				//Drop this column if it already exists:
				
				//No IF EXISTS option is available for Postgres <9.0
				if(array_key_exists($ts_details['ts_name'], $fieldList)){
					$fulltexts.="ALTER TABLE \"{$tableName}\" DROP COLUMN \"{$ts_details['ts_name']}\";";
				}
			
				$drop_triggers.= 'DROP TRIGGER IF EXISTS ts_' . strtolower($tableName) . '_' . strtolower($key) . ' ON "' . $tableName . '";';
				$alterIndexList[] = 'DROP INDEX IF EXISTS ix_' . strtolower($tableName) . '_' . strtolower($v['value']) . ';';
				
				//We'll execute these later:
				$fulltexts.="ALTER TABLE \"{$tableName}\" ADD COLUMN {$ts_details['fulltexts']};";
				$triggers.=$ts_details['triggers'];
			} else {
				if(isset($indexes[$v['value']])){
					if(is_array($v))
						$alterIndexList[] = 'DROP INDEX IF EXISTS ix_' . strtolower($tableName) . '_' . strtolower($v['value']) . ';';
					else
						$alterIndexList[] = 'DROP INDEX IF EXISTS ix_' . strtolower($tableName) . '_' . strtolower(trim($v, '()')) . ';';
								
					$k=$v['value'];
					$createIndex=$this->getIndexSqlDefinition($tableName, $k, $v);
					if($createIndex!==false)
						$alterIndexList[] .= $createIndex;
				}
			}
 		}

 		//If we have a fulltext search request, then we need to create a special column
		//for GiST searches
		//Pick up the new indexes here:
		if($newIndexes){
			foreach($newIndexes as $name=>$this_index){
				if($this_index['type']=='fulltext'){
					$ts_details=$this->fulltext($this_index, $tableName, $name);
					if(!isset($fieldList[$ts_details['ts_name']])){
						$fulltexts.="ALTER TABLE \"{$tableName}\" ADD COLUMN {$ts_details['fulltexts']};";
						$triggers.=$ts_details['triggers'];
					}
				}
			}
		}
		
		//Add the new indexes:
		if($newIndexes) foreach($newIndexes as $k=>$v){
 			//Check that this index doesn't already exist:
 			$indexes=$this->indexList($tableName);
 			if(!is_array($v)){
 				$name=trim($v, '()');
 			} else {
 				$name=(isset($v['name'])) ? $v['name'] : $k;
 			}
 			if(isset($indexes[$name])){
 				if(is_array($v)){
					$alterIndexList[] = 'DROP INDEX IF EXISTS ix_' . strtolower($tableName) . '_' . strtolower($v['value']) . ';';
				} else {
					$alterIndexList[] = 'DROP INDEX IF EXISTS ' . $indexes[$name]['indexname'] . ';';
				}
			}
					
			$createIndex=$this->getIndexSqlDefinition($tableName, $k, $v);
			if($createIndex!==false)
 				$alterIndexList[] = $createIndex;
 		}
 		
 		if($alterList) {
			$alterations = implode(",\n", $alterList);
			$this->query("ALTER TABLE \"$tableName\" " . $alterations);
		}
		
		//Do we need to create a tablespace for this item?
		if($advancedOptions && isset($advancedOptions['extensions']['tablespace'])){
			$extensions=$advancedOptions['extensions'];
			$this->createOrReplaceTablespace($extensions['tablespace']['name'], $extensions['tablespace']['location']);
		}
		
		if($alteredOptions && isset($this->class) && isset($alteredOptions[$this->class])) {
			$this->query(sprintf("ALTER TABLE \"%s\" %s", $tableName, $alteredOptions[$this->class]));
			Database::alteration_message(
				sprintf("Table %s options changed: %s", $tableName, $alteredOptions[$this->class]),
				"changed"
			);
		}
		
		//Create any fulltext columns and triggers here:
		if($fulltexts)
			$this->query($fulltexts);
		if($drop_triggers)
			$this->query($drop_triggers);
			
		if($triggers) {
			$this->query($triggers);

			$triggerbits=explode(';', $triggers);
			foreach($triggerbits as $trigger){
				$trigger_fields=$this->triggerFieldsFromTrigger($trigger);
				
				if($trigger_fields){
					//We need to run a simple query to force the database to update the triggered columns
					$this->query("UPDATE \"{$tableName}\" SET \"{$trigger_fields[0]}\"=\"$trigger_fields[0]\";");
				}
			}
		}
				
		foreach($alterIndexList as $alteration)
			$this->query($alteration);
			
		//If we have a partitioning requirement, we do that here:
		if($advancedOptions && isset($advancedOptions['partitions'])){
			$this->createOrReplacePartition($tableName, $advancedOptions['partitions']);
		}

		//Lastly, clustering goes here:
		if($advancedOptions && isset($advancedOptions['cluster'])){
			DB::query("CLUSTER \"$tableName\" USING ix_{$tableName}_{$advancedOptions['cluster']};"); 
		} else {
			//Check that clustering is not on this table, and if it is, remove it:
			
			//This is really annoying.  We need the oid of this table:
			$stats=DB::query("SELECT relid FROM pg_stat_user_tables WHERE relname='$tableName';")->first();
			$oid=$stats['relid'];
			
			//Now we can run a long query to get the clustered status:
			//If anyone knows a better way to get the clustered status, then feel free to replace this!
			$clustered=DB::query("SELECT c2.relname, i.indisclustered FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i WHERE c.oid = '$oid' AND c.oid = i.indrelid AND i.indexrelid = c2.oid AND indisclustered='t';")->first();
			
			if($clustered)
				DB::query("ALTER TABLE \"$tableName\" SET WITHOUT CLUSTER;");
					
		}
	}
	
	/*
	 * Creates an ALTER expression for a column in PostgreSQL
	 * 
	 * @param $tableName Name of the table to be altered
	 * @param $colName   Name of the column to be altered
	 * @param $colSpec   String which contains conditions for a column
	 * @return string
	 */
	private function alterTableAlterColumn($tableName, $colName, $colSpec){
		// First, we split the column specifications into parts
		// TODO: this returns an empty array for the following string: int(11) not null auto_increment
		//		 on second thoughts, why is an auto_increment field being passed through?
		$pattern = '/^([\w()]+)\s?((?:not\s)?null)?\s?(default\s[\w\']+)?\s?(check\s[\w()\'",\s]+)?$/i';
		preg_match($pattern, $colSpec, $matches);
		
		if(sizeof($matches)==0)
			return '';
			
		if($matches[1]=='serial8')
			return '';
			
		if(isset($matches[1])) {
			$alterCol = "ALTER COLUMN \"$colName\" TYPE $matches[1]\n";
		
			// SET null / not null
			if(!empty($matches[2])) $alterCol .= ",\nALTER COLUMN \"$colName\" SET $matches[2]";
			
			// SET default (we drop it first, for reasons of precaution)
			if(!empty($matches[3])) {
				$alterCol .= ",\nALTER COLUMN \"$colName\" DROP DEFAULT";
				$alterCol .= ",\nALTER COLUMN \"$colName\" SET $matches[3]";
			}
			
			// SET check constraint (The constraint HAS to be dropped)
			$existing_constraint=$this->query("SELECT conname FROM pg_constraint WHERE conname='{$tableName}_{$colName}_check';")->value();
			//Take this new constraint and see what's outstanding from the target table:
			$constraint_bits=explode('(', $matches[4]);
			$constraint_values=trim($constraint_bits[2], ')');
			$constraint_values_bits=explode(',', $constraint_values);
			$default=trim($constraint_values_bits[0], " '");
			
			//Now go and convert anything that's not in this list to 'Page'
			//We have to run this as a query, not as part of the alteration queries due to the way they are constructed.
			$updateConstraint='';
			$updateConstraint.="UPDATE \"{$tableName}\" SET \"$colName\"='$default' WHERE \"$colName\" NOT IN ($constraint_values);";
			if($this->hasTable("{$tableName}_Live"))
				$updateConstraint.="UPDATE \"{$tableName}_Live\" SET \"$colName\"='$default' WHERE \"$colName\" NOT IN ($constraint_values);";
			if($this->hasTable("{$tableName}_versions"))
				$updateConstraint.="UPDATE \"{$tableName}_versions\" SET \"$colName\"='$default' WHERE \"$colName\" NOT IN ($constraint_values);";
			
			DB::query($updateConstraint);
			 
			//First, delete any existing constraint on this column, even if it's no longer an enum
			if($existing_constraint)
				$alterCol .= ",\nDROP CONSTRAINT \"{$tableName}_{$colName}_check\"";
			
			//Now create the constraint (if we've asked for one)
			if(!empty($matches[4]))
				$alterCol .= ",\nADD CONSTRAINT \"{$tableName}_{$colName}_check\" $matches[4]";
		}
		
		return isset($alterCol) ? $alterCol : '';
	}
	
	public function renameTable($oldTableName, $newTableName) {
		$this->query("ALTER TABLE \"$oldTableName\" RENAME TO \"$newTableName\"");
	}
	
	/**
	 * Repairs and reindexes the table.  This might take a long time on a very large table.
	 * @var string $tableName The name of the table.
	 * @return boolean Return true if the table has integrity after the method is complete.
	 */
	public function checkAndRepairTable($tableName) {
		$this->runTableCheckCommand("VACUUM FULL ANALYZE \"$tableName\"");
		$this->runTableCheckCommand("REINDEX TABLE \"$tableName\"");
		return true;
	}
	
	/**
	 * Helper function used by checkAndRepairTable.
	 * @param string $sql Query to run.
	 * @return boolean Returns true no matter what; we're not currently checking the status of the command
	 */
	protected function runTableCheckCommand($sql) {
		$testResults = $this->query($sql);
		return true;
	}
	
	public function createField($tableName, $fieldName, $fieldSpec) {
		$this->query("ALTER TABLE \"$tableName\" ADD \"$fieldName\" $fieldSpec");
	}
	
	/**
	 * Change the database type of the given field.
	 * @param string $tableName The name of the tbale the field is in.
	 * @param string $fieldName The name of the field to change.
	 * @param string $fieldSpec The new field specification
	 */
	public function alterField($tableName, $fieldName, $fieldSpec) {
		$this->query("ALTER TABLE \"$tableName\" CHANGE \"$fieldName\" \"$fieldName\" $fieldSpec");
	}

	/**
	 * Change the database column name of the given field.
	 * 
	 * @param string $tableName The name of the tbale the field is in.
	 * @param string $oldName The name of the field to change.
	 * @param string $newName The new name of the field
	 */
	public function renameField($tableName, $oldName, $newName) {
		$fieldList = $this->fieldList($tableName);
		if(array_key_exists($oldName, $fieldList)) {
			$this->query("ALTER TABLE \"$tableName\" RENAME COLUMN \"$oldName\" TO \"$newName\"");
		}
	}
	
	public function fieldList($table) {
		//Query from http://www.alberton.info/postgresql_meta_info.html
		//This gets us more information than we need, but I've included it all for the moment....
		$fields = $this->query("SELECT ordinal_position, column_name, data_type, column_default, is_nullable, character_maximum_length, numeric_precision, numeric_scale FROM information_schema.columns WHERE table_name = '$table' ORDER BY ordinal_position;");
		
		$output = array();
		if($fields) foreach($fields as $field) {
			
			switch($field['data_type']){
				case 'character varying':
					//Check to see if there's a constraint attached to this column:
					//$constraint=$this->query("SELECT conname,pg_catalog.pg_get_constraintdef(r.oid, true) FROM pg_catalog.pg_constraint r WHERE r.contype = 'c' AND conname='" . $table . '_' . $field['column_name'] . "_check' ORDER BY 1;")->first();
					$constraint=$this->constraintExists($table . '_' . $field['column_name'] . '_check');
					$enum='';
					if($constraint){
						//Now we need to break this constraint text into bits so we can see what we have:
						//Examples:
						//CHECK ("CanEditType"::text = ANY (ARRAY['LoggedInUsers'::character varying, 'OnlyTheseUsers'::character varying, 'Inherit'::character varying]::text[]))
						//CHECK ("ClassName"::text = 'PageComment'::text)
						
						//TODO: replace all this with a regular expression!
						$value=$constraint['pg_get_constraintdef'];
						$value=substr($value, strpos($value,'='));
						$value=str_replace("''", "'", $value);
						
						$in_value=false;
						$constraints=Array();
						$current_value='';
						for($i=0; $i<strlen($value); $i++){
							$char=substr($value, $i, 1);
							if($in_value)
								$current_value.=$char;
							
							if($char=="'"){
								if(!$in_value)
									$in_value=true;
								else {
									$in_value=false;
									$constraints[]=substr($current_value, 0, -1);
									$current_value='';
								}
							}
						}
						
						if(sizeof($constraints)>0){
							//Get the default:
							$default=trim(substr($field['column_default'], 0, strpos($field['column_default'], '::')), "'");
							$output[$field['column_name']]=$this->enum(Array('default'=>$default, 'name'=>$field['column_name'], 'enums'=>$constraints));
						}
					} else{
						$output[$field['column_name']]='varchar(' . $field['character_maximum_length'] . ')';
					}
					break;
					
				case 'numeric':
					$output[$field['column_name']]='decimal(' . $field['numeric_precision'] . ',' . $field['numeric_scale'] . ') default ' . (int)$field['column_default'];
					break;
					
				case 'integer':
					$output[$field['column_name']]='integer default ' . (int)$field['column_default'];
					break;
					
				case 'timestamp without time zone':
					$output[$field['column_name']]='timestamp';
					break;
					
				case 'smallint':
					$output[$field['column_name']]='smallint default ' . (int)$field['column_default'];
					break;
					
				case 'time without time zone':
					$output[$field['column_name']]='time';
					break;
					
				case 'double precision':
					$output[$field['column_name']]='float';
					break;
					
				default:
					$output[$field['column_name']] = $field;
			}
			
		}
		
		return $output;
	}
	
	/**
	 * Create an index on a table.
	 * @param string $tableName The name of the table.
	 * @param string $indexName The name of the index.
	 * @param string $indexSpec The specification of the index, see Database::requireIndex() for more details.
	 */
	public function createIndex($tableName, $indexName, $indexSpec) {
		$createIndex=$this->getIndexSqlDefinition($tableName, $indexName, $indexSpec);
		if($createIndex!==false)
			$this->query();
	}
	
	/*
	 * This takes the index spec which has been provided by a class (ie static $indexes = blah blah)
	 * and turns it into a proper string.
	 * Some indexes may be arrays, such as fulltext and unique indexes, and this allows database-specific
	 * arrays to be created.
	 */
	public function convertIndexSpec($indexSpec, $asDbValue=false, $table=''){
		
		if(!$asDbValue){
			if(is_array($indexSpec)){
				//Here we create a db-specific version of whatever index we need to create.
				switch($indexSpec['type']){
					case 'fulltext':
						//We need to include the fields so if we change the columns it's indexing, but not the name,
						//then the change will be picked up.
						$indexSpec='(' . $indexSpec['name'] . ',' . $indexSpec['value'] . ')';
						break;
					case 'unique':
						$indexSpec='unique (' . $indexSpec['value'] . ')';
						break;
					case 'hash':
						$indexSpec='using hash (' . $indexSpec['value'] . ')';
						break;
					case 'index':
						//The default index is 'btree', which we'll use by default (below):
					default:
						$indexSpec='using btree (' . $indexSpec['value'] . ')';
						break;
				}
			}
		} else {
			$indexSpec='ix_' . $table . '_' . $indexSpec;
		}
		return $indexSpec;
	}
	
	protected function getIndexSqlDefinition($tableName, $indexName, $indexSpec, $asDbValue=false) {
	    
		//TODO: create table partition support
		//TODO: create clustering options
		
		//NOTE: it is possible for *_renamed tables to have indexes whose names are not updates
		//Therefore, we now check for the existance of indexes before we create them.
		//This is techically a bug, since new tables will not be indexed.
		
		if(!$asDbValue){
			
			$tableCol= 'ix_' . $tableName . '_' . $indexName;
			if(strlen($tableCol)>64){
				$tableCol=substr($indexName, 0, 59) . rand(1000, 9999);
			}
			
			//It is possible to specify indexes through strings: 
			if(!is_array($indexSpec)){
				$indexSpec=trim($indexSpec, '()');
				$bits=explode(',', $indexSpec);
				$indexes="\"" . implode("\",\"", $bits) . "\"";

				//One last check:
				$existing=DB::query("SELECT tablename FROM pg_indexes WHERE indexname='" . strtolower($tableCol) . "';")->first();
				if(!$existing)
					return "create index $tableCol ON \"" . $tableName . "\" (" . $indexes . ");";
				else
					return false;
			} else {
				
				//Arrays offer much more flexibility and many more options:
				
				//Misc options first:
				$fillfactor=$where='';
				if(isset($indexSpec['fillfactor']))
					$fillfactor='WITH (FILLFACTOR = ' . $indexSpec['fillfactor'] . ')';
				if(isset($indexSpec['where']))
					$where='WHERE ' . $indexSpec['where'];
				
				//Fix up the value entry to be quoted:
				$value_bits=explode(',', $indexSpec['value']);
				$new_values=Array();
				foreach($value_bits as $value){
					$new_values[]="\"" . trim($value, ' "') . "\"";
				}
				$indexSpec['value']=implode(',', $new_values);
					
				//One last check:
				$existing=DB::query("SELECT tablename FROM pg_indexes WHERE indexname='" . strtolower($tableCol) . "';");
				if(!$existing->first()){
					//create a type-specific index
					//NOTE:  hash should be removed.  This is only here to demonstrate how other indexes can be made		
					switch($indexSpec['type']){
						case 'fulltext':
							$spec="create index $tableCol ON \"" . $tableName . "\" USING " . $this->default_fts_cluster_method . "(\"ts_" . $indexName . "\") $fillfactor $where";
							break;
							
						case 'unique':
							$spec="create unique index $tableCol ON \"" . $tableName . "\" (" . $indexSpec['value'] . ") $fillfactor $where";
							break;
							
						case 'btree':
							$spec="create index $tableCol ON \"" . $tableName . "\" USING btree (" . $indexSpec['value'] . ") $fillfactor $where";
							break;
		
						case 'hash':
							//NOTE: this is not a recommended index type
							$spec="create index $tableCol ON \"" . $tableName . "\" USING hash (" . $indexSpec['value'] . ") $fillfactor $where";
							break;
							
						case 'index':
							//'index' is the same as default, just a normal index with the default type decided by the database.
						default:
							$spec="create index $tableCol ON \"" . $tableName . "\" (" . $indexSpec['value'] . ") $fillfactor $where";
					}
					
					return trim($spec) . ';';
				} else {
					
					return false;
				}
			}
		} else {
			$indexName=trim($indexName, '()');
			return $indexName;
		}
	}
	
	function getDbSqlDefinition($tableName, $indexName, $indexSpec){
		return $this->getIndexSqlDefinition($tableName, $indexName, $indexSpec, true);
	}
	
	/**
	 * Alter an index on a table.
	 * @param string $tableName The name of the table.
	 * @param string $indexName The name of the index.
	 * @param string $indexSpec The specification of the index, see Database::requireIndex() for more details.
	 */
	public function alterIndex($tableName, $indexName, $indexSpec) {
	    $indexSpec = trim($indexSpec);
	    if($indexSpec[0] != '(') {
	    	list($indexType, $indexFields) = explode(' ',$indexSpec,2);
	    } else {
	    	$indexFields = $indexSpec;
	    }
	    
	    if(!$indexType) {
	    	$indexType = "index";
	    }
    
		$this->query("DROP INDEX $indexName");
		$this->query("ALTER TABLE \"$tableName\" ADD $indexType \"$indexName\" $indexFields");
	}
	
	/**
	 * Return the list of indexes in a table.
	 * @param string $table The table name.
	 * @return array
	 */
	public function indexList($table) {
	
  		//Retrieve a list of indexes for the specified table
		$schema_SQL = pg_escape_string($this->dbConn, $this->schema); 
 		$indexes=DB::query("SELECT tablename, indexname, indexdef FROM pg_catalog.pg_indexes WHERE tablename='$table' AND schemaname = '{$schema_SQL}';");
 		 
		$indexList=Array();
  		foreach($indexes as $index) {
  			//We don't actually need the entire created command, just a few bits:
  			$prefix='';
  			
  			//Check for uniques:
  			if(substr($index['indexdef'], 0, 13)=='CREATE UNIQUE')
  				$prefix='unique ';
  				
  			//check for hashes, btrees etc:
  			if(strpos(strtolower($index['indexdef']), 'using hash ')!==false)
  				$prefix='using hash ';

  			//TODO: Fix me: btree is the default index type:
  			//if(strpos(strtolower($index['indexdef']), 'using btree ')!==false)
  			//	$prefix='using btree ';
  				
  			if(strpos(strtolower($index['indexdef']), 'using rtree ')!==false)
  				$prefix='using rtree ';

  			$value=explode(' ', substr($index['indexdef'], strpos($index['indexdef'], ' USING ')+7));
  			
  			if(sizeof($value)>2){
	  			for($i=2; $i<sizeof($value); $i++)
	  				$value[1].=$value[$i];
  			}
  			
  			$key=substr($value[1], 0, strpos($value[1], ')'));
  			$key=trim(trim(str_replace("\"", '', $key), '()'));
  			$indexList[$key]['indexname']=$index['indexname'];
  			$indexList[$key]['spec']=$prefix . '(' . $key . ')';
  			
  		}

  		return isset($indexList) ? $indexList : null;
		
	}

	/**
	 * Returns a list of all the tables in the database.
	 * Table names will all be in lowercase.
	 * @return array
	 */
	public function tableList() {
		$schema_SQL = pg_escape_string($this->dbConn, $this->schema); 
 		foreach($this->query("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = '{$schema_SQL}' AND tablename NOT ILIKE 'pg_%' AND tablename NOT ILIKE 'sql_%'") as $record) {
			//$table = strtolower(reset($record));
			$table = reset($record);
			$tables[$table] = $table;
		}
		
		//Return an empty array if there's nothing in this database
		return isset($tables) ? $tables : Array();
	}
	
	function TableExists($tableName){
		$schema_SQL = pg_escape_string($this->dbConn, $this->schema); 
 		$result=$this->query("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = '{$schema_SQL}' AND  tablename='$tableName';")->first(); 
		
		if($result)
			return true;
		else
			return false;
		
	}
	
	/**
	 * Find out what the constraint information is, given a constraint name.
	 * We also cache this result, so the next time we don't need to do a
	 * query all over again.
	 * 
	 * @param string $constraint
	 */
	function constraintExists($constraint){
		if(!isset(self::$cached_constraints[$constraint])){
			$exists=DB::query("SELECT conname,pg_catalog.pg_get_constraintdef(r.oid, true) FROM pg_catalog.pg_constraint r WHERE r.contype = 'c' AND conname='$constraint' ORDER BY 1;")->first();
			self::$cached_constraints[$constraint]=$exists;
		}
		
		return self::$cached_constraints[$constraint];
	}
	
	/**
	 * Return the number of rows affected by the previous operation.
	 * @return int
	 */
	public function affectedRows() {
		return pg_affected_rows(DB::$lastQuery);
	}
	
	/**
	 * A function to return the field names and datatypes for the particular table
	 */
	public function tableDetails($tableName){
		$schema_SQL = pg_escape_string($this->dbConn, $this->schema); 
 		$query="SELECT a.attname as \"Column\", pg_catalog.format_type(a.atttypid, a.atttypmod) as \"Datatype\" FROM pg_catalog.pg_attribute a WHERE a.attnum > 0 AND NOT a.attisdropped AND a.attrelid = ( SELECT c.oid FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname ~ '^($tableName)$' AND pg_catalog.pg_table_is_visible(c.oid) AND n.nspname = '{$schema_SQL}');";
 		 
		$result=DB::query($query);
		
		$table=Array();
		while($row=pg_fetch_assoc($result)){
			$table[]=Array('Column'=>$row['Column'], 'DataType'=>$row['DataType']);
		}
		
		return $table;
	}
	
	/**
	 * Pass a legit trigger name and it will be dropped
	 * This assumes that the trigger has been named in a unique fashion
	 */
	function dropTrigger($triggerName, $tableName){
		$exists=DB::query("SELECT tgname FROM pg_trigger WHERE tgname='$triggerName';")->first();
		if($exists){
			DB::query("DROP trigger $triggerName ON \"$tableName\";");
		}
	}
	
	/**
	 * This will return the fields that the trigger is monitoring
	 * @param string $trigger
	 * 
	 * @return array
	 */
	function triggerFieldsFromTrigger($trigger){
		
		if($trigger){
			$tsvector='tsvector_update_trigger';
			$ts_pos=strpos($trigger, $tsvector);
			$details=trim(substr($trigger, $ts_pos+strlen($tsvector)), '();');
			//Now split this into bits:
			$bits=explode(',', $details);
			
			$fields=$bits[2];
			
			$field_bits=explode(',', str_replace('"', '', $fields));
			$result=array();
			foreach($field_bits as $field_bit)
				$result[]=trim($field_bit);
			
			return $result;
		} else
			return false;
	}
	
	/**
	 * Return a boolean type-formatted string
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function boolean($values, $asDbValue=false){
		//Annoyingly, we need to do a good ol' fashioned switch here:
		($values['default']) ? $default='1' : $default='0';
		
		if(!isset($values['arrayValue']))
			$values['arrayValue']='';
			
		if($asDbValue)
			return Array('data_type'=>'smallint');
		else {
			if($values['arrayValue']!='')
				$default='';
			else
				$default=' default ' . (int)$values['default'];
				
			return "smallint{$values['arrayValue']}" . $default;
			
		}
	}
	
	/**
	 * Return a date type-formatted string
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function date($values){
		
		if(!isset($values['arrayValue']))
			$values['arrayValue']='';
			
		return "date{$values['arrayValue']}";
	}
	
	/**
	 * Return a decimal type-formatted string
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function decimal($values, $asDbValue=false){
		
		if(!isset($values['arrayValue']))
			$values['arrayValue']='';
			
		// Avoid empty strings being put in the db
		if($values['precision'] == '') {
			$precision = 1;
		} else {
			$precision = $values['precision'];
		}
		
		$defaultValue = '';
		if(isset($values['default']) && is_numeric($values['default'])) {
			$defaultValue = ' default ' . $values['default'];
		}
		
		if($asDbValue)
			return Array('data_type'=>'numeric', 'precision'=>$precision);
		else return "decimal($precision){$values['arrayValue']}$defaultValue";
	}
	
	/**
	 * Return a enum type-formatted string
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function enum($values){
		//Enums are a bit different. We'll be creating a varchar(255) with a constraint of all the usual enum options.
		//NOTE: In this one instance, we are including the table name in the values array
		if(!isset($values['arrayValue']))
			$values['arrayValue']='';
			
		if($values['arrayValue']!='')
			$default='';
		else
			$default=" default '{$values['default']}'";
			
		return "varchar(255){$values['arrayValue']}" . $default . " check (\"" . $values['name'] . "\" in ('" . implode('\', \'', $values['enums']) . "'))";
		
	}
	
	/**
	 * Return a float type-formatted string
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function float($values, $asDbValue=false){
		if(!isset($values['arrayValue']))
			$values['arrayValue']='';
			
		if($asDbValue)
			return Array('data_type'=>'double precision');
		else return "float{$values['arrayValue']}";
	}
	
	/**
	 * Return a float type-formatted string cause double is not supported
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function double($values, $asDbValue=false){
		return $this->float($values, $asDbValue);
	}
	
	/**
	 * Return a int type-formatted string
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function int($values, $asDbValue=false){
		
		if(!isset($values['arrayValue']))
			$values['arrayValue']='';
			
		if($asDbValue)
			return Array('data_type'=>'integer', 'precision'=>'32');
		else {
			if($values['arrayValue']!='')
				$default='';
			else
				$default=' default ' . (int)$values['default'];
		
			return "integer{$values['arrayValue']}" . $default;
		}
	}
	
	/**
	 * Return a datetime type-formatted string
	 * For PostgreSQL, we simply return the word 'timestamp', no other parameters are necessary
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function SS_Datetime($values, $asDbValue=false){
		
		if(!isset($values['arrayValue']))
			$values['arrayValue']='';
			
		if($asDbValue)
			return Array('data_type'=>'timestamp without time zone');
		else
			return "timestamp{$values['arrayValue']}";
	}
	
	/**
	 * Return a text type-formatted string
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function text($values, $asDbValue=false){
		
		if(!isset($values['arrayValue']))
			$values['arrayValue']='';
		
		if($asDbValue)
			return Array('data_type'=>'text');
		else
			return "text{$values['arrayValue']}";
	}
	
	/**
	 * Return a time type-formatted string
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function time($values){
		if(!isset($values['arrayValue']))
			$values['arrayValue']='';
			
		return "time{$values['arrayValue']}";
	}
	
	/**
	 * Return a varchar type-formatted string
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function varchar($values, $asDbValue=false){
		
		if(!isset($values['arrayValue']))
			$values['arrayValue']='';
			
		if(!isset($values['precision']))
			$values['precision']=255;
			
		if($asDbValue)
			return Array('data_type'=>'varchar', 'precision'=>$values['precision']);
		else
			return "varchar({$values['precision']}){$values['arrayValue']}";
	}
	
	/*
	 * Return a 4 digit numeric type.  MySQL has a proprietary 'Year' type.
	 * For Postgres, we'll use a 4 digit numeric
	 */
	public function year($values, $asDbValue=false){
		
		if(!isset($values['arrayValue']))
			$values['arrayValue']='';
			
		//TODO: the DbValue result does not include the numeric_scale option (ie, the ,0 value in 4,0)
		if($asDbValue)
			return Array('data_type'=>'decimal', 'precision'=>'4', 'default'=> (int)$values['default']);
		else return "decimal(4,0){$values['arrayValue']}"; 
	}
	
	function escape_character($escape=false){
		if($escape)
			return "\\\"";
		else
			return "\"";
	}
	
	/**
	 * Create a fulltext search datatype for PostgreSQL
	 * This will also return a trigger to be applied to this table
	 * 
	 * @todo: create custom functions to allow weighted searches
	 *
	 * @param array $spec
	 */
	function fulltext($this_index, $tableName, $name){
		//For full text search, we need to create a column for the index
		$columns=explode(',', $this_index['value']);
		for($i=0; $i<sizeof($columns);$i++)
			$columns[$i]="\"" . trim($columns[$i]) . "\"";

		$columns=implode(', ', $columns);
		
		$fulltexts="\"ts_$name\" tsvector";
		$triggerName="ts_{$tableName}_{$name}";
		$language=$this->get_search_language();
		
		$this->dropTrigger($triggerName, $tableName);
		$triggers="CREATE TRIGGER $triggerName BEFORE INSERT OR UPDATE
					ON \"$tableName\" FOR EACH ROW EXECUTE PROCEDURE
					tsvector_update_trigger(\"ts_$name\", 'pg_catalog.$language', $columns);";
		
		return Array('name'=>$name, 'ts_name'=>"ts_{$name}", 'fulltexts'=>$fulltexts, 'triggers'=>$triggers);
	}
	
	/**
	 * This returns the column which is the primary key for each table
	 * In Postgres, it is a SERIAL8, which is the equivalent of an auto_increment
	 *
	 * @return string
	 */
	function IdColumn($asDbValue=false){
		if($asDbValue)
			return 'bigint';
		else return 'serial8 not null';
	}
	
	/**
	 * Returns true if this table exists
	 */
	function hasTable($tableName) {
		$schema_SQL = pg_escape_string($this->dbConn, $this->schema); 
 		$result = $this->query("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = '{$schema_SQL}' AND tablename = '$tableName'"); 
 	
		if ($result->numRecords() > 0) return true;
		else return false;
	}
	
	/**
	 * Returns the SQL command to get all the tables in this database
	 */
	function allTablesSQL(){
		$schema_SQL = pg_escape_string($this->dbConn, $this->schema); 
 		return "SELECT table_name FROM information_schema.tables WHERE table_schema='{$schema_SQL}' AND table_type='BASE TABLE';"; 
	}
	
	/**
	 * Return enum values for the given field
	 * @todo Make a proper implementation
	 */
	function enumValuesForField($tableName, $fieldName) {
		//return array('SiteTree','Page');
		$constraints=$this->constraintExists("{$tableName}_{$fieldName}_check");
		$classes=Array();
		if($constraints)
			$classes=$this->EnumValuesFromConstraint($constraints['pg_get_constraintdef']);
		
		return $classes;
	}

	/**
	 * Get the actual enum fields from the constraint value:
	 */
	private function EnumValuesFromConstraint($constraint){
		$constraint=substr($constraint, strpos($constraint, 'ANY (ARRAY[')+11);
		$constraint=substr($constraint, 0, -11);
		$constraints=Array();
		$segments=explode(',', $constraint);
		foreach($segments as $this_segment){
			$bits=preg_split('/ *:: */', $this_segment);
			array_unshift($constraints, trim($bits[0], " '"));
		}
		
		return $constraints;
	}
	
	/**
	 * Because NOW() doesn't always work...
	 * MSSQL, I'm looking at you
	 *
	 */
	function now(){
		return 'NOW()';
	}
	
	/*
	 * Returns the database-specific version of the random() function
	 */
	function random(){
		return 'RANDOM()';
	}
	
	/*
	 * This is a lookup table for data types.
	 * For instance, Postgres uses 'INT', while MySQL uses 'UNSIGNED'
	 * So this is a DB-specific list of equivilents.
	 */
	function dbDataType($type){
		$values=Array(
			'unsigned integer'=>'INT'
		);
		
		if(isset($values[$type]))
			return $values[$type];
		else return '';
	}
	
	/**
	 * Convert a SQLQuery object into a SQL statement
	 * @todo There is a lot of duplication between this and MySQLDatabase::sqlQueryToString().  Perhaps they could both call a common
	 * helper function in Database?
	 */
	public function sqlQueryToString(SQLQuery $sqlQuery) {
		if (!$sqlQuery->from) return '';
		$distinct = $sqlQuery->distinct ? "DISTINCT " : "";
		if($sqlQuery->delete) {
			$text = "DELETE ";
		} else if($sqlQuery->select) {
			$text = "SELECT $distinct" . implode(", ", $sqlQuery->select);
		}
		$text .= " FROM " . implode(" ", $sqlQuery->from);

		if($sqlQuery->where) $text .= " WHERE (" . $sqlQuery->getFilter(). ")";
		if($sqlQuery->groupby) $text .= " GROUP BY " . implode(", ", $sqlQuery->groupby);
		if($sqlQuery->having) $text .= " HAVING ( " . implode(" ) AND ( ", $sqlQuery->having) . " )";
		if($sqlQuery->orderby) $text .= " ORDER BY " . $this->orderMoreSpecifically($sqlQuery->select,$sqlQuery->orderby);

		if($sqlQuery->limit) {
			$limit = $sqlQuery->limit;
			
			// Pass limit as array or SQL string value
			if(is_array($limit)) {
				
				if(isset($limit['start']) && $limit['start']!='')
					$text.=' OFFSET ' . $limit['start'];	
				if(isset($limit['limit']) && $limit['limit']!='')
					$text.=' LIMIT ' . $limit['limit'];
				
			} else {
				if(strpos($sqlQuery->limit, ',')){
					$limit=str_replace(',', ' LIMIT ',  $sqlQuery->limit);
					$text .= ' OFFSET ' . $limit;
				} else {
					$text.=' LIMIT ' . $sqlQuery->limit;
				}
			}
		}
		
		return $text;
	}
	
	protected function orderMoreSpecifically($select,$order) {
		
		$altered = false;

		// split expression into order terms
		$terms = explode(',', $order);

		foreach($terms as $i => $term) {
			$term = trim($term);

			// check if table is unspecified
			if(!preg_match('/\./', $term)) {
				$direction = '';
				if(preg_match('/( ASC)$|( DESC)$/i',$term)) list($term,$direction) = explode(' ', $term);

				// find a match in the SELECT array and replace
				foreach($select as $s) {
					if(preg_match('/"[a-z0-9_]+"\.[\'"]' . $term . '[\'"]/i', trim($s))) {
						$terms[$i] = $s . ' ' . $direction;
						$altered = true;
						break;
					}
				}
			}
		}

		return implode(',', $terms);
	}

	/*
	 * This will return text which has been escaped in a database-friendly manner
	 * Using PHP's addslashes method won't work in MSSQL
	 */
	function addslashes($value){
		return pg_escape_string($value);
	}
	
	/*
	 * This changes the index name depending on database requirements.
	 */
	function modifyIndex($index, $spec){
		
		if(is_array($spec) && $spec['type']=='fulltext')
			return 'ts_' . str_replace(',', '_', $index);
		else
			return str_replace('_', ',', $index);

	}
	
	/**
	 * The core search engine configuration.
	 * @todo Properly extract the search functions out of the core.
	 * 
	 * @param string $keywords Keywords as a space separated string
	 * @return object DataObjectSet of result pages
	 */
	public function searchEngine($classesToSearch, $keywords, $start, $pageLength, $sortBy = "ts_rank DESC", $extraFilter = "", $booleanSearch = false, $alternativeFileFilter = "", $invertedMatch = false) {
		
		//Fix the keywords to be ts_query compatitble:
		//Spaces must have pipes
		//@TODO: properly handle boolean operators here.
		$keywords=trim($keywords);
		$keywords=str_replace(' ', ' | ', $keywords);
		$keywords=str_replace('"', "'", $keywords);
		
		$keywords = Convert::raw2sql(trim($keywords));
		$htmlEntityKeywords = htmlentities($keywords, ENT_NOQUOTES);
		
		//We can get a list of all the tsvector columns though this query:
		//We know what tables to search in based on the $classesToSearch variable:
		$result=DB::query("SELECT table_name, column_name, data_type FROM information_schema.columns WHERE data_type='tsvector' AND table_name in ('" . implode("', '", $classesToSearch) . "');");
		if (!$result->numRecords()) throw new Exception('there are no full text columns to search');
		
		$tables=Array();
		
		// Make column selection lists
		$select = array(
			'SiteTree' => array("\"ClassName\"","\"SiteTree\".\"ID\"","\"ParentID\"","\"Title\"","\"URLSegment\"","\"Content\"","\"LastEdited\"","\"Created\"","NULL AS \"Filename\"", "NULL AS \"Name\"", "\"CanViewType\""),
			'File' => array("\"ClassName\"","\"File\".\"ID\"","NULL AS \"ParentID\"","\"Title\"","NULL AS \"URLSegment\"","\"Content\"","\"LastEdited\"","\"Created\"","\"Filename\"","\"Name\"", "NULL AS \"CanViewType\""),
		);
		
		foreach($result as $row){
			if($row['table_name']=='SiteTree')
				$showInSearch="AND \"ShowInSearch\"=1 ";
			else
				$showInSearch='';
				
			//public function extendedSQL($filter = "", $sort = "", $limit = "", $join = "", $having = ""){
			$query=singleton($row['table_name'])->extendedSql("\"" . $row['table_name'] . "\".\"" . $row['column_name'] . "\" " .  $this->default_fts_search_method . ' q '  . $showInSearch, '');
			
			$query->select=$select[$row['table_name']];
			$query->from['tsearch']=", to_tsquery('" . $this->get_search_language() . "', '$keywords') AS q";
			
			$query->select[]="ts_rank(\"{$row['table_name']}\".\"{$row['column_name']}\", q) AS \"Relevance\"";
			
			$query->orderby=null;
			
			//Add this query to the collection
			$tables[] = $query->sql();
		}
		
		$doSet=new DataObjectSet();
		
		$limit=$pageLength;
		$offset=$start;
		
		if($keywords)
			$orderBy=" ORDER BY $sortBy";
		else $orderBy='';
		
		$fullQuery = "SELECT * FROM (" . implode(" UNION ", $tables) . ") AS q1 $orderBy LIMIT $limit OFFSET $offset";
		
		// Get records
		$records = DB::query($fullQuery);
		$totalCount=0;
		foreach($records as $record){
			$objects[] = new $record['ClassName']($record);
			$totalCount++;
		}
		if(isset($objects)) $doSet = new DataObjectSet($objects);
		else $doSet = new DataObjectSet();
		
		$doSet->setPageLimits($start, $pageLength, $totalCount);
		return $doSet;
		
		
	}
	
	/*
	 * Does this database support transactions?
	 */
	public function supportsTransactions(){
		return $this->supportsTransactions;
	}
	
	/*
	 * This is a quick lookup to discover if the database supports particular extensions
	 */
	public function supportsExtensions($extensions=Array('partitions', 'tablespaces', 'clustering')){
		if(isset($extensions['partitions']))
			return true;
		elseif(isset($extensions['tablespaces']))
			return true;
		elseif(isset($extensions['clustering']))
			return true;
		else
			return false;
	}
	
	/*
	 * Start a prepared transaction
	 * See http://developer.postgresql.org/pgdocs/postgres/sql-set-transaction.html for details on transaction isolation options
	 */
	public function startTransaction($transaction_mode=false, $session_characteristics=false){
		DB::query('BEGIN;');

		if($transaction_mode)
			DB::query('SET TRANSACTION ' . $transaction_mode . ';');
		
		if($session_characteristics)
			DB::query('SET SESSION CHARACTERISTICS AS TRANSACTION ' . $session_characteristics . ';');
	}
	
	/*
	 * Create a savepoint that you can jump back to if you encounter problems
	 */
	public function transactionSavepoint($savepoint){
		DB::query("SAVEPOINT $savepoint;");
	}
	
	/*
	 * Rollback or revert to a savepoint if your queries encounter problems
	 * If you encounter a problem at any point during a transaction, you may
	 * need to rollback that particular query, or return to a savepoint
	 */
	public function transactionRollback($savepoint=false){
		
		if($savepoint)
			DB::query("ROLLBACK TO $savepoint;");
		else
			DB::query('ROLLBACK;');
	}
	
	/*
	 * Commit everything inside this transaction so far
	 */
	public function endTransaction(){
		DB::query('COMMIT;');
	}
	
	/*
	 * Given a tablespace and and location, either create a new one
	 * or update the existing one
	 */
	public function createOrReplaceTablespace($name, $location){
		$existing=DB::query("SELECT spcname, spclocation FROM pg_tablespace WHERE spcname='$name';")->first();
		
		//NOTE: this location must be empty for this to work
		//We can't seem to change the location of the tablespace through any ALTER commands :(
		
		//If a tablespace with this name exists, but the location has changed, then drop the current one
		//if($existing && $location!=$existing['spclocation'])
		//	DB::query("DROP TABLESPACE $name;");
		
		//If this is a new tablespace, or we have dropped the current one:
		if(!$existing || ($existing && $location!=$existing['spclocation']))
			DB::query("CREATE TABLESPACE $name LOCATION '$location';");
						
	}
	
	public function createOrReplacePartition($tableName, $partitions, $indexes, $extensions){
		
		//We need the plpgsql language to be installed for this to work:
		$this->createLanguage('plpgsql');
		
		$trigger='CREATE OR REPLACE FUNCTION ' . $tableName . '_insert_trigger() RETURNS TRIGGER AS $$ BEGIN ';
		$first=true;
		
		//Do we need to create a tablespace for this item?
		if($extensions && isset($extensions['tablespace'])){
			$this->createOrReplaceTablespace($extensions['tablespace']['name'], $extensions['tablespace']['location']);
			$tableSpace=' TABLESPACE ' . $extensions['tablespace']['name'];
		} else 
			$tableSpace='';
			
		foreach($partitions as $partition_name=>$partition_value){
			//Check that this child table does not already exist:
			if(!$this->TableExists($partition_name)){
				DB::query("CREATE TABLE \"$partition_name\" (CHECK (" . str_replace('NEW.', '', $partition_value) . ")) INHERITS (\"$tableName\")$tableSpace;");
			} else {
				//Drop the constraint, we will recreate in in the next line
				$existing_constraint=$this->query("SELECT conname FROM pg_constraint WHERE conname='{$partition_name}_pkey';");
				if($existing_constraint){
					DB::query("ALTER TABLE \"$partition_name\" DROP CONSTRAINT \"{$partition_name}_pkey\";");
				}
				$this->dropTrigger(strtolower('trigger_' . $tableName . '_insert'), $tableName);
			}
						
			DB::query("ALTER TABLE \"$partition_name\" ADD CONSTRAINT \"{$partition_name}_pkey\" PRIMARY KEY (\"ID\");");
			
			if($first){
				$trigger.='IF';
				$first=false;
			} else
				$trigger.='ELSIF';
				
			$trigger.=" ($partition_value) THEN INSERT INTO \"$partition_name\" VALUES (NEW.*);";
			
			if($indexes){
				// We need to propogate the indexes through to the child pages.
				// Some of this code is duplicated, and could be tidied up
				foreach($indexes as $name=>$this_index){
						
					if($this_index['type']=='fulltext'){
						$fillfactor=$where='';
						if(isset($this_index['fillfactor']))
							$fillfactor='WITH (FILLFACTOR = ' . $this_index['fillfactor'] . ')';
						if(isset($this_index['where']))
							$where='WHERE ' . $this_index['where'];
							
						DB::query("CREATE INDEX \"ix_{$partition_name}_{$this_index['name']}\" ON \"" . $partition_name . "\" USING " . $this->default_fts_cluster_method . "(\"ts_" . $name . "\") $fillfactor $where");
						$ts_details=$this->fulltext($this_index, $partition_name, $name);
						DB::query($ts_details['triggers']);
					} else {
						
						if(is_array($this_index))
							$index_name=$this_index['name'];
						else $index_name=trim($this_index, '()');
						
						$createIndex=$this->getIndexSqlDefinition($partition_name, $index_name, $this_index);
						if($createIndex!==false)
							DB::query($createIndex);
					}
				}
			}
			
			//Lastly, clustering goes here:
			if($extensions && isset($extensions['cluster'])){
				DB::query("CLUSTER \"$partition_name\" USING \"{$extensions['cluster']}\";");
			}
		}
		
		$trigger.='ELSE RAISE EXCEPTION \'Value id out of range.  Fix the ' . $tableName . '_insert_trigger() function!\'; END IF; RETURN NULL; END; $$ LANGUAGE plpgsql;';
 		$trigger.='CREATE TRIGGER trigger_' . $tableName . '_insert BEFORE INSERT ON "' . $tableName . '" FOR EACH ROW EXECUTE PROCEDURE ' . $tableName . '_insert_trigger();';
		
 		DB::query($trigger);

	}
	
	/*
	 * This will create a language if it doesn't already exist.
	 * This is used by the createOrReplacePartition function, which needs plpgsql
	 */
	public function createLanguage($language){
		$result=DB::query("SELECT lanname FROM pg_language WHERE lanname='$language';")->first();
		
		if(!$result){
			DB::query("CREATE LANGUAGE $language;");
		}
	}

	/**
	 * Function to return an SQL datetime expression that can be used with Postgres
	 * used for querying a datetime in a certain format
	 * @param string $date to be formated, can be either 'now', literal datetime like '1973-10-14 10:30:00' or field name, e.g. '"SiteTree"."Created"'
	 * @param string $format to be used, supported specifiers:
	 * %Y = Year (four digits)
	 * %m = Month (01..12)
	 * %d = Day (01..31)
	 * %H = Hour (00..23)
	 * %i = Minutes (00..59)
	 * %s = Seconds (00..59)
	 * %U = unix timestamp, can only be used on it's own
	 * @return string SQL datetime expression to query for a formatted datetime
	 */
	function formattedDatetimeClause($date, $format) {

		preg_match_all('/%(.)/', $format, $matches);
		foreach($matches[1] as $match) if(array_search($match, array('Y','m','d','H','i','s','U')) === false) user_error('formattedDatetimeClause(): unsupported format character %' . $match, E_USER_WARNING);

		$translate = array(
			'/%Y/' => 'YYYY',
			'/%m/' => 'MM',
			'/%d/' => 'DD',
			'/%H/' => 'HH24',
			'/%i/' => 'MI',
			'/%s/' => 'SS',
		);
		$format = preg_replace(array_keys($translate), array_values($translate), $format);

		if(preg_match('/^now$/i', $date)) {
			$date = "NOW()";
		} else if(preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/i', $date)) {
			$date = "TIMESTAMP '$date'";
		}

		if($format == '%U') return "FLOOR(EXTRACT(epoch FROM $date))";
		
		return "to_char($date, TEXT '$format')";
		
	}
	
	/**
	 * Function to return an SQL datetime expression that can be used with Postgres
	 * used for querying a datetime addition
	 * @param string $date, can be either 'now', literal datetime like '1973-10-14 10:30:00' or field name, e.g. '"SiteTree"."Created"'
	 * @param string $interval to be added, use the format [sign][integer] [qualifier], e.g. -1 Day, +15 minutes, +1 YEAR
	 * supported qualifiers:
	 * - years
	 * - months
	 * - days
	 * - hours
	 * - minutes
	 * - seconds
	 * This includes the singular forms as well
	 * @return string SQL datetime expression to query for a datetime (YYYY-MM-DD hh:mm:ss) which is the result of the addition
	 */
	function datetimeIntervalClause($date, $interval) {

		if(preg_match('/^now$/i', $date)) {
			$date = "NOW()";
		} else if(preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/i', $date)) {
			$date = "TIMESTAMP '$date'";
		}

		// ... when being too precise becomes a pain. we need to cut of the fractions.
		// TIMESTAMP(0) doesn't work because it rounds instead flooring
		return "CAST(SUBSTRING(CAST($date + INTERVAL '$interval' AS VARCHAR) FROM 1 FOR 19) AS TIMESTAMP)";
	}

	/**
	 * Function to return an SQL datetime expression that can be used with Postgres
	 * used for querying a datetime substraction
	 * @param string $date1, can be either 'now', literal datetime like '1973-10-14 10:30:00' or field name, e.g. '"SiteTree"."Created"'
	 * @param string $date2 to be substracted of $date1, can be either 'now', literal datetime like '1973-10-14 10:30:00' or field name, e.g. '"SiteTree"."Created"'
	 * @return string SQL datetime expression to query for the interval between $date1 and $date2 in seconds which is the result of the substraction
	 */
	function datetimeDifferenceClause($date1, $date2) {

		if(preg_match('/^now$/i', $date1)) {
			$date1 = "NOW()";
		} else if(preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/i', $date1)) {
			$date1 = "TIMESTAMP '$date1'";
		}

		if(preg_match('/^now$/i', $date2)) {
			$date2 = "NOW()";
		} else if(preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/i', $date2)) {
			$date2 = "TIMESTAMP '$date2'";
		}

		return "(FLOOR(EXTRACT(epoch FROM $date1)) - FLOOR(EXTRACT(epoch from $date2)))";
	}
	
	/**
	 * Return a set type-formatted string
	 * This is used for Multi-enum support, which isn't actually supported by Postgres.
	 * Throws a user error to show our lack of support, and return an "int", specifically for sapphire
	 * tests that test multi-enums. This results in a test failure, but not crashing the test run.
	 *  
	 * @param array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function set($values){
		user_error("PostGreSQL does not support multi-enum");
		return "int";
	}
	
	/**
	 * Set the current language for the tsearch functions
	 * 
	 * @todo: somehow link this to the locale options?
	 * 
	 * @param string $lang
	 */
	public function set_search_language($lang){
		$this->search_language=$lang;
	}
	
	/**
	 * Returns the current language for the tsearch functions
	 * 
	 * @param string $lang
	 */
	public function get_search_language(){
		return $this->search_language;
	}
}

/**
 * A result-set from a PostgreSQL database.
 * @package sapphire
 * @subpackage model
 */
class PostgreSQLQuery extends SS_Query {
	/**
	 * The MySQLDatabase object that created this result set.
	 * @var PostgreSQLDatabase
	 */
	private $database;
	
	/**
	 * The internal Postgres handle that points to the result set.
	 * @var resource
	 */
	private $handle;

	/**
	 * Hook the result-set given into a Query class, suitable for use by sapphire.
	 * @param database The database object that created this query.
	 * @param handle the internal Postgres handle that is points to the resultset.
	 */
	public function __construct(PostgreSQLDatabase $database, $handle) {
		$this->database = $database;
		$this->handle = $handle;
	}
	
	public function __destruct() {
		if(is_resource($this->handle)) pg_free_result($this->handle);
	}
	
	public function seek($row) {
		return pg_result_seek($this->handle, $row);
	}
	
	public function numRecords() {
		return pg_num_rows($this->handle);
	}
	
	public function nextRecord() {
		// Coalesce rather than replace common fields.
		if($data = pg_fetch_row($this->handle)) {
			foreach($data as $columnIdx => $value) {
				$columnName = pg_field_name($this->handle, $columnIdx);
				// $value || !$ouput[$columnName] means that the *last* occurring value is shown
				// !$ouput[$columnName] means that the *first* occurring value is shown
				if(isset($value) || !isset($output[$columnName])) {
					$output[$columnName] = $value;
				}
			}
			return $output;
		} else {
			return false;
		}
	}
	
	
}

?>