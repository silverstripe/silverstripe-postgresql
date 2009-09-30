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
class PostgreSQLDatabase extends Database {
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
		
		//assumes that the server and dbname will always be provided:
		$this->dbConn = pg_connect('host=' . $parameters['server'] . ' port=5432 dbname=' . $dbName . $username . $password);
		
		//By virtue of getting here, the connection is active:
		$this->active=true;
		$this->database = $dbName;
				
		if(!$this->dbConn) {
			$this->databaseError("Couldn't connect to PostgreSQL database");
			return false;
		}
		
		return true;
	}
	/**
	 * Not implemented, needed for PDO
	 */
	public function getConnect($parameters) {
		return null;
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
	 * The version of PostgreSQL.
	 * @var float
	 */
	private $pgsqlVersion;
	
	/**
	 * Get the version of MySQL.
	 * @return float
	 */
	public function getVersion() {
		if(!$this->pgsqlVersion) {
			//returns something like this: PostgreSQL 8.3.3 on i386-apple-darwin9.3.0, compiled by GCC i686-apple-darwin9-gcc-4.0.1 (GCC) 4.0.1 (Apple Inc. build 5465)
			$postgres=strlen('PostgreSQL ');
			$db_version=$this->query("SELECT VERSION()")->value();
			
			$this->pgsqlVersion = (float)trim(substr($db_version, $postgres, strpos($db_version, ' on ')));
		}
		return $this->pgsqlVersion;
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
	 * This will create a database based on whatever is in the $this->database value
	 * So you need to have called $this->selectDatabase() first, or used the __construct method
	 */
	public function createDatabase() {
		
		$this->query("CREATE DATABASE $this->database");
		
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
		//$this->database = $dbname;
		//if($this->databaseExists($this->database)) mysql_select_db($this->database, $this->dbConn);
		//if($this->databaseExists($this->database)) pg_d
		$this->database=$dbname;
		
		$this->tableList = $this->fieldList = $this->indexList = null;
				
		return true;
	}

	
	/**
	 * Returns true if the named database exists.
	 */
	public function databaseExists($name) {
		$SQL_name=Convert::raw2sql($name);
		$result=$this->query("SELECT datname FROM pg_database WHERE datname='$SQL_name';")->first();
		return $this->query("SELECT datname FROM pg_database WHERE datname='$SQL_name';")->first() ? true : false;
	}
	
	public function createTable($tableName, $fields = null, $indexes = null, $options = null) {
		$fieldSchemas = $indexSchemas = "";
		if($fields) foreach($fields as $k => $v) $fieldSchemas .= "\"$k\" $v,\n";
		if(isset($this->class)){
			$addOptions = (isset($options[$this->class])) ? $options[$this->class] : null;
		} else $addOptions=null;
		
		//First of all, does this table already exist
		$doesExist=$this->TableExists($tableName);
		if($doesExist)
			return false;
			
		//If we have a fulltext search request, then we need to create a special column
		//for GiST searches
		$fulltexts='';
		$triggers='';
		if($indexes){
			foreach($indexes as $name=>$this_index){
				if($this_index['type']=='fulltext'){
					
					//For full text search, we need to create a column for the index
					$columns=explode(',', $this_index['value']);
					for($i=0; $i<sizeof($columns);$i++)
						$columns[$i]="\"" . trim($columns[$i]) . "\"";
	
					$columns=implode(', ', $columns);
					
					$fulltexts .= "\"ts_$name\" tsvector, ";
					$triggerName="ts_{$tableName}_{$name}";
					
					$this->dropTrigger($triggerName, $tableName);
					$triggers.="CREATE TRIGGER $triggerName BEFORE INSERT OR UPDATE
								ON \"$tableName\" FOR EACH ROW EXECUTE PROCEDURE
								tsvector_update_trigger(\"ts_$name\", 'pg_catalog.english', $columns);";
				}
			}
		}
		if($indexes) foreach($indexes as $k => $v) $indexSchemas .= $this->getIndexSqlDefinition($tableName, $k, $v) . "\n";
		
		$this->query("CREATE TABLE \"$tableName\" (
				$fieldSchemas
				$fulltexts
				primary key (\"ID\")
			); $indexSchemas $addOptions");
				
		if($triggers!=''){
			$this->query($triggers);
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
	public function alterTable($tableName, $newFields = null, $newIndexes = null, $alteredFields = null, $alteredIndexes = null, $alteredOptions = null) {
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
		
		//DB ABSTRACTION: we need to change the constraints to be a separate 'add' command,
		//see http://www.postgresql.org/docs/8.1/static/sql-altertable.html
		$alterIndexList=Array();
		if($alteredIndexes) foreach($alteredIndexes as $v) {
			//We are only going to delete indexes which exist
			$indexes=$this->indexList($tableName);
			
			if(isset($indexes[$v['value']])){
				if(is_array($v))
					$alterIndexList[] = 'DROP INDEX ix_' . strtolower($tableName) . '_' . strtolower($v['value']) . ';';
				else
					$alterIndexList[] = 'DROP INDEX ix_' . strtolower($tableName) . '_' . strtolower(trim($v, '()')) . ';';
							
				$k=$v['value'];
				$alterIndexList[] .= $this->getIndexSqlDefinition($tableName, $k, $v);
			}
 		}

		//Add the new indexes:
		if($newIndexes) foreach($newIndexes as $k=>$v){
 			//Check that this index doesn't already exist:
 			$indexes=$this->indexList($tableName);
 			if(isset($indexes[trim($v, '()')])){
 				if(is_array($v)){
					$alterIndexList[] = 'DROP INDEX ix_' . strtolower($tableName) . '_' . strtolower($v['value']) . ';';
				} else {
					$alterIndexList[] = 'DROP INDEX ' . $indexes[trim($v, '()')]['indexname'] . ';';
				}
			}
					
 			$alterIndexList[] = $this->getIndexSqlDefinition($tableName, $k, $v);
 		}
 		
 		if($alterList) {
			$alterations = implode(",\n", $alterList);
			$this->query("ALTER TABLE \"$tableName\" " . $alterations);
		}
		
		if($alteredOptions && isset($alteredOptions[$this->class])) {
			$this->query(sprintf("ALTER TABLE \"%s\" %s", $tableName, $alteredOptions[$this->class]));
			Database::alteration_message(
				sprintf("Table %s options changed: %s", $tableName, $alteredOptions[$this->class]),
				"changed"
			);
		}
		
		foreach($alterIndexList as $alteration)
			$this->query($alteration);
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
		
		/*if (isset($matches)) {
			echo "sql:$colSpec <pre>";
			print_r($matches);
			echo '</pre>';
		}*/
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
			if(!empty($matches[4])) {
					$alterCol .= ",\nDROP CONSTRAINT \"{$tableName}_{$colName}_check\"";
					$alterCol .= ",\nADD CONSTRAINT \"{$tableName}_{$colName}_check\" $matches[4]";
			}
		}
		
		return isset($alterCol) ? $alterCol : '';
	}
	
	public function renameTable($oldTableName, $newTableName) {
		$this->query("ALTER TABLE \"$oldTableName\" RENAME \"$newTableName\"");
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
		$fields = $this->query("SELECT ordinal_position, column_name, data_type, column_default, is_nullable, character_maximum_length, numeric_precision FROM information_schema.columns WHERE table_name = '$table' ORDER BY ordinal_position;");
		
		$output = array();
		if($fields) foreach($fields as $field) {
			switch($field['data_type']){
				case 'character varying':
					//Check to see if there's a constraint attached to this column:
					$constraint=$this->query("SELECT conname,pg_catalog.pg_get_constraintdef(r.oid, true) FROM pg_catalog.pg_constraint r WHERE r.contype = 'c' AND conname='" . $table . '_' . $field['column_name'] . "_check' ORDER BY 1;")->first();
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
							//TODO: perhaps pass this to the enum function so we can 
							$default=trim(substr($field['column_default'], 0, strpos($field['column_default'], '::')), "'");
							$output[$field['column_name']]=$this->enum(Array('default'=>$default, 'name'=>$field['column_name'], 'enums'=>$constraints));
						}
					} else{
						$output[$field['column_name']]='varchar(' . $field['character_maximum_length'] . ')';
					}
					break;
					
				case 'numeric':
					$output[$field['column_name']]='numeric(' . $field['numeric_precision'] . ')';
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
		$this->query($this->getIndexSqlDefinition($tableName, $indexName, $indexSpec));
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
						$indexSpec='(ts_' . $indexSpec['name'] . ')';
						break;
					case 'unique':
						$indexSpec='unique (' . $indexSpec['value'] . ')';
						break;
					case 'btree':
						$indexSpec='using btree (' . $indexSpec['value'] . ')';
						break;
					case 'hash':
						$indexSpec='using hash (' . $indexSpec['value'] . ')';
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
		
		if(!$asDbValue){
			
			$tableCol= 'ix_' . $tableName . '_' . $indexName;
			if(strlen($tableCol)>64){
				$tableCol=substr($indexName, 0, 59) . rand(1000, 9999);
				//echo 'it is now ' . $tableCol . "\n\n";
			}
			
			if(!is_array($indexSpec)){
				$indexSpec=trim($indexSpec, '()');
				$bits=explode(',', $indexSpec);
				$indexes="\"" . implode("\",\"", $bits) . "\"";

				$indexSpec=$this->indexList($tableName);
				
				return "create index $tableCol ON \"" . $tableName . "\" (" . $indexes . ");";
			} else {
				
				//Misc options first:
				$fillfactor=$where='';;
				if(isset($indexSpec['fillfactor']))
					$fillfactor='WITH (FILLFACTOR = ' . $indexSpec['fillfactor'] . ')';
				if(isset($indexSpec['where']))
					$where='WHERE ' . $indexSpec['where'];
					
				//TODO: create tablespace and where clause support
				
				//create a type-specific index
				//NOTE:  hash should be removed.  This is only here to demonstrate how other indexes can be made		
				switch($indexSpec['type']){
					case 'fulltext':
						$spec="create index $tableCol ON \"" . $tableName . "\" USING " . $this->default_fts_cluster_method . "(\"ts_" . $indexName . "\") $fillfactor $where";
						break;
						
					case 'unique':
						$spec="create unique index $tableCol ON \"" . $tableName . "\" (\"" . $indexSpec['value'] . "\") $fillfactor $where";
						break;
						
					case 'btree':
						$spec="create index $tableCol ON \"" . $tableName . "\" USING btree (\"" . $indexSpec['value'] . "\") $fillfactor $where";
						break;
	
					case 'hash':
						//NOTE: this is not a recommended index type
						$spec="create index $tableCol ON \"" . $tableName . "\" USING hash (\"" . $indexSpec['value'] . "\") $fillfactor $where";
						break;
						
					default:
						$spec="create index $tableCol ON \"" . $tableName . "\" (\"" . $indexSpec['value'] . "\") $fillfactor $where";
				}
				
				return trim($spec) . ';';
	
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
		$indexes=DB::query("SELECT tablename, indexname, indexdef FROM pg_indexes WHERE tablename='$table';");
		
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
		foreach($this->query("SELECT tablename FROM pg_tables WHERE tablename NOT ILIKE 'pg_%' AND tablename NOT ILIKE 'sql_%'") as $record) {
			//$table = strtolower(reset($record));
			$table = reset($record);
			$tables[$table] = $table;
		}
		
		//Return an empty array if there's nothing in this database
		return isset($tables) ? $tables : Array();
	}
	
	function TableExists($tableName){
		$result=$this->query("SELECT tablename FROM pg_tables WHERE tablename='$tableName';")->first();
		
		if($result)
			return true;
		else
			return false;
		
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
		$query="SELECT a.attname as \"Column\", pg_catalog.format_type(a.atttypid, a.atttypmod) as \"Datatype\" FROM pg_catalog.pg_attribute a WHERE a.attnum > 0 AND NOT a.attisdropped AND a.attrelid = ( SELECT c.oid FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname ~ '^($tableName)$' AND pg_catalog.pg_table_is_visible(c.oid));";
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
	 * Return a boolean type-formatted string
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function boolean($values, $asDbValue=false){
		//Annoyingly, we need to do a good ol' fashioned switch here:
		($values['default']) ? $default='1' : $default='0';
		
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
	 * For MySQL, we simply return the word 'date', no other parameters are necessary
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function date($values){
		//For reference, this is what typically gets passed to this function:
		//$parts=Array('datatype'=>'date');
		//DB::requireField($this->tableName, $this->name, "date");

		return "date{$values['arrayValue']}";
	}
	
	/**
	 * Return a decimal type-formatted string
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function decimal($values, $asDbValue=false){
		//For reference, this is what typically gets passed to this function:
		//$parts=Array('datatype'=>'decimal', 'precision'=>"$this->wholeSize,$this->decimalSize");
		//DB::requireField($this->tableName, $this->name, "decimal($this->wholeSize,$this->decimalSize)");

		// Avoid empty strings being put in the db
		if($values['precision'] == '') {
			$precision = 1;
		} else {
			$precision = $values['precision'];
		}
		
		if($asDbValue)
			return Array('data_type'=>'numeric', 'precision'=>'9');
		else return "decimal($precision){$values['arrayValue']}";
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
	 * For MySQL, we simply return the word 'date', no other parameters are necessary
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function float($values, $asDbValue=false){
		//For reference, this is what typically gets passed to this function:
		//$parts=Array('datatype'=>'float');
		//DB::requireField($this->tableName, $this->name, "float");
		
		if($asDbValue)
			return Array('data_type'=>'double precision');
		else return "float{$values['arrayValue']}";
	}
	
	/**
	 * Return a int type-formatted string
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function int($values, $asDbValue=false){
		if($asDbValue)
			return Array('data_type'=>'numeric', 'precision'=>$values['precision']);
		else {
			if($values['arrayValue']!='')
				$default='';
			else
				$default=' default ' . (int)$values['default'];
		
			return "numeric(11){$values['arrayValue']}" . $default;
		}
	}
	
	/**
	 * Return a datetime type-formatted string
	 * For PostgreSQL, we simply return the word 'timestamp', no other parameters are necessary
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function ssdatetime($values, $asDbValue=false){
		//For reference, this is what typically gets passed to this function:
		//$parts=Array('datatype'=>'datetime');
		//DB::requireField($this->tableName, $this->name, $values);

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
		//For reference, this is what typically gets passed to this function:
		//$parts=Array('datatype'=>'mediumtext', 'character set'=>'utf8', 'collate'=>'utf8_general_ci');
		//DB::requireField($this->tableName, $this->name, "mediumtext character set utf8 collate utf8_general_ci");
		
		if($asDbValue)
			return Array('data_type'=>'text');
		else
			return "text{$values['arrayValue']}";
	}
	
	/**
	 * Return a time type-formatted string
	 * For MySQL, we simply return the word 'time', no other parameters are necessary
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function time($values){
		//For reference, this is what typically gets passed to this function:
		//$parts=Array('datatype'=>'time');
		//DB::requireField($this->tableName, $this->name, "time");
		
		return "time{$values['arrayValue']}";
	}
	
	/**
	 * Return a varchar type-formatted string
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function varchar($values, $asDbValue=false){
		//For reference, this is what typically gets passed to this function:
		//$parts=Array('datatype'=>'varchar', 'precision'=>$this->size, 'character set'=>'utf8', 'collate'=>'utf8_general_ci');
		//DB::requireField($this->tableName, $this->name, "varchar($this->size) character set utf8 collate utf8_general_ci");
		if(!isset($values['precision']))
			$values['precision']=255;
			
		if($asDbValue)
			return Array('data_type'=>'varchar', 'precision'=>$values['precision']);
		else
			return "varchar({$values['precision']}){$values['arrayValue']}";
	}
	
	/*
	 * Return a 4 digit numeric type.  MySQL has a proprietary 'Year' type.
	 */
	public function year($values, $asDbValue=false){
		if($asDbValue)
			return Array('data_type'=>'numeric', 'precision'=>'4');
		else return "numeric(4){$values['arrayValue']}"; 
	}
	
	function escape_character($escape=false){
		if($escape)
			return "\\\"";
		else
			return "\"";
	}
	
	/**
	 * Create a fulltext search datatype for PostgreSQL
	 *
	 * @param array $spec
	 */
	/*function fulltext($table, $spec){
		//$spec['name'] is the column we've created that holds all the words we want to index.
		//This is a coalesced collection of multiple columns if necessary
		$spec='create index ix_' . $table . '_' . $spec['name'] . ' on ' . $table . ' using ' . $this->default_fts_cluster_method . '(' . $spec['name'] . ');';
		
		return $spec;
	}*/
	
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
	 * @todo Make a proper implementation
	 */
	function hasTable($tableName) {
		return true;
	}
	
	/**
	 * Returns the SQL command to get all the tables in this database
	 */
	function allTablesSQL(){
		return "select table_name from information_schema.tables where table_schema='public' and table_type='BASE TABLE';";
	}
	
	/**
	 * Return enum values for the given field
	 * @todo Make a proper implementation
	 */
	function enumValuesForField($tableName, $fieldName) {
		return array('SiteTree','Page');
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
		if($sqlQuery->orderby) $text .= " ORDER BY " . $sqlQuery->orderby;

		if($sqlQuery->limit) {
			$limit = $sqlQuery->limit;
			// Pass limit as array or SQL string value
			if(is_array($limit)) {
				if(!array_key_exists('limit',$limit)) user_error('SQLQuery::limit(): Wrong format for $limit', E_USER_ERROR);

				if(isset($limit['start']) && is_numeric($limit['start']) && isset($limit['limit']) && is_numeric($limit['limit'])) {
					$combinedLimit = (int)$limit['start'] . ',' . (int)$limit['limit'];
				} elseif(isset($limit['limit']) && is_numeric($limit['limit'])) {
					$combinedLimit = (int)$limit['limit'];
				} else {
					$combinedLimit = false;
				}
				if(!empty($combinedLimit)) $this->limit = $combinedLimit;

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
	
	/*
	 * This will return text which has been escaped in a database-friendly manner
	 * Using PHP's addslashes method won't work in MSSQL
	 */
	function addslashes($value){
		return pg_escape_string($value);
	}
	
	/*
	 * This changes the index name depending on database requirements.
	 * MySQL doesn't need any changes.
	 */
	function modifyIndex($index, $spec){
		
		if(is_array($spec) && $spec['type']=='fulltext')
			return 'ts_' . str_replace(',', '_', $index);
		else
			return str_replace('_', ',', $index);

	}
	
	/**
	 * The core search engine configuration.
	 * @todo There is no result relevancy or ordering as it currently stands.
	 * 
	 * @param string $keywords Keywords as a space separated string
	 * @return object DataObjectSet of result pages
	 */
	public function searchEngine($classesToSearch, $keywords, $start, $pageLength, $sortBy = "ts_rank DESC", $extraFilter = "", $booleanSearch = false, $alternativeFileFilter = "", $invertedMatch = false) {
		/*
		$fileFilter = '';	 	
	 	$keywords = Convert::raw2sql($keywords);
		$htmlEntityKeywords = htmlentities($keywords);
		
		$extraFilters = array('SiteTree' => '', 'File' => '');
	 	
	 	//if($booleanSearch) $boolean = "IN BOOLEAN MODE";
		$boolean='';
		
	 	if($extraFilter) {
	 		$extraFilters['SiteTree'] = " AND $extraFilter";
	 		
	 		if($alternativeFileFilter) $extraFilters['File'] = " AND $alternativeFileFilter";
	 		else $extraFilters['File'] = $extraFilters['SiteTree'];
	 	}
	 	
		// Always ensure that only pages with ShowInSearch = 1 can be searched
		$extraFilters['SiteTree'] .= " AND ShowInSearch <> 0";

		$limit = $start . ", " . (int) $pageLength;
		
		$notMatch = $invertedMatch ? "NOT " : "";
		if($keywords) {
			
			$match['SiteTree'] = "
				MATCH (Title, MenuTitle, Content, MetaTitle, MetaDescription, MetaKeywords) AGAINST ('$keywords' $boolean)
				+ MATCH (Title, MenuTitle, Content, MetaTitle, MetaDescription, MetaKeywords) AGAINST ('$htmlEntityKeywords' $boolean)
			";
			$match['File'] = "MATCH (Filename, Title, Content) AGAINST ('$keywords' $boolean) AND ClassName = 'File'";
	
			// We make the relevance search by converting a boolean mode search into a normal one
			$relevanceKeywords = str_replace(array('*','+','-'),'',$keywords);
			$htmlEntityRelevanceKeywords = str_replace(array('*','+','-'),'',$htmlEntityKeywords);
			$relevance['SiteTree'] = "MATCH (Title, MenuTitle, Content, MetaTitle, MetaDescription, MetaKeywords) AGAINST ('$relevanceKeywords') + MATCH (Title, MenuTitle, Content, MetaTitle, MetaDescription, MetaKeywords) AGAINST ('$htmlEntityRelevanceKeywords')";
			$relevance['File'] = "MATCH (Filename, Title, Content) AGAINST ('$relevanceKeywords')";
		} else {
			$relevance['SiteTree'] = $relevance['File'] = 1;
			$match['SiteTree'] = $match['File'] = "1 = 1";
		}

		// Generate initial queries and base table names
		$baseClasses = array('SiteTree' => '', 'File' => '');
		foreach($classesToSearch as $class) {
			$queries[$class] = singleton($class)->extendedSQL($notMatch . $match[$class] . $extraFilters[$class], "");
			$baseClasses[$class] = reset($queries[$class]->from);
		}
		
		// Make column selection lists
		$select = array(
			'SiteTree' => array("ClassName","$baseClasses[SiteTree].ID","ParentID","Title","URLSegment","Content","LastEdited","Created","_utf8'' AS Filename", "_utf8'' AS Name", "$relevance[SiteTree] AS Relevance", "CanViewType"),
			'File' => array("ClassName","$baseClasses[File].ID","_utf8'' AS ParentID","Title","_utf8'' AS URLSegment","Content","LastEdited","Created","Filename","Name","$relevance[File] AS Relevance","NULL AS CanViewType"),
		);
		
		// Process queries
		foreach($classesToSearch as $class) {
			// There's no need to do all that joining
			$queries[$class]->from = array(str_replace('`','',$baseClasses[$class]) => $baseClasses[$class]);
			$queries[$class]->select = $select[$class];
			$queries[$class]->orderby = null;
		}

		// Combine queries
		$querySQLs = array();
		$totalCount = 0;
		foreach($queries as $query) {
			$querySQLs[] = $query->sql();
			$totalCount += $query->unlimitedRowCount();
		}
		$fullQuery = implode(" UNION ", $querySQLs) . " ORDER BY $sortBy LIMIT $limit";
		
		// Get records
		$records = DB::query($fullQuery);

		foreach($records as $record)
			$objects[] = new $record['ClassName']($record);
		
		if(isset($objects)) $doSet = new DataObjectSet($objects);
		else $doSet = new DataObjectSet();
		
		$doSet->setPageLimits($start, $pageLength, $totalCount);
		return $doSet;
*/
		$keywords = Convert::raw2sql(trim($keywords));
		$htmlEntityKeywords = htmlentities($keywords);
		
		//We can get a list of all the tsvector columns though this query:
		//We know what tables to search in based on the $classesToSearch variable:
		$result=DB::query("SELECT table_name, column_name, data_type FROM information_schema.columns WHERE data_type='tsvector' AND table_name in ('" . implode("', '", $classesToSearch) . "');");
		if (!$result->numRecords()) throw Exception('there are no full text columns to search');
		
		$tables=Array();
		
		foreach($result as $row){
			if($row['table_name']=='SiteTree')
				$showInSearch="AND \"ShowInSearch\"=1 ";
			else
				$showInSearch='';
				
			if($keywords){
				$thisSql = "SELECT \"ID\", '{$row['table_name']}' AS ClassName, ts_rank(\"{$row['column_name']}\", q) AS Relevance FROM \"{$row['table_name']}\", to_tsquery('english', '$keywords') AS q WHERE \"{$row['column_name']}\" " .  $this->default_fts_search_method . " q $showInSearch";
			} else {
				$thisSql = "SELECT \"ID\", '{$row['table_name']}' AS ClassName FROM \"{$row['table_name']}\" WHERE 1=1 $showInSearch";
			}
			
			//Add this query to the collection
			$tables[] = $thisSql;
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
			$item=DB::query("SELECT * FROM \"{$record['classname']}\" WHERE \"ID\"={$record['ID']};")->first();
			$objects[] = new $record['classname']($item);
			$totalCount++;
		}
		
		if(isset($objects)) $doSet = new DataObjectSet($objects);
		else $doSet = new DataObjectSet();
		
		$doSet->setPageLimits($start, $pageLength, $totalCount);
		
		return $doSet;
		
	}
}

/**
 * A result-set from a MySQL database.
 * @package sapphire
 * @subpackage model
 */
class PostgreSQLQuery extends Query {
	/**
	 * The MySQLDatabase object that created this result set.
	 * @var MySQLDatabase
	 */
	private $database;
	
	/**
	 * The internal MySQL handle that points to the result set.
	 * @var resource
	 */
	private $handle;

	/**
	 * Hook the result-set given into a Query class, suitable for use by sapphire.
	 * @param database The database object that created this query.
	 * @param handle the internal mysql handle that is points to the resultset.
	 */
	public function __construct(PostgreSQLDatabase $database, $handle) {
		$this->database = $database;
		$this->handle = $handle;
	}
	
	public function __destroy() {
		pg_free_result($this->handle);
	}
	
	public function seek($row) {
		return pg_result_seek($this-handle, $row);
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