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
	
	/**
	 * Connect to a PostgreSQL database.
	 * @param array $parameters An map of parameters, which should include:
	 *  - server: The server, eg, localhost
	 *  - username: The username to log on with
	 *  - password: The password to log on with
	 *  - database: The database to connect to
	 */
	public function __construct($parameters) {
		
		($parameters['username']!='') ? $username=' user=' . $parameters['username'] : $username='';
		($parameters['password']!='') ? $password=' password=' . $parameters['password'] : $password='';
		//assumes that the server and dbname will always be provided:
		$this->dbConn = pg_connect('host=' . $parameters['server'] . ' port=5432 dbname=' . $parameters['database'] . $username . $password);
		
		//By virtue of getting here, the connection is active:
		$this->active=true;
		$this->database = $parameters['database'];
		
		if(!$this->dbConn) {
			$this->databaseError("Couldn't connect to PostgreSQL database");
		}

		parent::__construct();
	}
	
	/**
	 * Not implemented, needed for PDO
	 */
	public function getConnect($parameters) {
		return null;
	}
	
	/**
	 * Returns true if this database supports collations
	 * @return boolean
	 */
	public function supportsCollations() {
		return $this->getVersion() >= 4.1;
	}
	
	/**
	 * The version of MySQL.
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
	 * Get the database server, namely mysql.
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
				
		//echo $sql . '<hr>';
		$handle = pg_query($this->dbConn, $sql);
		
		if(isset($_REQUEST['showqueries'])) {
			$endtime = round(microtime(true) - $starttime,4);
			Debug::message("\n$sql\n{$endtime}ms\n", false);
		}
		
		DB::$lastQuery=$handle;
		
		if(!$handle && $errorLevel) $this->databaseError("Couldn't run query: $sql | " . pgsql_error($this->dbConn), $errorLevel);
				
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
	
	public function createDatabase() {
		$this->query("CREATE DATABASE $this->database");
	}

	/**
	 * Drop the database that this object is currently connected to.
	 * Use with caution.
	 */
	public function dropDatabase() {
		$this->query("DROP DATABASE $this->database");
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
		$this->database = $dbname;
		if($this->databaseExists($this->database)) mysql_select_db($this->database, $this->dbConn);
		$this->tableList = $this->fieldList = $this->indexList = null;
	}

	/**
	 * Returns true if the named database exists.
	 */
	public function databaseExists($name) {
		//TODO: fix me to test for the existance of the database
		//For the moment, this always returns true
		//$SQL_name = Convert::raw2sql($name);
		//return $this->query("SHOW DATABASES LIKE '$SQL_name'")->value() ? true : false;
		return true;
	}
	
	public function createTable($tableName, $fields = null, $indexes = null) {
		$fieldSchemas = $indexSchemas = "";
		if($fields) foreach($fields as $k => $v) $fieldSchemas .= "\"$k\" $v,\n";
		
		//If we have a fulltext search request, then we need to create a special column
		//for GiST searches
		$fulltexts='';
		foreach($indexes as $name=>$this_index){
			if($this_index['type']=='fulltext'){
				//For full text search, we need to create a column for the index 
				$fulltexts .= "\"ts_$name\" tsvector, ";
				
			}
		}
		
		if($indexes) foreach($indexes as $k => $v) $indexSchemas .= $this->getIndexSqlDefinition($tableName, $k, $v) . "\n";
		
		$this->query("CREATE TABLE \"$tableName\" (
				$fieldSchemas
				$fulltexts
				primary key (\"ID\")
			); $indexSchemas");
	}

	/**
	 * Alter a table's schema.
	 * @param $table The name of the table to alter
	 * @param $newFields New fields, a map of field name => field schema
	 * @param $newIndexes New indexes, a map of index name => index type
	 * @param $alteredFields Updated fields, a map of field name => field schema
	 * @param $alteredIndexes Updated indexes, a map of index name => index type
	 */
	public function alterTable($tableName, $newFields = null, $newIndexes = null, $alteredFields = null, $alteredIndexes = null) {
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
			if(is_array($v))
				$alterIndexList[] = 'DROP INDEX ix_' . strtolower($tableName) . '_' . strtolower($v['value']) . ';';
			else
				$alterIndexList[] = 'DROP INDEX ix_' . strtolower($tableName) . '_' . strtolower(trim($v, '()')) . ';';
						
			$k=$v['value'];
			$alterIndexList[] .= $this->getIndexSqlDefinition($tableName, $k, $v);
 		}

		//Add the new indexes:
 		if($newIndexes) foreach($newIndexes as $k=>$v){
 			$alterIndexList[] = $this->getIndexSqlDefinition($tableName, $k, $v);
 		}
 		
 		if($alterList) {
			$alterations = implode(",\n", $alterList);
			$this->query("ALTER TABLE \"$tableName\" " . $alterations);
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
	 * Checks a table's integrity and repairs it if necessary.
	 * @var string $tableName The name of the table.
	 * @return boolean Return true if the table has integrity after the method is complete.
	 */
	public function checkAndRepairTable($tableName) {
		$this->runTableCheckCommand("VACUUM FULL \"$tableName\"");
		return true;
	}
	
	/**
	 * Helper function used by checkAndRepairTable.
	 * @param string $sql Query to run.
	 * @return boolean Returns if the query returns a successful result.
	 */
	protected function runTableCheckCommand($sql) {
		$testResults = $this->query($sql);
		foreach($testResults as $testRecord) {
			if(strtolower($testRecord['Msg_text']) != 'ok') {
				return false;
			}
		}
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
							//$field['data_type']="varchar(255) not null default '" . $default . "' check (\"" . $field['column_name'] . "\" in ('" . implode("', '", $constraints) . "'))";
							$field['data_type']=$this->enum(Array('default'=>$default, 'name'=>$field['column_name'], 'enums'=>$constraints));		
						}
					}
					
					$output[$field['column_name']]=$field;
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
						//$indexSpec='fulltext (' . str_replace(' ', '', $indexSpec['value']) . ')';
						$indexSpec='(ts_' . $indexSpec['indexName'] . ')';
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
	    
		if(!$asDbValue){
			if(!is_array($indexSpec)){
				$indexSpec=trim($indexSpec, '()');
				$bits=explode(',', $indexSpec);
				$indexes="\"" . implode("\",\"", $bits) . "\"";
				
				return 'create index ix_' . $tableName . '_' . $indexName . " ON \"" . $tableName . "\" (" . $indexes . ");";
			} else {
				//create a type-specific index
				switch($indexSpec['type']){
					case 'fulltext':
						$spec='create index ix_' . $tableName . '_' . $indexName . " ON \"" . $tableName . "\" USING gist(\"ts_" . $indexName . "\");";
						break;
						
					case 'unique':
						$spec='create unique index ix_' . $tableName . '_' . $indexName . " ON \"" . $tableName . "\" (\"" . $indexSpec['value'] . "\");";
						break;
						
					case 'btree':
						$spec='create index ix_' . $tableName . '_' . $indexName . " ON \"" . $tableName . "\" USING btree (\"" . $indexSpec['value'] . "\");";
						break;
	
					case 'hash':
						$spec='create index ix_' . $tableName . '_' . $indexName . " ON \"" . $tableName . "\" USING hash (\"" . $indexSpec['value'] . "\");";
						break;
						
					case 'rtree':
						$spec='create index ix_' . $tableName . '_' . $indexName . " ON \"" . $tableName . "\" USING rtree (\"" . $indexSpec['value'] . "\");";
						break;
						
					default:
						$spec='create index ix_' . $tableName . '_' . $indexName . " ON \"" . $tableName . "\" (\"" . $indexSpec['value'] . "\");";
				}
				
				return $spec;
	
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
  			
  			$key=trim(trim(str_replace("\"", '', $value[1])), '()');
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
			$table = strtolower(reset($record));
			$tables[$table] = $table;
		}
		return isset($tables) ? $tables : null;
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
		else
			return 'smallint not null default ' . $default;
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

		return 'date';
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
			return Array('data_type'=>'numeric', 'numeric_precision'=>'9');
		else return 'decimal(' . $precision . ') not null';
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
		
		return "varchar(255) not null default '" . $values['default'] . "' check (\"" . $values['name'] . "\" in ('" . implode('\', \'', $values['enums']) . "'))";
		
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
		else return 'float';
	}
	
	/**
	 * Return a int type-formatted string
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function int($values, $asDbValue=false){
		//We'll be using an 8 digit precision to keep it in line with the serial8 datatype for ID columns
		
		if($asDbValue)
			return Array('data_type'=>'numeric', 'numeric_precision'=>'8');
		else
			return 'numeric(8) not null default ' . (int)$values['default'];
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
			return 'timestamp';
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
			return 'text';
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
		
		return 'time';
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
		if($asDbValue)
			return Array('data_type'=>'character varying', 'character_maximum_length'=>'255');
		else
			return 'varchar(' . $values['precision'] . ')';
	}
	
	/*
	 * Return a 4 digit numeric type.  MySQL has a proprietary 'Year' type.
	 */
	public function year($values, $asDbValue=false){
		if($asDbValue)
			return Array('data_type'=>'numeric', 'numeric_precision'=>'4');
		else return 'numeric(4)'; 
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
	function fulltext($table, $spec){
		//$spec['name'] is the column we've created that holds all the words we want to index.
		//This is a coalesced collection of multiple columns if necessary
		$spec='create index ix_' . $table . '_' . $spec['name'] . ' on ' . $table . ' using gist(' . $spec['name'] . ');';
		
		return $spec;
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
				$text .= " LIMIT " . $sqlQuery->limit;
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
		parent::__construct();
	}
	
	public function __destroy() {
		//mysql_free_result($this->handle);
	}
	
	public function seek($row) {
		//return mysql_data_seek($this->handle, $row);
		//This is unnecessary in postgres.  You can just provide a row number with the fetch
		//command.
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