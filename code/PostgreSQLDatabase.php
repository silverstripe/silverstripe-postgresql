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
		
		
		echo 'sql: ' . $sql . '<br>';
		
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
		//$this->query("USE $this->database");

		//$this->tableList = $this->fieldList = $this->indexList = null;

		//if(mysql_select_db($this->database, $this->dbConn)) {
		//	$this->active = true;
		//	return true;
		//}
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
		$SQL_name = Convert::raw2sql($name);
		return $this->query("SHOW DATABASES LIKE '$SQL_name'")->value() ? true : false;
	}
	
	public function createTable($tableName, $fields = null, $indexes = null) {
		$fieldSchemas = $indexSchemas = "";
		if($fields) foreach($fields as $k => $v) $fieldSchemas .= "\"$k\" $v,\n";
		
		//if($indexes) foreach($indexes as $k => $v) $indexSchemas .= $this->getIndexSqlDefinition($k, $v) . ",\n";
		//we need to generate indexes like this: CREATE INDEX IX_vault_to_export ON vault (to_export);
		
		//If we have a fulltext search request, then we need to create a special column
		//for GiST searches
		echo 'creating table with these indexes:<pre>';
		print_r($indexes);
		echo '</pre>';
		$fulltexts='';
		foreach($indexes as $this_index){
			if($this_index['type']=='fulltext'){
				//ALTER TABLE tblMessages ADD COLUMN idxFTI tsvector;
				//CREATE INDEX ix_vault_indexed_words ON vault_indexed USING gist(words);
				
				//$fulltexts.=$this_index['name'] . ' tsvector, ';
			}
		}
		
		if($indexes) foreach($indexes as $k => $v) $indexSchemas .= $this->getIndexSqlDefinition($tableName, $k, $v) . "\n";
		
		$this->query("CREATE TABLE \"$tableName\" (
				\"ID\" SERIAL8 NOT NULL,
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
		//if($newIndexes) foreach($newIndexes as $k => $v) $alterList[] .= "ADD " . $this->getIndexSqlDefinition($tableName, $k, $v);
		if($alteredFields) foreach($alteredFields as $k => $v) $alterList[] .= "CHANGE \"$k\" \"$k\" $v";
		
		/*
		echo 'alterations:<pre>';
		print_r($newFields);
		echo '</pre>';
		*/
		
		//DB ABSTRACTION: we need to change the constraints to be a separate 'add' command,
		//see http://www.postgresql.org/docs/8.1/static/sql-altertable.html
		
		if($alteredIndexes) foreach($alteredIndexes as $k => $v) {
			$alterList[] .= "DROP INDEX \"$k\"";
			$alterList[] .= "ADD ". $this->getIndexSqlDefinition($tableName, $k, $v);
 		}

		if($alterList) {
			$alterations = implode(",\n", $alterList);
			$this->query("ALTER TABLE \"$tableName\" " . $alterations);
		}
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
		//if(!$this->runTableCheckCommand("VACUUM FULL \"$tableName\"")) {
		//	Database::alteration_message("Table $tableName: repaired","repaired");
		//	return $this->runTableCheckCommand("REPAIR TABLE \"$tableName\" USE_FRM");
		//} else {
		//	return true;
		//}
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
		// This wee function was built for MoT.  It will preserve the binary format of the content,
		// but change the character set
		/*
		$changes = $this->query("SELECT ID, \"$fieldName\" FROM \"$tableName\"")->map();
		*/

		$this->query("ALTER TABLE \"$tableName\" CHANGE \"$fieldName\" \"$fieldName\" $fieldSpec");

		// This wee function was built for MoT.  It will preserve the binary format of the content,
		// but change the character set
		/*
		echo "<li>Fixing " . sizeof($changes) . " page's contnet";
		foreach($changes as $id => $text) {
			$SQL_text = Convert::raw2sql($text);
			$this->query("UPDATE \"$tableName\" SET \"$fieldName\" = '$SQL_text' WHERE \"ID\" = '$id'");
		}
		*/
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
			$this->query("ALTER TABLE \"$tableName\" CHANGE \"$oldName\" \"$newName\" " . $fieldList[$oldName]);
		}
	}
	
	public function fieldList($table) {
		$fields = $this->query("SELECT b.attname FROM pg_class a 
			INNER JOIN pg_attribute b ON a.relfilenode=b.attrelid 
			WHERE a.relname='$table' 
			AND NOT b.attisdropped AND b.attnum>0")->column();
		
		$output = array();
		if($fields) foreach($fields as $field) {
			$output[$field] = true;
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
		//$this->query("ALTER TABLE \"$tableName\" ADD " . $this->getIndexSqlDefinition($indexName, $indexSpec));
		$this->query($this->getIndexSqlDefinition($tableName, $indexName, $indexSpec));
	}
	
	protected function getIndexSqlDefinition($tableName, $indexName, $indexSpec) {
	    //$indexSpec = trim($indexSpec);
	    //if($indexSpec[0] != '(') list($indexType, $indexFields) = explode(' ',$indexSpec,2);
	    //else $indexFields = $indexSpec;
	    //if(!isset($indexType)) {
		//	$indexType = 'create index';
		//}
		//CREATE INDEX IX_vault_to_export ON vault (to_export);
		
		echo 'index spec:<pre>';
		print_r($indexSpec);
		echo '</pre>';
		if(!isset($indexSpec['type'])){
			//It's not the best method, but to keep things simple, we've allowed unique indexes to be
			//specified as inline strings.  So now we need to detect 'unique (' as the first word, and
			//do things differently if that's the case.
			//The alternative is to force unique indexes to adopt the complex index method (below)
			
			$indexSpec=$indexSpec['sql'];
		
			$unique='';
			if(substr($indexSpec, 0, 8)=='unique ('){
				$unique='unique ';
				$indexSpec=substr($indexSpec, 8);
			}
			
			$indexSpec=trim($indexSpec, '()');
			$bits=explode(',', $indexSpec);
			$indexes="\"" . implode("\",\"", $bits) . "\"";
			echo 'the indexes are ' . $indexes . '<br>';
			return 'create ' . $unique . 'index ix_' . $tableName . '_' . $indexName . " ON \"" . $tableName . "\" (" . $indexes . ');';
		} else {
			//create a type-specific index
			if($indexSpec['type']=='fulltext'){
				//return 'create index ix_' . $tableName . '_' . $indexSpec['name'] . " ON \"" . $tableName . "\" USING gist(" . $indexSpec['name'] . ');';
				return '';
			}
		}
		
		
		//return "$indexType \"$indexName\" $indexFields";
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
    
		//$this->query("ALTER TABLE \"$tableName\" DROP INDEX \"$indexName\"");
		$this->query("DROP INDEX $indexName");
		$this->query("ALTER TABLE \"$tableName\" ADD $indexType \"$indexName\" $indexFields");
	}
	
	/**
	 * Return the list of indexes in a table.
	 * @param string $table The table name.
	 * @return array
	 */
	public function indexList($table) {
		/*$indexes = DB::query("SHOW INDEXES IN \"$table\"");
		
		foreach($indexes as $index) {
			$groupedIndexes[$index['Key_name']]['fields'][$index['Seq_in_index']] = $index['Column_name'];
			
			if($index['Index_type'] == 'FULLTEXT') {
				$groupedIndexes[$index['Key_name']]['type'] = 'fulltext ';
			} else if(!$index['Non_unique']) {
				$groupedIndexes[$index['Key_name']]['type'] = 'unique ';
			} else {
				$groupedIndexes[$index['Key_name']]['type'] = '';
			}
		}
		
		foreach($groupedIndexes as $index => $details) {
			ksort($details['fields']);
			$indexList[$index] = $details['type'] . '(' . implode(',',$details['fields']) . ')';
			
		}
		
		return $indexList;*/
		//Obtained by starting postgres with the -E option:
		$indexes=DB::query("SELECT c.relname as \"Name\",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' END as \"Type\",
  r.rolname as \"Owner\",
 c2.relname as \"Table\"
FROM pg_catalog.pg_class c
     JOIN pg_catalog.pg_roles r ON r.oid = c.relowner
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     LEFT JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
     LEFT JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid
WHERE c.relkind IN ('i','')
  AND n.nspname <> 'pg_catalog'
  AND n.nspname !~ '^pg_toast'
  AND pg_catalog.pg_table_is_visible(c.oid)
  AND c2.relname='$table' AND c.relkind='index';");
		
  		//DB ABSTRACTION: TODO: we are not getting actual index information here, just the basic existence stuff:
		foreach($indexes as $index) {
			/*$groupedIndexes[$index['Key_name']]['fields'][$index['Seq_in_index']] = $index['Column_name'];
			
			if($index['Index_type'] == 'FULLTEXT') {
				$groupedIndexes[$index['Key_name']]['type'] = 'fulltext ';
			} else if(!$index['Non_unique']) {
				$groupedIndexes[$index['Key_name']]['type'] = 'unique ';
			} else {
				$groupedIndexes[$index['Key_name']]['type'] = '';
			}*/
			$indexList[$index['Name']]=$index['Name'];
		}
		
		//foreach($groupedIndexes as $index => $details) {
			//ksort($details['fields']);
			//$indexList[$index] = $details['type'] . '(' . implode(',',$details['fields']) . ')';
			
		//}
		
		//echo 'index list: <pre>';
		//print_r($indexList);
		//echo '</pre>';
		/*
		echo "<p style='color:red; font-weight: bold;'>INDEX LIST TRIGGERED (LINE 375 POSTGRESQLDATABASE.PHP</p>";
		*/
		return $indexList;
		
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
	public function boolean($values){
		//Annoyingly, we need to do a good old fashioned switch here:
		($values['default']) ? $default='true' : $default='false';
		
		return 'boolean not null default ' . $default;
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
	public function decimal($values){
		//For reference, this is what typically gets passed to this function:
		//$parts=Array('datatype'=>'decimal', 'precision'=>"$this->wholeSize,$this->decimalSize");
		//DB::requireField($this->tableName, $this->name, "decimal($this->wholeSize,$this->decimalSize)");

		return 'decimal(' . (int)$values['precision'] . ')';
	}
	
	/**
	 * Return a enum type-formatted string
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function enum($values){
		//Enums are a bit different. We'll be creating a varchar(255) with a constraint of all the usual
		//enum options.
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
	public function float($values){
		//For reference, this is what typically gets passed to this function:
		//$parts=Array('datatype'=>'float');
		//DB::requireField($this->tableName, $this->name, "float");
		
		return 'float';
	}
	
	/**
	 * Return a int type-formatted string
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function int($values){
		//We'll be using an 8 digit precision to keep it in line with the serial8 datatype for ID columns
		return 'numeric(8) not null default ' . (int)$values['default'];
	}
	
	/**
	 * Return a datetime type-formatted string
	 * For PostgreSQL, we simply return the word 'timestamp', no other parameters are necessary
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function ssdatetime($values){
		//For reference, this is what typically gets passed to this function:
		//$parts=Array('datatype'=>'datetime');
		//DB::requireField($this->tableName, $this->name, $values);

		return 'timestamp';
	}
	
	/**
	 * Return a text type-formatted string
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function text($values){
		//For reference, this is what typically gets passed to this function:
		//$parts=Array('datatype'=>'mediumtext', 'character set'=>'utf8', 'collate'=>'utf8_general_ci');
		//DB::requireField($this->tableName, $this->name, "mediumtext character set utf8 collate utf8_general_ci");
		
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
	public function varchar($values){
		//For reference, this is what typically gets passed to this function:
		//$parts=Array('datatype'=>'varchar', 'precision'=>$this->size, 'character set'=>'utf8', 'collate'=>'utf8_general_ci');
		//DB::requireField($this->tableName, $this->name, "varchar($this->size) character set utf8 collate utf8_general_ci");
		
		return 'varchar(' . $values['precision'] . ')';
	}
	
	function escape_character($escape=false){
		if($escape)
			return "\\\"";
		else
			return "\"";
	}
	
	/**
	 * Create a fulltext search datatype for MySQL
	 *
	 * @param array $spec
	 */
	function fulltext($table, $spec){
		//CREATE INDEX ix_vault_indexed_words ON vault_indexed USING gist(words);
		//$spec['name'] is the column we've created that holds all the words we want to index.
		//This is a coalesced collection of multiple columns if necessary
		$spec='create index ix_' . $table . '_' . $spec['name'] . ' on ' . $table . ' using gist(' . $spec['name'] . ');';
		
		return $spec;
	}
	
	/**
	 * Returns true if this table exists
	 * @todo Make a proper implementation
	 */
	function hasTable($tableName) {
		return true;
	}
	
	/**
	 * Return enum values for the given field
	 * @todo Make a proper implementation
	 */
	function enumValuesForField($tableName, $fieldName) {
		return array('SiteTree','Page');
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