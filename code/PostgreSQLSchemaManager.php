<?php

/**
 * PostgreSQL schema manager
 * 
 * @package sapphire
 * @subpackage model
 */
class PostgreSQLSchemaManager extends DBSchemaManager {

	/**
	 * Identifier for this schema, used for configuring schema-specific table 
	 * creation options
	 */
	const ID = 'PostgreSQL';

	/**
	 * Instance of the database controller this schema belongs to
	 * 
	 * @var PostgreSQLDatabase
	 */
	protected $database = null;

	/**
	 * This holds a copy of all the constraint results that are returned
	 * via the function constraintExists().  This is a bit faster than
	 * repeatedly querying this column, and should allow the database
	 * to use it's built-in caching features for better queries.
	 *
	 * @var array
	 */
	protected static $cached_constraints = array();

	/**
	 *
	 * This holds a copy of all the queries that run through the function fieldList()
	 * This is one of the most-often called functions, and repeats itself a great deal in the unit tests.
	 *
	 * @var array
	 */
	protected static $cached_fieldlists = array();

	protected function indexKey($table, $index, $spec) {
		return $this->buildPostgresIndexName($table, $index);
	}

	/**
	 * Creates a postgres database, ignoring model_schema_as_database
	 * 
	 * @param string $name
	 */
	public function createPostgresDatabase($name) {
		$this->query("CREATE DATABASE \"$name\";");
	}

	public function createDatabase($name) {
		if(PostgreSQLDatabase::model_schema_as_database()) {
			$schemaName = $this->database->databaseToSchemaName($name);
			return $this->createSchema($schemaName);
		}
		return $this->createPostgresDatabase($name);
	}

	/**
	 * Determines if a postgres database exists, ignoring model_schema_as_database
	 * 
	 * @param string $name
	 * @return boolean
	 */
	public function postgresDatabaseExists($name) {
		$result = $this->preparedQuery("SELECT datname FROM pg_database WHERE datname = ?;", array($name));
		return $result->first() ? true : false;  
	}

	public function databaseExists($name) {
		if(PostgreSQLDatabase::model_schema_as_database()) {
			$schemaName = $this->database->databaseToSchemaName($name);
			return $this->schemaExists($schemaName);
		}
		return $this->postgresDatabaseExists($name);
	}

	/**
	 * Determines the list of all postgres databases, ignoring model_schema_as_database
	 * 
	 * @return array
	 */
	public function postgresDatabaseList() {
		return $this->query("SELECT datname FROM pg_database WHERE datistemplate=false;")->column();
	}

	public function databaseList() {
		if(PostgreSQLDatabase::model_schema_as_database()) {
			$schemas = $this->schemaList();
			$names = array();
			foreach($schemas as $schema) {
				$names[] = $this->database->schemaToDatabaseName($schema);
			}
			return array_unique($names);
		}
		return $this->postgresDatabaseList();
	}
	/**
	 * Drops a postgres database, ignoring model_schema_as_database
	 * 
	 * @param string $name
	 */
	public  function dropPostgresDatabase($name) {
		$nameSQL = $this->database->escapeIdentifier($name);
		$this->query("DROP DATABASE $nameSQL;");
	}

	public function dropDatabase($name) {
		if(PostgreSQLDatabase::model_schema_as_database()) {
			$schemaName = $this->database->databaseToSchemaName($name);
			return $this->dropSchema($schemaName);
		}
		$this->dropPostgresDatabase($name);
	}

	/**
	 * Returns true if the schema exists in the current database
	 * 
	 * @param string $name
	 * @return boolean
	 */
	public function schemaExists($name) {
		return $this->preparedQuery(
			"SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname = ?;",
			array($name)
		)->first() ? true : false;
	}

	/**
	 * Creates a schema in the current database
	 * 
	 * @param string $name
	 */
	public function createSchema($name) {
		$nameSQL = $this->database->escapeIdentifier($name);
		$this->query("CREATE SCHEMA $nameSQL;");
	}

	/**
	 * Drops a schema from the database. Use carefully!
	 * 
	 * @param string $name
	 */
	public function dropSchema($name) {
		$nameSQL = $this->database->escapeIdentifier($name);
		$this->query("DROP SCHEMA $nameSQL CASCADE;");
	}

	/**
	 * Returns the list of all available schemas on the current database
	 * 
	 * @return array
	 */
	public function schemaList() { 
		return $this->query("
			SELECT nspname
			FROM pg_catalog.pg_namespace
			WHERE nspname <> 'information_schema' AND nspname !~ E'^pg_'"
		)->column();
	}

	public function createTable($table, $fields = null, $indexes = null, $options = null, $advancedOptions = null) {

		$fieldSchemas = $indexSchemas = "";
		if($fields) foreach($fields as $k => $v) {
			$fieldSchemas .= "\"$k\" $v,\n";
		}
		if(!empty($options[self::ID])) {
			$addOptions = $options[self::ID];
		} elseif (!empty($options[get_class($this)])) {
			Deprecation::notice('3.2', 'Use PostgreSQLSchemaManager::ID for referencing postgres-specific table creation options');
			$addOptions = $options[get_class($this)];
		} else {
			$addOptions = null;
		}

		//First of all, does this table already exist
		$doesExist = $this->hasTable($table);
		if($doesExist) {
			// Table already exists, just return the name, in line with baseclass documentation.
			return $table;
		}

		//If we have a fulltext search request, then we need to create a special column
		//for GiST searches
		$fulltexts = '';
		$triggers = '';
		if($indexes) {
			foreach($indexes as $name => $this_index){
				if(is_array($this_index) && $this_index['type'] == 'fulltext') {
					$ts_details = $this->fulltext($this_index, $table, $name);
					$fulltexts .= $ts_details['fulltexts'] . ', ';
					$triggers .= $ts_details['triggers'];
				}
			}
		}

		if($indexes) foreach($indexes as $k => $v) {
			$indexSchemas .= $this->getIndexSqlDefinition($table, $k, $v) . "\n";
		}

		//Do we need to create a tablespace for this item?
		if($advancedOptions && isset($advancedOptions['tablespace'])){
			$this->createOrReplaceTablespace(
				$advancedOptions['tablespace']['name'],
				$advancedOptions['tablespace']['location']
			);
			$tableSpace = ' TABLESPACE ' . $advancedOptions['tablespace']['name'];
		} else
			$tableSpace = '';

		$this->query("CREATE TABLE \"$table\" (
				$fieldSchemas
				$fulltexts
				primary key (\"ID\")
			)$tableSpace; $indexSchemas $addOptions");

		if($triggers!=''){
			$this->query($triggers);
		}

		//If we have a partitioning requirement, we do that here:
		if($advancedOptions && isset($advancedOptions['partitions'])){
			$this->createOrReplacePartition($table, $advancedOptions['partitions'], $indexes, $advancedOptions);
		}

		//Lastly, clustering goes here:
		if($advancedOptions && isset($advancedOptions['cluster'])){
			$this->query("CLUSTER \"$table\" USING \"{$advancedOptions['cluster']}\";");
		}

		return $table;
	}

	/**
	 * Builds the internal Postgres index name given the silverstripe table and index name
	 * 
	 * @param string $tableName
	 * @param string $indexName 
	 * @param string $prefix The optional prefix for the index. Defaults to "ix" for indexes.
	 * @return string The postgres name of the index
	 */
	protected function buildPostgresIndexName($tableName, $indexName, $prefix = 'ix') {

		// Assume all indexes also contain the table name
		// MD5 the table/index name combo to keep it to a fixed length.
		// Exclude the prefix so that the trigger name can be easily generated from the index name
		$indexNamePG = "{$prefix}_" . md5("{$tableName}_{$indexName}");

		// Limit to 63 characters
		if (strlen($indexNamePG) > 63) {
			return substr($indexNamePG, 0, 63);
		} else {
			return $indexNamePG;
		}
	}

	/**
	 * Builds the internal Postgres trigger name given the silverstripe table and trigger name
	 * 
	 * @param string $tableName
	 * @param string $triggerName
	 * @return string The postgres name of the trigger
	 */
	function buildPostgresTriggerName($tableName, $triggerName) {
		// Kind of cheating, but behaves the same way as indexes
		return $this->buildPostgresIndexName($tableName, $triggerName, 'ts');
	}

	public function alterTable($table, $newFields = null, $newIndexes = null, $alteredFields = null, $alteredIndexes = null, $alteredOptions = null, $advancedOptions = null) {

		$alterList = array();
		if($newFields) foreach($newFields as $fieldName => $fieldSpec) {
			$alterList[] = "ADD \"$fieldName\" $fieldSpec";
		}

		if ($alteredFields) foreach ($alteredFields as $indexName => $indexSpec) {
			$val = $this->alterTableAlterColumn($table, $indexName, $indexSpec);
			if (!empty($val)) $alterList[] = $val;
		}

		//Do we need to do anything with the tablespaces?
		if($alteredOptions && isset($advancedOptions['tablespace'])){
			$this->createOrReplaceTablespace($advancedOptions['tablespace']['name'], $advancedOptions['tablespace']['location']);
			$this->query("ALTER TABLE \"$table\" SET TABLESPACE {$advancedOptions['tablespace']['name']};");
		}

		//DB ABSTRACTION: we need to change the constraints to be a separate 'add' command,
		//see http://www.postgresql.org/docs/8.1/static/sql-altertable.html
		$alterIndexList = array();
		//Pick up the altered indexes here:
		$fieldList = $this->fieldList($table);
		$fulltexts = false;
		$drop_triggers = false;
		$triggers = false;
		if($alteredIndexes) foreach($alteredIndexes as $indexName=>$indexSpec) {

			$indexSpec = $this->parseIndexSpec($indexName, $indexSpec);
			$indexNamePG = $this->buildPostgresIndexName($table, $indexName);

			if($indexSpec['type']=='fulltext') {
				//For full text indexes, we need to drop the trigger, drop the index, AND drop the column

				//Go and get the tsearch details:
				$ts_details = $this->fulltext($indexSpec, $table, $indexName);

				//Drop this column if it already exists:

				//No IF EXISTS option is available for Postgres <9.0
				if(array_key_exists($ts_details['ts_name'], $fieldList)){
					$fulltexts.="ALTER TABLE \"{$table}\" DROP COLUMN \"{$ts_details['ts_name']}\";";
				}

				// We'll execute these later:
				$triggerNamePG = $this->buildPostgresTriggerName($table, $indexName);
				$drop_triggers.= "DROP TRIGGER IF EXISTS \"$triggerNamePG\" ON \"$table\";";
				$fulltexts .= "ALTER TABLE \"{$table}\" ADD COLUMN {$ts_details['fulltexts']};";
				$triggers .= $ts_details['triggers'];
			}

			// Create index action (including fulltext)
			$alterIndexList[] = "DROP INDEX IF EXISTS \"$indexNamePG\";";
			$createIndex = $this->getIndexSqlDefinition($table, $indexName, $indexSpec);
			if($createIndex!==false) $alterIndexList[] = $createIndex;
		}

		//Add the new indexes:
		if($newIndexes) foreach($newIndexes as $indexName => $indexSpec){

			$indexSpec = $this->parseIndexSpec($indexName, $indexSpec);
			$indexNamePG = $this->buildPostgresIndexName($table, $indexName);
			//If we have a fulltext search request, then we need to create a special column
			//for GiST searches
			//Pick up the new indexes here:
			if($indexSpec['type']=='fulltext') {
				$ts_details=$this->fulltext($indexSpec, $table, $indexName);
				if(!isset($fieldList[$ts_details['ts_name']])){
					$fulltexts.="ALTER TABLE \"{$table}\" ADD COLUMN {$ts_details['fulltexts']};";
					$triggers.=$ts_details['triggers'];
				}
			}

			//Check that this index doesn't already exist:
			$indexes=$this->indexList($table);
			if(isset($indexes[$indexName])){
				$alterIndexList[] = "DROP INDEX IF EXISTS \"$indexNamePG\";";
			}

			$createIndex=$this->getIndexSqlDefinition($table, $indexName, $indexSpec);
			if($createIndex!==false)
				$alterIndexList[] = $createIndex;
		}

		if($alterList) {
			$alterations = implode(",\n", $alterList);
			$this->query("ALTER TABLE \"$table\" " . $alterations);
		}

		//Do we need to create a tablespace for this item?
		if($advancedOptions && isset($advancedOptions['extensions']['tablespace'])){
			$extensions=$advancedOptions['extensions'];
			$this->createOrReplaceTablespace($extensions['tablespace']['name'], $extensions['tablespace']['location']);
		}

		if($alteredOptions && isset($this->class) && isset($alteredOptions[$this->class])) {
			$this->query(sprintf("ALTER TABLE \"%s\" %s", $table, $alteredOptions[$this->class]));
			Database::alteration_message(
				sprintf("Table %s options changed: %s", $table, $alteredOptions[$this->class]),
				"changed"
			);
		}

		//Create any fulltext columns and triggers here:
		if($fulltexts) $this->query($fulltexts);
		if($drop_triggers) $this->query($drop_triggers);

		if($triggers) {
			$this->query($triggers);

			$triggerbits=explode(';', $triggers);
			foreach($triggerbits as $trigger){
				$trigger_fields=$this->triggerFieldsFromTrigger($trigger);

				if($trigger_fields){
					//We need to run a simple query to force the database to update the triggered columns
					$this->query("UPDATE \"{$table}\" SET \"{$trigger_fields[0]}\"=\"$trigger_fields[0]\";");
				}
			}
		}

		foreach($alterIndexList as $alteration) $this->query($alteration);

		//If we have a partitioning requirement, we do that here:
		if($advancedOptions && isset($advancedOptions['partitions'])){
			$this->createOrReplacePartition($table, $advancedOptions['partitions']);
		}

		//Lastly, clustering goes here:
		if ($advancedOptions && isset($advancedOptions['cluster'])) {
			$clusterIndex = $this->buildPostgresIndexName($table, $advancedOptions['cluster']);
			$this->query("CLUSTER \"$table\" USING \"$clusterIndex\";");
		} else {
			//Check that clustering is not on this table, and if it is, remove it:

			//This is really annoying.  We need the oid of this table:
			$stats = $this->preparedQuery(
				"SELECT relid FROM pg_stat_user_tables WHERE relname = ?;",
				array($table)
			)->first();
			$oid=$stats['relid'];

			//Now we can run a long query to get the clustered status:
			//If anyone knows a better way to get the clustered status, then feel free to replace this!
			$clustered = $this->preparedQuery("
				SELECT c2.relname, i.indisclustered 
				FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i
				WHERE c.oid = ? AND c.oid = i.indrelid AND i.indexrelid = c2.oid AND indisclustered='t';",
				array($oid)
			)->first();

			if($clustered) {
				$this->query("ALTER TABLE \"$table\" SET WITHOUT CLUSTER;");
			}
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

		if(sizeof($matches)==0) return '';

		if($matches[1]=='serial8') return '';

		if(isset($matches[1])) {
			$alterCol = "ALTER COLUMN \"$colName\" TYPE $matches[1]\n";

			// SET null / not null
			if(!empty($matches[2])) {
				$alterCol .= ",\nALTER COLUMN \"$colName\" SET $matches[2]";
			}

			// SET default (we drop it first, for reasons of precaution)
			if(!empty($matches[3])) {
				$alterCol .= ",\nALTER COLUMN \"$colName\" DROP DEFAULT";
				$alterCol .= ",\nALTER COLUMN \"$colName\" SET $matches[3]";
			}

			// SET check constraint (The constraint HAS to be dropped)
			$existing_constraint=$this->query("SELECT conname FROM pg_constraint WHERE conname='{$tableName}_{$colName}_check';")->value();
			if(isset($matches[4])) {
				//Take this new constraint and see what's outstanding from the target table:
				$constraint_bits=explode('(', $matches[4]);
				$constraint_values=trim($constraint_bits[2], ')');
				$constraint_values_bits=explode(',', $constraint_values);
				$default=trim($constraint_values_bits[0], " '");

				//Now go and convert anything that's not in this list to 'Page'
				//We have to run this as a query, not as part of the alteration queries due to the way they are constructed.
				$updateConstraint='';
				$updateConstraint.="UPDATE \"{$tableName}\" SET \"$colName\"='$default' WHERE \"$colName\" NOT IN ($constraint_values);";
				if($this->hasTable("{$tableName}_Live")) {
					$updateConstraint.="UPDATE \"{$tableName}_Live\" SET \"$colName\"='$default' WHERE \"$colName\" NOT IN ($constraint_values);";
				}
				if($this->hasTable("{$tableName}_versions")) {
					$updateConstraint.="UPDATE \"{$tableName}_versions\" SET \"$colName\"='$default' WHERE \"$colName\" NOT IN ($constraint_values);";
				}

				$this->query($updateConstraint);
			}

			//First, delete any existing constraint on this column, even if it's no longer an enum
			if($existing_constraint) {
				$alterCol .= ",\nDROP CONSTRAINT \"{$tableName}_{$colName}_check\"";
			}

			//Now create the constraint (if we've asked for one)
			if(!empty($matches[4])) {
				$alterCol .= ",\nADD CONSTRAINT \"{$tableName}_{$colName}_check\" $matches[4]";
			}
		}

		return isset($alterCol) ? $alterCol : '';
	}

	public function renameTable($oldTableName, $newTableName) {
		$this->query("ALTER TABLE \"$oldTableName\" RENAME TO \"$newTableName\"");
		unset(self::$cached_fieldlists[$oldTableName]);
	}

	public function checkAndRepairTable($tableName) {
		$this->query("VACUUM FULL ANALYZE \"$tableName\"");
		$this->query("REINDEX TABLE \"$tableName\"");
		return true;
	}

	public function createField($table, $field, $spec) {
		$this->query("ALTER TABLE \"$table\" ADD \"$field\" $spec");
	}

	/**
	 * Change the database type of the given field.
	 * 
	 * @param string $tableName The name of the tbale the field is in.
	 * @param string $fieldName The name of the field to change.
	 * @param string $fieldSpec The new field specification
	 */
	public function alterField($tableName, $fieldName, $fieldSpec) {
		$this->query("ALTER TABLE \"$tableName\" CHANGE \"$fieldName\" \"$fieldName\" $fieldSpec");
	}

	public function renameField($tableName, $oldName, $newName) {
		$fieldList = $this->fieldList($tableName);
		if(array_key_exists($oldName, $fieldList)) {
			$this->query("ALTER TABLE \"$tableName\" RENAME COLUMN \"$oldName\" TO \"$newName\"");

			//Remove this from the cached list:
			unset(self::$cached_fieldlists[$tableName]);
		}
	}

	public function fieldList($table) {
		//Query from http://www.alberton.info/postgresql_meta_info.html
		//This gets us more information than we need, but I've included it all for the moment....

		//if(!isset(self::$cached_fieldlists[$table])){
			$fields = $this->preparedQuery("
				SELECT ordinal_position, column_name, data_type, column_default,
				is_nullable, character_maximum_length, numeric_precision, numeric_scale
				FROM information_schema.columns WHERE table_name = ? and table_schema = ?
				ORDER BY ordinal_position;",
				array($table, $this->database->currentSchema())
			);

			$output = array();
			if($fields) foreach($fields as $field) {

				switch($field['data_type']){
					case 'character varying':
						//Check to see if there's a constraint attached to this column:
						//$constraint=$this->query("SELECT conname,pg_catalog.pg_get_constraintdef(r.oid, true) FROM pg_catalog.pg_constraint r WHERE r.contype = 'c' AND conname='" . $table . '_' . $field['column_name'] . "_check' ORDER BY 1;")->first();
						$constraint = $this->constraintExists($table . '_' . $field['column_name'] . '_check');
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

		//	self::$cached_fieldlists[$table]=$output;
		//}

		//return self::$cached_fieldlists[$table];

		return $output;
	}

	function clearCachedFieldlist($tableName=false){
		if($tableName) unset(self::$cached_fieldlists[$tableName]);
		else self::$cached_fieldlists=array();
		return true;
	}

	/**
	 * Create an index on a table.
	 * 
	 * @param string $tableName The name of the table.
	 * @param string $indexName The name of the index.
	 * @param string $indexSpec The specification of the index, see Database::requireIndex() for more details.
	 */
	public function createIndex($tableName, $indexName, $indexSpec) {
		$createIndex = $this->getIndexSqlDefinition($tableName, $indexName, $indexSpec);
		if($createIndex !== false) $this->query($createIndex);
	}

	/*
	 * @todo - factor out? Is DBSchemaManager::convertIndexSpec sufficient?
	public function convertIndexSpec($indexSpec, $asDbValue=false, $table=''){

		if(!$asDbValue){
			if(is_array($indexSpec)){
				//Here we create a db-specific version of whatever index we need to create.
				switch($indexSpec['type']){
					case 'fulltext':
						$indexSpec='fulltext (' . $indexSpec['value'] . ')';
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
			$indexSpec = $this->buildPostgresIndexName($table, $indexSpec);
		}
		return $indexSpec;
	}*/

	protected function getIndexSqlDefinition($tableName, $indexName, $indexSpec, $asDbValue=false) {

		//TODO: create table partition support
		//TODO: create clustering options

		//NOTE: it is possible for *_renamed tables to have indexes whose names are not updates
		//Therefore, we now check for the existance of indexes before we create them.
		//This is techically a bug, since new tables will not be indexed.

		// If requesting the definition rather than the DDL
		if($asDbValue) {
			$indexName=trim($indexName, '()');
			return $indexName;
		}

		// Determine index name
		$tableCol = $this->buildPostgresIndexName($tableName, $indexName);

		// Consolidate/Cleanup spec into array format
		$indexSpec = $this->parseIndexSpec($indexName, $indexSpec);

		//Misc options first:
		$fillfactor = $where = '';
		if (isset($indexSpec['fillfactor'])) {
			$fillfactor = 'WITH (FILLFACTOR = ' . $indexSpec['fillfactor'] . ')';
		}
		if (isset($indexSpec['where'])) {
			$where = 'WHERE ' . $indexSpec['where'];
		}

		//create a type-specific index
		// NOTE:  hash should be removed.  This is only here to demonstrate how other indexes can be made
		// NOTE: Quote the index name to preserve case sensitivity 
		switch ($indexSpec['type']) {
			case 'fulltext':
				// @see fulltext() for the definition of the trigger that ts_$IndexName uses for fulltext searching
				$clusterMethod = PostgreSQLDatabase::default_fts_cluster_method();
				$spec = "create index \"$tableCol\" ON \"$tableName\" USING $clusterMethod(\"ts_" . $indexName . "\") $fillfactor $where";
				break;

			case 'unique':
				$spec = "create unique index \"$tableCol\" ON \"$tableName\" (" . $indexSpec['value'] . ") $fillfactor $where";
				break;

			case 'btree':
				$spec = "create index \"$tableCol\" ON \"$tableName\" USING btree (" . $indexSpec['value'] . ") $fillfactor $where";
				break;

			case 'hash':
				//NOTE: this is not a recommended index type
				$spec = "create index \"$tableCol\" ON \"$tableName\" USING hash (" . $indexSpec['value'] . ") $fillfactor $where";
				break;

			case 'index':
			//'index' is the same as default, just a normal index with the default type decided by the database.
			default:
				$spec = "create index \"$tableCol\" ON \"$tableName\" (" . $indexSpec['value'] . ") $fillfactor $where";
		}
		return trim($spec) . ';';
	}

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

		$this->query("DROP INDEX \"$indexName\"");
		$this->query("ALTER TABLE \"$tableName\" ADD $indexType \"$indexName\" $indexFields");
	}

	/**
	 * Given a trigger name attempt to determine the columns upon which it acts
	 *
	 * @param string $triggerName Postgres trigger name
	 * @return array List of columns
	 */
	protected function extractTriggerColumns($triggerName) {
		$trigger = $this->preparedQuery(
			"SELECT tgargs FROM pg_catalog.pg_trigger WHERE tgname = ?",
			array($triggerName)
		)->first();

		// Option 1: output as a string
		if(strpos($trigger['tgargs'],'\000') !== false) {
			$argList = explode('\000', $trigger['tgargs']);
			array_pop($argList);

		// Option 2: hex-encoded (not sure why this happens, depends on PGSQL config)
		} else {
			$bytes = str_split($trigger['tgargs'],2);
			$argList = array();
			$nextArg = "";
			foreach($bytes as $byte) {
				if($byte == "00") {
					$argList[] = $nextArg;
					$nextArg = "";
				} else {
					$nextArg .= chr(hexdec($byte));
				}
			}
		}

		// Drop first two arguments (trigger name and config name) and implode into nice list
		return array_slice($argList, 2);
	}

	public function indexList($table) {
		//Retrieve a list of indexes for the specified table
		$indexes = $this->preparedQuery("
			SELECT tablename, indexname, indexdef
			FROM pg_catalog.pg_indexes
			WHERE tablename = ? AND schemaname = ?;",
			array($table, $this->database->currentSchema())
		);

		$indexList = array();
		foreach($indexes as $index) {
			// Key for the indexList array.  Differs from other DB implementations, which is why
			// requireIndex() needed to be overridden
			$indexName = $index['indexname']; 

			//We don't actually need the entire created command, just a few bits:
			$type = '';

			//Check for uniques:
			if(substr($index['indexdef'], 0, 13)=='CREATE UNIQUE') {
				$type = 'unique';
			}

			//check for hashes, btrees etc:
			if(strpos(strtolower($index['indexdef']), 'using hash ')!==false) {
				$type = 'hash';
			}

			//TODO: Fix me: btree is the default index type:
			//if(strpos(strtolower($index['indexdef']), 'using btree ')!==false)
			//	$prefix='using btree ';

			if(strpos(strtolower($index['indexdef']), 'using rtree ')!==false) {
				$type = 'rtree';
			}

			// For fulltext indexes we need to extract the columns from another source
			if (stristr($index['indexdef'], 'using gin')) {
				$type = 'fulltext';
				// Extract trigger information from postgres
				$triggerName = preg_replace('/^ix_/', 'ts_', $index['indexname']);
				$columns = $this->extractTriggerColumns($triggerName);
				$columnString = $this->implodeColumnList($columns);
			} else {
				$columnString = $this->quoteColumnSpecString($index['indexdef']);
			}

			$indexList[$indexName] = $this->parseIndexSpec($index, array(
				'name' => $indexName, // Not the correct name in the PHP, as this will be a mangled postgres-unique code
				'value' => $columnString,
				'type' => $type
			));
		}

		return $indexList;

	}

	public function tableList() {
		$tables = array();
		$result = $this->preparedQuery(
			"SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = ? AND tablename NOT ILIKE 'pg\\\_%' AND tablename NOT ILIKE 'sql\\\_%'",
			array($this->database->currentSchema())
		);
		foreach($result as $record) {
			$table = reset($record);
			$tables[strtolower($table)] = $table;
		}
		return $tables;
	}

	/**
	 * Find out what the constraint information is, given a constraint name.
	 * We also cache this result, so the next time we don't need to do a
	 * query all over again.
	 *
	 * @param string $constraint
	 */
	protected function constraintExists($constraint){
		if(!isset(self::$cached_constraints[$constraint])){
			$exists = $this->preparedQuery("
				SELECT conname,pg_catalog.pg_get_constraintdef(r.oid, true)
				FROM pg_catalog.pg_constraint r WHERE r.contype = 'c' AND conname = ? ORDER BY 1;",
				array($constraint)
			)->first();
			self::$cached_constraints[$constraint]=$exists;
		}

		return self::$cached_constraints[$constraint];
	}

	/**
	 * A function to return the field names and datatypes for the particular table
	 * 
	 * @param string $tableName
	 * @return array List of columns an an associative array with the keys Column and DataType
	 */
	public function tableDetails($tableName) {
		$query = "SELECT a.attname as \"Column\", pg_catalog.format_type(a.atttypid, a.atttypmod) as \"Datatype\"
				FROM pg_catalog.pg_attribute a
				WHERE a.attnum > 0 AND NOT a.attisdropped AND a.attrelid = (
					SELECT c.oid
					FROM pg_catalog.pg_class c
					LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
					WHERE c.relname = ? AND pg_catalog.pg_table_is_visible(c.oid) AND n.nspname = ?
				);";

		$result = $this->preparedQuery($query, $tableName, $this->database->currentSchema());

		$table = array();
		while($row = pg_fetch_assoc($result)) {
			$table[] = array(
				'Column' => $row['Column'],
				'DataType' => $row['DataType']
			);
		}

		return $table;
	}

	/**
	 * Pass a legit trigger name and it will be dropped
	 * This assumes that the trigger has been named in a unique fashion
	 *
	 * @param string $triggerName Name of the trigger
	 * @param string $tableName Name of the table
	 */
	protected function dropTrigger($triggerName, $tableName){
		$exists = $this->preparedQuery("
			SELECT trigger_name
			FROM information_schema.triggers
			WHERE trigger_name = ? AND trigger_schema = ?;",
			array($triggerName, $this->database->currentSchema())
		)->first();
		if($exists){
			$this->query("DROP trigger IF EXISTS $triggerName ON \"$tableName\";");
		}
	}

	/**
	 * This will return the fields that the trigger is monitoring
	 *
	 * @param string $trigger Name of the trigger
	 * @return array
	 */
	protected function triggerFieldsFromTrigger($trigger) {
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
		} else {
			return false;
		}
	}

	/**
	 * Return a boolean type-formatted string
	 *
	 * @param array $values Contains a tokenised list of info about this data type
	 * @param boolean $asDbValue
	 * @return string
	 */
	public function boolean($values, $asDbValue=false){
		//Annoyingly, we need to do a good ol' fashioned switch here:
		$default = $values['default'] ? '1' : '0';

		if(!isset($values['arrayValue'])) {
			$values['arrayValue']='';
		}

		if($asDbValue) {
			return array('data_type'=>'smallint');
		} 

		if($values['arrayValue'] != '') {
			$default = '';
		} else {
			$default = ' default ' . (int)$values['default'];
		}
		return "smallint{$values['arrayValue']}" . $default;
	}

	/**
	 * Return a date type-formatted string
	 *
	 * @param array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function date($values){

		if(!isset($values['arrayValue'])) {
			$values['arrayValue']='';
		}

		return "date{$values['arrayValue']}";
	}

	/**
	 * Return a decimal type-formatted string
	 *
	 * @param array $values Contains a tokenised list of info about this data type
	 * @param boolean $asDbValue
	 * @return string
	 */
	public function decimal($values, $asDbValue=false){

		if(!isset($values['arrayValue'])) {
			$values['arrayValue']='';
		}

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

		if($asDbValue) {
			return array('data_type' => 'numeric', 'precision' => $precision);
		} else {
			return "decimal($precision){$values['arrayValue']}$defaultValue";
		}
	}

	/**
	 * Return a enum type-formatted string
	 *
	 * @param array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function enum($values){
		//Enums are a bit different. We'll be creating a varchar(255) with a constraint of all the usual enum options.
		//NOTE: In this one instance, we are including the table name in the values array
		if(!isset($values['arrayValue'])) {
			$values['arrayValue']='';
		}

		if($values['arrayValue']!='') {
			$default = '';
		} else {
			$default = " default '{$values['default']}'";
		}

		return "varchar(255){$values['arrayValue']}" . $default . " check (\"" . $values['name'] . "\" in ('" . implode('\', \'', $values['enums']) . "'))";

	}

	/**
	 * Return a float type-formatted string
	 *
	 * @param array $values Contains a tokenised list of info about this data type
	 * @param boolean $asDbValue
	 * @return string
	 */
	public function float($values, $asDbValue = false){
		if(!isset($values['arrayValue'])) {
			$values['arrayValue']='';
		}

		if($asDbValue) {
			return array('data_type' => 'double precision');
		} else {
			return "float{$values['arrayValue']}";
		}
	}

	/**
	 * Return a float type-formatted string cause double is not supported
	 *
	 * @param array $values Contains a tokenised list of info about this data type
	 * @param boolean $asDbValue
	 * @return string
	 */
	public function double($values, $asDbValue=false){
		return $this->float($values, $asDbValue);
	}

	/**
	 * Return a int type-formatted string
	 *
	 * @param array $values Contains a tokenised list of info about this data type
	 * @param boolean $asDbValue
	 * @return string
	 */
	public function int($values, $asDbValue = false){

		if(!isset($values['arrayValue'])) {
			$values['arrayValue']='';
		}

		if($asDbValue) {
			return Array('data_type'=>'integer', 'precision'=>'32');
		} 

		if($values['arrayValue']!='') {
			$default='';
		} else {
			$default=' default ' . (int)$values['default'];
		}

		return "integer{$values['arrayValue']}" . $default;
	}

	/**
	 * Return a bigint type-formatted string
	 *
	 * @param array $values Contains a tokenised list of info about this data type
	 * @param boolean $asDbValue
	 * @return string
	 */
	public function bigint($values, $asDbValue = false){

		if(!isset($values['arrayValue'])) {
			$values['arrayValue']='';
		}

		if($asDbValue) {
			return Array('data_type'=>'bigint', 'precision'=>'64');
		} 

		if($values['arrayValue']!='') {
			$default='';
		} else {
			$default=' default ' . (int)$values['default'];
		}

		return "bigint{$values['arrayValue']}" . $default;
	}

	/**
	 * Return a datetime type-formatted string
	 * For PostgreSQL, we simply return the word 'timestamp', no other parameters are necessary
	 *
	 * @param array $values Contains a tokenised list of info about this data type
	 * @param boolean $asDbValue
	 * @return string
	 */
	public function SS_Datetime($values, $asDbValue = false){

		if(!isset($values['arrayValue'])) {
			$values['arrayValue']='';
		}

		if($asDbValue) {
			return array('data_type'=>'timestamp without time zone');
		} else {
			return "timestamp{$values['arrayValue']}";
		}
	}

	/**
	 * Return a text type-formatted string
	 *
	 * @param array $values Contains a tokenised list of info about this data type
	 * @param boolean $asDbValue
	 * @return string
	 */
	public function text($values, $asDbValue = false){

		if(!isset($values['arrayValue'])) {
			$values['arrayValue'] = '';
		}

		if($asDbValue) {
			return array('data_type'=>'text');
		} else {
			return "text{$values['arrayValue']}";
		}
	}

	/**
	 * Return a time type-formatted string
	 *
	 * @param array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function time($values){
		if(!isset($values['arrayValue'])) {
			$values['arrayValue'] = '';
		}

		return "time{$values['arrayValue']}";
	}

	/**
	 * Return a varchar type-formatted string
	 *
	 * @param array $values Contains a tokenised list of info about this data type
	 * @param boolean $asDbValue
	 * @return string
	 */
	public function varchar($values, $asDbValue=false){

		if(!isset($values['arrayValue'])) {
			$values['arrayValue'] = '';
		}

		if(!isset($values['precision'])) {
			$values['precision'] = 255;
		}

		if($asDbValue) {
			return array('data_type'=>'varchar', 'precision'=>$values['precision']);
		} else {
			return "varchar({$values['precision']}){$values['arrayValue']}";
		}
	}

	/*
	 * Return a 4 digit numeric type.  MySQL has a proprietary 'Year' type.
	 * For Postgres, we'll use a 4 digit numeric
	 * 
	 * @param array $values Contains a tokenised list of info about this data type
	 * @param boolean $asDbValue
	 * @return string
	 */
	public function year($values, $asDbValue = false){

		if(!isset($values['arrayValue'])) {
			$values['arrayValue'] = '';
		}

		//TODO: the DbValue result does not include the numeric_scale option (ie, the ,0 value in 4,0)
		if($asDbValue) {
			return array('data_type'=>'decimal', 'precision'=>'4');
		} else {
			return "decimal(4,0){$values['arrayValue']}";
		}
	}

	/**
	 * Create a fulltext search datatype for PostgreSQL
	 * This will also return a trigger to be applied to this table
	 *
	 * @todo: create custom functions to allow weighted searches
	 *
	 * @param array $this_index Index specification for the fulltext index
	 * @param string $tableName
	 * @param string $name
	 * @param array $spec
	 */
	protected function fulltext($this_index, $tableName, $name){
		//For full text search, we need to create a column for the index
		$columns = $this->quoteColumnSpecString($this_index['value']);

		$fulltexts = "\"ts_$name\" tsvector";
		$triggerName = $this->buildPostgresTriggerName($tableName, $name);
		$language = PostgreSQLDatabase::search_language();

		$this->dropTrigger($triggerName, $tableName);
		$triggers = "CREATE TRIGGER \"$triggerName\" BEFORE INSERT OR UPDATE
					ON \"$tableName\" FOR EACH ROW EXECUTE PROCEDURE
					tsvector_update_trigger(\"ts_$name\", 'pg_catalog.$language', $columns);";

		return array(
			'name' => $name,
			'ts_name' => "ts_{$name}",
			'fulltexts' => $fulltexts,
			'triggers' => $triggers
		);
	}

	public function IdColumn($asDbValue = false, $hasAutoIncPK = true){
		if($asDbValue) return 'bigint';
		else return 'serial8 not null';
	}

	public function hasTable($tableName) {
		$result = $this->preparedQuery(
			"SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = ? AND tablename = ?;",
			array($this->database->currentSchema(), $tableName)
		);
		return ($result->numRecords() > 0);
	}

	/**
	 * Returns the values of the given enum field
	 *
	 * @todo Make a proper implementation
	 *
	 * @param string $tableName Name of table to check
	 * @param string $fieldName name of enum field to check
	 * @return array List of enum values
	 */
	public function enumValuesForField($tableName, $fieldName) {
		//return array('SiteTree','Page');
		$constraints = $this->constraintExists("{$tableName}_{$fieldName}_check");
		if($constraints) {
			return $this->enumValuesFromConstraint($constraints['pg_get_constraintdef']);
		} else {
			return array();
		}
	}

	/**
	 * Get the actual enum fields from the constraint value:
	 *
	 * @param string $constraint
	 * @return array
	 */
	protected function enumValuesFromConstraint($constraint){
		$constraint = substr($constraint, strpos($constraint, 'ANY (ARRAY[')+11);
		$constraint = substr($constraint, 0, -11);
		$constraints = array();
		$segments = explode(',', $constraint);
		foreach($segments as $this_segment){
			$bits = preg_split('/ *:: */', $this_segment);
			array_unshift($constraints, trim($bits[0], " '"));
		}
		return $constraints;
	}

	public function dbDataType($type){
		$values = array(
			'unsigned integer' => 'INT'
		);

		if(isset($values[$type])) return $values[$type];
		else return '';
	}

	/*
	 * Given a tablespace and and location, either create a new one
	 * or update the existing one
	 * 
	 * @param string $name
	 * @param string $location
	 */
	public function createOrReplaceTablespace($name, $location){
		$existing = $this->preparedQuery(
			"SELECT spcname, spclocation FROM pg_tablespace WHERE spcname = ?;",
			array($name)
		)->first();

		//NOTE: this location must be empty for this to work
		//We can't seem to change the location of the tablespace through any ALTER commands :(

		//If a tablespace with this name exists, but the location has changed, then drop the current one
		//if($existing && $location!=$existing['spclocation'])
		//	DB::query("DROP TABLESPACE $name;");

		//If this is a new tablespace, or we have dropped the current one:
		if(!$existing || ($existing && $location != $existing['spclocation'])) {
			$this->query("CREATE TABLESPACE $name LOCATION '$location';");
		}
	}

	/**
	 * 
	 * @param string $tableName
	 * @param array $partitions
	 * @param array $indexes
	 * @param array $extensions
	 */
	public function createOrReplacePartition($tableName, $partitions, $indexes, $extensions){

		//We need the plpgsql language to be installed for this to work:
		$this->createLanguage('plpgsql');

		$trigger='CREATE OR REPLACE FUNCTION ' . $tableName . '_insert_trigger() RETURNS TRIGGER AS $$ BEGIN ';
		$first=true;

		//Do we need to create a tablespace for this item?
		if($extensions && isset($extensions['tablespace'])){
			$this->createOrReplaceTablespace($extensions['tablespace']['name'], $extensions['tablespace']['location']);
			$tableSpace=' TABLESPACE ' . $extensions['tablespace']['name'];
		} else {
			$tableSpace='';
		}

		foreach($partitions as $partition_name => $partition_value){
			//Check that this child table does not already exist:
			if(!$this->hasTable($partition_name)){
				$this->query("CREATE TABLE \"$partition_name\" (CHECK (" . str_replace('NEW.', '', $partition_value) . ")) INHERITS (\"$tableName\")$tableSpace;");
			} else {
				//Drop the constraint, we will recreate in in the next line
				$existing_constraint = $this->preparedQuery(
					"SELECT conname FROM pg_constraint WHERE conname = ?;",
					array("{$partition_name}_pkey")
				);
				if($existing_constraint){
					$this->query("ALTER TABLE \"$partition_name\" DROP CONSTRAINT \"{$partition_name}_pkey\";");
				}
				$this->dropTrigger(strtolower('trigger_' . $tableName . '_insert'), $tableName);
			}

			$this->query("ALTER TABLE \"$partition_name\" ADD CONSTRAINT \"{$partition_name}_pkey\" PRIMARY KEY (\"ID\");");

			if($first){
				$trigger.='IF';
				$first=false;
			} else {
				$trigger.='ELSIF';
			}

			$trigger .= " ($partition_value) THEN INSERT INTO \"$partition_name\" VALUES (NEW.*);";

			if($indexes){
				// We need to propogate the indexes through to the child pages.
				// Some of this code is duplicated, and could be tidied up
				foreach($indexes as $name => $this_index){

					if($this_index['type']=='fulltext'){
						$fillfactor = $where = '';
						if(isset($this_index['fillfactor'])) {
							$fillfactor = 'WITH (FILLFACTOR = ' . $this_index['fillfactor'] . ')';
						}
						if(isset($this_index['where'])) {
							$where = 'WHERE ' . $this_index['where'];
						}
						$clusterMethod = PostgreSQLDatabase::default_fts_cluster_method();
						$this->query("CREATE INDEX \"" . $this->buildPostgresIndexName($partition_name, $this_index['name'])  . "\" ON \"" . $partition_name . "\" USING $clusterMethod(\"ts_" . $name . "\") $fillfactor $where");
						$ts_details = $this->fulltext($this_index, $partition_name, $name);
						$this->query($ts_details['triggers']);
					} else {

						if(is_array($this_index)) {
							$index_name = $this_index['name'];
						} else {
							$index_name = trim($this_index, '()');
						}

						$createIndex = $this->getIndexSqlDefinition($partition_name, $index_name, $this_index);
						if($createIndex !== false) {
							$this->query($createIndex);
						}
					}
				}
			}

			//Lastly, clustering goes here:
			if($extensions && isset($extensions['cluster'])){
				$this->query("CLUSTER \"$partition_name\" USING \"{$extensions['cluster']}\";");
			}
		}

		$trigger .= 'ELSE RAISE EXCEPTION \'Value id out of range.  Fix the ' . $tableName . '_insert_trigger() function!\'; END IF; RETURN NULL; END; $$ LANGUAGE plpgsql;';
		$trigger .= 'CREATE TRIGGER trigger_' . $tableName . '_insert BEFORE INSERT ON "' . $tableName . '" FOR EACH ROW EXECUTE PROCEDURE ' . $tableName . '_insert_trigger();';

		$this->query($trigger);
	}

	/*
	 * This will create a language if it doesn't already exist.
	 * This is used by the createOrReplacePartition function, which needs plpgsql
	 * 
	 * @param string $language Language name
	 */
	public function createLanguage($language){
		$result = $this->preparedQuery(
			"SELECT lanname FROM pg_language WHERE lanname = ?;",
			array($language)
		)->first();

		if(!$result) {
			$this->query("CREATE LANGUAGE $language;");
		}
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
		user_error("PostGreSQL does not support multi-enum", E_USER_ERROR);
		return "int";
	}
}
