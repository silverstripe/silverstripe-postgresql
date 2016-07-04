<?php

namespace SilverStripe\PostgreSQL;

use SilverStripe\Framework\Core\Configurable;
use SilverStripe\ORM\DB;
use SilverStripe\ORM\DataObject;
use SilverStripe\ORM\ArrayList;
use SilverStripe\ORM\Connect\SS_Database;
use Config;
use ErrorException;
use Exception;
use PaginatedList;

/**
 * PostgreSQL connector class.
 *
 * @package sapphire
 * @subpackage model
 */
class PostgreSQLDatabase extends SS_Database
{
    use Configurable;

    /**
     * Database schema manager object
     *
     * @var PostgreSQLSchemaManager
     */
    protected $schemaManager;

    /**
     * The currently selected database schema name.
     *
     * @var string
     */
    protected $schema;

    /**
     * Toggle if transactions are supported. Defaults to true.
     *
     * @var bool
     */
    protected $supportsTransactions = true;

    /**
     * Determines whether to check a database exists on the host by
     * querying the 'postgres' database and running createDatabase.
     *
     * Some locked down systems prevent access to the 'postgres' table in
     * which case you need to set this to false.
     *
     * If allow_query_master_postgres is false, and model_schema_as_database is also false,
     * then attempts to create or check databases beyond the initial connection will
     * result in a runtime error.
     *
     * @config
     * @var bool
     */
    private static $allow_query_master_postgres = true;

    /**
     * For instances where multiple databases are used beyond the initial connection
     * you may set this option to true to force database switches to switch schemas
     * instead of using databases. This may be useful if the database user does not
     * have cross-database permissions, and in cases where multiple databases are used
     * (such as in running test cases).
     *
     * If this is true then the database will only be set during the initial connection,
     * and attempts to change to this database will use the 'public' schema instead
     *
     * If this is false then errors may be generated during some cross database operations.
     */
    private static $model_schema_as_database = true;

    /**
     * Override the language that tsearch uses.  By default it is 'english, but
     * could be any of the supported languages that can be found in the
     * pg_catalog.pg_ts_config table.
     */
    private static $search_language = 'english';

    /*
     * Describe how T-search will work.
     * You can use either GiST or GIN, and '@@' (gist) or '@@@' (gin)
     * Combinations of these two will also work, so you'll need to pick
     * one which works best for you
     */
    private static $default_fts_cluster_method = 'GIN';

    /*
     * Describe how T-search will work.
     * You can use either GiST or GIN, and '@@' (gist) or '@@@' (gin)
     * Combinations of these two will also work, so you'll need to pick
     * one which works best for you
     */
    private static $default_fts_search_method = '@@@';

    const MASTER_DATABASE = 'postgres';

    const MASTER_SCHEMA = 'public';

    /**
     * Full text cluster method. (e.g. GIN or GiST)
     *
     * @return string
     */
    public static function default_fts_cluster_method()
    {
        return static::config()->default_fts_cluster_method;
    }

    /**
     * Full text search method.
     *
     * @return string
     */
    public static function default_fts_search_method()
    {
        return static::config()->default_fts_search_method;
    }

    /**
     * Determines whether to check a database exists on the host by
     * querying the 'postgres' database and running createDatabase.
     *
     * Some locked down systems prevent access to the 'postgres' table in
     * which case you need to set this to false.
     *
     * If allow_query_master_postgres is false, and model_schema_as_database is also false,
     * then attempts to create or check databases beyond the initial connection will
     * result in a runtime error.
     *
     * @return bool
     */
    public static function allow_query_master_postgres()
    {
        return static::config()->allow_query_master_postgres;
    }

    /**
     * For instances where multiple databases are used beyond the initial connection
     * you may set this option to true to force database switches to switch schemas
     * instead of using databases. This may be useful if the database user does not
     * have cross-database permissions, and in cases where multiple databases are used
     * (such as in running test cases).
     *
     * If this is true then the database will only be set during the initial connection,
     * and attempts to change to this database will use the 'public' schema instead
     *
     * @return bool
     */
    public static function model_schema_as_database()
    {
        return static::config()->model_schema_as_database;
    }

    /**
     * Override the language that tsearch uses.  By default it is 'english, but
     * could be any of the supported languages that can be found in the
     * pg_catalog.pg_ts_config table.
     *
     * @return string
     */
    public static function search_language()
    {
        return static::config()->search_language;
    }

    /**
     * The database name specified at initial connection
     *
     * @var string
     */
    protected $databaseOriginal = '';

    /**
     * The schema name specified at initial construction. When model_schema_as_database
     * is set to true selecting the $databaseOriginal database will instead reset
     * the schema to this
     *
     * @var string
     */
    protected $schemaOriginal = '';

    /**
     * Connection parameters specified at inital connection
     *
     * @var array
     */
    protected $parameters = array();

    public function connect($parameters)
    {
        // Check database name
        if (empty($parameters['database'])) {
            // Check if we can use the master database
            if (!self::allow_query_master_postgres()) {
                throw new ErrorException('PostegreSQLDatabase::connect called without a database name specified');
            }
            // Fallback to master database connection if permission allows
            $parameters['database'] = self::MASTER_DATABASE;
        }
        $this->databaseOriginal = $parameters['database'];

        // check schema name
        if (empty($parameters['schema'])) {
            $parameters['schema'] = self::MASTER_SCHEMA;
        }
        $this->schemaOriginal = $parameters['schema'];

        // Ensure that driver is available (required by PDO)
        if (empty($parameters['driver'])) {
            $parameters['driver'] = $this->getDatabaseServer();
        }

        // Ensure port number is set (required by postgres)
        if (empty($parameters['port'])) {
            $parameters['port'] = 5432;
        }

        $this->parameters = $parameters;

        // If allowed, check that the database exists. Otherwise naively assume
        // that the original database exists
        if (self::allow_query_master_postgres()) {
            // Use master connection to setup initial schema
            $this->connectMaster();
            if (!$this->schemaManager->postgresDatabaseExists($this->databaseOriginal)) {
                $this->schemaManager->createPostgresDatabase($this->databaseOriginal);
            }
        }

        // Connect to the actual database we're requesting
        $this->connectDefault();

        // Set up the schema if required
        $this->setSchema($this->schemaOriginal, true);

        // Set the timezone if required.
        if (isset($parameters['timezone'])) {
            $this->selectTimezone($parameters['timezone']);
        }
    }

    protected function connectMaster()
    {
        $parameters = $this->parameters;
        $parameters['database'] = self::MASTER_DATABASE;
        $this->connector->connect($parameters, true);
    }

    protected function connectDefault()
    {
        $parameters = $this->parameters;
        $parameters['database'] = $this->databaseOriginal;
        $this->connector->connect($parameters, true);
    }

    /**
     * Sets the system timezone for the database connection
     *
     * @param string $timezone
     */
    public function selectTimezone($timezone)
    {
        if (empty($timezone)) {
            return;
        }
        $this->query("SET SESSION TIME ZONE '$timezone';");
    }

    public function supportsCollations()
    {
        return true;
    }

    public function supportsTimezoneOverride()
    {
        return true;
    }

    public function getDatabaseServer()
    {
        return "postgresql";
    }

    /**
     * Returns the name of the current schema in use
     *
     * @return string Name of current schema
     */
    public function currentSchema()
    {
        return $this->schema;
    }

    /**
     * Utility method to manually set the schema to an alternative
     * Check existance & sets search path to the supplied schema name
     *
     * @param string $schema Name of the schema
     * @param boolean $create Flag indicating whether the schema should be created
     * if it doesn't exist. If $create is false and the schema doesn't exist
     * then an error will be raised
     * @param int|boolean $errorLevel The level of error reporting to enable for
     * the query, or false if no error should be raised
     * @return boolean Flag indicating success
     */
    public function setSchema($schema, $create = false, $errorLevel = E_USER_ERROR)
    {
        if (!$this->schemaManager->schemaExists($schema)) {
            // Check DB creation permisson
            if (!$create) {
                if ($errorLevel !== false) {
                    user_error("Schema $schema does not exist", $errorLevel);
                }
                $this->schema = null;
                return false;
            }
            $this->schemaManager->createSchema($schema);
        }
        $this->setSchemaSearchPath($schema);
        $this->schema = $schema;
        return true;
    }

    /**
     * Override the schema search path. Search using the arguments supplied.
     * NOTE: The search path is normally set through setSchema() and only
     * one schema is selected. The facility to add more than one schema to
     * the search path is provided as an advanced PostgreSQL feature for raw
     * SQL queries. Sapphire cannot search for datamodel tables in alternate
     * schemas, so be wary of using alternate schemas within the ORM environment.
     *
     * @param string ...$arg Schema name to use. Add additional schema names as extra arguments.
     */
    public function setSchemaSearchPath($arg = null)
    {
        if (!$arg) {
            user_error('At least one Schema must be supplied to set a search path.', E_USER_ERROR);
        }
        $schemas = array_values(func_get_args());
        $this->query("SET search_path TO \"" . implode("\",\"", $schemas) . "\"");
    }

    /**
     * The core search engine configuration.
     * @todo Properly extract the search functions out of the core.
     *
     * @param array $classesToSearch
     * @param string $keywords Keywords as a space separated string
     * @param int $start
     * @param int $pageLength
     * @param string $sortBy
     * @param string $extraFilter
     * @param bool $booleanSearch
     * @param string $alternativeFileFilter
     * @param bool $invertedMatch
     * @return PaginatedList List of result pages
     * @throws Exception
     */
    public function searchEngine($classesToSearch, $keywords, $start, $pageLength, $sortBy = "ts_rank DESC", $extraFilter = "", $booleanSearch = false, $alternativeFileFilter = "", $invertedMatch = false)
    {
        //Fix the keywords to be ts_query compatitble:
        //Spaces must have pipes
        //@TODO: properly handle boolean operators here.
        $keywords= trim($keywords);
        $keywords= str_replace(' ', ' | ', $keywords);
        $keywords= str_replace('"', "'", $keywords);

        $keywords = $this->quoteString(trim($keywords));

        //We can get a list of all the tsvector columns though this query:
        //We know what tables to search in based on the $classesToSearch variable:
        $classesPlaceholders = DB::placeholders($classesToSearch);
        $result = $this->preparedQuery("
			SELECT table_name, column_name, data_type
			FROM information_schema.columns
			WHERE data_type='tsvector' AND table_name in ($classesPlaceholders);",
            $classesToSearch
        );
        if (!$result->numRecords()) {
            throw new Exception('there are no full text columns to search');
        }

        $tables = array();
        $tableParameters = array();

        // Make column selection lists
        $select = array(
            'SiteTree' => array(
                '"ClassName"',
                '"SiteTree"."ID"',
                '"ParentID"',
                '"Title"',
                '"URLSegment"',
                '"Content"',
                '"LastEdited"',
                '"Created"',
                'NULL AS "Name"',
                '"CanViewType"'
            ),
            'File' => array(
                '"ClassName"',
                '"File"."ID"',
                '0 AS "ParentID"',
                '"Title"',
                'NULL AS "URLSegment"',
                'NULL AS "Content"',
                '"LastEdited"',
                '"Created"',
                '"Name"',
                'NULL AS "CanViewType"'
            )
        );

        foreach ($result as $row) {
            $conditions = array();
            if ($row['table_name'] === 'SiteTree' || $row['table_name'] === 'File') {
                $conditions[] = array('"ShowInSearch"' => 1);
            }

            $method = self::default_fts_search_method();
            $conditions[] = "\"{$row['table_name']}\".\"{$row['column_name']}\" $method q ";
            $query = DataObject::get($row['table_name'], $conditions)->dataQuery()->query();

            // Could parameterise this, but convention is only to to so for where conditions
            $query->addFrom(array(
                'tsearch' => ", to_tsquery('" . self::search_language() . "', $keywords) AS q"
            ));
            $query->setSelect(array());

            foreach ($select[$row['table_name']] as $clause) {
                if (preg_match('/^(.*) +AS +"?([^"]*)"?/i', $clause, $matches)) {
                    $query->selectField($matches[1], $matches[2]);
                } else {
                    $query->selectField($clause);
                }
            }

            $query->selectField("ts_rank(\"{$row['table_name']}\".\"{$row['column_name']}\", q)", 'Relevance');
            $query->setOrderBy(array());

            //Add this query to the collection
            $tables[] = $query->sql($parameters);
            $tableParameters = array_merge($tableParameters, $parameters);
        }

        $limit = $pageLength;
        $offset = $start;

        if ($keywords) {
            $orderBy = " ORDER BY $sortBy";
        } else {
            $orderBy='';
        }

        $fullQuery = "SELECT * FROM (" . implode(" UNION ", $tables) . ") AS q1 $orderBy LIMIT $limit OFFSET $offset";

        // Get records
        $records = $this->preparedQuery($fullQuery, $tableParameters);
        $totalCount=0;
        foreach ($records as $record) {
            $objects[] = new $record['ClassName']($record);
            $totalCount++;
        }

        if (isset($objects)) {
            $results = new ArrayList($objects);
        } else {
            $results = new ArrayList();
        }
        $list = new PaginatedList($results);
        $list->setLimitItems(false);
        $list->setPageStart($start);
        $list->setPageLength($pageLength);
        $list->setTotalItems($totalCount);
        return $list;
    }

    public function supportsTransactions()
    {
        return $this->supportsTransactions;
    }

    /*
     * This is a quick lookup to discover if the database supports particular extensions
     */
    public function supportsExtensions($extensions=array('partitions', 'tablespaces', 'clustering'))
    {
        if (isset($extensions['partitions'])) {
            return true;
        } elseif (isset($extensions['tablespaces'])) {
            return true;
        } elseif (isset($extensions['clustering'])) {
            return true;
        } else {
            return false;
        }
    }

    public function transactionStart($transaction_mode = false, $session_characteristics = false)
    {
        $this->query('BEGIN;');

        if ($transaction_mode) {
            $this->query("SET TRANSACTION {$transaction_mode};");
        }

        if ($session_characteristics) {
            $this->query("SET SESSION CHARACTERISTICS AS TRANSACTION {$session_characteristics};");
        }
    }

    public function transactionSavepoint($savepoint)
    {
        $this->query("SAVEPOINT {$savepoint};");
    }

    public function transactionRollback($savepoint = false)
    {
        if ($savepoint) {
            $this->query("ROLLBACK TO {$savepoint};");
        } else {
            $this->query('ROLLBACK;');
        }
    }

    public function transactionEnd($chain = false)
    {
        $this->query('COMMIT;');
    }

    public function comparisonClause($field, $value, $exact = false, $negate = false, $caseSensitive = null, $parameterised = false)
    {
        if ($exact && $caseSensitive === null) {
            $comp = ($negate) ? '!=' : '=';
        } else {
            $comp = ($caseSensitive === true) ? 'LIKE' : 'ILIKE';
            if ($negate) {
                $comp = 'NOT ' . $comp;
            }
            $field.='::text';
        }

        if ($parameterised) {
            return sprintf("%s %s ?", $field, $comp);
        } else {
            return sprintf("%s %s '%s'", $field, $comp, $value);
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
    public function formattedDatetimeClause($date, $format)
    {
        preg_match_all('/%(.)/', $format, $matches);
        foreach ($matches[1] as $match) {
            if (array_search($match, array('Y', 'm', 'd', 'H', 'i', 's', 'U')) === false) {
                user_error('formattedDatetimeClause(): unsupported format character %' . $match, E_USER_WARNING);
            }
        }

        $translate = array(
            '/%Y/' => 'YYYY',
            '/%m/' => 'MM',
            '/%d/' => 'DD',
            '/%H/' => 'HH24',
            '/%i/' => 'MI',
            '/%s/' => 'SS',
        );
        $format = preg_replace(array_keys($translate), array_values($translate), $format);

        if (preg_match('/^now$/i', $date)) {
            $date = "NOW()";
        } elseif (preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/i', $date)) {
            $date = "TIMESTAMP '$date'";
        }

        if ($format == '%U') {
            return "FLOOR(EXTRACT(epoch FROM $date))";
        }

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
    public function datetimeIntervalClause($date, $interval)
    {
        if (preg_match('/^now$/i', $date)) {
            $date = "NOW()";
        } elseif (preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/i', $date)) {
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
    public function datetimeDifferenceClause($date1, $date2)
    {
        if (preg_match('/^now$/i', $date1)) {
            $date1 = "NOW()";
        } elseif (preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/i', $date1)) {
            $date1 = "TIMESTAMP '$date1'";
        }

        if (preg_match('/^now$/i', $date2)) {
            $date2 = "NOW()";
        } elseif (preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/i', $date2)) {
            $date2 = "TIMESTAMP '$date2'";
        }

        return "(FLOOR(EXTRACT(epoch FROM $date1)) - FLOOR(EXTRACT(epoch from $date2)))";
    }

    public function now()
    {
        return 'NOW()';
    }

    public function random()
    {
        return 'RANDOM()';
    }

    /**
     * Determines the name of the current database to be reported externally
     * by substituting the schema name for the database name.
     * Should only be used when model_schema_as_database is true
     *
     * @param string $schema Name of the schema
     * @return string Name of the database to report
     */
    public function schemaToDatabaseName($schema)
    {
        switch ($schema) {
            case $this->schemaOriginal: return $this->databaseOriginal;
            default: return $schema;
        }
    }

    /**
     * Translates a requested database name to a schema name to substitute internally.
     * Should only be used when model_schema_as_database is true
     *
     * @param string $database Name of the database
     * @return string Name of the schema to use for this database internally
     */
    public function databaseToSchemaName($database)
    {
        switch ($database) {
            case $this->databaseOriginal: return $this->schemaOriginal;
            default: return $database;
        }
    }

    public function dropSelectedDatabase()
    {
        if (self::model_schema_as_database()) {
            // Check current schema is valid
            $oldSchema = $this->schema;
            if (empty($oldSchema)) {
                return;
            } // Nothing selected to drop

            // Select another schema
            if ($oldSchema !== $this->schemaOriginal) {
                $this->setSchema($this->schemaOriginal);
            } elseif ($oldSchema !== self::MASTER_SCHEMA) {
                $this->setSchema(self::MASTER_SCHEMA);
            } else {
                $this->schema = null;
            }

            // Remove this schema
            $this->schemaManager->dropSchema($oldSchema);
        } else {
            parent::dropSelectedDatabase();
        }
    }

    public function getSelectedDatabase()
    {
        if (self::model_schema_as_database()) {
            return $this->schemaToDatabaseName($this->schema);
        }
        return parent::getSelectedDatabase();
    }

    public function selectDatabase($name, $create = false, $errorLevel = E_USER_ERROR)
    {
        // Substitute schema here as appropriate
        if (self::model_schema_as_database()) {
            // Selecting the database itself should be treated as selecting the public schema
            $schemaName = $this->databaseToSchemaName($name);
            return $this->setSchema($schemaName, $create, $errorLevel);
        }

        // Database selection requires that a new connection is established.
        // This is not ideal postgres practise
        if (!$this->schemaManager->databaseExists($name)) {
            // Check DB creation permisson
            if (!$create) {
                if ($errorLevel !== false) {
                    user_error("Attempted to connect to non-existing database \"$name\"", $errorLevel);
                }
                // Unselect database
                $this->connector->unloadDatabase();
                return false;
            }
            $this->schemaManager->createDatabase($name);
        }

        // New connection made here, treating the new database name as the new original
        $this->databaseOriginal = $name;
        $this->connectDefault();
        return true;
    }

    /**
     * Delete all entries from the table instead of truncating it.
     *
     * This gives a massive speed improvement compared to using TRUNCATE, with
     * the caveat that primary keys are not reset etc.
     *
     * @see DatabaseAdmin::clearAllData()
     *
     * @param string $table
     */
    public function clearTable($table)
    {
        $this->query('DELETE FROM "'.$table.'";');
    }
}
