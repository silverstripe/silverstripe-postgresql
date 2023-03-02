<?php

namespace SilverStripe\PostgreSQL;

use SilverStripe\ORM\Connect\DBConnector;
use ErrorException;

/**
 * PostgreSQL connector class using the PostgreSQL specific api
 *
 * The connector doesn't know anything about schema selection, so code related to
 * masking multiple databases as schemas should be handled in the database controller
 * and schema manager.
 */
class PostgreSQLConnector extends DBConnector
{
    /**
     * Connection to the PG Database database
     *
     * @var resource
     */
    protected $dbConn = null;

    /**
     * Name of the currently selected database
     *
     * @var string
     */
    protected $databaseName = null;

    /**
     * Reference to the last query result (for pg_affected_rows)
     *
     * @var resource
     */
    protected $lastQuery = null;

    /**
     * Last parameters used to connect
     *
     * @var array
     */
    protected $lastParameters = null;

    protected $lastRows = 0;

    /**
     * Escape a parameter to be used in the connection string
     *
     * @param array $parameters All parameters
     * @param string $key The key in $parameters to pull from
     * @param string $name The connection string parameter name
     * @param mixed $default The default value, or null if optional
     * @return string The completed fragment in the form name=value
     */
    protected function escapeParameter($parameters, $key, $name, $default = null)
    {
        if (empty($parameters[$key])) {
            if ($default === null) {
                return '';
            }
            $value = $default;
        } else {
            $value = $parameters[$key];
        }
        return "$name='" . addslashes($value) . "'";
    }

    public function connect($parameters, $selectDB = false)
    {
        $this->lastParameters = $parameters;

        // Note: Postgres always behaves as though $selectDB = true, ignoring
        // any value actually passed in. The controller passes in true for other
        // connectors such as PDOConnector.

        // Escape parameters
        $arguments = array(
            $this->escapeParameter($parameters, 'server', 'host', 'localhost'),
            $this->escapeParameter($parameters, 'port', 'port', 5432),
            $this->escapeParameter($parameters, 'database', 'dbname', 'postgres'),
            $this->escapeParameter($parameters, 'username', 'user'),
            $this->escapeParameter($parameters, 'password', 'password')
        );

        // Close the old connection
        if ($this->dbConn) {
            pg_close($this->dbConn);
        }

        // Connect
        $this->dbConn = @pg_connect(implode(' ', $arguments));
        if ($this->dbConn === false) {
            // Extract error details from PHP error handling
            $error = error_get_last();
            if ($error && preg_match('/function\\.pg-connect\\<\\/a\\>\\]\\: (?<message>.*)/', $error['message'], $matches)) {
                $this->databaseError(html_entity_decode($matches['message']));
            } else {
                $this->databaseError("Couldn't connect to PostgreSQL database.");
            }
        } elseif (pg_connection_status($this->dbConn) != PGSQL_CONNECTION_OK) {
            throw new ErrorException($this->getLastError());
        }

        //By virtue of getting here, the connection is active:
        $this->databaseName = empty($parameters['database']) ? PostgreSQLDatabase::MASTER_DATABASE : $parameters['database'];
    }

    public function affectedRows()
    {
        return $this->lastRows;
    }

    public function getGeneratedID($table)
    {
        return $this->query("SELECT currval('\"{$table}_ID_seq\"')")->value();
    }

    public function getLastError()
    {
        return pg_last_error($this->dbConn);
    }

    public function getSelectedDatabase()
    {
        return $this->databaseName;
    }

    public function getVersion()
    {
        $version = pg_version($this->dbConn);
        if (isset($version['server'])) {
            return $version['server'];
        } else {
            return false;
        }
    }

    public function isActive()
    {
        return $this->databaseName && $this->dbConn;
    }

    /**
     * Determines if the SQL fragment either breaks into or out of a string literal
     * by counting single quotes
     *
     * Handles double-quote escaped quotes as well as slash escaped quotes
     *
     * @todo Test this!
     *
     * @see http://www.postgresql.org/docs/8.3/interactive/sql-syntax-lexical.html#SQL-SYNTAX-STRINGS
     *
     * @param string $input The SQL fragment
     * @return boolean True if the string breaks into or out of a string literal
     */
    public function checkStringTogglesLiteral($input)
    {
        // Remove escaped backslashes, count them!
        $input = preg_replace('/\\\\\\\\/', '', $input);

        // Count quotes
        $totalQuotes = substr_count($input, "'"); // Includes double quote escaped quotes
        $escapedQuotes = substr_count($input, "\\'");
        return (($totalQuotes - $escapedQuotes) % 2) !== 0;
    }

    /**
     * Iteratively replaces all question marks with numerical placeholders
     * E.g. "Title = ? AND Name = ?" becomes "Title = $1 AND Name = $2"
     *
     * @todo Better consider question marks in string literals
     *
     * @param string $sql Paramaterised query using question mark placeholders
     * @return string Paramaterised query using numeric placeholders
     */
    public function replacePlaceholders($sql)
    {
        $segments = preg_split('/\?/', $sql);
        $joined = '';
        $inString = false;
        $num = 0;
        for ($i = 0; $i < count($segments); $i++) {
            // Append next segment
            $joined .= $segments[$i];

            // Don't add placeholder after last segment
            if ($i === count($segments) - 1) {
                break;
            }

            // check string escape on previous fragment
            if ($this->checkStringTogglesLiteral($segments[$i])) {
                $inString = !$inString;
            }

            // Append placeholder replacement
            if ($inString) {
                $joined .= "?";
            } else {
                $joined .= '$' . ++$num;
            }
        }
        return $joined;
    }

    public function preparedQuery($sql, $parameters, $errorLevel = E_USER_ERROR)
    {
        // Reset state
        $this->lastQuery = null;
        $this->lastRows = 0;

        // Replace question mark placeholders with numeric placeholders
        if (!empty($parameters)) {
            $sql = $this->replacePlaceholders($sql);
            $parameters = $this->parameterValues($parameters);
        }

        // Execute query
        // Unfortunately error-suppression is required in order to handle sql errors elegantly.
        // Please use PDO if you can help it
        if (!empty($parameters)) {
            $result = @pg_query_params($this->dbConn, $sql, $parameters);
        } else {
            $result = @pg_query($this->dbConn, $sql);
        }

        // Handle error
        if (!$result) {
            $this->databaseError($this->getLastError(), $errorLevel, $sql, $parameters);
            return null;
        }

        // Save and return results
        $this->lastQuery = $result;
        $this->lastRows = pg_affected_rows($result);
        return new PostgreSQLQuery($result);
    }

    public function query($sql, $errorLevel = E_USER_ERROR)
    {
        return $this->preparedQuery($sql, array(), $errorLevel);
    }

    public function quoteString($value)
    {
        if (function_exists('pg_escape_literal')) {
            return pg_escape_literal($this->dbConn, $value);
        } else {
            return "'" . $this->escapeString($value) . "'";
        }
    }

    public function escapeString($value)
    {
        return pg_escape_string($this->dbConn, $value);
    }

    public function selectDatabase($name)
    {
        if ($name !== $this->databaseName) {
            user_error("PostgreSQLConnector can't change databases. Please create a new database connection", E_USER_ERROR);
        }
        return true;
    }

    public function unloadDatabase()
    {
        $this->databaseName = null;
    }
}
