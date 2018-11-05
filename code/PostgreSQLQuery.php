<?php

namespace SilverStripe\PostgreSQL;

use SilverStripe\ORM\Connect\Query;

/**
 * A result-set from a PostgreSQL database.
 *
 * @package sapphire
 * @subpackage model
 */
class PostgreSQLQuery extends Query
{
    /**
     * The internal Postgres handle that points to the result set.
     * @var resource
     */
    private $handle;

    private $columnNames = [];

    /**
     * Mapping of postgresql types to PHP types
     * Note that the bool => int mapping is by design, designed to mimic MySQL's behaviour
     * @var array
     */
    protected static $typeMapping = [
        'bool' => 'int',
        'int2' => 'int',
        'int4' => 'int',
        'int8' => 'int',
        'float4' => 'float',
        'float8' => 'float',
        'numeric' => 'float',
    ];

    /**
     * Hook the result-set given into a Query class, suitable for use by sapphire.
     * @param resource $handle the internal Postgres handle that is points to the resultset.
     */
    public function __construct($handle)
    {
        $this->handle = $handle;

        $numColumns = pg_num_fields($handle);
        for ($i = 0; $i<$numColumns; $i++) {
            $this->columnNames[$i] = pg_field_name($handle, $i);
        }
    }

    public function __destruct()
    {
        if (is_resource($this->handle)) {
            pg_free_result($this->handle);
        }
    }

    public function seek($row)
    {
        pg_result_seek($this->handle, $row);
        return $this->nextRecord();
    }

    public function numRecords()
    {
        return pg_num_rows($this->handle);
    }

    public function nextRecord()
    {
        $row = pg_fetch_array($this->handle, null, PGSQL_NUM);

        // Correct non-string types
        if ($row) {
            $record = [];

            foreach ($row as $i => $v) {
                $k = $this->columnNames[$i];
                $record[$k] = $v;
                $type = pg_field_type($this->handle, $i);
                if (isset(self::$typeMapping[$type])) {
                    settype($record[$k], self::$typeMapping[$type]);
                }
            }

            return $record;
        }

        return false;
    }
}
