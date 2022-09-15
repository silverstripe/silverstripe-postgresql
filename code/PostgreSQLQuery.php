<?php

namespace SilverStripe\PostgreSQL;

use Iterator;
use PgSql\Result;
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
     * @var Result
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
     * @param Result $handle the internal Postgres handle that is points to the resultset.
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
        pg_free_result($this->handle);
    }

    public function getIterator(): Iterator
    {
        while ($row = pg_fetch_array($this->handle, null, PGSQL_NUM)) {
            yield $this->parseResult($row);
        }
    }

    public function numRecords()
    {
        return pg_num_rows($this->handle);
    }

    /**
     * @param array $row
     * @return array
     */
    protected function parseResult(array $row)
    {
        $record = [];

        foreach ($row as $i => $v) {
            $k = $this->columnNames[$i];
            $record[$k] = $v;
            $type = pg_field_type($this->handle, $i);
            if (isset(self::$typeMapping[$type])) {
                if ($type === 'bool' && $record[$k] === 't') {
                    $record[$k] = 1;

                    // Note that boolean 'f' will be converted to 0 by this
                } else {
                    settype($record[$k], self::$typeMapping[$type]);
                }
            }
        }

        return $record;
    }
}
