<?php

namespace SilverStripe\PostgreSQL;

use SilverStripe\ORM\Connect\SS_Query;

/**
 * A result-set from a PostgreSQL database.
 *
 * @package sapphire
 * @subpackage model
 */
class PostgreSQLQuery extends SS_Query
{
    /**
     * The internal Postgres handle that points to the result set.
     * @var resource
     */
    private $handle;

    /**
     * Hook the result-set given into a Query class, suitable for use by sapphire.
     * @param resource $handle the internal Postgres handle that is points to the resultset.
     */
    public function __construct($handle)
    {
        $this->handle = $handle;
    }

    public function __destruct()
    {
        if (is_resource($this->handle)) {
            pg_free_result($this->handle);
        }
    }

    public function seek($row)
    {
        return pg_result_seek($this->handle, $row);
    }

    public function numRecords()
    {
        return pg_num_rows($this->handle);
    }

    public function nextRecord()
    {
        return pg_fetch_assoc($this->handle);
    }
}
