<?php

namespace SilverStripe\PostgreSQL;

use SilverStripe\ORM\Queries\SQLConditionalExpression;
use SilverStripe\ORM\Queries\SQLExpression;
use SilverStripe\ORM\Queries\SQLSelect;
use SilverStripe\ORM\Connect\DBQueryBuilder;
use InvalidArgumentException;

class PostgreSQLQueryBuilder extends DBQueryBuilder
{
    /**
     * Max table length.
     * Aliases longer than this will be re-written
     */
    const MAX_TABLE = 63;

    /**
     * Return the LIMIT clause ready for inserting into a query.
     *
     * @param SQLSelect $query The expression object to build from
     * @param array $parameters Out parameter for the resulting query parameters
     * @return string The finalised limit SQL fragment
     */
    public function buildLimitFragment(SQLSelect $query, array &$parameters)
    {
        $nl = $this->getSeparator();

        // Ensure limit is given
        $limit = $query->getLimit();
        if (empty($limit)) {
            return '';
        }

        // For literal values return this as the limit SQL
        if (! is_array($limit)) {
            return "{$nl}LIMIT $limit";
        }

        // Assert that the array version provides the 'limit' key
        if (! array_key_exists('limit', $limit) || ($limit['limit'] !== null && ! is_numeric($limit['limit']))) {
            throw new InvalidArgumentException(
                'DBQueryBuilder::buildLimitSQL(): Wrong format for $limit: '. var_export($limit, true)
            );
        }

        if ($limit['limit'] === null) {
            $limit['limit'] = 'ALL';
        }

        $clause = "{$nl}LIMIT {$limit['limit']}";
        if (isset($limit['start']) && is_numeric($limit['start']) && $limit['start'] !== 0) {
            $clause .= " OFFSET {$limit['start']}";
        }
        return $clause;
    }

    public function buildSQL(SQLExpression $query, &$parameters)
    {
        $sql = parent::buildSQL($query, $parameters);
        return $this->rewriteLongIdentifiers($query, $sql);
    }

    /**
     * Find and generate table aliases necessary in the given query
     *
     * @param SQLConditionalExpression $query
     * @return array List of replacements
     */
    protected function findRewrites(SQLConditionalExpression $query)
    {
        $rewrites = [];
        foreach ($query->getFrom() as $alias => $from) {
            $table = is_array($from) ? $from['table'] : $from;
            if ($alias === $table || "\"{$alias}\"" === $table) {
                continue;
            }
            // Don't complain about aliases shorter than max length
            if (strlen($alias) <= self::MAX_TABLE) {
                continue;
            }
            $replacement = substr(sha1($alias), 0, 7) . '_' . substr($alias, 8 - self::MAX_TABLE);
            $rewrites["\"{$alias}\""] = "\"{$replacement}\"";
        }
        return $rewrites;
    }

    /**
     * Rewrite all ` AS "Identifier"` with strlen(Identifier) > 63
     *
     * @param SQLExpression $query
     * @param string $sql
     * @return string
     */
    protected function rewriteLongIdentifiers(SQLExpression $query, $sql)
    {
        // Check if this query has aliases
        if ($query instanceof SQLConditionalExpression) {
            $rewrites = $this->findRewrites($query);
            if ($rewrites) {
                return str_replace(array_keys($rewrites), array_values($rewrites), $sql);
            }
        }
        return $sql;
    }
}
