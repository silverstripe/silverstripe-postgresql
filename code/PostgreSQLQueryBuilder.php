<?php

class PostgreSQLQueryBuilder extends DBQueryBuilder {

	/**
	 * Return the LIMIT clause ready for inserting into a query.
	 *
	 * @param SQLSelect $query The expression object to build from
	 * @param array $parameters Out parameter for the resulting query parameters
	 * @return string The finalised limit SQL fragment
	 */
	public function buildLimitFragment(SQLSelect $query, array &$parameters) {
		$nl = $this->getSeparator();

		// Ensure limit is given
		$limit = $query->getLimit();
		if(empty($limit)) return '';

		// For literal values return this as the limit SQL
		if( ! is_array($limit)) {
			return "{$nl}LIMIT $limit";
		}

		// Assert that the array version provides the 'limit' key
		if( ! array_key_exists('limit', $limit) || ($limit['limit'] !== null && ! is_numeric($limit['limit']))) {
			throw new InvalidArgumentException(
				'DBQueryBuilder::buildLimitSQL(): Wrong format for $limit: '. var_export($limit, true)
			);
		}

		if($limit['limit'] === null) {
			$limit['limit'] = 'ALL';
		}

		$clause = "{$nl}LIMIT {$limit['limit']}";
		if(isset($limit['start']) && is_numeric($limit['start']) && $limit['start'] !== 0) {
			$clause .= " OFFSET {$limit['start']}";
		}
		return $clause;
	}

    /**
     * Adds a specific PostgreSQL command that returns the affected rows when the 'Update' query is run
     *
     * @param SQLUpdate $query
     * @param array $parameters
     * @return string
     */
    protected function buildUpdateQuery(SQLUpdate $query, array &$parameters) {
        $sql = parent::buildUpdateQuery($query, $parameters);
        $sql .= ' RETURNING *';
        return $sql;
    }

}
