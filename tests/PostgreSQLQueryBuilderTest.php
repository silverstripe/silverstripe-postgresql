<?php

namespace SilverStripe\PostgreSQL\Tests;

use SilverStripe\Dev\SapphireTest;
use SilverStripe\ORM\Queries\SQLSelect;
use SilverStripe\PostgreSQL\PostgreSQLQueryBuilder;

class PostgreSQLQueryBuilderTest extends SapphireTest
{
    public function testLongAliases()
    {
        $query = new SQLSelect();
        $longstring = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        $alias2 = $longstring . $longstring;
        $query->selectField('*');
        $query->addFrom('"Base"');
        $query->addLeftJoin(
            'Joined',
            "\"Base\".\"ID\" = \"{$alias2}\".\"ID\"",
            $alias2
        );
        $query->addWhere([
            "\"{$alias2}\".\"Title\" = ?" => 'Value',
        ]);

        $identifier = "c4afb43_hijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        $this->assertEquals(PostgreSQLQueryBuilder::MAX_TABLE, strlen($identifier));

        $expected = <<<SQL
SELECT *
 FROM "Base" LEFT JOIN "Joined" AS "c4afb43_hijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
 ON "Base"."ID" = "c4afb43_hijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"."ID"
 WHERE ("c4afb43_hijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"."Title" = ?)
SQL;
        $builder = new PostgreSQLQueryBuilder();
        $sql = $builder->buildSQL($query, $params);

        $this->assertSQLEquals($expected, $sql);
        $this->assertEquals(['Value'], $params);
    }
}
