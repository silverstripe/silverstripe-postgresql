<?php

namespace SilverStripe\PostgreSQL\Tests;

use SilverStripe\Core\ClassInfo;
use SilverStripe\Dev\SapphireTest;
use SilverStripe\ORM\Connect\Database;
use SilverStripe\ORM\Connect\DatabaseException;
use SilverStripe\ORM\DB;
use SilverStripe\PostgreSQL\PostgreSQLConnector;
use SilverStripe\PostgreSQL\PostgreSQLSchemaManager;

class PostgreSQLSchemaManagerTest extends SapphireTest
{

    protected $usesTransactions = false;

    public function testAlterTable()
    {
        try {
            /** @var PostgreSQLSchemaManager $dbSchema */
            $dbSchema = DB::get_schema();
            $dbSchema->quiet();

            $this->createSS3Table();

            try {
                DB::query('INSERT INTO "ClassNamesUpgrade" ("ClassName") VALUES (\'App\MySite\FooBar\')');
                $this->assertFalse(true, 'SS3 Constaint should have blocked the previous insert.');
            } catch (DatabaseException $ex) { }

            $dbSchema->schemaUpdate(function () use ($dbSchema) {
                $dbSchema->requireTable(
                    'ClassNamesUpgrade',
                    [
                        'ID' => 'PrimaryKey',
                        'ClassName' => 'Enum(array("App\\\\MySite\\\\FooBar"))',
                    ]
                );
            });

            DB::query('INSERT INTO "ClassNamesUpgrade" ("ClassName") VALUES (\'App\MySite\FooBar\')');
            $count = DB::query('SELECT count(*) FROM "ClassNamesUpgrade" WHERE "ClassName" = \'App\MySite\FooBar\'')
                ->value();

            $this->assertEquals(1, $count);
        } finally {
            DB::query('DROP TABLE IF EXISTS "ClassNamesUpgrade"');
            DB::query('DROP SEQUENCE IF EXISTS "ClassNamesUpgrade_ID_seq"');
        }

    }

    private function createSS3Table()
    {
        DB::query(<<<SQL
CREATE SEQUENCE "ClassNamesUpgrade_ID_seq" start 1 increment 1;       
CREATE TABLE "ClassNamesUpgrade"
(
  "ID" bigint NOT NULL DEFAULT nextval('"ClassNamesUpgrade_ID_seq"'::regclass),
  "ClassName" character varying(255) DEFAULT 'ClassNamesUpgrade'::character varying,
  CONSTRAINT "ClassNamesUpgrade_pkey" PRIMARY KEY ("ID"),
  CONSTRAINT "ClassNamesUpgrade_ClassName_check" CHECK ("ClassName"::text = ANY (ARRAY['FooBar'::character varying::text]))
)
WITH (
  OIDS=FALSE
);
SQL
        );
    }

    public function testRenameTable()
    {
        try {
            /** @var PostgreSQLSchemaManager $dbSchema */
            $dbSchema = DB::get_schema();
            $dbSchema->quiet();

            $this->createSS3VersionedTable();

            $this->assertConstraintCount(1, 'ClassNamesUpgrade_versioned_ClassName_check');

            $dbSchema->schemaUpdate(function () use ($dbSchema) {
                $dbSchema->renameTable(
                    'ClassNamesUpgrade_versioned',
                    'ClassNamesUpgrade_Versioned'
                );
            });

            $this->assertTableCount(0, 'ClassNamesUpgrade_versioned');
            $this->assertTableCount(1, 'ClassNamesUpgrade_Versioned');
            $this->assertConstraintCount(0, 'ClassNamesUpgrade_versioned_ClassName_check');
            $this->assertConstraintCount(1, 'ClassNamesUpgrade_Versioned_ClassName_check');

        } finally {
            DB::query('DROP TABLE IF EXISTS "ClassNamesUpgrade_Versioned"');
            DB::query('DROP TABLE IF EXISTS "ClassNamesUpgrade_versioned"');
            DB::query('DROP SEQUENCE IF EXISTS "ClassNamesUpgrade_versioned_ID_seq"');
        }

    }

    private function assertConstraintCount($expected, $constraintName) {
        $count = DB::prepared_query(
            'SELECT count(*) FROM pg_catalog.pg_constraint WHERE conname like ?',
            [$constraintName]
        )->value();

        $this->assertEquals($expected, $count);
    }

    private function assertTableCount($expected, $tableName) {
        $count = DB::prepared_query(
            'SELECT count(*) FROM pg_catalog.pg_tables WHERE "tablename" like ?',
            [$tableName]
        )->value();

        $this->assertEquals($expected, $count);
    }

    private function createSS3VersionedTable()
    {
        DB::query(<<<SQL
CREATE SEQUENCE "ClassNamesUpgrade_versioned_ID_seq" start 1 increment 1;       
CREATE TABLE "ClassNamesUpgrade_versioned"
(
  "ID" bigint NOT NULL DEFAULT nextval('"ClassNamesUpgrade_versioned_ID_seq"'::regclass),
  "ClassName" character varying(255) DEFAULT 'ClassNamesUpgrade'::character varying,
  CONSTRAINT "ClassNamesUpgrade_pkey" PRIMARY KEY ("ID"),
  CONSTRAINT "ClassNamesUpgrade_versioned_ClassName_check" CHECK ("ClassName"::text = ANY (ARRAY['FooBar'::character varying::text]))
)
WITH (
  OIDS=FALSE
);
SQL
        );
    }
}
