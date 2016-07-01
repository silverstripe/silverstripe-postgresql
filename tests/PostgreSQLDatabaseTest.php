<?php

namespace SilverStripe\PostgreSQL\Test;

use SapphireTest;


use Page;
use Exception;

use SilverStripe\ORM\DB;
use SilverStripe\PostgreSQL\PostgreSQLDatabase;
use SilverStripe\ORM\DataObject;


/**
 * @package postgresql
 * @subpackage tests
 */
class PostgreSQLDatabaseTest extends SapphireTest
{
    public function testReadOnlyTransaction()
    {
        if (
            DB::get_conn()->supportsTransactions() == true
            && DB::get_conn() instanceof PostgreSQLDatabase
        ) {
            $page=new Page();
            $page->Title='Read only success';
            $page->write();

            DB::get_conn()->transactionStart('READ ONLY');

            try {
                $page=new Page();
                $page->Title='Read only page failed';
                $page->write();
            } catch (Exception $e) {
                //could not write this record
                //We need to do a rollback or a commit otherwise we'll get error messages
                DB::get_conn()->transactionRollback();
            }

            DB::get_conn()->transactionEnd();

            DataObject::flush_and_destroy_cache();

            $success=DataObject::get('Page', "\"Title\"='Read only success'");
            $fail=DataObject::get('Page', "\"Title\"='Read only page failed'");

            //This page should be in the system
            $this->assertTrue(is_object($success) && $success->exists());

            //This page should NOT exist, we had 'read only' permissions
            $this->assertFalse(is_object($fail) && $fail->exists());
        } else {
            $this->markTestSkipped('Current database is not PostgreSQL');
        }
    }
}
