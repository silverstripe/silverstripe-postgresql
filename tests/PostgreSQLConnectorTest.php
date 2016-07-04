<?php

use SilverStripe\PostgreSQL\PostgreSQLConnector;

/**
 * Description of PostgreSQLConnectorTest
 *
 * @author Damian
 */
class PostgreSQLConnectorTest extends SapphireTest
{
    public function testSubstitutesPlaceholders()
    {
        $connector = new PostgreSQLConnector();

        // basic case
        $this->assertEquals(
            "SELECT * FROM Table WHERE ID = $1",
            $connector->replacePlaceholders("SELECT * FROM Table WHERE ID = ?")
        );

        // Multiple variables
        $this->assertEquals(
            "SELECT * FROM Table WHERE ID = $1 AND Name = $2",
            $connector->replacePlaceholders("SELECT * FROM Table WHERE ID = ? AND Name = ?")
        );

        // Ignoring question mark placeholders within string literals
        $this->assertEquals(
                "SELECT * FROM Table WHERE ID = $1 AND Name = $2 AND Content = '<p>What is love?</p>'",
            $connector->replacePlaceholders(
                "SELECT * FROM Table WHERE ID = ? AND Name = ? AND Content = '<p>What is love?</p>'"
            )
        );

        // Ignoring question mark placeholders within string literals with escaped slashes
        $this->assertEquals(
                "SELECT * FROM Table WHERE ID = $1 AND Title = '\\'' AND Content = '<p>What is love?</p>' AND Name = $2",
            $connector->replacePlaceholders(
                "SELECT * FROM Table WHERE ID = ? AND Title = '\\'' AND Content = '<p>What is love?</p>' AND Name = ?"
            )
        );

        // same as above, but use double single quote escape syntax
        $this->assertEquals(
                "SELECT * FROM Table WHERE ID = $1 AND Title = '''' AND Content = '<p>What is love?</p>' AND Name = $2",
            $connector->replacePlaceholders(
                "SELECT * FROM Table WHERE ID = ? AND Title = '''' AND Content = '<p>What is love?</p>' AND Name = ?"
            )
        );
    }
}
