# PostgreSQL Module Module

## Maintainer Contact

 * Sam Minnee (Nickname: sminnee) <sam@silverstripe.com>

## Requirements

* SilverStripe 3.0
* PostgreSQL 8.3.x or greater must be installed
* PostgreSQL <8.3.0 may work if T-Search is manually installed
*  Known to work on OS X Leopard, Windows Server 2008 R2 and Linux

## Installation

 1. Extract the contents so they reside as a **postgresql** directory inside your SilverStripe project code
 2. Open the installer by browsing to install.php, e.g. http://localhost/silverstripe/install.php
 3. Select PostgreSQL in the database list and enter your database details

## Usage Overview

See docs/en for more information about configuring the module.
	
## Known issues

All column and table names must be double-quoted.  PostgreSQL automatically 
lower-cases columns, and your queries will fail if you don't.

Ts_vector columns are not automatically detected by the built-in search 
filters.  That means if you're doing a search through the CMS on a ModelAdmin
object, it will use LIKE queries which are very slow.  If you're writing your 
own front-end search system, you can specify the columns to use for search 
purposes, and you get the full benefits of T-Search.

If you are using unsupported modules, there may be instances of MySQL-specific 
SQL queries which will need to be made database-agnostic where possible.