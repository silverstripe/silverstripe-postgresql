# PostgreSQL Database Module

SilverStripe now has tentative support for PostgreSQL ('Postgres').

## Requirements

SilverStripe 2.4.0 or greater. (PostgreSQL support is NOT available 
in 2.3.).

SilverStripe supports Postgres versions 8.3.x, 8.4.x and onwards. 
Postgres 8.3.0 launched in February 2008, so SilverStripe has a fairly 
modern but not bleeding edge Postgres version requirement.

Support for 8.2.x is theoretically possible if you're willing to manually 
install T-search. 8.2.x has not been tested either, so there may be other 
compatibility issues. The EnterpriseDB versions of Postgres also work, if 
you'd prefer a tuned version.

## Installation

You have three options to install PostgreSQL support with SilverStripe. 

### Option 1 - Installer

The first option is to use the installer. However, this is currently only 
supported since SilverStripe 2.4.0 beta2 (or using the daily builds).

1. Set up SilverStripe somewhere where you can start the installer - you 
should only see one database “MySQL” to install with.
2. Download a copy of the “postgresql” module from here: 
http://silverstripe.org/postgresql-module
3. Extract the archive you downloaded. Rename the directory from 
“postgresql-trunk-rxxxx” to “postgresql” and copy it into the SilverStripe 
directory you just set up
4. Open the installer once again, and a new option “PostgreSQL” should appear. 
You can now proceed through the installation without having to change any code.

### Option 2 - Manual

The second option is to setup PostgreSQL support manually. This can be achieved 
by following these instructions:

1. Set up a fresh working copy of SilverStripe
2. Download a copy of the “postgresql” module from here: http://silverstripe.org/postgresql-module
3. Extract the archive you downloaded. Rename the directory from 
“postgresql-trunk-rxxxx” to “postgresql” and copy it into the SilverStripe 
directory you just set up.
4. Open up your mysite/_config.php file and add (or update) the $databaseConfig 
array like so:

> $databaseConfig = array(
>	'type' => 'PostgreSQLDatabase',
>	'server' => '[server address e.g. localhost]',
>	'username' => 'postgres',
>	'password' => 'mypassword',
>	'database' => 'SS_mysite'
> );

Finally, visit dev/build so that SilverStripe can build the database schema and 
default records.

### Option 3 - Environment file

Finally, the third option is to change your environment to point to 
PostgreSQLDatabase as a database class. Do this if you're currently using an
 _ss_environment.php file.

1. Download a copy of the “postgresql” module from here: http://silverstripe.org/postgresql-module
2. Extract the archive you downloaded. Rename the directory from 
postgresql-trunk-rxxxx” to “postgresql” and copy it into your SS directory
3. Add the following to your existing _ss_environment.php file:

> define('SS_DATABASE_CLASS', 'PostgreSQLDatabase');

Last steps:

1. Ensure your SS_DATABASE_USERNAME and SS_DATABASE_PASSWORD defines in 
_ss_environment.php are correct to the PostgreSQL server.
2. Ensure that your mysite/_config.php file has a database name defined, such 
as “SS_mysite”.
3. Visit dev/build so that SilverStripe can build the database schema and 
default records

## Features

Here is a quick list of what's different in the Postgres module (a full 
description follows afterwards):

* T-Search
* Extended index support
* Array data types
* Transactions
* Table partitioning
* Tablespaces
* Index clustering

If you don't know much about databases, or don't want to use any of the 
advanced features that this module provides, then you don't need to read 
any further.

The use of any of these features, especially the advanced options, implies 
that you have some level of comfort in administrating a Postgres database.

### T-Search

T-Search support is provided via both GiST and GIN. You can cluster and 
search columns with combinations of these methods. It is up to you to 
decide which is most appropriate for your data.

The dev/build process automatically creates a special column on each table, 
and a trigger is automatically set up to update this column whenever the 
targeted columns are changed. T-Search uses this column to return matches 
for search criteria.

Please see tutorial 4 for information how to enable fulltext search and the 
necessary controller hooks.

### Extended index support

Indexes have been extended to include support for more options. These new 
options include:

* The ability to specify index methods (btree/hash/). Btree is probably 
fine nearly all indexes, and it is the default. 'Unique' is also supported.
* Partial indexes. This is especially handy for creating an index while i
gnoring nulls or default data.
* Multiple column indexing. If your WHERE clauses always use the same 
columns, then you can create one index covering all of these at once.
* Fill factor. If your table content is static, then you can reduce the 
physical disk space your index uses. Also, if you use clustering, giving the 
fillfactor a low number may help performance for updates.

Examples:

**Hash index**:

> public static $indexes = array(
>    'Address'=>Array('type'=>'hash', 'name'=>'Address'),
> );

**Where clause**:

> public static $indexes = array(
>    'Address'=>Array('type'=>'unique', 'name'=>'Address', 'where'=>"\"Address\" IS NOT NULL"),
> );

**Fill factor**:

> public static $indexes = array(
>    'Address'=>Array('type'=>'unique', 'name'=>'Address', 'fillfactor'=>'50'),
> );

### Array data types

Nearly all data types in SilverStripe can now be expressed as an array. For 
example, you can specify an int as this:

> $db = array (
>     'Quantity'=>'Int[]'
> )

You would populate this like so:

> $item->Quantity='Array[1,2,3...]';

It also takes object literals if you're more familiar with that or it suits 
your purpose better, like this:

> $item->Quantity='{1,2,3}';

Using arrays as data types means that you can avoid join tables. This is not 
recommended if the SilverStripe ORM would expect a has_one or has_many etc under 
normal circumstances, but it could be useful in the case where you have a very 
large join table. You can also index these arrays with GIN indexes.

Please consult the official Postgres documentation for more information.

### Transactions

Transactions are supported at the database connection level. The relevant 
functions are:

* DB::get_conn()→startTransaction($transaction_mode, $session_characteristics)
* DB::get_conn()→transactionSavepoint($name)
* DB::get_conn()→transactionRollback($savepoint)
* DB::get_conn()→endTransaction();

You can create a savepoint by passing a name to the function, and then rollback 
either all of the uncommited transactions, or if you pass a savepoint name, 
jump back to the point you'd prefer.

$transaction_mode and $session_characteristic take the full range of isolation 
levels supported by Postgres.

Please consult the official Postgres documentation for more information.

### Table Partitioning

**This is an experimental feature.**

If you have a very large table, you can split it into many child tables. The 
advantages of this depend on your particular situation. Generally speaking, 
if your table is very large, queries should be faster.

You can create a partitioned table like this:

> public static $database_extensions = array(
>    'partitions'=>array(
>         'child_table_1'=>'NEW."ID">0 AND NEW."ID"<=100',
>         'child_table_2'=>'NEW."ID">100 AND NEW."ID"<=200'
>    )
> );

'NEW.' is a required part of the configuration string.

Partitioning should be set up right from the beginning. Partitioning a table 
which already has data may have unpredictable results.

Please consult the official Postgres documentation for more information.

### Tablespaces

**This is an experimental feature.**

Tablespaces are good for moving the physical files to a faster device (or slower 
and less used if that's a better option). You can set up a tablespace like this:

> public static $database_extensions = array(
>    'tablespace'=>Array('name'=>'fastspace', 'location'=>'/faster_location'),
> );

The '/faster_location' path must be owned by the postgres user. If you try to 
delete a tablespace via the 'drop tablespace' command, then this directory must
be empty.

Changing the location of the tablespace through the SilverStripe 
$database_extensions array will cause the dev/build process to attempt to delete 
the old location. An error message will be displayed if this location is not 
empty.

Please consult the official Postgres documentation for more information.

### Index Clustering

**This is an experimental feature.**

Index clustering allows you to reorganise the way rows are ordered inside a 
table according to an index specification. This can be a very intensive disk 
operation. You specify an index cluster like this:

> public static $database_extensions = array(
>    'cluster'=>'index_name'
> );

Clustering is only applied on a table on the second instance of a dev/build 
command being run on it (running a cluster command on an empty table is 
pointless).

Clustering needs to be reapplied on a regular basis if you're updating this 
table. You can also decrease the fillfactor on that index as well for 
potential performance gains.

As an alternative, clustering isn't necessary if you rebuild a table with 
an ORDER BY clause, where the ORDER BY column is the same as what you'd be 
clustering it by. The dev/build process does not do table rebuilds, so this 
is something you'd have to do yourself.

Please consult the official Postgres documentation for more information.

**A note about these advanced features**

The advanced features are here as an experimental offering. They have not 
been fully tested and their functionality and purpose may change in the 
future. They are primarily here to offer the ability to handle very large 
datasets.

They are also features which require the user to be very familiar with both 
Postgres and how their data works. If you can't predict how your database 
will be populated, then most of these features will be of little use.

## User contributed information

**Provided by dompie**

If you want to install this on a more secure postgresql server, go to 
PostgreSQLDatabase.php and set "public static $check_database_exists = false;"

Moreover you have to replace in PostgreSQLDatabaseConfigurationHelper.php 
occurrences of

> $connstring = "host=$server port=5432 dbname=postgres {$userPart}{$passwordPart}";

with

> $dbname = $databaseConfig['database']?$databaseConfig['database']: 'postgres';
> $connstring = "host=$server port=5432 dbname=$dbname {$userPart}{$passwordPart}";


Otherwise this extension will try to connect to "postgres" Database to check DB 
connection, no matter what you entered in the "Database Name" field during 
installation.

Make sure you have set the "search_path" correct for your database user. 

## Known Issues

All column and table names must be double-quoted.  PostgreSQL automatically 
lower-cases columns, and your queries will fail if you don't.

Ts_vector columns are not automatically detected by the built-in search filters.  
That means if you're doing a search through the CMS on a ModelAdmin object, it 
will use LIKE queries which are very slow.  

If you're writing your own front-end search system, you can specify the columns 
to use for search purposes, and you get the full benefits of T-Search.

If you are using unsupported modules, there may be instances of MySQL-specific 
SQL queries which will need to be made database-agnostic where possible.



