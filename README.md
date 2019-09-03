# PostgreSQL Module Module

[![Build Status](https://travis-ci.org/silverstripe/silverstripe-postgresql.png?branch=master)](https://travis-ci.org/silverstripe/silverstripe-postgresql)
[![SilverStripe supported module](https://img.shields.io/badge/silverstripe-supported-0071C4.svg)](https://www.silverstripe.org/software/addons/silverstripe-commercially-supported-module-list/)

## Requirements

* SilverStripe 4.0
* PostgreSQL 8.3.x or greater must be installed
* PostgreSQL <8.3.0 may work if T-Search is manually installed

## Installation

 1. Install via composer `composer require silverstripe/postgresql` or extract the contents
    so they reside as a `postgresql` directory inside your SilverStripe project code
 2. Open the installer by browsing to install.php, e.g. http://localhost/silverstripe/install.php
 3. Select PostgreSQL in the database list and enter your database details

## Usage Overview

See docs/en for more information about configuring the module.
	
## Known issues

All column and table names must be double-quoted.  PostgreSQL automatically 
lower-cases columns, and your queries will fail if you don't.

Collations have known issues when installed on Alpine, MacOS X and BSD derivatives
(see [PostgreSQL FAQ](https://wiki.postgresql.org/wiki/FAQ#Why_do_my_strings_sort_incorrectly.3F)).  
We do not support such installations, although they still may work correctly for you.  
As a workaround for PostgreSQL 10+ you could manually switch to ICU collations (e.g. und-x-icu).
There are no known workarounds for PostgreSQL <10.

Ts_vector columns are not automatically detected by the built-in search 
filters.  That means if you're doing a search through the CMS on a ModelAdmin
object, it will use LIKE queries which are very slow.  If you're writing your 
own front-end search system, you can specify the columns to use for search 
purposes, and you get the full benefits of T-Search.

If you are using unsupported modules, there may be instances of MySQL-specific 
SQL queries which will need to be made database-agnostic where possible.
