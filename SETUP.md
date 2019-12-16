Setup instructions
============

- Install python 2.7
  - Windows: https://www.python.org/downloads/ - download 2.7.12, run installer,
  specify "add to PATH" during installer setup.
  - Ubuntu/Debian: sudo apt-get install python2.7 python2.7-dev
  
- Install python package management
  -  Windows: Download get-pip.py from https://bootstrap.pypa.io/get-pip.py and run python get-pip.py
  -  Ubuntu/Debian: sudo apt-get install python-pip or download get-pip.py and run like Windows
  
- Install generic python dependency
  -  pip install python-dateutil==2.8.1
 
- Install 'sqlite3' binary
  - Windows: Download/install/add folder of sqlite3.exe to
  PATH (Environment Variables).
  - Ubuntu/Debian: sudo apt-get install sqlite3

- Install other dependencies:
  - Ubuntu:
    *for PostgreSQL:
    sudo pip install psycopg2==2.6.1
    *for MSSQL - not tested on Ubuntu though:*
    sudo apt-get install unixodbc-dev
    sudo pip install pyodbc
  - Windows:
    *for PostgreSQL - not tested on Windows though:
    pip install psycopg2==2.6.1
    *for MSSQL:*
    pip install pyodbc
  
Next, except for merging into sqlite3 databases - you need to configure
'ODBC' access to your target Database Management System - either PostgreSQL, MySQL
or Microsoft SQL Server - please see config info below.

PostgreSQL
----------

Ubuntu: (see tutorial: https://www.digitalocean.com/community/tutorials/how-to-install-and-use-postgresql-on-ubuntu-14-04)

sudo apt-get install postgresql postgresql-contrib


Microsoft SQL Server Configs
----------------------------

- Install Python 'pyodbc' package via command_prompt/terminal command:  
pip install pyodbc

(Tested on Microsoft SQL Server 2014 Developer Edition)

- Create a database

- Create a login (for example, in sqlservermanagementstudio - select server
  - drill down to Security > Logins - right-click and choose "New Login"...),
  allow it to access the database above.
  
- Allow the login to edit the database:
  - Goto SQL Server management studio > choose the database > Permissions >
  check/enable: Alter, Connect, Create table, Delete, Insert
  - Click 'view server permissions' > choose the login name in logins or roles,
  check/grant 'Administer bulk operations'.
  
- Open Windows ODBC Data Source Administrator >
  Add "SQL Server Natve Client 11.0" >
  - config to the above user and database (uncheck 'translate...') 


