tabql - Use SQLite to help work with tab-delimited text files.
=====

SQL is a powerful tool for data analysis, however, it requires setting up
a database and the maintenance that entails. One of the easiest ways to handle
data storage is tab-delimited text files, but this does not lend itself to
easy analysis using SQL.

Enter SQLite...

tabql will import tab-delimited files into a dynamically created SQLite
database. You can import multiple tab-delimited files as different tables
and then be able to run queries across multiple tables. The SQLite database
can be temporary (and deleted upon completion) or permanently saved. If your
text file includes a header (commented out or otherwise), it can be used
to automatically name your columns in the SQLite schema.

Here are some examples:

    $ tabql -tab filename.txt select '*' from tbl

    $ tabql -tab table1:filename.txt select '*' from table1

    $ tabql -tab filename1.txt -tab filename2.txt select '*' from tbl1 left join tbl2


Text files can be gzip compressed

    $ tabql -tab filename.gz select 'count(*) from tbl'


You can also pipe data into tabql

    $ grep foo filename.txt | tabql select 'count(*) from tbl'

## License
BSD - three clause

## Similar projects

* Q - https://github.com/harelba/q (Python, GPL)
* TextQL - https://github.com/dinedal/textql (Go)
