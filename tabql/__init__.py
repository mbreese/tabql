# Copyright (c) 2014, Marcus Breese
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice, this
#   list of conditions and the following disclaimer in the documentation and/or
#   other materials provided with the distribution.
#
# * Neither the names of the authors nor contributors may not be used to endorse or
#   promote products derived from this software without specific prior written
#   permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
# ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
# ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


import sys
import os
import sqlite3
import tempfile

import tab

class TabQL(object):
    def __init__(self, fnames, dbfname=None, noheader=False, headercomment=False, tmpdir=None, verbose=False):
        self.fnames = fnames

        self.noheader = noheader
        self.headercomment = headercomment
        self.verbose = verbose

        if tmpdir == None:
            if 'TMPDIR' in os.environ:
                self.tmpdir = os.environ['TMPDIR']
            elif 'TMP' in os.environ:
                self.tmpdir = os.environ['TMP']
            else:
                self.tmpdir = '/tmp'
        else:
            self.tmpdir = tmpdir


        if dbfname:
            self.dbfname = dbfname
            self._istmpdb = False
        else:
            tmp = tempfile.NamedTemporaryFile(prefix='.tmp', suffix='.db', dir=tmpdir)
            self.dbfname = tmp.name
            tmp.close()
            self._istmpdb = True

        self.__log('Using SQLite database: %s' % self.dbfname)
        self.conn = sqlite3.connect(self.dbfname)
        self.__setup()

    def __log(self, msg):
        if self.verbose:
            sys.stderr.write('%s\n' % msg)
            sys.stderr.flush()

    def __setup(self):
        for i, (file_type, tablename, fname) in enumerate(self.fnames):
            self.__log('Importing table %s from %s' % (tablename, fname))

            if file_type == '-tab':
                reader = tab.TabReader(fname, noheader=self.noheader, headercomment=self.headercomment)

                coldefs = ["'%s' %s" % (x,y) for x,y in zip(reader.headers, reader.coltypes)]
                schema = 'CREATE TABLE %s (%s);' % (tablename, ','.join(coldefs))
                if self.verbose:
                    sys.stderr.write('%s\n' % schema)
                self.conn.execute(schema)
                self.conn.commit()

                buffer = []

                sql = 'INSERT INTO %s (%s) VALUES (%s)' % (tablename, ','.join(["'%s'" % x for x in reader.headers]), ','.join(['?',] * len(reader.headers)))

                i=0
                for cols in reader.get_values():
                    i += 1
                    buffer.append(cols)
                    if len(buffer) > 1000:
                        self.conn.executemany(sql, buffer)
                        self.conn.commit()
                        buffer = []

                if buffer:
                    self.conn.executemany(sql, buffer)
                    self.conn.commit()

                self.__log('%s rows imported' % i)

    def close(self):
        if self._istmpdb:
            self.__log('Removing SQLite database: %s' % self.dbfname)
            os.unlink(self.dbfname)

    def execute(self, query, args=()):
        if not self.conn:
            self.conn = sqlite3.connect(self.dbfname)

        c = self.conn.cursor()
        self.__log('Query: %s' % query)

        try:
            colnames = None
            for row in c.execute(query, args):
                if not colnames:
                    colnames = [x[0] for x in c.description]
                yield (colnames, row)
        except sqlite3.OperationalError, e:
            sys.stderr.write('SQL Error: %s\n' % e.message)
            return

        c.close()
        self.conn.close()

