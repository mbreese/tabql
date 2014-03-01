'''
tabql - an interface linking tab-delimited text files (or CSV?) and SQLite.
'''
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
import gzip
import sqlite3
import tempfile


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
        for i, (file_type, fname) in enumerate(self.fnames):
            if ':' in fname:
                tablename, fname = fname.split(':', 1)
            else:
                if len(self.fnames) > 1:
                    tablename = 'tbl%s' % i
                else:
                    tablename = 'tbl'

            self.__log('Importing table %s from %s' % (tablename, fname))

            if file_type == '-tab':
                reader = TabReader(fname, noheader=self.noheader, headercomment=self.headercomment)

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
        c.execute(query, args)

        for row in c.execute(query, args):
            yield (c, row)

        c.close()
        self.conn.close()



class TabReader(object):
    def __init__(self, fname, noheader=False, headercomment=False, comment_char='#', delim='\t', auto_type_rows=100):
        if fname == '-':
            self.fileobj = sys.stdin
        elif fname[-3:] == '.gz':
            self.fileobj = gzip.open(os.path.expanduser(fname))
        else:
            self.fileobj = open(os.path.expanduser(fname))

        self.noheader = noheader
        self.headercomment = headercomment
        self.comment_char = comment_char
        self.delim = delim
        self.auto_type_rows = auto_type_rows
        self._last = []
        self.headers = self.__readheaders()
        self.coltypes = self.__autotypes()

    def __readline(self):
        while self._last:
            s = self._last[0]
            self._last = self._last[1:]
            yield s

        for line in self.fileobj:
            yield line

    def close(self):
        if self.fileobj != sys.stdin:
            self.fileobj.close()

    def __autotypes(self):
        '''
        Order of preference:
            INTEGER > REAL > TEXT
        '''
        buffer = []

        coltypes = ['INTEGER', ] * len(self.headers)

        for line in self.__readline():
            if line[0] == self.comment_char:
                continue

            cols = line.rstrip().split(self.delim)
            for i, col in enumerate(cols):
                if coltypes[i] == 'TEXT':
                    # texts can't be changed.
                    continue

                try:
                    val = int(col)
                    coltype = 'INTEGER'
                except:
                    try:
                        val = float(col)
                        if coltypes[i] == 'INTEGER':
                            coltypes[i] = 'REAL'
                    except:
                        coltypes[i] = 'TEXT'

            buffer.append(line)

            if len(buffer) > self.auto_type_rows:
                break

        self._last.extend(buffer)
        return coltypes

    def __readheaders(self):
        last = None

        for line in self.__readline():
            if line[0] == self.comment_char:
                last = line
            elif self.noheader:
                self._last.append(line)
                cols = line.rstrip().split(self.delim)
                headers = []
                for i, val in enumerate(cols):
                    headers.append('c%s' % (i+1))
                return headers

            elif self.headercomment:
                self._last.append(line)
                break
            else:
                last = line
                break

        if last[0] == self.comment_char:
            last = last[1:]

        return last.rstrip().split(self.delim)

    def __autotype(self, val, i):
        if self.coltypes[i] == 'TEXT':
            return val
        if self.coltypes[i] == 'REAL':
            return float(val)
        if self.coltypes[i] == 'INTEGER':
            return int(val)


    def get_values(self):
        for line in self.__readline():
            if line[0] != self.comment_char:
                cols = [self.__autotype(x, i) for i, x in enumerate(line.rstrip().split(self.delim))]
                yield cols

    def get_values_dict(self):
        for line in self.__readline():
            if line[0] != self.comment_char:
                d = {}
                cols = [self.__autotype(x, i) for i, x in enumerate(line.rstrip().split(self.delim))]
                for header, val in zip(self.headers, cols):
                    d[header] = val
                yield d
