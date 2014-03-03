import sys
import os
import gzip

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

        try:
            for line in self.fileobj:
                yield line
        except KeyboardInterrupt:
            sys.stdout.write('\n')
            sys.exit(1)

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
