import sys, os, ntpath
from os.path import expanduser

class HoffImporter:
    dir = None
    filecontent = None
    sources = None
    def __init__(self, filepath):
        head, tail = ntpath.split(filepath)
        home = expanduser("~")
        self.dir = home + '/.pghoffserver/datadump/' + tail.split('.')[0] + '/'
        if not os.path.exists(self.dir):
            os.makedirs(self.dir)
        self.filecontent = self.read_import_file(filepath)
        self.sources = self.get_import_sources()

    def get_from_queries(self):
        return {
                'alias': self.sources.get('from'),
                'queries': self.build_import(self.filecontent, 'from')
                }

    def get_to_queries(self):
        return {
                'alias': self.sources.get('to'),
                'queries': self.build_import(self.filecontent, 'to')
                }

    def read_import_file(self, filepath):
        with open(filepath, 'r') as my_file:
            return my_file.readlines()

    def get_import_sources(self):
        for current_line in self.filecontent:
            if current_line[:7] == '#source':
                source = current_line[8:].rstrip('\n')
            if current_line[:7] == '#target':
                target = current_line[8:].rstrip('\n')
        if not target or not source:
            return None
        return {'from': source, 'to': target}

    def build_from_line(self, table, columns, filter):
        output = '\copy (SELECT ' + (columns.rstrip('\n') if columns else '*') + ' FROM ' + table.rstrip('\n') + ((' WHERE ' + filter.rstrip('\n')) if filter else '') + ')'
        output += ' TO ' + '\'' + self.dir + table.rstrip('\n') + '.csv' + '\'' + ' WITH CSV HEADER'
        return output + '\n'

    def build_to_line(self, table, columns, filter):
        output = []
        #output.append('TRUNCATE TABLE  ' + table.rstrip('\n') + ' CASCADE')
        output.append('ALTER TABLE ' + table.rstrip('\n') + ' DISABLE TRIGGER ALL')
        output.append('\copy ' + table.rstrip('\n') + ' FROM ' + '\'' + self.dir + table.rstrip('\n') + '.csv' + '\'' + ' WITH CSV HEADER')
        output.append('ALTER TABLE ' + table.rstrip('\n') + ' ENABLE TRIGGER ALL')

        return output

    def build_import(self, filecontent, import_part):
        output = []
        state = None
        old_state = None
        table = None
        columns = None
        filter = None
        linenumber = 0
        for current_line in filecontent:
            old_state = state
            linenumber += 1
            if current_line[:6] == '#table':
                state = 'table'
                new_table = current_line[7:]
            elif current_line[:8] == '#columns':
                state = 'columns'
                columns = current_line[9:]
            elif current_line[:7] == '#filter':
                state = 'filter'
                filter = current_line[8:]
            else:
                continue
            if state == 'table' and not new_table or new_table == '':
                print('Parse error! Unknown table, line:', linenumber)
            elif state == 'columns' and not columns or columns == '':
                print('Parse error! Unknown columns, line:', linenumber)
            elif state == 'filter' and not filter or filter == '':
                print('Parse error! Unknown filter, line:', linenumber)

            # if new state is table and old state is not a table we can cunstruct an output
            # if both are table then we wait for columns and/or filter or to the end of the file to cunstruct the next output
            if state == 'table' and old_state and old_state != 'table':
                if import_part == 'from':
                    output.append(self.build_from_line(table, columns, filter))
                elif import_part == 'to':
                    output += self.build_to_line(table, columns, filter)

                columns = None
                filter = None
            table = new_table
        if table:
            if import_part == 'from':
                output.append(self.build_from_line(table, columns, filter))
            elif import_part == 'to':
                output += self.build_to_line(table, columns, filter)
        return output
