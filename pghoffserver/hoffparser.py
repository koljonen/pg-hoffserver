import sqlparse, itertools
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML, Name

def is_subselect(parsed):
    if not parsed.is_group:
        return False
    for item in parsed.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            return True
    return False

def extract_from_part(parsed):
    from_seen = False
    for item in parsed.tokens:
        if item.ttype is Name:
            yield item
        if item.is_group:
            for x in extract_from_part(item):
                yield x
        if from_seen:
            if is_subselect(item):
                for x in extract_from_part(item):
                    yield x
            elif item.ttype is Keyword and item.value.upper() in ['ORDER', 'GROUP', 'BY', 'HAVING']:
                from_seen = False
                StopIteration
            else:
                yield item
        if item.ttype is Keyword and item.value.upper() == 'FROM':
            from_seen = True

def extract_table_identifiers(token_stream):
    for item in token_stream:
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                value = identifier.value.replace('"', '').lower()
                fullvalue = value
                value = value.split(' ')[0].split('.')
                schema = value[0] if len(value) > 1 else 'public'
                value = value[0] if len(value) == 1 else value[1]
                yield {'schema': schema, 'table': value, 'expression': fullvalue}
        elif isinstance(item, Identifier):
            value = item.value.replace('"', '').lower()
            fullvalue = value
            value = value.split(' ')[0].split('.')
            schema = value[0] if len(value) > 1 else 'public'
            value = value[0] if len(value) == 1 else value[1]
            yield {'schema': schema, 'table': value, 'expression': fullvalue}
        #elif item.ttype is Name:
        #    yield item.value

def extract_tables(sql):
    extracted_tables = []
    statements = list(sqlparse.parse(sql))
    for statement in statements:
        if statement.get_type() != 'UNKNOWN':
            stream = extract_from_part(statement)
            extracted_tables = list(extract_table_identifiers(stream))
            #extracted_tables.append())
    return extracted_tables
