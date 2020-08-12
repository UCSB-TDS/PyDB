""" sql parser """

from parsec import *

whitespace = regex(r'\s*', re.MULTILINE)

lexeme = lambda p: p << whitespace

lbrace = lexeme(string('{'))
rbrace = lexeme(string('}')) 
variable = lexeme(regex(r'[a-zA-z]+'))
dot = lexeme(string('.'))
comma = lexeme(string(','))

number = lexeme(
            regex(r'-?(0|[1-9][0-9]*)([.][0-9]+)?([eE][+-]?[0-9]+)?')
        ).parsecmap(float)


true = lexeme(string('true')).result(True)

value = number | true

@generate
def column_ref():
    e1 = yield variable 
    yield dot
    e2 = yield variable 
    return ['column_ref', e1, e2]

select_item = column_ref | number

select = lexeme(string('select'))

@generate 
def select_clause():
    yield select
    e = yield sepBy(select_item, comma)
    return ['select', e]


