""" sql parser """
from PyDBError import *
from heap_file import *
from operators import *
import copy
from parsec import *

whitespace = regex(r'\s*', re.MULTILINE)

lexeme = lambda p: p << whitespace

lbrace = lexeme(string('{'))
rbrace = lexeme(string('}'))
lbracket = lexeme(string('('))
rbracket = lexeme(string(')'))
variable = lexeme(regex(r'[a-zA-Z]+'))
string_variable = regex(r'[_a-zA-Z][_a-zA-Z0-9]*')
dot = lexeme(string('.'))
comma = lexeme(string(','))

equal = lexeme(string('='))
bigger = lexeme(string('>'))
smaller = lexeme(string('<'))
compare = equal | bigger | smaller

Sum = lexeme(string('SUM'))
Avg = lexeme(string('AVG'))
Count = lexeme(string('COUNT'))
Min = lexeme(string('min'))
Max = lexeme(string('MAX'))
agg_op = Sum | Avg | Count | Max | Min  # .....cannot use MIN | MAX ...??? can use min | MAX?

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

@generate
def agg_ref():
    e1 = yield agg_op
    yield lbracket
    e2 = yield column_ref
    yield rbracket
    return ['agg_ref', e1, e2]

select_item = column_ref | number

select = lexeme(string('select')) | lexeme(string('SELECT'))

@generate
def select_clause():
    yield select
    e = yield sepBy(agg_ref | select_item, comma)
    yield lexeme(string('from')) | lexeme(string('FROM'))
    e2 = yield sepBy(variable, comma)
    return ['select', e,'from',e2]

where_item = column_ref | number | variable

where = lexeme(string('where')) | lexeme(string('WHERE'))

@generate
def where_clause():
    yield where
    e = yield sepBy(where_item + compare + (string_variable | where_item), comma)
    return ['where', e]

join_item = column_ref  # join c on a.b=c.d

join = lexeme(string('join')) | lexeme(string('JOIN'))

@generate
def join_clause():
    yield join
    e1 = yield variable  # skip the first sentence
    yield lexeme(string('on')) | lexeme(string('ON'))
    e2 = yield sepBy(join_item + compare + join_item,comma)
    return ['join',e1, e2]

group_by = lexeme(string('group by')) | lexeme(string('GROUP BY'))

@generate
def group_by_clause():
    yield group_by
    e = yield sepBy(column_ref | variable,comma)
    return ['group by', e]


''' composer with parser'''
class ParserComposer:
    '''structure of input: SELECT  listA(could be cols in a table or sum()) FROM listT WHERE P GROUP BY listA
    or SELECT * FROM listA JOIN listB ON a.field=b.field' '''
    def __init__(self,input_string,table_list):
        '''input at most 2 table files, at least 1 table file'''
        self.has_filter=input_string.find('WHERE')
        self.has_groupby=input_string.find('GROUP BY')
        self.has_join=input_string.find('JOIN')
        if input_string.find('SELECT')==-1:
            raise PyDBInternalError('miss SELECT')
        if input_string.find('FROM')==-1:
            raise PyDBInternalError('miss FROM, loss the origin of information')

        '''output and input of this command'''
        self.output=select_clause(input_string,1)[2][1]  # [['column_ref', table name, field name], ...]
        self.input=select_clause(input_string,1)[2][3]  # [table name1, table name2,...]
        self.input_table=table_list  # list of tables, like [table1, table2, ...]

        for table in self.input:
            flag=-1
            for file in self.input_table:
                if table==file.file_schema_data[0]:  # compare with file's schema
                    flag=1
                    break
            if flag==-1:
                raise PyDBInternalError('table not found in the input table_list')

        self.out_field=[]
        self.out_tableNO=[]
        self.out_agg=[]

        '''has join or not'''
        if self.has_join!=-1:
            join_string=' '+input_string[self.has_join:len(input_string)]
            self.join_info=join_clause(join_string,1)[2][2][0]
            self.join_table1=self.input[0]
            self.join_table2=join_clause(join_string,1)[2][1]
            self.input.append(self.join_table2)
            temp_op=join_string[join_string.find('ON')+3:len(join_string)]
            if temp_op.find('=')!=-1:
                self.join_field1=self.join_info[0][0][2]
                self.join_field2=self.join_info[1][2]
                self.join_op='='
            else:
                raise PyDBInternalError('join only suit for = currently')
        else:
            self.join_info=None

        '''whether output is valid, with aggregate(afield) determined'''
        for k in range(len(self.output)):
            flag=-1
            for q in range(len(self.input)):
                if self.output[k][0]=='column_ref':
                    if self.output[k][1]==self.input[q]:
                        flag=1
                        self.out_tableNO.append(q)
                        temp_field=self.output[k][2]
                        agg_op=None
                        self.out_agg.append(agg_op)
                        if q>=len(table_list):
                            raise PyDBInternalError('table cannot found')
                        field_name_existence(table_list[q].data,temp_field)
                        self.out_field.append(temp_field)
                        break
                elif self.output[k][0]=='agg_ref':
                    if self.output[k][2][1]==self.input[q]:
                        flag=1
                        self.out_tableNO.append(q)
                        if self.output[k][1]=='SUM':
                            temp_field=self.output[k][2][2]
                            agg_op='SUM'
                        elif self.output[k][1]=='AVG':
                            temp_field=self.output[k][2][2]
                            agg_op='AVG'
                        elif self.output[k][1]=='COUNT':
                            temp_field=self.output[k][2][2]
                            agg_op='COUNT'
                        elif self.output[k][1]=='min':
                            temp_field=self.output[k][2][2]
                            agg_op='min'
                        elif self.output[k][1]=='MAX':
                            temp_field=self.output[k][2][2]
                            agg_op='MAX'
                        else:
                            temp_field=self.output[k][2]
                            agg_op=None
                        self.out_agg.append(agg_op)
                        field_name_existence(table_list[q].data,temp_field)
                        self.out_field.append(temp_field)
                        break
            if flag==-1:
                raise PyDBInternalError('output table not found in input')

        '''has filter or not'''
        if self.has_filter!=-1:
            where_string=' '+input_string[self.has_filter:len(input_string)]
            self.filter_op=where_clause(where_string,1)[2][1]  # list of where clauses
            '''get table field operand of filter'''
            self.filter_info=[]  # structure:lists of [[table],[field],[operand]], note operand like '>90','>' or '<['col_ref',..]','<'
            for i in range(len(self.filter_op)):
                if self.filter_op[i][0][1]!='=' and self.filter_op[i][0][1]!='>' and self.filter_op[i][0][1]!='<':
                    raise PyDBInternalError('only suit for < or > or = filter ')
                split_signal=self.filter_op[i][0][1]
                if isinstance(self.filter_op[i][1],list)!=True:
                    '''compare with a const'''
                    if type(self.filter_op[i][1])==float:  # only suit for int type
                        self.filter_info.append([[self.filter_op[i][0][0][1]],[self.filter_op[i][0][0][2]],str(int(self.filter_op[i][1])),split_signal])
                    else:
                        self.filter_info.append([[self.filter_op[i][0][0][1]],[self.filter_op[i][0][0][2]],str(self.filter_op[i][1]),split_signal])
                    '''whether is the operation tables in input or not'''
                    flag=-1
                    for j in range(len(self.input)):
                        if self.filter_info[i][0][0]==self.input[j]:
                            flag=1
                else:
                    '''compare with another data in table'''
                    self.filter_info.append([[self.filter_op[i][0][0][1],self.filter_op[i][1][1]],
                        [self.filter_op[i][0][0][2],self.filter_op[i][1][2]],str(self.filter_op[i][1]),split_signal])
                    '''whether is the operation tables in input or not'''
                    flag=-1
                    for j in range(len(self.input)):
                        if self.filter_info[i][0][0]==self.input[j]:
                            for k in range(len(self.input)):
                                if self.filter_info[i][0][1]==self.input[k]:
                                    flag=1
                if flag==-1:
                    raise PyDBInternalError('table not found in input lists')
        else:
            self.filter_op=None

        '''has groupby or not'''
        if self.has_groupby!=-1:
            group_by_string=' '+input_string[self.has_groupby:len(input_string)]
            self.groupby_op=group_by_clause(group_by_string,1)[2][1][0]  # only GROUP BY one field each time
        else:
            self.groupby_op=None

    def do_comp(self):
        '''get the result from the translation
        structure:[[output1,[data]],[output2,[data]]...]'''
        result=[]
        for element in self.output:
            result.append([element,[]])

        '''do the Filter first,
        do the join the next,
        do the aggregate(sum,... and groupby) in the last at same step'''
        for index in range(len(result)):
            '''subresult:[output1,[data]]'''
            if self.has_groupby==-1:
                if self.has_join==-1:
                    '''no JOIN, no GROUP BY'''
                    if self.has_filter==-1:
                        '''SELECT ... FROM ...'''
                        if self.out_agg[index]==None:
                            for Tuples in Iterator(self.input_table[self.out_tableNO[index]].data).fetch_next():
                                if Tuples!=None:
                                    result[index][1].append(Tuples[3+field_name_existence_in_tuples(Tuples,self.out_field[index])])
                        else:
                            '''has agg_op'''
                            result[index][1]=Aggregate(table=self.input_table[self.out_tableNO[index]],afield=self.out_field[index],
                                      gfield=-1,operand=self.out_agg[index]).do_calc()[0]
                    else:
                        '''SELECT ... FROM ... WHERE ... '''
                        #print(self.filter_info,self.input,self.out_agg)
                        if self.out_agg[index]==None:
                            lists_child_op=[]
                            for i in range(len(self.filter_info)):
                                temp_table=None
                                for j in range(len(self.input)):
                                    if self.filter_info[i][0][0]==self.input[j]:
                                        try:
                                            temp_table=self.input_table[j]
                                        except:
                                            raise PyDBInternalError('input table not correct')
                                if temp_table==None:
                                    raise PyDBInternalError('input table not found')
                                if len(self.filter_info)==0:
                                    lists_child_op.append(None)
                                elif i<len(self.filter_info):
                                    if i==0:
                                        lists_child_op.append([Filter,self.filter_info[i][1][0],self.filter_info[i][3]+' '+self.filter_info[i][2],None])
                                    else:
                                        lists_child_op.append([Filter,self.filter_info[i][1][0],self.filter_info[i][3]+' '+self.filter_info[i][2],lists_child_op[i-1]])
                            child_op=lists_child_op[-1]
                            for Tuples in Filter(temp_table,self.filter_info[i][1][0],self.filter_info[i][3]+' '+self.filter_info[i][2],child_op).do_filter():
                                if Tuples!=None:
                                    result[index][1].append(Tuples[3+field_name_existence_in_tuples(Tuples,self.out_field[index])])
                        else:
                            '''has agg_op'''
                            for i in range(len(self.filter_info)):
                                temp_table=None
                                for j in range(len(self.input)):
                                    if self.filter_info[i][0][0]==self.input[j]:
                                        try:
                                            temp_table=self.input_table[j]
                                        except:
                                            raise PyDBInternalError('input table not correct')
                                if temp_table==None:
                                    raise PyDBInternalError('input table not found')
                                if len(self.filter_info)==0:
                                    lists_child_op=None
                                    break
                                elif i<len(self.filter_info):
                                    if i==0:
                                        lists_child_op.append([Filter,self.filter_info[i][1][0],self.filter_info[i][3]+' '+self.filter_info[i][2],None])
                                    else:
                                        lists_child_op.append([Filter,self.filter_info[i][1][0],self.filter_info[i][3]+' '+self.filter_info[i][2],lists_child_op[i-1]])
                            child_op=lists_child_op[-1]
                            result[index][1]=Aggregate(table=self.input_table[self.out_tableNO[index]],afield=self.out_field[index],
                                      gfield=-1,operand=self.out_agg[index],child_op=child_op).do_calc()[0]
                else:
                    '''has JOIN, no GROUP BY'''
                    if self.has_filter==-1:
                        '''SELECT ... FROM ...JOIN ... ON'''
                        if self.out_agg[index]==None:
                            for Tuples in Join(self.input_table[0],self.input_table[1],self.join_field1,self.join_field2,self.join_op).do_join():
                                if Tuples!=None:
                                    result[index][1].append(Tuples[3+field_name_existence_in_tuples(Tuples,self.out_field[index])])
                        else:
                            '''has agg_op'''
                            result[index][1]=Aggregate(table=self.input_table[self.out_tableNO[index]],afield=self.out_field[index],
                                      gfield=-1,operand=self.out_agg[index]).do_calc()[0]
                    else:
                        '''SELECT ... FROM ...JOIN... ON ... WHERE ... '''
                        if self.out_agg[index]==None:
                            filter_child=[None,None]
                            lists_child_op=[]
                            for i in range(len(self.filter_info)):
                                temp_table=None
                                for j in range(len(self.input)):
                                    if self.filter_info[i][0][0]==self.input[j]:
                                        try:
                                            temp_table=self.input_table[j]
                                        except:
                                            raise PyDBInternalError('input table not correct')
                                    if self.input[j]!=self.filter_info[i-1][0][0]:
                                        lists_child_op=[]
                                if temp_table==None:
                                    raise PyDBInternalError('input table not found')
                                if lists_child_op==[]:
                                    child_op=[Filter,self.filter_info[i][1][0],self.filter_info[i][3]+' '+self.filter_info[i][2],None]
                                    lists_child_op.append(child_op)
                                else:
                                    child_op=[Filter,self.filter_info[i][1][0],self.filter_info[i][3]+' '+self.filter_info[i][2],lists_child_op[i-1]]
                                    lists_child_op.append(child_op)
                                filter_child[i]=lists_child_op[-1]

                            for Tuples in Join(self.input_table[0],self.input_table[1],self.join_field1,self.join_field2,self.join_op,
                                               filter_child[0],filter_child[1]).do_join():
                                if Tuples!=None:
                                    result[index][1].append(Tuples[3+field_name_existence_in_tuples(Tuples,self.out_field[index])])
                        else:
                            '''has agg_op'''
                            filter_child=[None,None]
                            lists_child_op=[]
                            for i in range(len(self.filter_info)):
                                temp_table=None
                                for j in range(len(self.input)):
                                    if self.filter_info[i][0][0]==self.input[j]:
                                        try:
                                            temp_table=self.input_table[j]
                                        except:
                                            raise PyDBInternalError('input table not correct')
                                    if self.input[j]!=self.filter_info[i-1][0][0]:
                                        lists_child_op=[]
                                if temp_table==None:
                                    raise PyDBInternalError('input table not found')
                                if lists_child_op==[]:
                                    child_op=[Filter,self.filter_info[i][1][0],self.filter_info[i][3]+' '+self.filter_info[i][2],None]
                                    lists_child_op.append(child_op)
                                else:
                                    child_op=[Filter,self.filter_info[i][1][0],self.filter_info[i][3]+' '+self.filter_info[i][2],lists_child_op[i-1]]
                                    lists_child_op.append(child_op)
                                filter_child[i]=lists_child_op[-1]

                            join_child=[Join,[self.input_table[0],self.join_field1],[self.input_table[1],self.join_field2],self.join_op,filter_child[0],filter_child[1]]
                            result[index][1]=Aggregate(table=self.input_table[self.out_tableNO[index]],afield=self.out_field[index],
                                      gfield=-1,operand=self.out_agg[index],child_op=join_child).do_calc()[0]

            else:
                '''has GROUP BY'''
                if self.has_join==-1:
                    '''no JOIN'''
                    if self.has_filter==-1:
                        '''SELECT ... FROM ... GROUP BY ...'''
                        if self.out_agg[index]==None:
                            for Tuples in Aggregate(table=self.input_table[self.out_tableNO[index]],afield=-1,gfield=self.groupby_op[2],operand=None).group_by():
                                if Tuples!=None:
                                    temp_result=[]
                                    for Tuple in Tuples:
                                        temp_result.append(Tuple[3+field_name_existence_in_tuples(Tuple,self.out_field[index])])
                                    result[index][1].append(temp_result)
                        else:
                            '''has agg_op'''
                            result[index][1]=Aggregate(table=self.input_table[self.out_tableNO[index]],afield=self.out_field[index],
                                      gfield=self.groupby_op[2],operand=self.out_agg[index]).do_calc()

                    else:
                        '''SELECT ... FROM ... WHERE ... GROUP BY ...'''
                        if self.out_agg[index]==None:
                            filter_child=[None,None]  # only onr filter_child
                            lists_child_op=[]
                            for i in range(len(self.filter_info)):
                                temp_table=None
                                for j in range(len(self.input)):
                                    if self.filter_info[i][0][0]==self.input[j]:
                                        try:
                                            temp_table=self.input_table[j]
                                        except:
                                            raise PyDBInternalError('input table not correct')
                                    if self.input[j]!=self.filter_info[i-1][0][0]:
                                        lists_child_op=[]
                                if temp_table==None:
                                    raise PyDBInternalError('input table not found')
                                if lists_child_op==[]:
                                    child_op=[Filter,self.filter_info[i][1][0],self.filter_info[i][3]+' '+self.filter_info[i][2],None]
                                    lists_child_op.append(child_op)
                                else:
                                    child_op=[Filter,self.filter_info[i][1][0],self.filter_info[i][3]+' '+self.filter_info[i][2],lists_child_op[i-1]]
                                    lists_child_op.append(child_op)
                                filter_child[i]=lists_child_op[-1]

                            for Tuples in Aggregate(table=self.input_table[self.out_tableNO[index]],afield=-1,gfield=self.groupby_op[2],
                                                    operand=None,child_op=filter_child[0]).group_by():
                                if Tuples!=None:
                                    temp_result=[]
                                    for Tuple in Tuples:
                                        temp_index=field_name_existence_in_tuples(Tuple,self.out_field[index])
                                        name=Tuple[3+field_name_existence_in_tuples(Tuple,self.groupby_op[2])]
                                        temp_result.append(Tuple[3+temp_index])
                                    temp_result.append(name)
                                    result[index][1].append(temp_result)
                        else:
                            '''has agg_op'''
                            for i in range(len(self.filter_info)):
                                temp_table=None
                                for j in range(len(self.input)):
                                    if self.filter_info[i][0][0]==self.input[j]:
                                        try:
                                            temp_table=self.input_table[j]
                                        except:
                                            raise PyDBInternalError('input table not correct')
                                if temp_table==None:
                                    raise PyDBInternalError('input table not found')
                                if len(self.filter_info)==0:
                                    lists_child_op.append(None)
                                elif i<len(self.filter_info):
                                    if i==0:
                                        lists_child_op.append([Filter,self.filter_info[i][1][0],self.filter_info[i][3]+' '+self.filter_info[i][2],None])
                                    else:
                                        lists_child_op.append([Filter,self.filter_info[i][1][0],self.filter_info[i][3]+' '+self.filter_info[i][2],lists_child_op[i-1]])
                            child_op=lists_child_op[-1]
                            result[index][1]=Aggregate(table=self.input_table[self.out_tableNO[index]],afield=self.out_field[index],
                                      gfield=self.groupby_op[2],operand=self.out_agg[index],child_op=child_op).do_calc()

                else:
                    '''has JOIN'''
                    # havenot creat this part yet, may need to create another temporary heap file?
        return result
