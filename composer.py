from PyDBError import *
from heap_file import *
from operators import *
import copy

HEAPFILE_SIZE = 40960
PAGE_SIZE = 5120
SLOT_SIZE = 2048
MAX_HEADER_SIZE = 1024
MAX_SLOTS = (PAGE_SIZE-MAX_HEADER_SIZE)//SLOT_SIZE  # 4 TOTAL SLOTS, Each slot can hold one tuple
MAX_PAGES = (HEAPFILE_SIZE-MAX_HEADER_SIZE)//PAGE_SIZE

class Composer:
    '''structure of input: SELECT  listA(could be cols in a table or sum()) FROM listT WHERE P GROUP BY listA
    or SELECT * FROM listA JOIN listB ON a.field=b.field' '''
    def __init__(self,input_string,table1,table2=None):
        '''input at most 2 table files, at least 1 table file'''
        self.has_filter=input_string.find('WHERE')
        self.has_groupby=input_string.find('GROUP BY')
        self.has_join=input_string.find('JOIN')
        if input_string.find('SELECT')==-1:
            raise PyDBInternalError('miss SELECT')
        if input_string.find('FROM')==-1:
            raise PyDBInternalError('miss FROM, loss the origin of information')

        '''output and input of this command'''
        self.output=input_string.split('SELECT ')[1].split(' FROM')[0].split(',')  # contains aggregate(sum...)
        self.input=input_string.split('SELECT ')[1].split(' FROM')[1].split(' ')[1].split(',')
        self.input_table=[table1,table2]
        self.out_field=[]
        self.out_tableNO=[]
        self.out_agg=[]

        '''has join or not'''
        if self.has_join!=-1:
            self.join_info=input_string.split('ON')[1].split(' ')[1]
            self.join_table1=input_string.split('FROM ')[1].split(' JOIN ')[0]
            self.join_table2=input_string.split('FROM ')[1].split(' JOIN ')[1].split(' ON')[0]
            self.input.append(self.join_table2)
            temp_op=input_string.split('FROM ')[1].split(' JOIN ')[1].split(' ON ')[1].split(' ')[0]
            if temp_op.find('=')!=-1:
                self.join_field1=temp_op.split('=')[0].split('.')[1]
                self.join_field2=temp_op.split('=')[1].split('.')[1]
                self.join_op='='
            else:
                raise PyDBInternalError('join only suit for = currently')
        else:
            self.join_info=None

        '''whether output is valid, with aggregate(afield) determined'''
        for k in range(len(self.output)):
            flag=-1
            for q in range(len(self.input)):
                if self.output[k].find(self.input[q])!=-1:
                    flag=1
                    self.out_tableNO.append(q)
                    if self.output[k].find('SUM')!=-1:
                        temp_field=self.output[k].split('.')[1][:-1]
                        agg_op='SUM'
                    elif self.output[k].find('AVG')!=-1:
                        temp_field=self.output[k].split('.')[1][:-1]
                        agg_op='AVG'
                    elif self.output[k].find('COUNT')!=-1:
                        temp_field=self.output[k].split('.')[1][:-1]
                        agg_op='COUNT'
                    elif self.output[k].find('MIN')!=-1:
                        temp_field=self.output[k].split('.')[1][:-1]
                        agg_op='MIN'
                    elif self.output[k].find('MAX')!=-1:
                        temp_field=self.output[k].split('.')[1][:-1]
                        agg_op='MAX'
                    else:
                        temp_field=self.output[k].split('.')[1]
                        agg_op=None
                    self.out_agg.append(agg_op)
                    if q==0:
                        field_name_existence(table1.data,temp_field)
                        self.out_field.append(temp_field)
                    elif q==1:
                        field_name_existence(table2.data,temp_field)
                        self.out_field.append(temp_field)
                    else:
                        raise PyDBInternalError('table cannot found')
                    break
            if flag==-1:
                raise PyDBInternalError('output table not found in input')

        '''has filter or not'''
        if self.has_filter!=-1:
            if self.has_groupby!=-1:
                self.filter_op=input_string.split('WHERE ')[1].split(' GROUP BY')[0].split(',')
            else:
                self.filter_op=input_string.split('WHERE ')[1].split(',')
            '''get table field operand of filter'''
            self.filter_info=[]  # structure:lists of [[table],[field],[operand]]
            for i in range(len(self.filter_op)):
                if self.filter_op[i].find('like')!=-1:
                    split_signal=' like '
                elif self.filter_op[i].find('=')!=-1:
                    split_signal='='
                elif self.filter_op[i].find('>')!=-1 and self.filter_op[i].find('<')==-1:
                    split_signal='>'
                elif self.filter_op[i].find('<')!=-1 and self.filter_op[i].find('>')==-1:
                    split_signal='<'
                elif self.filter_op[i].find('<')!=-1 and self.filter_op[i].find('>')!=-1:
                    split_signal='<>'  # not equal to
                else:
                    raise PyDBInternalError('only suit for like or <>= filter ')
                if self.filter_op[i].split(split_signal)[1].find('.')==-1:
                    '''compare with a const'''
                    self.filter_info.append([[self.filter_op[i].split(split_signal)[0].split('.')[0]],
                        [self.filter_op[i].split(split_signal)[0].split('.')[1]],(split_signal+self.filter_op[i].split(split_signal)[1])[1:],
                                             split_signal])
                    '''whether is the operation tables in input or not'''
                    flag=-1
                    for j in range(len(self.input)):
                        if self.filter_info[i][0][0]==self.input[j]:
                            flag=1
                else:
                    '''compare with another data in table'''
                    self.filter_info.append([[self.filter_op[i].split(split_signal)[0].split('.')[0],self.filter_op[i].split(split_signal)[1].split('.')[0]],
                        [self.filter_op[i].split(split_signal)[0].split('.')[1],self.filter_op[i].split(split_signal)[1].split('.')[1]],
                                             (split_signal+self.filter_op[i].split(split_signal)[1])[1:]])
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
            self.groupby_op=input_string.split('GROUP BY ')[1]  # only GROUP BY one field each time
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
                                if len(self.filter_info)==1:
                                    lists_child_op.append(None)
                                elif i<len(self.filter_info)-1:
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
                                if len(self.filter_info)==1:
                                    child_op=None
                                elif len(self.filter_info)==2 and i==0:
                                    child_op=[Filter,self.filter_info[i][1][0],self.filter_info[i][3]+' '+self.filter_info[i][2],None]
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
                            print(self.groupby_op)
                            for Tuples in Aggregate(table=self.input_table[self.out_tableNO[index]],afield=-1,gfield=self.groupby_op,operand=None).group_by():
                                if Tuples!=None:
                                    temp_result=[]
                                    for Tuple in Tuples:
                                        temp_result.append(Tuple[3+field_name_existence_in_tuples(Tuple,self.out_field[index])])
                                    result[index][1].append(temp_result)
                        else:
                            '''has agg_op'''
                            result[index][1]=Aggregate(table=self.input_table[self.out_tableNO[index]],afield=self.out_field[index],
                                      gfield=self.groupby_op,operand=self.out_agg[index]).do_calc()

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

                            for Tuples in Aggregate(table=self.input_table[self.out_tableNO[index]],afield=-1,gfield=self.groupby_op,
                                                    operand=None,child_op=filter_child[0]).group_by():
                                if Tuples!=None:
                                    temp_result=[]
                                    for Tuple in Tuples:
                                        temp_index=field_name_existence_in_tuples(Tuple,self.out_field[index])
                                        name=Tuple[3+field_name_existence_in_tuples(Tuple,self.groupby_op)]
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
                                if len(self.filter_info)==1:
                                    child_op=None
                                elif len(self.filter_info)==2 and i==0:
                                    child_op=[Filter,self.filter_info[i][1][0],self.filter_info[i][3]+' '+self.filter_info[i][2],None]
                            result[index][1]=Aggregate(table=self.input_table[self.out_tableNO[index]],afield=self.out_field[index],
                                      gfield=self.groupby_op,operand=self.out_agg[index],child_op=child_op).do_calc()

                else:
                    '''has JOIN'''
                    # havenot creat this part yet, may need to create another temporary heap file?
        return result
