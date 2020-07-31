from PyDBError import *
from heap_file import *

HEAPFILE_SIZE = 40960
PAGE_SIZE = 5120
SLOT_SIZE = 2048
MAX_HEADER_SIZE = 1024
MAX_SLOTS = (PAGE_SIZE-MAX_HEADER_SIZE)//SLOT_SIZE  # 4 TOTAL SLOTS, Each slot can hold one tuple
MAX_PAGES = (HEAPFILE_SIZE-MAX_HEADER_SIZE)//PAGE_SIZE

def field_name_existence(table_data,field_name):
    '''examine the existence of a certain column in relation, return the index of this field name in schema'''
    flag=0
    index=-1
    for x in table_data[0][2][2]:
        index+=1
        if x==field_name:
            flag=1
            return index
    if flag==0:
        raise PyDBInternalError('col_name not found in schema')

def field_name_existence_in_tuples(tuple_data,field_name):
    '''examine the existence of a certain column in relation, return the index of this field name in schema'''
    flag=0
    index=-1
    for x in tuple_data[2][2]:
        index+=1
        if x==field_name:
            flag=1
            return index
    if flag==0:
        raise PyDBInternalError('field_name not found in schema')

def valid_value_examine(schema_data,field,value,Type='unknown'):
    '''examine whether the value is valid in schema'''
    for i in range(schema_data[1]):
        if field==schema_data[2][i]:
            if Type=='str':  # limit to str
                N=len(value)
                if N<256:
                    Type='CHAR(255)'  # all N less than 256 is equal to N=255
                else:
                    raise PyDBInternalError('N of CHAR(N) should be less than 256')
            else:
                Type=get_type(value)
            if Type!=schema_data[3][i]:
                raise PyDBInternalError('field_value not match with field_domain in schema')

class Filter:
    '''filter operator'''
    def __init__(self,table,field,operand,child_op=None):
        '''from query structure: table.field operand,output qualified tuples, '*'means all
        structure of operand: '><= value' or 'in (value_range1,value_range2)' or 'like value' or 'between...and... '''
        self.field_index=field_name_existence(table.data,field)
        self.input_table=table
        self.data=table.data
        self.schema=table.data[0][2]
        self.operand=operand
        self.field=field
        self.child_op=child_op
        self.value=''

    def do_filter(self):
        '''do the operation, need to transform in and between and to others'''
        self.value=''
        if self.operand=='*':
            '''return all tuples'''
            return self.all_func()
        elif self.operand.find('in ')!=-1:
            '''in (value_range1,value_range2) or in (value1,value2,...,value3), expressed in ieq_func'''
            self.value=self.operand.split(' ')[1][1:-1].split(',')  # list of value
            range_flag=-1  # whether is in a range or not
            if len(self.value)==2:
                range_flag=1
            for temp in self.value:
                valid_value_examine(self.schema,self.field,temp)
        elif self.operand.find('like ')!=-1:
            '''like value'''
            return self.like_func()
        elif self.operand.find('=')!=-1 or self.operand.find('>')!=-1 or self.operand.find('<')!=-1:
            '''><= value, suit for between... and... or in (range)'''
            return self.ieq_func()  # yield qualified tuples iterately
        elif self.operand.find('between ')!=-1 and self.operand.find(' and ')!=-1:
            '''between value1 and value2'''
            try:
                value1=int(self.operand.split(' ')[1])
                value2=int(self.operand.split(' ')[3])
            except:
                raise PyDBInternalError('only suit for int')
        else:
            raise PyDBInternalError('only suit for in or like or <>= filter ')

    def all_func(self):
        '''return all the tuple in this table that has certain field'''
        for temp_tuple in Iterator(self.data).fetch_next():
            if temp_tuple!=None:
                yield temp_tuple

    def like_func(self):
        '''yield the tuples contains the given words
        only suit for str, as cannot distinct if int is as str'''
        i=0
        for string in self.operand.split('like '):
            if i==0:  # skip the first element
                i+=1
                continue
            self.value+=string
        valid_value_examine(self.schema,self.field,self.value,'str')  # limit to str
        # iterate the input
        for temp_tuple in Iterator(self.data).fetch_next():
            if temp_tuple!=None:
                '''has the words or not'''
                if temp_tuple[self.field_index+3].find(self.value)!=-1:
                    yield temp_tuple

    def ieq_func(self):
        '''yield the tuples fit the ><= value'''
        for temp_tuple in Iterator(self.data).fetch_next():
            if temp_tuple!=None:
                if self.operand.find('=')!=-1:
                    '''equal'''
                    # print('DEBUG2',temp_tuple,self.field_index,temp_tuple[self.field_index+3],self.value)
                    self.value=self.operand.split('= ')[1]
                    valid_value_examine(self.schema,self.field,self.value)
                    try:
                        value=int(self.value)
                    except:
                        value=self.value
                    if temp_tuple[self.field_index+3]==value:
                        yield temp_tuple
                elif self.operand.find('>')!=-1 and self.operand.find('<')==-1:
                    '''bigger'''
                    self.value=self.operand.split('> ')[1]
                    valid_value_examine(self.schema,self.field,self.value)
                    try:
                        value=int(self.value)
                    except:
                        raise PyDBInternalError('> only suit for INT ')
                    if temp_tuple[self.field_index+3]>value:
                        yield temp_tuple
                elif self.operand.find('<')!=-1 and self.operand.find('>')==-1:
                    '''smaller'''
                    self.value=self.operand.split('< ')[1]
                    valid_value_examine(self.schema,self.field,self.value)
                    try:
                        value=int(self.value)
                    except:
                        raise PyDBInternalError('< only suit for INT ')
                    if temp_tuple[self.field_index+3]<value:
                        yield temp_tuple
                else:
                    '''inequal'''
                    self.value=self.operand.split('<> ')[1]
                    valid_value_examine(self.schema,self.field,self.value)
                    try:
                        value=int(self.value)
                    except:
                        value=self.value
                    if temp_tuple[self.field_index+3]!=value:
                        yield temp_tuple

class Aggregate:
    '''computes an aggregate (counter, sum, avg, max, min) and group by, only support a single column'''
    def __init__(self,table,afield,gfield,operand,child_op=None):
        self.input_table=table
        self.data=table.data
        self.schema=table.data[0][2]
        self.operand=operand  # SUM, COUNT, MIN, MAX, AVG
        self.child_op=child_op  # child_op structure:[operation,field,operand]
        self.afield=afield  # diff from gfield, means the field will implement operations besides group by -1 means not active
        self.gfield=gfield  # field will implement group by, -1 means not active
        self.group_value=[0,[]]  # [group_index,[group_content]]
        self.calc_result=[]  # result of the calculation operation of each group
        if self.afield!=-1:
            self.afield_index=field_name_existence(self.data,self.afield)
        if self.gfield!=-1:
            self.gfield_index=field_name_existence(self.data,self.gfield)

    def do_calc(self):
        '''do the (counter, sum, avg, max, min) of a table based on child operation'''
        if self.afield!=-1:
            '''do calc operation'''
            if self.gfield!=-1:  # if gfield==-1,means no group_by, each tuple is regarded as a group
                '''do the group_by first'''
                for group_tuples in self.group_by():
                    self.calc_result.append(self.calc_func(group_tuples))
            else:  # no group by, do the calc to each tuples, or calc the result of the whole column
                self.group_value[1].append('*')
                group_tuples=[]
                if self.child_op==None:
                    for Tuples in Iterator(self.data).fetch_next():
                        if Tuples!=None:
                            group_tuples.append(Tuples)
                else:
                    for Tuples in self.child_op:
                        if Tuples!=None:
                            group_tuples.append(Tuples)
                self.calc_result.append(self.calc_func(group_tuples))
            self.calc_result.append(self.group_value[1])  # output structure: [results of each group,[group content]
            return self.calc_result
        else:
            '''no calc operation'''
            return None

    def calc_func(self,tuples_in_group):
        '''sum,min,max,avr,count operations to tuples in group'''
        temp=0
        if self.operand=='SUM':
            for Tuple in tuples_in_group:
                if Tuple[3+self.afield_index]!=None:
                    temp+=Tuple[3+self.afield_index]
            return temp
        elif self.operand=='COUNT':
            for Tuple in tuples_in_group:
                if Tuple[3+self.afield_index]!=None:
                    temp+=1
            return temp
        elif self.operand=='AVG':
            num=0
            for Tuple in tuples_in_group:
                if Tuple[3+self.afield_index]!=None:
                    temp+=Tuple[3+self.afield_index]
                    num+=1
            return temp/num
        elif self.operand=='MIN':
            init_num=2**31
            for Tuple in tuples_in_group:
                if Tuple[3+self.afield_index]!=None:
                    if Tuple[3+self.afield_index]<init_num:
                        init_num=Tuple[3+self.afield_index]
            return init_num
        elif self.operand=='MAX':
            init_num=-2**31
            for Tuple in tuples_in_group:
                if Tuple[3+self.afield_index]!=None:
                    if Tuple[3+self.afield_index]>init_num:
                        init_num=Tuple[3+self.afield_index]
            return init_num
        else:
            raise PyDBInternalError("only suit for MIN,MAX,SUM,COUNT,AVG")

    def group_by(self):
        '''implement group by, field each group of tuples, result=[list of[tuple]s]'''
        '''do the iterator'''
        if self.child_op!=None:
            '''child_op structure:[operation,field,operand]'''
            if self.child_op[0]==Filter:
                '''get the pre-info of group by'''
                for Tuple in Filter(self.input_table,self.child_op[1],self.child_op[2]).do_filter():
                    if Tuple!=None:
                        if Tuple[3+self.gfield_index]!=None:
                            if self.group_value[0]==0:
                                '''init'''
                                self.group_value[1].append(Tuple[3+self.gfield_index])
                                self.group_value[0]+=1
                            else:
                                flag=False
                                for value in self.group_value[1]:
                                    if Tuple[3+self.gfield_index]==value:
                                        flag=True
                                        break
                                '''whether is a new field_value'''
                                if flag==False:
                                    self.group_value[0]+=1
                                    self.group_value[1].append(Tuple[3+self.gfield_index])
                '''Filter as child operation'''
                for i in range(self.group_value[0]):
                    temp_group=[]  # output list of tuples in a group
                    for Tuple in Filter(self.input_table,self.child_op[1],self.child_op[2]).do_filter():
                        if Tuple!=None:
                            if Tuple[3+self.gfield_index]==self.group_value[1][i]:
                                temp_group.append(Tuple)
                    if temp_group!=[]:
                        yield temp_group
        else:  # no child_op ahead, regard the iterator as child_op
            '''get the pre-info of group by'''
            for Tuple in Iterator(self.data).fetch_next():
                if Tuple!=None:
                    if Tuple[3+self.gfield_index]!=None:
                        if self.group_value[0]==0:
                            '''init'''
                            self.group_value[1].append(Tuple[3+self.gfield_index])
                            self.group_value[0]+=1
                        else:
                            flag=False
                            for value in self.group_value[1]:
                                if Tuple[3+self.gfield_index]==value:
                                    flag=True
                                    break
                            '''whether is a new field_value'''
                            if flag==False:
                                self.group_value[0]+=1
                                self.group_value[1].append(Tuple[3+self.gfield_index])
            '''do the iteration'''
            for i in range(self.group_value[0]):
                temp_group=[]  # output list of tuples in a group
                for Tuple in Iterator(self.data).fetch_next():
                    if Tuple!=None:
                        if Tuple[3+self.gfield_index]==self.group_value[1][i]:
                            temp_group.append(Tuple)
                if temp_group!=[]:
                    yield temp_group

class Join:
    '''The Join operator implements the relational join operation.'''
    def __init__(self,child1,child2,field1,field2,op):
        '''child1 and child2 are two tuples from distinct DBIterators,
        field1 and field2 are the field name of each tuple
        op stand for the 5 operations: =,<,>,>= or <='''
        self.findex1=field_name_existence_in_tuples(child1,field1)
        self.findex2=field_name_existence_in_tuples(child2,field2)
        self.child1=child1
        self.child2=child2
        self.op=op

    def do_join(self):
        '''do The Join operator'''
        output=[[0,0],SLOT_SIZE]
        if self.op=='=':
            '''equal to'''
            if self.child1[3+self.findex1]==self.child2[3+self.findex2]:
                '''get the schema of join tuple'''
                temp_schema=['join result',self.child1[2][1]+self.child2[2][1]-1]  # [relation_name,degree,[field_name],[field_type]]
                temp_schema.append(self.child1[2][2])  # field name
                temp_schema.append(self.child1[2][3])  # field type
                for i in range(self.child2[2][1]):  # range(degree)
                    if i!=self.findex2:
                        temp_schema[2].append(self.child2[2][2][i])
                        temp_schema[3].append(self.child2[2][3][i])
                output.append(temp_schema)
                '''join the data of two tuples'''
                for i in range(self.child1[2][1]):
                    output.append(self.child1[3+i])
                for j in range(self.child2[2][1]):
                    if j!=self.findex2:
                        output.append(self.child2[3+j])
                return output
            else:
                return None
