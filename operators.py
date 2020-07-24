from PyDBError import *

HEAPFILE_SIZE = 40960
PAGE_SIZE = 5120
SLOT_SIZE = 2048
MAX_HEADER_SIZE = 1024
MAX_SLOTS = (PAGE_SIZE-MAX_HEADER_SIZE)//SLOT_SIZE  # 4 TOTAL SLOTS, Each slot can hold one tuple
MAX_PAGES = (HEAPFILE_SIZE-MAX_HEADER_SIZE)//PAGE_SIZE

def col_name_existence(relation_data,col_name):
    '''examine the existence of a certain column in relation'''
    flag=0
    for x in relation_data['header']['schema']:
        if x==col_name:
            flag=1
            return 1
    if flag==0:
        raise PyDBInternalError('col_name not found in schema')

def get_type(input_value):
    '''get the type according the form of this DBMS'''
    try:
        if int(input_value)<2**31 and int(input_value)>=-2**31:
            return 'INT32'
        elif int(input_value)<2**63 and int(input_value)>=-2**63:
            return 'INT64'
        else:
            raise PyDBInternalError('int should be either INT32 or INT63')
    except:
        N=len(input_value)
        if N<256:
            return 'CHAR(255)'  # all N less than 256 is equal to N=255
        else:
            raise PyDBInternalError('N of CHAR(N) should be less than 256')

class Iterator:
    '''iterator of a relation file'''
    def __init__(self, input_file):
        self.data=input_file.get_file_dict()
        self.current=0

    def __iter__(self):
        # get file id
        # print('id of this file:',self.data['header']['file_id'])
        # get schema
        # print('schema of this heap file:',self.data['header']['schema'])
        return self

    def __next__(self):
        # get tuples in heap file
        self.current += 1
        if self.current<(MAX_PAGES)*MAX_SLOTS:
            if self.current%MAX_SLOTS==0:  # boundary requirement
                temp='page'+str(self.current//MAX_SLOTS)
            else:
                temp='page'+str(1+self.current//MAX_SLOTS)
            temp_page=self.data[temp]
            if temp_page==None:
                self.current+=MAX_SLOTS-1
                print(temp,'empty block')
                return None
            else:
                j=self.current%MAX_SLOTS
                if j==0:  # j should be MAX_SLOTS rather than 0
                    j=MAX_SLOTS
                temp='tuple'+str(j)
                temp_tuple=temp_page[temp]
                if temp_tuple['size']==0:
                    print(temp,'empty slot')
                    return None
                else:
                    return temp_tuple
        raise StopIteration

class Filter:
    '''filter operator'''
    def __init__(self,relation,operand):
        '''operand structure: 'field_name'='field_value' '''
        self.input_file=relation
        self.data=relation.get_file_dict()
        self.schema=self.data['header']['schema']
        for i in range(len(operand)):
            if operand[i]=='=':
                break
        self.field_name=operand[0:i]
        self.field_value=operand[i+1:len(operand)+1]
        col_name_existence(self.data,self.field_name)
        if get_type(self.field_value)!=self.schema[self.field_name]:
            print(get_type(self.field_value),self.schema[self.field_name])
            raise PyDBInternalError('field_value not match with domain in schema')

    def get_tuple(self):
        '''return the tuples fit the operand in filter'''
        result=[]
        for c in Iterator(input_file=self.input_file):
            if c!=None:
                if c[self.field_name]==self.field_value:
                    result.append(c)
        if result==[]:
            print('no tuple match the requirement')
            return None
        else:
            return result

class Join:
    '''a simple nested loops join'''
    def __init__(self,relation1,relation2,operand):
        ''' operand structure: 'relation1.colname=relation2.colname' '''
        dot_position1=0
        dot_position2=0
        eq_position=0
        dot_index=0
        for i in range(len(operand)):
            if operand[i]=='=':  # is there other symbols other than =?
                eq_position=i
            elif operand[i]=='.' and dot_index==0:
                dot_index+=1
                dot_position1=i
            elif operand[i]=='.' and dot_index==1:
                dot_position2=i
        self.relation1_field_name=operand[dot_position1+1:eq_position]
        self.relation2_field_name=operand[dot_position2+1:len(operand)+1]  # ;print(self.relation1_field_name,self.relation2_field_name)

        self.relation1_file=relation1
        self.relation2_file=relation2
        self.relation1_data=relation1.get_file_dict()
        self.relation2_data=relation2.get_file_dict()
        # test whether the operand is feasible
        col_name_existence(self.relation1_data,self.relation1_field_name)
        col_name_existence(self.relation2_data,self.relation2_field_name)

    def get_join(self):
        '''return the tuples fit the operand in join'''
        join_result=[]
        dict_id=0
        for tuple1 in Iterator(input_file=self.relation1_file):
            if tuple1==None:
                continue
            for tuple2 in Iterator(input_file=self.relation2_file):
                if tuple2==None:
                    continue
                if tuple1[self.relation1_field_name]==tuple2[self.relation2_field_name]:
                    '''get the dict of the join result from two tuples'''
                    dict_id+=1
                    join_dict={}
                    join_dict['join_ID']=dict_id
                    join_dict[self.relation1_field_name]=tuple1[self.relation1_field_name]
                    for key in tuple1:
                        if key!='recordID' and key!='size' and key!=self.relation1_field_name:
                            join_dict[key]=tuple1[key]
                    for key in tuple2:
                        if key!='recordID' and key!='size' and key!=self.relation2_field_name:
                            join_dict[key]=tuple2[key]
                    join_result.append(join_dict)
        return join_result

class Aggregate:
    '''computes an aggregate (e.g., sum, avg, max, min) and group by, only support a single column'''
    def __init__(self,relation,col_name):
        self.relation_file=relation
        self.relation_data=relation.get_file_dict()
        self.col_name=col_name
        col_name_existence(self.relation_data,self.col_name)

    def GroupBy(self,gcol_name):
        '''return the distinct elements of a column'''
        col_name_existence(self.relation_data,gcol_name)
        result=[]
        for Tuple in Iterator(input_file=self.relation_file):
            if Tuple!=None:
                flag=0
                for element in result:
                    if element==Tuple[gcol_name]:
                        flag=1
                        break
                if flag==0:
                    result.append(Tuple[gcol_name])
        return result

    def SUM(self,gcol_name):
        '''return the sum of values of a column, INT gets sum, and CHAR(N) gets joint of str.'''
        if gcol_name==-1:
            '''calculate without GroupBy'''
            Sum=0
            for Tuple in Iterator(input_file=self.relation_file):
                if Tuple!=None:
                    Sum+=Tuple[self.col_name]
            return {self.col_name:Sum}
        else:
            '''calculate with GroupBy'''
            col_name_existence(self.relation_data,gcol_name)
            col_group=self.GroupBy(gcol_name)
            sum_result={}
            for elem in col_group:
                sum_result[elem]=0
            for Tuple in Iterator(input_file=self.relation_file):
                if Tuple!=None:
                    for elem in col_group:
                        if Tuple[gcol_name]==elem:
                            sum_result[elem]+=Tuple[self.col_name]
                            break
            return sum_result

    def COUNT(self,gcol_name):
        '''return the number of meaningful value of a column'''
        if gcol_name==-1:
            '''calculate without GroupBy'''
            Count=0
            for Tuple in Iterator(input_file=self.relation_file):
                if Tuple!=None:
                    if Tuple[self.col_name]!=None:
                        Count+=1
            return {self.col_name:Count}
        else:
            '''calculate with GroupBy'''
            col_name_existence(self.relation_data,gcol_name)
            col_group=self.GroupBy(gcol_name)
            count_result={}
            for elem in col_group:
                count_result[elem]=0
            for Tuple in Iterator(input_file=self.relation_file):
                if Tuple!=None:
                    for elem in col_group:
                        if Tuple[gcol_name]==elem:
                            count_result[elem]+=1
                            break
            return count_result

    def AVG(self,gcol_name):
        '''return the average value of a column, only suit for INT32 or INT64'''
        if self.relation_data['header']['schema'][self.col_name]!='INT32' and 'INT64':
            raise PyDBInternalError("only suit for INT32 and INT64")
        if gcol_name==-1:
            return self.SUM(-1)[self.col_name]/self.COUNT(-1)[self.col_name]
        else:
            col_name_existence(self.relation_data,gcol_name)
            sum_result=self.SUM(gcol_name)
            count_result=self.COUNT(gcol_name)
            avg_result={}
            for key in sum_result:
                avg_result[key]=sum_result[key]/count_result[key]
            return avg_result

    def MAX(self,gcol_name):
        '''return the max element of a column, INT32 and INT64 only'''
        if self.relation_data['header']['schema'][self.col_name]!='INT32' and 'INT64':
            raise PyDBInternalError("only suit for INT32 and INT64")
        if gcol_name==-1:
            '''calculate without GroupBy'''
            Max=-2**63
            for Tuple in Iterator(input_file=self.relation_file):
                if Tuple!=None:
                    if Tuple[self.col_name]>Max:
                        Max=Tuple[self.col_name]
            return {self.col_name:Max}
        else:
            '''calculate with GroupBy'''
            col_name_existence(self.relation_data,gcol_name)
            col_group=self.GroupBy(gcol_name)
            max_result={}
            for elem in col_group:
                max_result[elem]=-2**63
            for Tuple in Iterator(input_file=self.relation_file):
                if Tuple!=None:
                    for elem in col_group:
                        if Tuple[gcol_name]==elem:
                            if Tuple[self.col_name]>max_result[elem]:
                                max_result[elem]=Tuple[self.col_name]
                                break
            return max_result

    def MIN(self,gcol_name):
        '''return the max element of a column, INT32 and INT64 only'''
        if self.relation_data['header']['schema'][self.col_name]!='INT32' and 'INT64':
            raise PyDBInternalError("only suit for INT32 and INT64")
        if gcol_name==-1:
            '''calculate without GroupBy'''
            Min=-2**63
            for Tuple in Iterator(input_file=self.relation_file):
                if Tuple!=None:
                    if Tuple[self.col_name]>Min:
                        Min=Tuple[self.col_name]
            return {self.col_name:Min}
        else:
            '''calculate with GroupBy'''
            col_name_existence(self.relation_data,gcol_name)
            col_group=self.GroupBy(gcol_name)
            min_result={}
            for elem in col_group:
                min_result[elem]=2**63
            for Tuple in Iterator(input_file=self.relation_file):
                if Tuple!=None:
                    for elem in col_group:
                        if Tuple[gcol_name]==elem:
                            if Tuple[self.col_name]<min_result[elem]:
                                min_result[elem]=Tuple[self.col_name]
                                break
            return min_result

class Insert:
    '''Inserts tuples read from the child operator into the relation'''
    def __init__(self,input_tuple,input_relation):
        self.relation_data=input_relation.get_file_dict()
        # exam whether the schema matches or not
        for key in input_tuple:
            if key!='recordID' and key!='size':
                col_name_existence(self.relation_data,key)
        # insertion
        input_relation.insert_tuple(input_tuple)

class Delete:
    '''Delete reads tuples from its child operator and removes them from the relation they belong to'''
    def __init__(self,input_tuple,input_relation):
        input_relation.delete_tuple(input_tuple['recordID'])
