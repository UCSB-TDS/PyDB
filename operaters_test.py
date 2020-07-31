import unittest
from operators import *
from heap_file import *

class OperatorTest(unittest.TestCase):
    def test_operators(self):
        '''test schema'''
        schema_test1=Schema(input_data=[('colname1','CHAR(255)'),('colname2','INT32')],relation_name='test relation1')
        schema_test2=Schema(input_data=[('colname1','CHAR(255)'),('colname3','INT32')],relation_name='test relation2')
        heap_page_test1=HeapPage(schema_test1)
        heap_page_test1.insert_tuple(Tuple=[[0,0],SLOT_SIZE,schema_test1.data,'hello world',22])
        heap_page_test1.insert_tuple(Tuple=[[0,0],SLOT_SIZE,schema_test1.data,'hello DBMS',3331])
        '''test heap file'''
        heap_file_test=HeapFile(schema_test1.data)
        heap_file_test2=HeapFile(schema_test2.data)
        heap_file_test.write_page(heap_page_test1)
        '''test HeapFile tuple insertion'''
        heap_file_test.insert_tuple(input_tuple=[[0,0],SLOT_SIZE,schema_test1.data,'hello python',333])
        heap_file_test.insert_tuple(input_tuple=[[0,0],SLOT_SIZE,schema_test1.data,'hel crypto',333])
        heap_file_test.insert_tuple(input_tuple=[[0,0],SLOT_SIZE,schema_test1.data,'hello DBMS',3332])
        heap_file_test2.insert_tuple(input_tuple=[[0,0],SLOT_SIZE,schema_test2.data,'hello DBMS',123])
        heap_file_test2.insert_tuple(input_tuple=[[0,0],SLOT_SIZE,schema_test2.data,'hello join',1234])

        '''test iterator'''
        print('ITERATOR of a heap_file')
        for value in Iterator(heap_file_test.data).fetch_next():
            print(value)
        print('ITERATOR of a heap_page')
        for value in Iterator(heap_page_test1.data).fetch_next():
            print(value)

        '''test filter'''
        print('filter =><','colname2','> 333')
        for i in Filter(heap_file_test,'colname2','> 333').do_filter():
            print(i)
        print('filter like str','colname1','like hello')
        for j in Filter(heap_file_test,'colname1','like hello').do_filter():
            print(j)
        #for j in Filter(heap_file_test,'colname1','*').do_filter():
        #    print(j)

        '''test aggregate'''
        print('group by colname1 based on child_op=[Filter,colname1,like hello]')
        for Tuple in Aggregate(table=heap_file_test,afield=-1,gfield='colname1',operand=None,child_op=[Filter,'colname1','like hello']).group_by():
            print(Tuple)
        print('calc the avg on colname2 based on child_op=[Filter,colname1,like hello]')
        print(Aggregate(table=heap_file_test,afield='colname2',gfield='colname1',operand='AVG',child_op=[Filter,'colname1','like hello']).do_calc())

        '''join'''
        print('join of two tuples from DBIterator')
        for tuple1 in Filter(heap_file_test,'colname1','like hello').do_filter():
            if tuple1!=None:
                for tuple2 in Iterator(heap_file_test2.data).fetch_next():
                    if tuple2!=None:
                        print(Join(tuple1,tuple2,'colname1','colname1',op='=').do_join())


if __name__ == '__main__':
    unittest.main()
