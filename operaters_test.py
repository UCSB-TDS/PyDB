import unittest
from operators import *
from heap_file import *

class OperatorTest(unittest.TestCase):
    def test_operators(self):
        # relation1
        schema_test1=Schema(input_data=[('colname1','CHAR(255)'),('colname2','INT32')],relation_name='test relation1')
        heap_page_test1=HeapPage(schema_test1)
        heap_page_test1.insert_tuple(Tuple={'recordID':[0,0],'size':SLOT_SIZE,'colname1':'hello world','colname2':1})
        heap_page_test1.insert_tuple(Tuple={'recordID':[0,0],'size':SLOT_SIZE,'colname1':'hello DBMS','colname2':22})
        heap_page_test2=HeapPage(schema_test1)
        heap_page_test2.insert_tuple(Tuple={'recordID':[0,0],'size':SLOT_SIZE,'colname1':'another page','colname2':333})
        heap_file_test1=HeapFile(schema_test1)
        heap_file_test1.write_page(heap_page_test1)
        heap_file_test1.write_page(heap_page_test2)
        # relation2
        schema_test2=Schema(input_data=[('colname3','CHAR(255)'),('colname4','INT32')],relation_name='test relation2')
        heap_page_test3=HeapPage(schema_test2)
        heap_page_test3.insert_tuple(Tuple={'recordID':[0,0],'size':SLOT_SIZE,'colname3':'test','colname4':1})
        heap_page_test3.insert_tuple(Tuple={'recordID':[0,0],'size':SLOT_SIZE,'colname3':'join','colname4':22})
        heap_page_test4=HeapPage(schema_test2)
        heap_page_test4.insert_tuple(Tuple={'recordID':[0,0],'size':SLOT_SIZE,'colname3':'join','colname4':333})
        heap_file_test2=HeapFile(schema_test2)
        heap_file_test2.write_page(heap_page_test3)
        heap_file_test2.write_page(heap_page_test4)

        '''test iterator'''
        print('ITERATOR')
        for c in Iterator(input_file=heap_file_test2):
            if c:
                print(c)

        '''filter test'''
        print('filter test:\n',
              Filter(relation=heap_file_test1,operand='colname1=hello DBMS').get_tuple())

        '''join test'''
        print('join test:\n',
              Join(relation1=heap_file_test1,relation2=heap_file_test2,operand='relation1.colname2=relation2.colname4').get_join())

        '''aggregate test'''
        agg=Aggregate(relation=heap_file_test2,col_name='colname4')
        print('aggregate test:\n','sum=',agg.SUM('colname3'),'count=',agg.COUNT('colname3'),'\naverage=',agg.AVG('colname3'),
              'max=',agg.MAX('colname3'),'\nmin=',agg.MIN('colname3'),'groupby col3',agg.GroupBy('colname3'))

        '''insert and delete test'''
        Insert(input_tuple={'recordID':[0,0],'size':SLOT_SIZE,'colname3':'hello DBMS','colname4':22},
               input_relation=heap_file_test2)
        print('slot_num of page',heap_file_test2.file_pages[1].slot_num)
        Delete(input_tuple={'recordID':[4,0],'size':SLOT_SIZE,'colname3':'hello DBMS','colname4':22},
               input_relation=heap_file_test2)
        print('slot_num of page',heap_file_test2.file_pages[1].slot_num)


if __name__ == '__main__':
    unittest.main()
