import unittest
from buffer_pool import *

class BufferPoolTest(unittest.TestCase):

    def test_Buffer_Pool(self):
        '''test buffer_pool.py, an implementation of buffer pool'''
        buffer_pool_test=buffer_pool(numPages=1)
        # relation1
        schema_test1=Schema(input_data=[('colname1','CHAR(255)'),('colname2','INT32')],relation_name='test relation1')
        heap_page_test1=HeapPage(schema_test1)
        heap_page_test1.insert_tuple(Tuple=[[0,0],SLOT_SIZE,schema_test1.data,'hello world1',1])
        heap_page_test1.insert_tuple(Tuple=[[0,0],SLOT_SIZE,schema_test1.data,'hello DBMS',22])
        heap_page_test2=HeapPage(schema_test1)
        heap_page_test2.insert_tuple(Tuple=[[0,0],SLOT_SIZE,schema_test1.data,'hello world2',333])
        table1=HeapFile(schema_test1.data)
        table1.write_page(heap_page_test1)
        table1.write_page(heap_page_test2)

        buffer_pool_test.insert_tuple(tid=1,table=table1,Tuple=[[0,0],SLOT_SIZE,schema_test1.data,'another page',333])
        buffer_pool_test.insert_tuple(tid=1,table=table1,Tuple=[[0,0],SLOT_SIZE,schema_test1.data,'another page',4444])
        print('page objects',buffer_pool_test.page_array,'\npage index',buffer_pool_test.page_index)
        buffer_pool_test.delete_tuple(tid=1,tupleID=[3,0])
        print('page index:',buffer_pool_test.page_index)


if __name__ == '__main__':
    unittest.main()
    suite = unittest.TestSuite()
    tests = BufferPoolTest("test_Buffer_Pool")
    suite.addTests(tests)

    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
