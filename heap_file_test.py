import unittest
from heap_file import *
from operators import *


class HeapFileTest(unittest.TestCase):
    def test_heap_file(self):
        '''type encode'''
        print('type encoding and decoding',typestr_to_bytes('CHAR(255)'),typebytes_to_str(b'\x03\xff'))

        '''test schema'''
        schema_test1=Schema(input_data=[('colname1','CHAR(255)'),('colname2','INT32')],relation_name='test relation')
        print(schema_test1.field_domain)
        bytestest1=schema_test1.serialize()
        print(schema_test1.deserialize(bytestest1))

        '''test heap page'''
        '''tuple structure:[RecordID(pageID,slotNo.),size,schema,data in each column]'''
        heap_page_test1=HeapPage(schema_test1)
        heap_page_test1.insert_tuple(Tuple=[[0,0],SLOT_SIZE,schema_test1.data,'hello world',22])
        heap_page_test1.insert_tuple(Tuple=[[0,0],SLOT_SIZE,schema_test1.data,'hello DBMS',333])
        array_test=heap_page_test1.get_page_data()
        heap_page_test2=HeapPage(schema_test1)
        heap_page_test2.deserialize(msg_str=array_test)
        heap_page_test1.print_for_bugs()
        print("deserialized page")
        heap_page_test2.print_for_bugs()
        heap_page_test3=HeapPage(schema_test1)

        '''test heap file'''
        print("HEAP FILE")
        heap_file_test=HeapFile(schema_test1.data)
        heap_file_test.write_page(heap_page_test1)
        #heap_file_test.read_page(1).print_for_bugs()
        heap_file_test.write_page(heap_page_test2)  # deserialize result of page1
        heap_file_test.write_page(heap_page_test3)
        #heap_file_test.read_page(3).print_for_bugs()
        #heap_file_test.read_page(4)
        print("page index in the header of the heap file:",heap_file_test.header_index)
        heap_file_test.get_file()
        #print('content in a heap file',heap_file_test.get_file_dict())

        '''test HeapFile tuple insertion'''
        heap_file_test.insert_tuple(input_tuple=[[0,0],SLOT_SIZE,schema_test1.data,'hello python',333])
        heap_file_test.insert_tuple(input_tuple=[[0,0],SLOT_SIZE,schema_test1.data,'hel crypto',333])
        heap_file_test.insert_tuple(input_tuple=[[0,0],SLOT_SIZE,schema_test1.data,'hello DBMS',333])

        print("page index in the header of the heap file:",heap_file_test.header_index)
        '''test HeapFile tuple deletion'''

        heap_file_test.delete_tuple([3,1])

if __name__ == '__main__':
    unittest.main()
    suite = unittest.TestSuite()
    tests = HeapFileTest("heap_file_test")
    suite.addTests(tests)

    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
