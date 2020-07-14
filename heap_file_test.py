import unittest
from heap_file import *

class HeapFileTest(unittest.TestCase):
    def test_heap_file(self):
        '''type encode'''
        print('type encoding and decoding',typestr_to_bytes('CHAR(255)'),typebytes_to_str(b'\x03\xff'))

        '''test schema'''
        schematest1=Schema(input_data=[('colname1','CHAR(255)'),('colname2','INT32')],relation_name='test relation')
        print(schematest1.field_domain)
        bytestest1=schematest1.serialize()
        # print(schematest1.deserialize(bytestest1))

        '''test heap page'''
        heappagetest1=heap_page(schematest1)
        heappagetest1.insert_tuple(Tuple={'recordID':[0,0],'size':SLOT_SIZE,'colname1':'hello world','colname2':22})
        heappagetest1.insert_tuple(Tuple={'recordID':[0,0],'size':SLOT_SIZE,'colname1':'hello DBMS','colname2':333})
        arraytest=heappagetest1.get_page_data()
        heappagetest2=heap_page(schematest1)
        heappagetest2.deserialize(msg_str=arraytest)
        heappagetest1.print_for_bugs()
        print("deserialized page")
        heappagetest2.print_for_bugs()
        heappagetest3=heap_page(schematest1)

        '''test heap file'''
        print("HEAP FILE")
        heapfiletest=heap_file(schematest1)
        heapfiletest.write_page(heappagetest1)
        heapfiletest.read_page(1).print_for_bugs()
        heapfiletest.write_page(heappagetest2)
        heapfiletest.write_page(heappagetest3)
        heapfiletest.read_page(3).print_for_bugs()
        heapfiletest.read_page(4)
        print("page index in the header of the heap file:",heapfiletest.header_index)
        heapfiletest.get_file()

if __name__ == '__main__':
    unittest.main()
    suite = unittest.TestSuite()
    tests = HeapFileTest("heap_file_test")
    suite.addTests(tests)

    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
