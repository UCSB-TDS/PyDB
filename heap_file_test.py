import unittest
from heap_file import *

class HeapFileTest(unittest.TestCase):
    def test_heap_file(self):
        '''test schema'''
        schematest1=Schema(inputdata=[('colname1','varchar(256)'),('colname2','int32')],relationname='test relation')
        print(schematest1.field_domain)
        bytestest1=schematest1.serialize()
        schematest1.deserialize(bytestest1)
        print(schematest1.field_domain)

        '''test heap page'''
        heappagetest1=heap_page(schematest1)
        heappagetest1.insert_tuple(Tuple=[[0,0],SLOT_SIZE,schematest1,'hello world',22])
        heappagetest1.insert_tuple(Tuple=[[0,0],SLOT_SIZE,schematest1,'hello DBMS',333])
        heappagetest1.get_header()
        arraytest=heappagetest1.get_page_data()
        heappagetest2=heap_page(schematest1)
        heappagetest2.deserialize(bytesarray=arraytest)
        heappagetest1.print_for_bugs()
        print("deserialized page")
        heappagetest2.print_for_bugs()
        heappagetest3=heap_page(schematest1)

        '''test heap file'''
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
