import unittest
from buffer_pool import *

class BufferPoolTest(unittest.TestCase):

    def test_Buffer_Pool(self):
        '''  test field  '''
        print("TEST FIELD")
        testfield=Fields()
        testfield.set_fields(Inputtype='str',Inputname='student',Inputcols=['name','gender','nation'])
        bytesoffields=testfield.serialize()
        testdefield=Fields()
        testdefield.deserialize(bytesoffields)
        print(testdefield.field_cols)


        ''' test tuple '''
        print("TEST TUPLE")
        testtuple=Tuple()
        testtuple.set_field(testdefield)
        testtuple.fulfill_info(Listofdata=['1','22','333'])
        print(testtuple.toString())
        bytesoftuple=testtuple.serialize()
        testdetuple=Tuple()
        testdetuple.deserialize(bytesoftuple,testdefield.field_size)
        print(testdetuple.toString())

        ''' TEST PAGE FILE '''
        print("TEST PAGE FILE")
        testpage=page_file()
        testpage.insert_tuple(testdetuple)
        testpage.print_for_bugs()
        #print(testpage.get_page_data().raw)
        testpage2=page_file()
        print('id',testpage2.PAGE_ID)
        testpage2.deserialize(testpage.get_page_data(),testdefield.field_size)
        print(testpage2.print_for_bugs())
        print("tuple'information of the page after deserialize:",testpage2.page_tuples[0].toString())

        ''' test buffer pool'''
        print("TEST BUFFER POOL")
        testbufferpool=buffer_pool(size=PAGE_SIZE*3)
        testbufferpool.discard_page(0)
        testbufferpool.pool_pages.append(testpage)
        testbufferpool.pool_num+=1
        print(testpage.page_id)
        t=Tuple()
        testbufferpool.delete_tuple([1,0],t)
        testbufferpool.discard_page(1)

if __name__ == '__main__':
    unittest.main()
    suite = unittest.TestSuite()
    tests = BufferPoolTest("buffer_pool_test")
    suite.addTests(tests)

    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
