import unittest
from buffer_pool_edited import *

class Testbuffer_pool(unittest.TestCase):
    '''
    def test_field_and_tuple(self):
        #  test field
        print("create a field")
        F1=Fields()
        F1.set_fields()
        # test tuple
        print("create a tuple")
        T1=Tuple()
        T1.set_field(F1)
        T1.fulfill_info()
        print(T1.toString())
    def test_page_file(self):
        # test page_file
        print("create a page_file")
        print("create a page_file,by gettin page from a physical file")
        PF1=page_file()
        f=open("pagefile1.dat","wb")
        f.close()
        tempid=PF1.get_id("pagefile1.dat")
        tempdata=PF1.get_page_data("pagefile1.dat")
        PF1.insert_tuple(T1)
        print("page ID:",tempid,"\n","page bytes DATA:",tempdata)
    def test_buffer_pool(self):
        # test buffer pool
        print("buffer pool")
        size=8*PAGE_SIZE
        bp1=buffer_pool(size)
        #PF2=page_file()
        #PF2.page_id=PF1.page_id
        #PF2.page_slotnum=PF1.page_slotnum
        #PF2.insert_tuple(T1)
        bp1.pool_pages[0]=PF1
        bp1.pool_num=1
        pid=PF1.page_id
        print("pid",pid,"page file slots:",PF1.page_slotnum)
        print("total number of space/pages:", len(bp1.pool_pages))
        print("get page with pid=",pid,"\n",bp1.get_page(pid))
        #print(bp1.discard_page(pid))
        ttemp=Tuple()
        print(PF1.page_data,"DEBUG1",T1,"DEBUG2")
        print(bp1.delete_tuple(T1.tuple_tid,ttemp))
        print(bp1.discard_page(pid))
    def test_file(self):
        #  test insert_tuple to file
        PF1.insert_tupletofile(T1,"pagefile1.dat")
        print("show new info")
        tempid=PF1.get_id("pagefile1.dat")
        tempdata=PF1.get_page_data("pagefile1.dat")
        print("page ID:",tempid,"\n","page bytes DATA:",tempdata)
    '''
    def test_Buffer_Pool(self):
        '''  test field  '''
        print("create a field")
        F1=Fields()
        F1.set_fields()
        '''  test tuple  '''
        print("create a tuple")
        T1=Tuple()
        T1.set_field(F1)
        T1.fulfill_info()
        print(T1.toString())
        '''  test page_file  '''
        print("create a page_file")
        print("create a page_file,by gettin page from a physical file")
        PF1=page_file()
        f=open("pagefile1.dat","wb")
        f.close()
        tempid=PF1.get_id("pagefile1.dat")
        tempdata=PF1.get_page_data("pagefile1.dat")
        PF1.insert_tuple(T1)
        print("page ID:",tempid,"\n","page bytes DATA:",tempdata)
        '''  test buffer pool  '''
        print("buffer pool")
        size=8*PAGE_SIZE
        bp1=buffer_pool(size)
        #PF2=page_file()
        #PF2.page_id=PF1.page_id
        #PF2.page_slotnum=PF1.page_slotnum
        #PF2.insert_tuple(T1)
        bp1.pool_pages[0]=PF1
        bp1.pool_num=1
        pid=PF1.page_id
        print("pid",pid,"page file slots:",PF1.page_slotnum)
        print("total number of space/pages:", len(bp1.pool_pages))
        print("get page with pid=",pid,"\n",bp1.get_page(pid))
        #print(bp1.discard_page(pid))
        ttemp=Tuple()
        print(PF1.page_data,"DEBUG1",T1,"DEBUG2")
        print(bp1.delete_tuple(T1.tuple_tid,ttemp))
        print(bp1.discard_page(pid))
        '''  test insert_tuple to file  '''
        PF1.insert_tupletofile(T1,"pagefile1.dat")
        print("show new info")
        tempid=PF1.get_id("pagefile1.dat")
        tempdata=PF1.get_page_data("pagefile1.dat")
        print("page ID:",tempid,"\n","page bytes DATA:",tempdata)

if __name__ == '__main__':
    unittest.main()
    suite = unittest.TestSuite()
    #tests = [Testbuffer_pool("test_all"), Testbuffer_pool("test_page_file"),Testbuffer_pool("test_buffer_pool"),Testbuffer_pool("test_file")]
    tests = Testbuffer_pool("test_Buffer_Pool")
    suite.addTests(tests)

    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
