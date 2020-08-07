import unittest
from composer import *


class ComposerTest(unittest.TestCase):
    def test_composer(self):
        '''test table input'''
        schema_test1=Schema(input_data=[('ID','CHAR(255)'),('score','INT32'),('Dept','CHAR(255)')],relation_name='student')
        schema_test2=Schema(input_data=[('Dept','CHAR(255)'),('num of staff','INT32')],relation_name='department')
        Student=HeapFile(schema_test1.data)
        Department=HeapFile(schema_test2.data)
        Student.insert_tuple(input_tuple=[[0,0],SLOT_SIZE,schema_test1.data,'PB100',100,'EE'])
        Student.insert_tuple(input_tuple=[[0,0],SLOT_SIZE,schema_test1.data,'PB101',70,'BIO'])
        Student.insert_tuple(input_tuple=[[0,0],SLOT_SIZE,schema_test1.data,'PB102',65,'BIO'])
        Student.insert_tuple(input_tuple=[[0,0],SLOT_SIZE,schema_test1.data,'PB103',59,'PHYSICS'])
        Student.insert_tuple(input_tuple=[[0,0],SLOT_SIZE,schema_test1.data,'PB104',49,'PHYSICS'])
        Department.insert_tuple(input_tuple=[[0,0],SLOT_SIZE,schema_test2.data,'PHYSICS',123])
        Department.insert_tuple(input_tuple=[[0,0],SLOT_SIZE,schema_test2.data,'BIO',12])
        Department.insert_tuple(input_tuple=[[0,0],SLOT_SIZE,schema_test2.data,'EE',1])

        '''SELECT ... FROM ...'''
        print(Composer('SELECT Student.ID,SUM(Student.score) FROM Student',Student,Department).do_comp())
        '''SELECT ... FROM ... WHERE ... '''
        print(Composer('SELECT Student.ID,AVG(Student.score) FROM Student,Department WHERE Student.ID=PB103,Student.score<60,Student.Dept=PHYSICS',Student,Department).do_comp())

        '''SELECT ... FROM ... JOIN ... ON ...'''
        print(Composer('SELECT Student.ID,COUNT(Student.Dept) FROM Student JOIN Department ON Student.Dept=Department.Dept'
                       ,Student,Department).do_comp())
        '''SELECT ... FROM ... JOIN ... ON ... WHERE ...'''
        print(Composer('SELECT Student.ID,COUNT(Student.Dept) FROM Student JOIN Department ON Student.Dept=Department.Dept WHERE Student.score<80,Department.num of staff>100'
                       ,Student,Department).do_comp())

        '''SELECT ... FROM ... GROUP BY ... '''
        print(Composer('SELECT Student.ID,MIN(Student.score) FROM Student GROUP BY Dept',Student,Department).do_comp())
        '''SELECT ... FROM ... WHERE ... GROUP BY ... '''
        print(Composer('SELECT Student.ID,MAX(Student.score) FROM Student WHERE Student.score<80 GROUP BY Dept',Student,Department).do_comp())




if __name__ == '__main__':
    unittest.main()
