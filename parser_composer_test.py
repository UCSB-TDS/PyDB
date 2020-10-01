import unittest
from parser_composer import *


class ParserTest(unittest.TestCase):
    def test_parsec(self):
        '''test parsec seperate'''

        print(agg_ref(' SUM(student.score) ',1))  # note: must leave a whitespace at the beginning
        print(column_ref(' student.ID ',1)[2])
        print(select_clause(' select student.ID,student.dept from student,department GROUP BY student.dept ',1))  # student cannot be Student???
        print(where_clause(' WHERE student.ID=qee,student.ID>10 GROUP BY student.dept ',1)) # can only compare with number and const string?
        print(join_clause(' JOIN studentA ON studentA.score=studentB.score,q.c=p.d WHERE q.a>1, q.b=2 GROUP BY abcs ',1))  # skip the first variable really matters?
        print(group_by_clause(' GROUP BY student.dept ',1))

        schema_test1=Schema(input_data=[('ID','CHAR(255)'),('score','INT32'),('Dept','CHAR(255)')],relation_name='student')
        schema_test2=Schema(input_data=[('Dept','CHAR(255)'),('numofstaff','INT32')],relation_name='department')
        schema_test3=Schema(input_data=[('name','CHAR(255)'),('years','INT32')],relation_name='school')
        student=HeapFile(schema_test1.data)
        department=HeapFile(schema_test2.data)
        school=HeapFile(schema_test3.data)
        school.insert_tuple(input_tuple=[[0,0],SLOT_SIZE,schema_test3.data,'USTC',70])
        student.insert_tuple(input_tuple=[[0,0],SLOT_SIZE,schema_test1.data,'PB100',100,'EE'])
        student.insert_tuple(input_tuple=[[0,0],SLOT_SIZE,schema_test1.data,'PB101',70,'BIO'])
        student.insert_tuple(input_tuple=[[0,0],SLOT_SIZE,schema_test1.data,'PB102',65,'BIO'])
        student.insert_tuple(input_tuple=[[0,0],SLOT_SIZE,schema_test1.data,'PB103',59,'PHYSICS'])
        student.insert_tuple(input_tuple=[[0,0],SLOT_SIZE,schema_test1.data,'PB104',49,'PHYSICS'])
        department.insert_tuple(input_tuple=[[0,0],SLOT_SIZE,schema_test2.data,'PHYSICS',123])
        department.insert_tuple(input_tuple=[[0,0],SLOT_SIZE,schema_test2.data,'BIO',12])
        department.insert_tuple(input_tuple=[[0,0],SLOT_SIZE,schema_test2.data,'EE',1])

        '''SELECT ... FROM ...'''
        print(ParserComposer(' SELECT student.ID,student.score,department.Dept,school.name FROM student,department,school',[student,department,school]).do_comp())
        '''SELECT ... FROM ... WHERE ... '''
        print(ParserComposer(' SELECT student.ID,AVG(student.score) FROM student,department WHERE student.ID=PB103,student.score<60,student.Dept=PHYSICS',[student,department]).do_comp())

        '''SELECT ... FROM ... JOIN ... ON ...'''
        print(ParserComposer(' SELECT student.ID,COUNT(student.Dept) FROM student JOIN department ON student.Dept=department.Dept'
                       ,[student,department]).do_comp())
        '''SELECT ... FROM ... JOIN ... ON ... WHERE ...'''
        print(ParserComposer(' SELECT student.ID,COUNT(student.Dept) FROM student JOIN department ON student.Dept=department.Dept WHERE student.score<80,department.numofstaff>100'
                       ,[student,department]).do_comp())

        '''SELECT ... FROM ... GROUP BY ... '''
        print(ParserComposer(' SELECT student.ID,SUM(student.score) FROM student GROUP BY student.Dept',[student,department]).do_comp())
        '''SELECT ... FROM ... WHERE ... GROUP BY ... '''
        print(ParserComposer(' SELECT student.ID,MAX(student.score) FROM student WHERE student.score<80 GROUP BY student.Dept',[student,department]).do_comp())



if __name__ == '__main__':
    unittest.main()
