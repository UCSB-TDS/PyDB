from ctypes import *
from PyDBError import *
import numpy as np
from os import popen
import struct
import msgpack
HEAPFILE_SIZE = 40960
PAGE_SIZE = 5120
SLOT_SIZE = 2048
MAX_HEADER_SIZE = 1024
MAX_SLOTS = (PAGE_SIZE-MAX_HEADER_SIZE)//SLOT_SIZE  # 4 TOTAL SLOTS, Each slot can hold one tuple
MAX_PAGES = (HEAPFILE_SIZE-MAX_HEADER_SIZE)//PAGE_SIZE

LEN_String: int = 256  # bytes of constant string
LEN_Int32 = 4  # bytes of int, static
LEN_Type = 2  # stands for int32 or varchar(256)
# note that int length also should be less than 8

PAGE_ID = 0  # unique id for each page
StringLength = LEN_String  # length of string in tuple

def get_type(input_value):                                              # what if input int as str?
    '''get the type according the form of this DBMS'''
    try:
        if int(input_value)<2**31 and int(input_value)>=-2**31:
            return 'INT32'
        elif int(input_value)<2**63 and int(input_value)>=-2**63:
            return 'INT64'
        else:
            raise PyDBInternalError('int should be either INT32 or INT64')
    except:
        N=len(input_value)
        if N<256:
            return 'CHAR(255)'  # all N less than 256 is equal to N=255
        else:
            raise PyDBInternalError('N of CHAR(N) should be less than 256')

def tuple_exam(input_tuple):
    '''examine whether the data in the input tuple is in accordance with its schema'''
    if (len(input_tuple)-3)!=input_tuple[2][1]:
        raise PyDBInternalError("degrees not match between input tuple and page_schema in this page")
    for i in range(len(input_tuple)-3):
        if input_tuple[3+i]!=None:
            if get_type(input_tuple[3+i])!=input_tuple[2][3][i]:
                raise PyDBInternalError("data in tuple not match with its schema")

class Iterator:
    '''iterator, input suits for both heap_file and heap_page'''
    def __init__(self,input):
        self.data=input
        self.is_page=False
        if input[0][3]==True or False:
            self.is_page=True   # has is_dirty means is page

    def schema(self):
        '''return the schema of input'''
        return self.data[0][2]

    def header(self):
        '''return the header of input'''
        return self.data[0]

    def fetch_next(self):
        '''return the next Tuple in input table or page, will skip empty pages'''
        if self.is_page==True:
            '''page iterator'''
            for i in range(len(self.data[1])):
                yield self.data[1][i]
        else:
            '''table iterator'''
            for page in self.data[1]:
                if page!=None:
                    for Tuple in page[1]:
                        yield Tuple

def iter(input):
    '''iterator, input suits for both heap_file and heap_page'''
    yield input[0]
    for i in range(len(input[1])):
        yield input[1][i]

def int32_to_bytes(x: int) -> bytes:
    '''convertion between int and bytes, note int = 4 bytes'''
    try:
        temp=struct.pack('i',x)
        return temp
    except:
        raise PyDBInternalError("int32 input out of range")

def int32_from_bytes(xbytes):
    try:
        temp=struct.unpack('i',xbytes)[0]
        return temp
    except:
        raise PyDBInternalError("bytes length input out of range")

def typestr_to_bytes(tstring):
    '''turn string of type to bytes'''
    if tstring=='INT32':
        tbytes=int32_to_bytes(0)[0:2]  # 0x00 means INT32
    elif tstring=='INT64':
        tbytes=int32_to_bytes(1)[0:2]  # 0x01 means INT32
    elif tstring[0:4]=='CHAR':
        N=int(tstring[5:-1])
        tbytes=int32_to_bytes(3+256*N)[0:2]  # 0x03 means CHAR,the last one encodes N(<=255)
    else:
        raise PyDBInternalError("only suit for INT32, INT64,CHAR(N),N<=255")
    return tbytes

def typebytes_to_str(tbytes):
    '''turn bytes of type to string'''
    if tbytes==b'\x00\x00':  # 0x00 means INT32
        tstring='INT32'
    elif tbytes==b'\x01\x00':  # 0x01 means INT32
        tstring='INT64'
    elif tbytes[0]==3:  # 0x03 means CHAR,the last one encodes N(<=255)
        N=tbytes[1]
        tstring='CHAR('+str(N)+')'
    else:
        raise PyDBInternalError("only suit for INT32, INT64,CHAR(N),N<=255")
    return tstring

def get_size(fileobject):
    '''get size of file'''
    fileobject.seek(0,2)  # move the cursor to the end of the file
    size = fileobject.tell()
    return size

class Schema:
    '''relation schema'''
    def __init__(self,input_data,relation_name):
        " initialize schema class with name, count, type "
        self.relation_name=relation_name
        self.schema_degree=len(input_data)  # degree of relation
        # structure: table_name+degrees+field_name+field_type
        self.schema_size=LEN_String+LEN_Int32+LEN_String*self.schema_degree+LEN_Type*self.schema_degree  # size of the schema

        self.field_name=[]  # name of each col
        self.field_domain=[]  # int32 or varchar(N)
        for (name, data_type) in input_data:
            if data_type!='INT32' and data_type!='INT64':
                if not (data_type[:5] == 'CHAR(' and  data_type[-1] == ')'):
                    raise PyDBInternalError("only INT32,INT64 and CHAR(N) are allowed in Schema")
                if int(data_type[5:-1]) > 255 and int(data_type[5:-1]) < 0:
                    raise PyDBInternalError("only 0<=N<=255 CHAR(N) is allowed")

            self.field_name.append(name)
            self.field_domain.append(data_type)

        self.data=[self.relation_name,self.schema_degree,self.field_name,self.field_domain]  # all the data in schema

    def serialize(self):
        '''return the bytes array of schema
           structure: [relation_name,degree,[field_name],[field_type]]'''
        msg_str = msgpack.packb(self.data)
        return msg_str 

    def deserialize(self,msg_str):
        '''return the dict of schema from bytes array
           structure: [relation_name,degree,[field_name],[field_type]]'''
        self.data = msgpack.unpackb(msg_str)
        self.relation_name=self.data[0]
        self.schema_degree=self.data[1]
        self.field_name=self.data[2]
        self.field_domain=self.data[3]
        return self.data

    def __eq__(self, other):
        return self.data==other.data

class HeapPage:

    '''Each instance of HeapPage stores data for one page of HeapFiles and implements the Page interface that is used by BufferPool.
      structure of heap_page: [header[pageId, slotNum, schema, isdirty],[[tuple1],[tuple2],..,[tupleN]]]
      structure of header: [pageId, slotNum, schema, isdirty]'''
    PAGE_ID=0  # unique page id
    def __init__(self,input_schema):
        '''self.data need to be refreshed before become callable'''
        self.page_size=PAGE_SIZE

        HeapPage.PAGE_ID+=1
        self.page_id=HeapPage.PAGE_ID  # 4 bytes for int type
        self.slot_num=0                 # number of slots that is used
        self.page_schema_data=input_schema.data   # describes the schema of tuples.
        self.header_size=LEN_Int32*2+input_schema.schema_size
        if self.header_size>MAX_HEADER_SIZE:
            PyDBInternalError("ERROR: header oversize")

        self.page_tuples=[]             # tuple data, store tuples in dictionary formation
        for i in range(0,MAX_SLOTS):
            self.page_tuples.append(None)
        self.is_dirty=False
        self.header=[self.page_id,self.slot_num,self.page_schema_data,self.is_dirty]
        self.data=[self.header,self.page_tuples]

    def get_header(self):
        '''return the byte array of header of this page'''
        # header contains [pageId, slotNum, schema, isdirty] of this page
        self.header=[self.page_id,self.slot_num,self.page_schema_data,self.is_dirty]
        self.data=[self.header,self.page_tuples]
        return self.header

    def get_id(self):
        """ get the id of this page
        """
        return self.page_id  #string of page id

    def insert_tuple(self,Tuple):
        """ adds the specified tuple to this page
            tuple structure:[RecordID(pageID,slotNo.),size,schema,data in each column]
        """
        if self.slot_num>=MAX_SLOTS:
            print("NO MORE EMPTY SLOTS")
            return 1  # return 1 means need to create an overflow page

        # check schema
        tuple_exam(Tuple)
        if Tuple[2]!=self.page_schema_data:
            raise PyDBInternalError("schema not match")

        # check whether a slot can tolerate this tuple, and store the size
        if len(msgpack.packb(Tuple))>SLOT_SIZE:
            raise PyDBInternalError("Tuple's size surpass the limit")

        # insert tuple
        for i in range(0,MAX_SLOTS):
            if self.page_tuples[i]==None:
                Tuple[0]=[self.page_id,i]
                self.page_tuples[i]=Tuple
                print("insertion succeed")
                self.slot_num+=1
                self.is_dirty=True
                self.get_header()
                return 0

    def delete_tuple(self,tuple_id):
        '''delete tuple in heap page, tuple_id is an INT
        tuple structure:[RecordID(pageID,slotNo.),size,schema,data in each column]'''
        if tuple_id>MAX_SLOTS:
            raise PyDBInternalError("tuple not exist, tuple ID too large")
        try:
            if self.page_tuples[tuple_id][0][1]==tuple_id:
                self.page_tuples[tuple_id]=None
                self.is_dirty=True
                self.slot_num-=1
                self.get_header()
                return 'delete successfully'
            else:
                raise PyDBInternalError("tuple ID not match")
        except:
            return 'page is empty'

    def get_page_data(self):
        """ return the byte array data contained in this page, include header, need to implement get_header first
        """
        self.get_header()
        msg_str = msgpack.packb(self.data)  # serialization
        return msg_str

    def deserialize(self,msg_str):
        ''' deserialize the page_file from bytes array'''

        # structure of page_file: [header,[[tuple1],[tuple2],..,[tupleN]]]
        self.data = msgpack.unpackb(msg_str)

        # header fulfil
        self.header=self.data[0]
        self.page_id=self.header[0]
        self.slot_num=self.header[1]
        self.page_schema_data=self.header[2]
        self.is_dirty=self.header[3]

        # data fulfil
        self.page_tuples=self.data[1]
        return self.data

    def print_for_bugs(self):
        '''print the content in page'''
        print("pageid:",self.page_id)
        print("tuples in this page:",self.page_tuples)
        return 0

class HeapFile:
    ''' a collection of those pages
        structure of heap file:[header,block of pages]
        structure of header:[file_id,page_num,schema,header_index] '''
    HeapFile_ID=0  # unique heap file's id
    def __init__(self,input_schema):
        self.file_size=HEAPFILE_SIZE

        HeapFile.HeapFile_ID+=1
        self.file_id=HeapFile.HeapFile_ID
        self.page_num=0
        self.file_schema_data=input_schema  # schema of each page
        self.header_index=[]  # the metadata of page id, structure:[pid1,pid2,...]

        self.file_pages_data=[]
        for i in range(0,MAX_PAGES):
            self.file_pages_data.append(None)
            self.header_index.append(-1)
        self.header=[self.file_id,self.page_num,self.file_schema_data,self.header_index]
        self.data=[self.header,self.file_pages_data]

    def read_page(self,pid):
        '''Read the specified page from disk
          tuple structure:[RecordID(pageID,slotNo.),size,schema,data in each column]
          structure of page_file: [header,[[tuple1],[tuple2],..,[tupleN]]]
          header contains [pageId, slotNum, schema] of this page '''
        flag=0  # flag of whether target page is found
        for i in range(0,MAX_PAGES):
            # tempid=int32_from_bytes(self.file_page_bytes[i].raw[0:LEN_Int32])
            if self.header_index[i]==pid:
                flag=1
                break
        if flag==0:
            print("page not found")
            return 0
        # get the heap page object
        return self.file_pages_data[i]

    def write_page(self,input_page):
        '''Push the specified page to file,input page_data'''
        # schema check
        if self.file_schema_data!=input_page.page_schema_data:
            raise PyDBInternalError("schema not match")

        # size check
        if len(input_page.get_page_data())>PAGE_SIZE:
            raise PyDBInternalError("page is oversized")

        # if the page exist
        for i in range(0,MAX_PAGES):
            if input_page.page_id==self.header_index[i]:  # int32_from_bytes(self.file_page_bytes[i].raw[0:LEN_Int32]):
                self.file_pages_data[i]=input_page.data
                self.data=[self.header,self.file_pages_data]
                return 0

        # page not exist in it
        for i in range(0,MAX_PAGES):
            if self.file_pages_data[i]==None:
                self.file_pages_data[i]=input_page.data
                self.header_index[i]=input_page.page_id
                self.page_num+=1
                self.header=[self.file_id,self.page_num,self.file_schema_data,self.header_index]
                self.data=[self.header,self.file_pages_data]
                return 0
        print("no more space in heap file")
        return 1

    def get_file(self):
        '''Returns the File backing this HeapFile on disk
           structure of heap file_data:[header,block of pages],
           structure of header:[file_id,page_num,schema,header_index]'''
        self.header=[self.file_id,self.page_num,self.file_schema_data,self.header_index]
        self.data=[self.header,self.file_pages_data]
        msg_str = msgpack.packb(self.data)
        return msg_str

    def insert_tuple(self,input_tuple):
        '''insert a tuple to this heap file'''
        i=-1
        for page_data in self.file_pages_data:
            '''structure of heap_page: [header[pageId, slotNum, schema, isdirty],[[tuple1],[tuple2],..,[tupleN]]]
            tuple structure:[RecordID(pageID,slotNo.),size,schema,data in each column]'''
            i+=1
            if page_data!=None:
                if page_data[0][1]<MAX_SLOTS:
                    '''insert tuple'''
                    if page_data[0][2]!=input_tuple[2]:
                        raise PyDBInternalError("schema not match")
                    if len(msgpack.packb(input_tuple))>SLOT_SIZE:
                        raise PyDBInternalError("Tuple's size surpass the limit")
                    if page_data[0][2][1]!=len(input_tuple)-3:
                        raise PyDBInternalError("degrees not match between input tuple and page_schema in this page")

                    for j in range(MAX_SLOTS):
                        if page_data[1][j]==None:
                            input_tuple[0]=[page_data[0][0],j]
                            page_data[0][1]+=1
                            page_data[0][3]=True
                            page_data[1][j]=input_tuple
                            self.file_pages_data[i]=page_data
                            print('insertion succeed')
                    return page_data

        # pages are all full, need to creat an overflow page
        for i in range(MAX_PAGES):
            if self.file_pages_data[i]==None:
                '''schema_data=[self.relation_name,self.schema_degree,self.field_name,self.field_domain]'''
                input_sdata=[]
                for j in range(len(self.file_schema_data[2])):
                    input_sdata.append((self.file_schema_data[2][j],self.file_schema_data[3][j]))
                overflow_page=HeapPage(input_schema=Schema(input_sdata,self.file_schema_data[0]))
                overflow_page.insert_tuple(input_tuple)
                self.file_pages_data[i]=overflow_page.data
                self.header_index[i]=overflow_page.page_id
                self.page_num+=1
                self.header=[self.file_id,self.page_num,self.file_schema_data,self.header_index]
                self.data=[self.header,self.file_pages_data]
                return overflow_page.data
        raise PyDBInternalError("no enough space in this heap file")

    def delete_tuple(self,RecordID):
        '''remove the specific tuple according to recordID'''
        page_id=RecordID[0]
        tuple_id=RecordID[1]
        if tuple_id>MAX_SLOTS:
            raise PyDBInternalError("tuple not exist, tuple ID too large")
        for index in range(MAX_PAGES):
            if self.header_index[index]==page_id:
                self.file_pages_data[index][1][tuple_id]=None
                self.file_pages_data[index][0][1]-=1
                self.file_pages_data[index][0][3]=True
                return 0
        raise PyDBInternalError("tuple not exist,due to the lack of certain page")
