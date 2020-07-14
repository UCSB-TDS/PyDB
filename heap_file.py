from ctypes import *
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

def int32_to_bytes(x: int) -> bytes:
    '''convertion between int and bytes, note int = 4 bytes'''
    try:
        temp=struct.pack('i',x)
        return temp
    except:
        print("int32 input out of range")
        return b''

def int32_from_bytes(xbytes):
    try:
        temp=struct.unpack('i',xbytes)[0]
        return temp
    except:
        print("bytes length input out of range")
        return b''

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
        print("only suit for INT32, INT64,CHAR(N),N<=255")
        return 0
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
        print("only suit for INT32, INT64,CHAR(N),N<=255")
        return 0
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

    def get_dict(self):
        # structure: relation_name+degree+field_name+field_type in dict type
        schema_dict = {
            'relation_name':self.relation_name,
            'schema_degree':self.schema_degree
        }
        for i in range(0,self.schema_degree):
            schema_dict[self.field_name[i]]=self.field_domain[i]
        return schema_dict

    def serialize(self):
        '''return the bytes array of schema'''
        # structure: relation_name+degree+field_name+field_type in dict type
        # serialize the dict of schema
        schema_dict=self.get_dict()
        for i in range(0,self.schema_degree):
            schema_dict[self.field_name[i]]=typestr_to_bytes(self.field_domain[i])
        msg_str = msgpack.packb(schema_dict)
        return msg_str

    def deserialize(self,msg_str):
        '''return the dict of schema from bytes array'''
        # structure: relation_name+degree+field_name+field_type in dict type
        schema_dict = msgpack.unpackb(msg_str, use_list=False)
        self.relation_name=schema_dict['relation_name']
        self.schema_degree=schema_dict['schema_degree']
        self.field_name=[]
        self.field_domain=[]
        for key in schema_dict:
            if key!='relation_name' and key!='schema_degree':
                self.field_name.append(key)
                self.field_domain.append(typebytes_to_str(schema_dict[key]))
        return self.get_dict()

    def __eq__(self, other):
        return self.get_dict()==other.get_dict()

class heap_page:
    '''Each instance of HeapPage stores data for one page of HeapFiles and implements the Page interface that is used by BufferPool.'''
    # structure of heap_page: [header,[tuple1],[tuple2],..,[tupleN]]
    # structure of header: [pageId, slotNum, schema]
    PAGE_ID=0  # unique page id
    def __init__(self,inputschema):
        self.page_size=PAGE_SIZE
        self.page_bytesdata=create_string_buffer(PAGE_SIZE)  # bytes arrays, contain all the bytes information of page file

        heap_page.PAGE_ID+=1
        self.page_id=heap_page.PAGE_ID  # 4 bytes for int type
        self.slot_num=0  # number of slots that is used
        self.page_schema=inputschema  # describes the schema of tuples.

        self.header_size=LEN_Int32*2+self.page_schema.schema_size
        if self.header_size>MAX_HEADER_SIZE: print("ERROR: header oversize")

        self.page_tuples=[]  # tuple data, store tuples in dictionary formation
        for i in range(0,MAX_SLOTS):
            tuple1 = {
                'recordID':[self.page_id,i],
                'size':0  # size=0 means empty slot, size= SLOT_SIZE means tuple with data
            }
            tempschema=self.page_schema
            j=0
            for x in tempschema.field_name:
                tuple1[x]=None
                j+=1
            self.page_tuples.append(tuple1)
        self.get_header()

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
        if self.page_schema.schema_degree!=len(Tuple)-2:
            print("degrees not match between input tuple and page_schema in this page")
            return 0
        index=0
        for key in Tuple:
            if key!='recordID' and key!='size':
                if key!=self.page_schema.field_name[index]:
                    print("schema not match")
                    return 0
                index+=1

        # check whether a slot can tolerate this tuple, and store the size
        if len(msgpack.packb(Tuple))>SLOT_SIZE:
            print("Tuple's size surpass the limit")
            return 0

        # insert tuple
        for i in range(0,MAX_SLOTS):
            if self.page_tuples[i]['size']==0:
                Tuple['recordID']=[self.page_id,i]
                self.page_tuples[i]=Tuple
                print("insertion succeed")
                self.slot_num+=1
                self.get_header()
                return 0

    def get_header(self):
        '''return the byte array of header of this page'''
        # header contains [pageId, slotNum, schema] of this page
        self.header = {
            "pageID":self.page_id,
            "slotNum":self.slot_num,
            "schema":self.page_schema
        }
        return self.header

    def get_page_dict(self):
        '''get the page in dict form'''
        # header contains [pageId, slotNum, schema] of this page, in dict formation
        header = {
            "pageID":self.page_id,
            "slotNum":self.slot_num,
            "schema":self.page_schema.get_dict()
        }

        # structure of page_file: [header,[[tuple1],[tuple2],..,[tupleN]]], in dict formation
        page ={
            "header":header
        }
        for t in range(1,1+MAX_SLOTS):
            temp='tuple'+str(t)
            page[temp]=self.page_tuples[t-1]
        return page

    def get_page_data(self):
        """ return the byte array data contained in this page, include header, need to implement get_header first
        """
        page_dict=self.get_page_dict()
        # serialization
        msg_str = msgpack.packb(page_dict)
        return msg_str

    def deserialize(self,msg_str):
        ''' deserialize the page_file from bytes array'''
        # structure of page_file: [header,[[tuple1],[tuple2],..,[tupleN]]]
        page_dict = msgpack.unpackb(msg_str, use_list=False)
        # header fulfil
        tempheader=page_dict['header']
        self.page_id=tempheader["pageID"]
        self.slot_num=tempheader["slotNum"]
        # structure: table_name+degrees+field_name+field_type
        tempname=[]
        tempdomain=[]
        for key in tempheader["schema"]:
            if key!='relation_name' and key!='schema_degree':
                tempname.append(key)
                tempdomain.append(tempheader["schema"][key])
        tempinput=[]
        for i in range(0,tempheader["schema"]['schema_degree']):
            tempinput.append((tempname[i],tempdomain[i]))
        name=tempheader["schema"]['relation_name']
        self.page_schema=Schema(input_data=tempinput,relation_name=name)
        self.get_header()

        # data fulfil
        for t in range(1,1+MAX_SLOTS):
            temp='tuple'+str(t)
            self.page_tuples[t-1]=page_dict[temp]
        return page_dict

    def print_for_bugs(self):
        '''print the content in page'''
        print("pageid:",self.page_id)
        print("tuples in this page:",self.page_tuples)
        # print("bytesdata of the page:",self.page_bytesdata)
        return 0

class heap_file:
    ''' a collection of those pages'''
    # structure of heap file:[header,block of pages]
    # structure of header:[file_id,page_num,schema,header_index]
    HeapFile_ID=0  # unique heap file's id
    def __init__(self,inputschema):
        self.file_size=HEAPFILE_SIZE

        heap_file.HeapFile_ID+=1
        self.file_id=heap_file.HeapFile_ID
        self.page_num=0
        self.file_schema=inputschema  # schema of each page
        self.header_index=[]  # the metadata of page id, structure:[pid1,pid2,...]

        self.file_pages=[]
        for i in range(0,MAX_PAGES):
            self.file_pages.append(None)
            self.header_index.append(0)

    def read_page(self,pid):
        '''Read the specified page from disk'''
        # tuple structure:[RecordID(pageID,slotNo.),size,schema,data in each column]
        # structure of page_file: [header,[[tuple1],[tuple2],..,[tupleN]]]
        # header contains [pageId, slotNum, schema] of this page
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
        return self.file_pages[i]

    def write_page(self,inputpage):
        '''Push the specified page to file'''
        # schema check
        if self.file_schema!=inputpage.page_schema:
            print("schema not match")
            return 0

        # size check
        if len(inputpage.get_page_data())>PAGE_SIZE:
            print("page is oversized")
            return 0

        # if the page exist
        for i in range(0,MAX_PAGES):
            if inputpage.page_id==self.header_index[i]:  # int32_from_bytes(self.file_page_bytes[i].raw[0:LEN_Int32]):
                self.file_pages[i]=inputpage
                return 0

        # page not exist in it
        for i in range(0,MAX_PAGES):
            if self.file_pages[i]==None:
                self.file_pages[i]=inputpage
                self.header_index[i]=inputpage.page_id
                self.page_num+=1
                return 0
        print("no more space in heap file")
        return 1

    def get_file_dict(self):
        '''get the file in dict form'''
        # structure of header:[file_id,page_num,schema,header_index]
        header={
            'file_id':self.file_id,
            'page_num':self.page_num,
            'schema':self.file_schema.get_dict(),
            'header_index':self.header_index
        }
        # structure of heap file:[header,block of pages]
        file={
            'header':header
        }
        for i in range(1,1+MAX_PAGES):
            temp='page'+str(i)
            if self.file_pages[i-1]!=None:
                file[temp]=self.file_pages[i-1].get_page_dict()
            else:
                file[temp]=None

    def get_file(self):
        '''Returns the File backing this HeapFile on disk'''
        # structure of heap file:[header,block of pages],structure of header:[file_id,page_num,schema,header_index]
        msg_str = msgpack.packb(self.get_file_dict())
        return msg_str
