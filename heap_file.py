from ctypes import *
import numpy as np
from os import popen
import struct
HEAPFILE_SIZE = 40960
PAGE_SIZE = 5120
SLOT_SIZE = 2048
MAX_HEADER_SIZE = 1024
MAX_SLOTS = (PAGE_SIZE-MAX_HEADER_SIZE)//SLOT_SIZE  # 4 TOTAL SLOTS, Each slot can hold one tuple
MAX_PAGES = (HEAPFILE_SIZE-MAX_HEADER_SIZE)//PAGE_SIZE

LEN_String: int = 256  # bytes of constant string
LEN_Int32 = 4  # bytes of int, static
LEN_Type = 16  # stands for int32 or varchar(256)
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

def get_size(fileobject):
    '''get size of file'''
    fileobject.seek(0,2)  # move the cursor to the end of the file
    size = fileobject.tell()
    return size

class Schema:
    '''relation schema'''
    def __init__(self,inputdata,relationname):
        " initialize schema class with name, count, type "
        self.relation_name=relationname
        self.schema_degree=len(inputdata)  # degree of relation
        # structure: table_name+degrees+field_name+field_type
        self.schema_bytesdata=b''  # bytes data of schema

        self.schema_size=LEN_String+LEN_Int32+LEN_String*self.schema_degree+LEN_Type*self.schema_degree  # size of the schema

        self.field_name=[]  # name of each col
        self.field_domain=[]  # int32 or varchar(N)
        for i in range(0,self.schema_degree):
            if inputdata[i][1]!='int32' and inputdata[i][1]!='varchar('+str(LEN_String)+')':
                print("only int32 and varchar(N) are allowed")
                self.field_domain=None
                self.field_name=None
                break
            self.field_name.append(inputdata[i][0])
            self.field_domain.append(inputdata[i][1])

    def serialize(self):
        '''return the bytes array of fields'''
        # structure: relation_name+degree+field_name+field_type
        self.schema_bytesdata=create_string_buffer(self.schema_size)
        tempname=create_string_buffer(LEN_String)
        tempname.raw=self.relation_name.encode()
        tempdegree=create_string_buffer(LEN_Int32)
        tempdegree.raw=int32_to_bytes(self.schema_degree)

        tempname1=b''
        temptype1=b''
        for i in range(0,self.schema_degree):
            tempcolname=create_string_buffer(LEN_String)
            tempcoltype=create_string_buffer(LEN_Type)
            tempcolname.raw=self.field_name[i].encode()
            tempname1+=tempcolname.raw

            tempcoltype.raw=self.field_domain[i].encode()
            temptype1+=tempcoltype.raw

        self.schema_bytesdata.raw=tempname.raw+tempdegree.raw+tempname1+temptype1
        return self.schema_bytesdata.raw

    def deserialize(self,bytesarray):
        '''return the field object from bytes array'''
        # structure: relation_name+degree+field_name+field_type
        self.schema_size=len(bytesarray)

        self.relation_name=bytesarray[0:LEN_String].decode()

        self.schema_degree=int32_from_bytes(bytesarray[LEN_String:LEN_String+LEN_Int32])

        stringinfo2=bytesarray[LEN_String+LEN_Int32:LEN_String+LEN_Int32+LEN_String*self.schema_degree]
        stringinfo3=bytesarray[LEN_String+LEN_Int32+LEN_String*self.schema_degree:self.schema_size]
        colnum=self.schema_degree
        self.field_name=[]
        self.field_domain=[]
        for i in range(0,colnum):
            tempname=create_string_buffer(LEN_String)
            tempname.raw=stringinfo2[i*LEN_String:(1+i)*LEN_String]
            self.field_name.append(tempname.value.decode())
            temptype=create_string_buffer(LEN_Type)
            temptype.raw=stringinfo3[i*LEN_Type:(1+i)*LEN_Type]
            self.field_domain.append(temptype.value.decode())
        self.schema_size=LEN_String+LEN_Int32+LEN_String*self.schema_degree+LEN_Type*self.schema_degree

class heap_page:
    '''Each instance of HeapPage stores data for one page of HeapFiles and implements the Page interface that is used by BufferPool.'''
    # structure of heap_page: [header,[tuple1],[tuple2],..,[tupleN]]
    # structure of header: [pageId, slotNum, tupleDesc]
    PAGE_ID=0  # unique page id
    def __init__(self,inputschema):
        self.page_size=PAGE_SIZE
        self.page_bytesdata=create_string_buffer(PAGE_SIZE)  # bytes arrays, contain all the bytes information of page file

        heap_page.PAGE_ID+=1
        self.page_id=heap_page.PAGE_ID  # 4 bytes for int type
        self.slot_num=0  # number of slots that is used
        self.tuple_desc=inputschema  # describes the schema of tuples.

        self.header_size=LEN_Int32*2+self.tuple_desc.schema_size
        if self.header_size>MAX_HEADER_SIZE: print("ERROR: header oversize")
        self.header=create_string_buffer(MAX_HEADER_SIZE)

        self.page_tuples=[]  # tuple data
        for i in range(0,MAX_SLOTS):
            self.page_tuples.append([[self.page_id,i],0,self.tuple_desc,None])
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

        if self.tuple_desc.schema_degree!=len(Tuple)-3:
            print("elements not match between input tuple and tuple_desc in this page")
            return 0

        # check whether a slot can tolerate this tuple, and store the size
        tempTupleSize=LEN_Int32*3+len(Tuple[2].serialize())
        for i in range(0,self.tuple_desc.schema_degree):
            if Tuple[2].field_domain[i]=='int32':
                tempTupleSize+=LEN_Int32
            elif Tuple[2].field_domain[i]=='varchar('+str(LEN_String)+')':
                tempTupleSize+=LEN_String
            else:
                print("only int32 or varchar(LEN_String) is allowed")
                return 0
        Tuple[1]=tempTupleSize
        if tempTupleSize>SLOT_SIZE:
            print("size of the tuple surpasses the upper bound size of a slot")
            return 0

        if self.tuple_desc!=Tuple[2]:
            print("schema not match")
            return 0

        # insert tuple
        for i in range(0,MAX_SLOTS):
            if self.page_tuples[i][1]==0:
                Tuple[0][1]=i  # set slotNo.
                Tuple[0][0]=self.page_id
                self.page_tuples[i]=Tuple
                print("insertion succeed")
                return 0

    def get_header(self):
        '''return the byte array of header of this page'''
        # header contains [pageId, slotNum, tupleDesc] of this page
        tempid=create_string_buffer(LEN_Int32)
        tempnum=create_string_buffer(LEN_Int32)
        tempschema=create_string_buffer(self.tuple_desc.schema_size)
        tempid.raw=int32_to_bytes(self.page_id)
        tempnum.raw=int32_to_bytes(self.slot_num)
        tempschema.raw=self.tuple_desc.serialize()

        self.header.raw=tempid.raw+tempnum.raw+tempschema.raw
        return self.header.raw

    def get_page_data(self):
        """ return the byte array data contained in this page, include header, need to implement get_header first
        """
        # tuple structure:[RecordID(pageID,slotNo.),size,schema,data in each column]
        # structure of page_file: [header,[[tuple1],[tuple2],..,[tupleN]]]
        self.page_bytesdata=create_string_buffer(PAGE_SIZE)  # refresh the space
        tempdata=b''

        # for each tuple(=slot)
        for i in range(0,MAX_SLOTS):
            temptuple=self.page_tuples[i]
            temp=create_string_buffer(SLOT_SIZE)
            tempslot=b''
            # record ID
            tempslot+=int32_to_bytes(temptuple[0][0])
            tempslot+=int32_to_bytes(temptuple[0][1])
            # size of tuple
            tempslot+=int32_to_bytes(temptuple[1])
            # schema
            tempschema=temptuple[2]
            tempslot+=tempschema.serialize()
            # data in each column
            if self.page_tuples[i][3]==None:
                tempnone=create_string_buffer(SLOT_SIZE-(3*LEN_Int32+tempschema.schema_size))
                tempslot+=tempnone.raw
            else:
                for j in range(0,tempschema.schema_degree):
                    if tempschema.field_domain[j]=='int32':
                        tempint=create_string_buffer(LEN_Int32)
                        tempint.raw=int32_to_bytes(temptuple[3+j])
                        tempslot+=tempint.raw
                    else:  # str
                        tempstr=create_string_buffer(LEN_String)
                        tempstr.raw=temptuple[3+j].encode()
                        tempslot+=tempstr.raw
            temp.raw=tempslot
            tempdata+=temp.raw
        self.page_bytesdata.raw=self.header.raw+tempdata
        return self.page_bytesdata.raw  # return the byte arrays

    def deserialize(self,bytesarray):
        ''' deserialize the page_file from bytes array'''
        # structure of page_file: [header,[[tuple1],[tuple2],..,[tupleN]]]
        self.page_bytesdata=bytesarray

        # header part, header contains [pageId, slotNum, tupleDesc] of this page
        self.page_id=int32_from_bytes(bytesarray[0:LEN_Int32])
        self.slot_num=int32_from_bytes((bytesarray[LEN_Int32:LEN_Int32*2]))
        tempschema=Schema(inputdata=[('colname','int32')],relationname='for initialization purpose')
        tempschema.deserialize(bytesarray[LEN_Int32*2:MAX_HEADER_SIZE])
        self.tuple_desc=tempschema
        self.header.raw=bytesarray[0:MAX_HEADER_SIZE]

        self.page_tuples=[]  # tuple data
        for i in range(0,MAX_SLOTS):
            self.page_tuples.append([[self.page_id,i],0,self.tuple_desc])
        # page_tuples
        for i in range(0,MAX_SLOTS):
            # tuple structure:[RecordID(pageID,slotNo.),size,schema,data in each column]
            tempbytes=create_string_buffer(SLOT_SIZE)
            tempbytes.raw=bytesarray[MAX_HEADER_SIZE+i*SLOT_SIZE:MAX_HEADER_SIZE+(i+1)*SLOT_SIZE]
            # record ID and tuple size
            temptuple=[[self.page_id,i],0]
            temptuple[1]=SLOT_SIZE
            # schema
            temptuple.append(self.tuple_desc)
            # tuple data
            tempfield_domain=tempschema.field_domain
            tempcol=tempschema.schema_degree
            tempsize=tempschema.schema_size
            # exam whether this tuple is empty
            empty_exam=create_string_buffer(SLOT_SIZE-(3*LEN_Int32+tempsize))
            empty_exam.raw=tempbytes.raw[3*LEN_Int32+tempsize:SLOT_SIZE]
            if empty_exam.value==b'':
                temptuple.append(None)
                self.page_tuples[i]=temptuple
                continue
            intnum=0
            strnum=0
            for j in range(0,tempcol):
                if tempfield_domain[j]=='int32':
                    temptuple.append(int32_from_bytes(tempbytes.raw[3*LEN_Int32+tempsize+intnum*LEN_Int32+strnum*LEN_String:
                                     3*LEN_Int32+tempsize+(intnum+1)*LEN_Int32+strnum*LEN_String]))
                    intnum+=1
                else:
                    tempstr=create_string_buffer(LEN_String)
                    tempstr.raw=tempbytes.raw[3*LEN_Int32+tempsize+intnum*LEN_Int32+strnum*LEN_String:
                                     3*LEN_Int32+tempsize+intnum*LEN_Int32+(strnum+1)*LEN_String]
                    temptuple.append(tempstr.value.decode())
                    strnum+=1
            self.page_tuples[i]=temptuple

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
        self.file_bytesdata=create_string_buffer(HEAPFILE_SIZE)

        heap_file.HeapFile_ID+=1
        self.file_id=heap_file.HeapFile_ID
        self.page_num=0
        self.tuple_desc=inputschema  # schema of each page

        self.header_index=[]  # the metadata of page id, structure:[pid1,pid2,...]
        self.file_pages=[]
        self.file_page_bytes=[]
        for i in range(0,MAX_PAGES):
            self.file_pages.append(None)
            temp=create_string_buffer(PAGE_SIZE)
            self.file_page_bytes.append(temp)
            tempindex=int32_from_bytes(self.file_page_bytes[i].raw[0:LEN_Int32])
            self.header_index.append(tempindex)

    def read_page(self,pid):
        '''Read the specified page from disk'''
        # tuple structure:[RecordID(pageID,slotNo.),size,schema,data in each column]
        # structure of page_file: [header,[[tuple1],[tuple2],..,[tupleN]]]
        # header contains [pageId, slotNum, tupleDesc] of this page
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
        heappage_space=heap_page(self.tuple_desc)
        heappage_space.deserialize(self.file_page_bytes[i].raw)
        self.file_pages[i]=heappage_space
        return heappage_space

    def write_page(self,inputpage):
        '''Push the specified page to disk'''
        # if the page exist
        for i in range(0,MAX_PAGES):
            if inputpage.page_id==self.header_index[i]:  # int32_from_bytes(self.file_page_bytes[i].raw[0:LEN_Int32]):
                self.file_pages[i]=inputpage
                self.file_page_bytes[i].raw=inputpage.get_page_data()
                return 0

        # page not exist in it
        for i in range(0,MAX_PAGES):
            if self.file_page_bytes[i].value==b'':
                self.file_page_bytes[i].raw=inputpage.get_page_data()
                self.file_pages[i]=inputpage
                self.header_index[i]=inputpage.page_id
                return 0
        print("no more space in heap file")
        return 1

    def get_file(self):
        '''Returns the File backing this HeapFile on disk'''
        # structure of heap file:[file_id,page_num,tuple_desc,header_index,page_bytes]
        tempid=int32_to_bytes(self.file_id)
        temppagenum=int32_to_bytes(self.page_num)
        temptupledesc=self.tuple_desc.serialize()
        tempheader_index=b''
        for i in range(0,MAX_PAGES):
            tempheader_index+=int32_to_bytes(self.header_index[i])
        temppage=b''
        tempheader=create_string_buffer(MAX_HEADER_SIZE)
        try:
            tempheader.raw=tempid+temppagenum+temptupledesc+tempheader_index
        except:
            print("header oversized")
            return 0

        # page data
        for i in range(0,MAX_PAGES):
            temppage+=self.file_page_bytes[i]

        self.file_bytesdata.raw=tempheader.raw+temppage
        return self.file_bytesdata.raw
