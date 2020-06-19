# task 1: implement page file
# task 2: buffer pool
from ctypes import *
import numpy as np
from os import popen
import struct
HEAPfile_SIZE = 40960
PAGE_SIZE = 4096
SLOT_SIZE = 1024
MAX_SLOTS = PAGE_SIZE//SLOT_SIZE-1  # 4 TOTAL SLOTS, Each slot can hold one tuple

LEN_String: int = 9  # bytes of constant string
LEN_Int = 4  # bytes of int, static
# note that int length also should be less than 8

PAGE_ID = 0  # unique id for each page
StringLength = LEN_String  # length of string in tuple

# convertion between int and bytes, note int = 8 bytes
def int_to_bytes(x: int) -> bytes:
	return struct.pack('i',x)
	#return x.to_bytes((x.bit_length() + 7) // 8, 'big')

def int_from_bytes(xbytes):
	temp=struct.unpack('i',xbytes)[0]
	return temp

# convertion between string ending with ' ' to int
def int_from_string(string):
	return int(string[0:string.find(' ')])

# get size of file
def getSize(fileobject):
	fileobject.seek(0,2)  # move the cursor to the end of the file
	size = fileobject.tell()
	return size

class Fields:
	" initialize fields class with name, count, type "
	def __init__(self):
		self.field_name=None
		self.field_cols=[]  # name of each col
		# self.field_count=0  # 8 bytes
		self.type=None  # INT_TYPE or STRING_TYPE
		# structure: name+type+count+colnames
		self.field_size=0  # size of the field
		self.field_bytesdata=None  # bytes data of field
	'''field set up'''
	def set_fields(self,Inputtype,Inputname,Inputcols):
		# for size limitation
		try:
			temp=create_string_buffer(LEN_String)
			for i in range(0,len(Inputcols)):
				temp.raw=Inputcols[i].encode()
			temp.raw=Inputname.encode()
		except:
			print("name as string should have the length less than LEN_String")
			return 0

		self.type=Inputtype.ljust(LEN_String," ") # int or str as STRING, stands for INT_TYPE and STRING_TYPE
		self.field_name=Inputname.ljust(LEN_String," ")  # table name, less than LEN_String
		for i in range(0,len(Inputcols)):
			self.field_cols.append(Inputcols[i].ljust(LEN_String," "))  # name of each col
		self.field_size=(2+len(Inputcols))*LEN_String # name+type+colnames+count
	'''get the type of field'''
	def get_ftype(self):
		return self.type
	'''return the bytes array of fields'''
	def serialize(self):
		# structure: name+type+colnames
		self.field_bytesdata=create_string_buffer(self.field_size)
		tempname=create_string_buffer(LEN_String)
		tempname.raw=self.field_name.encode()
		temptype=create_string_buffer(LEN_String)
		temptype.raw=self.type.encode()

		self.field_bytesdata.raw=tempname.raw+temptype.raw

		tempcolname=create_string_buffer(LEN_String)
		temp=b''
		for i in range(0,len(self.field_cols)):
			tempcolname.raw=self.field_cols[i].encode()
			temp+=tempcolname.raw
		self.field_bytesdata.raw=tempname.raw+temptype.raw+temp
		return self.field_bytesdata
	'''return the field object from bytes array'''
	def deserialize(self,bytesarray):
		# structure: name+type+count+colnames
		self.field_size=sizeof(bytesarray)

		self.field_name=bytesarray.raw[0:LEN_String].decode()

		self.type=bytesarray.raw[LEN_String:LEN_String*2].decode()

		stringinfo2=bytesarray.raw[LEN_String*2:self.field_size]
		colnum=int((self.field_size-2*LEN_String)/LEN_String)
		for i in range(0,colnum):
			self.field_cols.append(stringinfo2[i*LEN_String:(1+i)*LEN_String].decode())

class Tuple:
	# structure of tuple_data:[col1,col2,...,colN]
	def __init__(self):
		self.tuple_colnum=0
		self.tuple_fields=Fields()
		self.tuple_data=None
		self.tuple_tid=0      # tuple ID in each field
		self.tuple_Rid=[0,0]  # (PageID,slotNum)
		# structure: tid+Rid+field+data
		self.tuple_colnum=0
		self.tuple_size=0  # size of tuple
		self.tuple_bytesdata=None  # bytes array of tuple

	'''set the field for the tuple'''
	def set_field(self,Fields):
		self.tuple_colnum=len(Fields.field_cols)
		self.tuple_fields=Fields
		# data depends on type of fields
		if Fields.type!='int'.ljust(LEN_String," ") and Fields.type!='str'.ljust(LEN_String," "):
			print(Fields.type,"TYPE ERROR: only INT_TYPE and STRING_TYPE are allowed")
			return 0

	'''get the field of a tuple'''
	def get_Field(self):
		return self.tuple_fields

	'''input information to field'''
	def fulfill_info(self,Listofdata):
		# input new tuple value

		# type of input
		if type(Listofdata[0])==int:temptype='int'.ljust(LEN_String," ")
		elif type(Listofdata[0])==str:temptype='str'.ljust(LEN_String," ")  # have to
		else: temptype=None

		if len(Listofdata)>self.tuple_colnum:
			print("column number out of range")
			return 0
		elif temptype!=self.tuple_fields.type:
			print("type not match")
			return 0
		else:
			self.tuple_data=Listofdata
			return 1

	'''output tuple_data and fields_data to string'''
	def toString(self):
		tupleid=0
		temp=str(self.tuple_tid)+str(self.tuple_Rid)+\
			str(self.tuple_data)  # string of tuple data
		temp+=str(self.tuple_fields.field_name)+\
			 str(self.tuple_fields.type)+str(self.tuple_fields.field_cols)  # string of Fields
		return temp
	'''return the bytes array of the tuple'''
	def serialize(self):
		# structure: tid+Rid+field+data
		temptid=create_string_buffer(LEN_Int)
		temptid.raw=int_to_bytes(self.tuple_tid)
		# temptid.value=str(self.tuple_tid).ljust(LEN_String," ").encode()
		tempRid=create_string_buffer(LEN_Int*2)
		tempRid.raw=int_to_bytes(self.tuple_Rid[0])+int_to_bytes(self.tuple_Rid[1])
		# tempRid.value=str(self.tuple_Rid[0]).ljust(LEN_String," ").encode()+str(self.tuple_Rid[1]).ljust(LEN_String," ").encode()
		tempfield=create_string_buffer(self.tuple_fields.field_size)
		tempfield.raw=self.tuple_fields.serialize().raw
		temptuple=b''
		if self.tuple_fields.type=='int'.ljust(LEN_String," "):

			tempdata=create_string_buffer(LEN_Int)
			self.tuple_size=LEN_Int*3+self.tuple_fields.field_size+LEN_Int*self.tuple_colnum
			for i in range(0,self.tuple_colnum):
				tempdata.raw=int_to_bytes(self.tuple_data[i])
				temptuple+=tempdata.raw
		elif self.tuple_fields.type=='str'.ljust(LEN_String," "):

			tempdata=create_string_buffer(LEN_String)
			self.tuple_size=LEN_Int*3+self.tuple_fields.field_size+LEN_String*self.tuple_colnum
			for i in range(0,self.tuple_colnum):
				tempdata.raw=self.tuple_data[i].ljust(LEN_String," ").encode()
				temptuple+=tempdata
		else:
			print("only INT and STR can pass")
			return 0
		self.tuple_bytesdata=create_string_buffer(self.tuple_size)
		self.tuple_bytesdata.raw=temptid.raw+tempRid.raw+tempfield.raw+temptuple
		return self.tuple_bytesdata
	'''return the tuple object from bytes array'''
	def deserialize(self,bytesarray,fieldsize):
		# structure: tid+Rid+field+data
		self.tuple_size=sizeof(bytesarray)
		stringinfo1=create_string_buffer(fieldsize)
		stringinfo1.raw=bytesarray.raw[3*LEN_Int:3*LEN_Int+fieldsize]
		stringinfo2=create_string_buffer(self.tuple_size-(3*LEN_Int+fieldsize))
		stringinfo2.raw=bytesarray.raw[3*LEN_Int+fieldsize:self.tuple_size]
		# tid and Rid
		self.tuple_tid=int_from_bytes(bytesarray.raw[0:LEN_Int])
		self.tuple_Rid[0]=int_from_bytes(bytesarray.raw[LEN_Int:LEN_Int*2])
		self.tuple_Rid[1]=int_from_bytes(bytesarray.raw[LEN_Int*2:LEN_Int*3])

		# field
		print(self.tuple_tid,self.tuple_Rid,"test")
		tempfield=Fields()

		tempfield.deserialize(stringinfo1)
		self.set_field(tempfield)
		# data
		self.tuple_data=[]
		if self.tuple_fields.type=='int'.ljust(LEN_String," "):
			for i in range(0,self.tuple_colnum):
				tempdata=int_from_bytes(stringinfo2.raw[LEN_Int*i:LEN_Int*(1+i)])
				self.tuple_data.append(tempdata)
		elif self.tuple_fields.type=='str'.ljust(LEN_String," "):
			for i in range(0,self.tuple_colnum):
				tempdata=stringinfo2.raw[LEN_String*i:LEN_String*(1+i)].decode()
				self.tuple_data.append(tempdata)
		else:
			print("TYPE ERROR, only 'int' and 'str' type are allowed")
			return 0

class page_file:
	PAGE_ID=0  # unique page id
	# structure of page_file: [pageid,slotnum,[tuple1],[tuple2],..,[tupleN]]
	def __init__(self):
		page_file.PAGE_ID+=1
		self.page_size=PAGE_SIZE
		self.page_bytesdata=create_string_buffer(PAGE_SIZE)  # bytes arrays, contain all the bytes information of page file

		self.page_id=page_file.PAGE_ID  # 8 bytes for int type
		self.page_slotnum=0

		self.page_tuples=[]  # tuple data

	'''get the id of a page'''
	def get_id(self):
		""" get the id of this page
		"""
		return self.page_id  #string of page id

	'''get the bytes page_data in a page'''
	def get_page_data(self):
		""" return the byte array data contained in this page
		"""
		# structure of page_file: [pageid,slotnum,[tuple1],[tuple2],..,[tupleN]]
		self.page_bytesdata=create_string_buffer(PAGE_SIZE)  # refresh the space
		# the first 8 bytes in page_file stands for page's record number
		tempid=create_string_buffer(LEN_Int)
		tempid.raw=int_to_bytes(self.page_id)
		tempnum=create_string_buffer(LEN_Int)  # number of records in page = 8bytes
		tempnum.raw=int_to_bytes(self.page_slotnum)

		# the remaining space for storing tuples
		temp1=b''
		for i in range(0,MAX_SLOTS):
			temp=create_string_buffer(SLOT_SIZE)
			try:
				print(self.page_tuples[i].tuple_size)
				temp.raw=self.page_tuples[i].serialize().raw  # input the bytes array of tuple to temp

				temp1+=temp.raw

			except:
				temp1+=temp.raw
		self.page_bytesdata.raw=tempid.raw+tempnum.raw+temp1
		return self.page_bytesdata  # return the byte arrays

	''' deserialize the page_file from bytes array'''
	def deserialize(self,bytesarray,fieldsize):
		self.page_id=int_from_bytes(bytesarray.raw[0:LEN_Int])
		self.page_slotnum=int_from_bytes(bytesarray.raw[LEN_Int:LEN_Int*2])

		for i in range(0,self.page_slotnum):
			tempt=Tuple()
			tempbytes=create_string_buffer(SLOT_SIZE)
			tempbytes.raw=bytesarray.raw[LEN_Int*2+i*SLOT_SIZE:LEN_Int*2+(i+1)*SLOT_SIZE]
			tempt.deserialize(tempbytes,fieldsize)
			self.page_tuples.append(tempt)

		return 0

	'''insert a tuple to file_page'''
	def insert_tuple(self,Tuple):
		""" adds the specified tuple to this page
		"""
		# free space in the middle of page files
		for i in range(0,len(self.page_tuples)):
			if self.page_tuples[i]==None:
				if Tuple.tuple_size>SLOT_SIZE:
					print("insert fail:Tuple is too large to fit in a SLOT")
					return 0
				else:
					Tuple.tuple_Rid[0]=self.page_id
					Tuple.tuple_Rid[1]=self.page_slotnum
					self.page_tuples[i]=Tuple
					self.page_slotnum+=1
					return 0

		if self.page_slotnum+1>MAX_SLOTS:
			print("no space, need to create an overflow page")
			# create overflow page
			return 1  # means to create an overflow page
		elif Tuple.tuple_size>SLOT_SIZE:
			print("insert fail:Tuple is too large to fit in a SLOT")
			return 0
		else:
			# change the tuple_Rid to (PageId, slotnum), slotnum begin with 0
			Tuple.tuple_Rid[0]=self.page_id
			Tuple.tuple_Rid[1]=self.page_slotnum
			self.page_tuples.append(Tuple)
			self.page_slotnum+=1
		return 0

	'''print the content in page'''
	def print_for_bugs(self):
		print("pageid:",self.page_id)
		print("tuples in this page:",self.page_tuples)
		print("bytesdata of the page:",self.page_bytesdata)
		return 0

# create an  overflow page, while insert returns 1
def Overflow_page(Tuple):
	newpf=page_file()
	newpf.insert_tuple(Tuple)
	return newpf

class buffer_pool:

	def __init__(self,size):
		#PAGE_SIZE = 4096
		self.pool_bytesdata=create_string_buffer(size)
		self.pool_size=size
		self.DEFAULT_PAGES = size//PAGE_SIZE  # MAX number of pages
		self.pool_pages=[] #buffer pages, from 0-defaul_pages
		self.pool_num=0

	'''delete the tuple in buffer pool according to tuple id, and store the deleted one in t'''
	def delete_tuple(self,tid, t):													#tuple id or record id?
		# tid=tuple _Rid
		tPage_id=tid[0]
		tSlotno=tid[1]  # start from 1
		for i in range(0,self.DEFAULT_PAGES):
			try:
				# tid == tuple Rid
				if self.pool_pages[i].page_id==tPage_id:
					#print(tPage_id,self.pool_pages[i].page_id)
					#search in this page
					t=self.pool_pages[i].page_tuples[tSlotno]
					if not t.tuple_colnum:
						print("tuple not found")
						return None
					self.pool_pages[i].page_tuples[tSlotno]=None  # delete the target tuple
					print("tuple delete successfully!")
					return t
			except:  # if self.pool_pages[i]==None
				continue
		print("tuple not found")
		return None
	'''discard the page file from the buffer according to page id'''
	def discard_page(self,pid):
		for i in range(0,self.DEFAULT_PAGES):
			try:
				if self.pool_pages[i].page_id==pid:
					# discard page from buffer by freeing the space of pages[i]
					self.pool_pages[i]=None
					self.pool_num-=1
					print("page discard successfully!")
					return 0
			except:  # if self.pool_pages[i]==None
				continue
		print("page not found")
		return 0
	'''get the bytes data of this buffer pool'''
	def get_data(self):
		self.pool_bytesdata=create_string_buffer(self.pool_size)
		tempnum=create_string_buffer(8)
		tempsize=create_string_buffer(8)
		self.pool_bytesdata+=tempnum.raw
		self.pool_bytesdata+=tempsize.raw
		for i in range(0,self.DEFAULT_PAGES):
			if self.pool_pages[i]:
				self.pool_bytesdata+=self.pool_pages[i].get_page_data().raw
			else:  # if self.pool_pages[i]==None
				emptypage=create_string_buffer(PAGE_SIZE)
				self.pool_bytesdata+=emptypage.raw
		return self.pool_bytesdata
	'''get the page file from the buffer according to page id(pid)'''
	def get_page(self,pid):
		for i in range(0,self.DEFAULT_PAGES):
			try:
				if self.pool_pages[i].page_id==pid:
					# get the page
					output = self.pool_pages[i]
					return output
			except:  # if self.pool_pages[i]==None
				continue
		print("page not found")
		return 0
	'''read a new page to buffer pool from heap file'''
