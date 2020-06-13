# task 1: implement page file
# task 2: buffer pool
import unittest
from os import popen
try:
	import cPickle as pickle
except:
	import pickle
HEAPfile_SIZE = 40960
PAGE_SIZE = 4096															# 6size of binary arrays? or real data
SLOT_SIZE = 256  # 14 TOTAL SLOTS
NUMOFRECORDS_SIZE = 17  # record the num of total slots in page
MAX_RECORDS = (PAGE_SIZE-NUMOFRECORDS_SIZE)/SLOT_SIZE

# get size of file
def getSize(fileobject):
	fileobject.seek(0,2)  # move the cursor to the end of the file
	size = fileobject.tell()
	return size

class Fields:
	def __init__(self):
		self.field_name=[]
		self.field_count=0
		self.type=''  # INT_TYPE or STRING_TYPE

	def set_fields(self):
		self.type=input("field type, INT_TYPE or STRING_TYPE:",)
		if self.type!='INT_TYPE' and self.type!='STRING_TYPE':
			print("only INT_TYPE or STRING_TYPE is admitted")
			self.type=''
			return 0
		temp=''
		i=1
		while 1:
			temp = input("fieldname"+str(i)+"(input nothing to continue):",)
			i+=1
			if temp == '':
				break
			self.field_name.append(temp)

	def get_ftype(self):
		return self.type

class Tuple:
	# structure of tuple_data:[col1,col2,...,colN]
	def __init__(self):
		self.tuple_data=[]
		self.tuple_colnum=0
		self.tuple_fields=''
		self.tuple_tid=0
		self.tuple_Rid=[0,0]  # (PageID,slotNum)
		# self.tuple_data.append(self.tuple_Rid)
		# self.tuple_data.append(self.tuple_fields)

	def fulfill_info(self):
		# input new tuple value
		for i in range(0,self.tuple_colnum):
			tempval=input("Fieldname--"+self.tuple_fields.field_name[i]+":",)
			self.tuple_data.append(tempval)

	def set_field(self,Fields):
		self.tuple_colnum=len(Fields.field_name)
		self.tuple_fields=Fields
		# count++
		Fields.field_count+=1
		self.tuple_tid=Fields.field_count

	def get_Field(self):
		return self.tuple_fields

	def toString(self):
		outString=''
		for i in range(0,self.tuple_colnum):
			outString+=str(self.tuple_data[i])+'\t'
		outString+='\n'
		return outString

class page_file:													# 1heap files contains many pages or only page?
	# structure of page_file: [slotnum,[tuple1],[tuple2],..,[tupleN]]
	def __init__(self):
		self.page_id=''
		# self.page_type='int'# how to decide?
		self.page_data=[]  # normal data
		self.page_bytesarrays=[]  # bytes arrays
		self.page_size=PAGE_SIZE
		self.page_slotnum=0
		self.page_num=0  # num in heap file

	def get_id(self,fileNAME):
		""" get the id of this page
		"""
		tempid=popen(fr"fsutil file queryfileid " +fileNAME).read() #get id of page file
		startp=tempid.rfind("0x")
		self.page_id=tempid[startp:-1] # -hash(tempid[startp:-1])  # hash the file's id
		return self.page_id  #string of page id

	def get_page_data(self,fileNAME):
		""" return the byte array data contained in this page
		"""
		f = open(fileNAME, 'r+b')
		self.page_bytesarrays=f.read()  # get the byte arrays
		f.seek(0)
		try:
			temp=pickle.load(f)  # convert the byte arrays to data
			self.page_data=temp
			self.page_slotnum=temp[0]
		except:
			print("empty file")
		f.close()
		return self.page_bytesarrays  # return the byte arrays

	def insert_tuple(self,Tuple):
		""" adds the specified tuple to this page
		"""
		if self.page_slotnum+1>MAX_RECORDS:
			print("no space, need to create an overflow page")
			# create overflow page
			return 0
		else:
			self.page_slotnum+=1
			# change the tuple_Rid to (PageId, slotnum)
			Tuple.tuple_Rid[0]=self.page_id
			Tuple.tuple_Rid[1]=self.page_slotnum
			#Tuple.tuple_data[0][0]=self.page_id
			#Tuple.tuple_data[0][1]=self.page_slotnum
			self.page_data.append(Tuple)							#2:how to convert to binary?
		return 0

	def insert_tupletofile(self,Tuple,fileNAME):
		# write into the file
		f = open(fileNAME, 'r+b')
		try:
			temp=pickle.load(f)#convert the byte arrays to data
		except:
			print("empty file2")
			temp=[0]
		f.close()
		if temp[0]>MAX_RECORDS:
			print("no space, need to create an overflow page")
			return 0
			# need an overflow page
		else :
			f = open(fileNAME, 'w+b')
			temp[0]+=1
			f.seek(0)
			f.truncate()
			pickle.dump(temp, f)#change the content of slot count
			#f.close()
			#f = open(fileNAME, 'a+b')
			pickle.dump(Tuple, f) #convert the data to byte arrays
			f.close()
		return 0

	def print_for_bugs(self):
		print("pageid:",self.page_id)
		print("pagedata",self.page_data)
		print("pagebytesarrays:",self.page_bytesarrays)
		return 0

def Overflow_page(Tuple):
	newpf=page_file()
	Tuple.tuple_Rid[0]=newpf.page_id
	Tuple.tuple_Rid[1]=newpf.page_slotnum
	newpf.page_data.append(Tuple)
	return newpf

class buffer_pool:

	def __init__(self,size):
		#PAGE_SIZE = 4096
		self.MAX_PAGES = size//PAGE_SIZE
		self.pool_pages=[] #buffer pages, from 0-defaul_pages
		for i in range(0,self.MAX_PAGES):
			self.pool_pages.append(page_file())
		self.pool_num=0
	
	def delete_tuple(self,tid, t):													#tuple id or record id?
		#tid=tuple _Rid
		#tPage_id=tid[0]
		#tSlotnum=tid[1]#start from 1, as pages[0]stands for slot num
		for i in range(0,self.pool_num):
			#tid == tuple id
			for j in range(0,self.pool_pages[i].page_slotnum):
				print("DEBUG",tid,self.pool_pages[i].page_data[j].tuple_tid)
				if self.pool_pages[i].page_data[j].tuple_tid==tid:
					t=self.pool_pages[i].page_data[j]
					self.pool_pages[i].page_data[j]=[]
					s="tuple with ID:"+str(tid)+" delete successfully"
					return s
			#if tid stands for tuple Rid
			'''
			if self.pool_pages[i].page_id==tPage_id:
				#search in this page
				t=self.pool_pages[i].page_data[tSlotnum]
				self.pool_pages[i].page_data[tSlotnum]=[]#delete the target tuple
				return t
			'''
		#tid == tuple id
		return "tuple not found"

	def discard_page(self,pid):
		for i in range(0,self.pool_num):
			if self.pool_pages[i].page_id==pid:
				#discard page from buffer by freeing the space of pages[i]
				self.pool_pages[i]=page_file()
				return "page discard successfully!"
		return "page not found"

	def get_page(self,pid):											#4from the buffer?
		output=page_file()
		for i in range(0,self.pool_num):
			if self.pool_pages[i].page_id==pid:
				#get the page
				output = self.pool_pages[i]
				return output
		return "page not found"

	def read_page(self,pid,heap_file):										#5add new page from the heap file?
		pass
