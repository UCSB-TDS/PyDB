from PyDBError import *
from heap_file import *
import random
import msgpack

HEAPFILE_SIZE = 40960
PAGE_SIZE = 5120
SLOT_SIZE = 2048
MAX_HEADER_SIZE = 1024
MAX_SLOTS = (PAGE_SIZE-MAX_HEADER_SIZE)//SLOT_SIZE  # 4 TOTAL SLOTS, Each slot can hold one tuple
MAX_PAGES = (HEAPFILE_SIZE-MAX_HEADER_SIZE)//PAGE_SIZE
# numPages = 4  # maximum number of pages in this buffer pool.

class buffer_pool:
	'''BufferPool manages the reading and writing of pages into memory from
	disk. Access methods call into it to retrieve pages, and it fetches
	pages from the appropriate location'''
	def __init__(self,numPages):
		self.num_pages=numPages
		self.page_array=[]
		self.lock=[]
		self.page_index=[]
		for i in range(numPages):
			self.page_array.append(None)
			self.lock.append(False)
			self.page_index.append(-1)

	def evict_pages(self):
		'''randomly choose the page that will be evicted
		Discards a page from the buffer pool.
		Flushes the page to disk to ensure dirty pages are updated on disk.'''
		random_NO=random.randint(0,self.num_pages-1)
		self.flush_page(self.page_index[random_NO])
		self.page_index[random_NO]=-1
		self.page_array[random_NO]=None
		return random_NO

	def flush_page(self,pid):
		'''Flushes a certain page to disk'''
		pindex=-1
		for i in range(self.num_pages):
			if self.page_index[i]==pid:
				pindex=i
		if pindex==-1:
			raise PyDBInternalError("page not found in buffer pool")

		if self.page_array[pindex][0][3]==False:  # dirty or not
			print('this page is not dirty, no need to flush')
			return 0
		else:
			# flush the page to heap file
			heap_file=HeapFile(self.page_array[pindex][0][2])  # really need to create a new file?
			schema_test1=Schema(input_data=[('colname1','CHAR(255)'),('colname2','INT32')],relation_name='test relation')
			heap_page=HeapPage(schema_test1)
			heap_page.deserialize(msgpack.packb(self.page_array[pindex]))
			heap_file.write_page(heap_page)  #

	def delete_tuple(self,tid,tupleID):
		'''Remove the specified tuple from the buffer pool'''
		pindex=-1
		for i in range(self.num_pages):
			if self.page_index[i]==tupleID[0]:
				pindex=i
				break
		if pindex==-1:
			raise PyDBInternalError("page not found in buffer pool")
		tuple_id=tupleID[1]
		if tuple_id>MAX_SLOTS:
			raise PyDBInternalError("tuple not exist, tuple ID too large")
		if self.page_array[pindex][1][tuple_id]==None:
			return 'already empty'
		else:
			self.page_array[pindex][1][tuple_id]=None
			self.page_array[pindex][0][1]-=1
			self.page_array[pindex][0][3]=True
			return None

	def insert_tuple(self,tid,table,Tuple):
		'''Add a tuple to the specified table behalf of transaction tid'''
		page=table.insert_tuple(Tuple)
		flag=-1
		for i in range(self.num_pages):
			if self.page_array[i]==None:
				self.page_array[i]=page
				self.page_index[i]=page[0][0]
				flag=1
				return 0
		if flag==-1:
			evict_id=self.evict_pages()
			self.page_array[evict_id]=page
			self.page_index[evict_id]=page[0][0]
			return 0

	def discard_page(self,pid):
		'''Remove the specific page from the buffer pool'''
		for i in range(self.num_pages):
			if self.page_index[i]==pid:
				self.flush_page(pid)
				self.page_array[i]=None
				self.page_index[i]=-1
				return 0
		print('page not found')
		return 0
