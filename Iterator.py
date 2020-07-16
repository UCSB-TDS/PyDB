HEAPFILE_SIZE = 40960
PAGE_SIZE = 5120
SLOT_SIZE = 2048
MAX_HEADER_SIZE = 1024
MAX_SLOTS = (PAGE_SIZE-MAX_HEADER_SIZE)//SLOT_SIZE  # 4 TOTAL SLOTS, Each slot can hold one tuple
MAX_PAGES = (HEAPFILE_SIZE-MAX_HEADER_SIZE)//PAGE_SIZE

class iterator:
    def __init__(self, input_file):
        self.data=input_file.get_file_dict()
        self.current=0

    def __iter__(self):
        # get file id
        print('id of this file:',self.data['header']['file_id'])
        # get schema
        print('schema of this heap file:',self.data['header']['schema'])
        return self

    def __next__(self):
        # get tuples in heap file
        self.current += 1
        if self.current<MAX_PAGES+1:
            temp='page'+str(self.current)
            temp_page=self.data[temp]
            if temp_page==None:
                print('empty page')
            else:
                for j in range(1,1+MAX_SLOTS):
                    temp='tuple'+str(j)
                    temp_tuple=temp_page[temp]
                    if temp_tuple['size']==0:
                        s='empty slot'
                        print(s)
                    else:
                        print(temp_tuple)
            output='page'+str(self.current)
            return output
        raise StopIteration
