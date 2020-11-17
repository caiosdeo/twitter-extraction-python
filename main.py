import os
from multiprocessing import Pool
import threading

processes = ('set1.py', 'set2.py', 'set3.py', 'set4.py')

def run_process(process):                                                             
    os.system('python {}'.format(process))                                                                                                                      

class servico(threading.Thread):
    def __init__(self, threadID, name, key):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.key = key
    def run(self):
        os.system('python set{}.py'.format(self.key))

def main():

    servico1 = servico(1, 'set1', 1)
    servico2 = servico(2, 'set2', 2)
    servico3 = servico(3, 'set3', 3)
    servico4 = servico(4, 'set4', 4)

    servico1.start()
    servico2.start()
    servico3.start()
    servico4.start()

    # pool = Pool(processes=4)                                                        
    # pool.map(run_process, processes) 

if __name__ == "__main__":
    try:
        main()    
    except Exception as ex:
        print("Exception on main occured: " + str(ex))