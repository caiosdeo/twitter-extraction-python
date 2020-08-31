import os
from multiprocessing import Pool

processes = ('set1.py', 'set2.py', 'set3.py', 'set4.py')

def run_process(process):                                                             
    os.system('python {}'.format(process))                                                                                                                      

def main():

    pool = Pool(processes=4)                                                        
    pool.map(run_process, processes) 

if __name__ == "__main__":
    try:
        main()    
    except Exception as ex:
        print("Exception on main occured: " + str(ex))