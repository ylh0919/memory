import os
from multiprocessing import Pool
import time

#restart sufeng's hedge strategy
#strategy_path = scripts_dir + strategyFile

scripts_dir = "/shared/"
strategyFileList = ["hedge.py"]

def start_strategy(index):
    file = strategyFileList[index]
    cmd = "/usr/bin/wingchun strategy -n " + file[0:len(file)-3] + " -p " + file
    print(cmd)
    os.chdir(scripts_dir)
    os.system(cmd)

if __name__=='__main__':
    p = Pool(len(strategyFileList))
    for i in range(len(strategyFileList)):
        p.apply_async(start_strategy, args=(i,))
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')
