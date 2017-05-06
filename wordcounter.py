#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function, division, unicode_literals
import sys, re, time, os
import operator, chardet
from collections import Counter
from functools import reduce
from multiprocessing import Pool, cpu_count
from datetime import datetime
from utils import humansize, humantime, processbar

def wrap(wcounter,  fn, p1, p2, f_size):
    return wcounter.count_multi(fn, p1, p2, f_size)
    
class WordCounter(object):
    def __init__(self, from_file, to_file=None, workers=None, coding=None,
                    max_direct_read_size=10000000):
        if not os.path.isfile(from_file):
            raise Exception('No such file: 文件不存在')
        self.f1 = from_file
        self.filesize = os.path.getsize(from_file)
        self.f2 = to_file
        if self.filesize < max_direct_read_size:
            self.workers = 0
        else:
            self.workers = cpu_count() * 64 if workers is None else workers
        if coding is None:
            with open(from_file, 'rb') as f:    
                coding = chardet.detect(f.read(1000))['encoding']            
        self.coding = coding
        self._c = Counter()
        
    def run(self):
        start = time.time()
        if self.workers == 0:
            self.count_direct(self.f1)
        elif self.workers == 1:
            self.count_single(self.f1, self.filesize)
        else:
            pool = Pool(self.workers)
            res_list = []
            for i in range(self.workers):
                p1 = self.filesize * i // self.workers 
                p2 = self.filesize * (i+1) // self.workers 
                args = [self, self.f1, p1, p2, self.filesize]
                res = pool.apply_async(func=wrap, args=args)
                res_list.append(res)
            pool.close()
            pool.join()
            self._c.update(reduce(operator.add, [r.get() for r in res_list]))            
        if self.f2:
            with open(self.f2, 'wb') as f:
                f.write(self.result.encode(self.coding))
        else:
            print(self.result)
        cost = '{:.1f}'.format(time.time()-start)
        size = humansize(self.filesize)
        tip = '\nFile size: {}. Workers: {}. Cost time: {} seconds'     
        print(tip.format(size, self.workers, cost))
        self.cost = cost + 's'
                
    def count_single(self, from_file, f_size):
        '''单进程读取文件并统计词频'''
        start = time.time()
        with open(from_file, 'rb') as f:
            for line in f:
                self._c.update(self.parse(line))
                processbar(f.tell(), f_size, from_file, f_size, start)   

    def count_direct(self, from_file):
        '''直接把文件内容全部读进内存并统计词频'''
        start = time.time()
        with open(from_file, 'rb') as f:
            line = f.read()
        self._c.update(self.parse(line))  
                
    def count_multi(self, fn, p1, p2, f_size):  
        c = Counter()
        with open(fn, 'rb') as f:    
            if p1:  # 为防止字被截断的，分段处所在行不处理，从下一行开始正式处理
                f.seek(p1-1)
                while b'\n' not in f.read(1):
                    pass
            start = time.time()
            while 1:                           
                line = f.readline()
                c.update(self.parse(line))   
                pos = f.tell()  
                if p1 == 0: #显示进度
                    processbar(pos, p2, fn, f_size, start)
                if pos >= p2:               
                    return c      
                    
    def parse(self, line):  #解析读取的文件流
        return Counter(re.sub(r'\s+','',line.decode(self.coding)))
        
    def flush(self):
        self._c = Counter()

    @property
    def counter(self):        
        return self._c
                    
    @property
    def result(self):
        ss = ['{}: {}'.format(i, j) for i, j in self._c.most_common()]
        return '\n'.join(ss)
        
def main():
    if len(sys.argv) != 3:
        print('Usage: python wordcounter.py from_file to_file')
        exit(1)
    from_file, to_file = sys.argv[1:3]
    w = WordCounter(from_file, to_file)
    w.run()
    
if __name__ == '__main__':
    main()
        
