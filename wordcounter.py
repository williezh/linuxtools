#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function, division, unicode_literals
import sys, re, time, os
from collections import Counter
from functools import reduce
from multiprocessing import Pool, cpu_count

WORKERS = min(cpu_count() * 8, 20)  #要启动的进程数,cpu数量的8倍，不超过20
CODING = 'gbk'

def filesize(fn):
    """获取要读取文件的大小"""
    with open(fn) as f:
        f.seek(0,os.SEEK_END)
        size = f.tell()
    return size

def humansize(size):
    """将文件的大小转成带单位的形式"""
    units = ['B', 'KB', 'M', 'G', 'T']    
    for unit in units:
        if size < 1024:
            break
        size = size // 1024
    return '{} {}'.format(size, unit)
        
def word_count(fn, p1, p2, f_size):  
    """分段读取大文件并统计词频    
    Args:
        fn:所读文件名称
        p1:文件段的起始位置
        p2:文件段的结尾位置
        f_size:所读文件的大小，单位为B    
    ret:
        该进程所负责的文件段的词频统计结果
        type == collections.Counter
    """
    c = Counter()
    with open(fn, 'rb') as f:    
        if p1:  # 为防止字被截断的，分段处所在行不处理，从下一行开始正式处理
            f.seek(p1-1)
            while b'\n' not in f.read(1):
                pass
        
        while 1:    
            pos = f.tell()          
            line = f.readline().decode(CODING)
            c.update(Counter(re.sub(r'\s+','',line)))      
            if p1 == 0: #显示进度
                percent = min(pos * 10000 // p2, 10000)
                done = '=' * (percent//1000)
                half = '-' if (percent) % 1000 > 5 else ''
                tobe = ' ' * (10 - (percent)//1000 - len(half))
                tip = '{}Parsing {}, '.format('\33[?25l', fn)  #隐藏光标              
                print('\r{}{:.2f}% [{}{}{}] {:,}/{:,}'.format(tip, percent/100, 
                    done, half, tobe, 
                    min(pos*(WORKERS+1), f_size), f_size), 
                    end='')
            if pos >= p2:               
                return c  
   
def write_result(counter, to_file):   
    '''将Counter类型的统计结果，按设定编码，写入文件to_file中'''       
    ss = ['{}: {}'.format(i, j) for i, j in counter.most_common()]
    s = ('\n'.join(ss)).encode(CODING)
    with open(to_file, 'wb') as f:
        f.write(s)
#    ''' 打印统计结果的前几项
    with open(to_file, 'rb') as f:
        s = f.read()
    print()            
    print(s.decode(CODING)[:50], '\n', '...')
#    '''        

def main():
    if len(sys.argv) != 3:
        print('Usage: python word count text!')
        exit(1)
    start = time.time()
    from_file, to_file = sys.argv[1:3]
    f_size = filesize(from_file)
    pool = Pool(WORKERS)
    res_list = []
    for i in range(WORKERS):
        p1, p2 = f_size // WORKERS * i,  f_size // WORKERS * (i+1)
        res = pool.apply_async(func=word_count, args=[from_file,p1,p2,f_size])
        res_list.append(res)
    pool.close()
    pool.join()
    c = reduce(lambda x, y: x+y, [res.get() for res in res_list])
    write_result(c, to_file)
    cost = time.time()-start
    size = humansize(f_size)
    print('\33[?25h')    #显示光标    
    print('File size: {}. Cost time: {:.1f} seconds'.format(size, cost))
    
if __name__ == '__main__':
    main()
        
