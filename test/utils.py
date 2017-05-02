#coding=utf-8
from __future__ import print_function, division, unicode_literals
import os
import time

def humansize(size):
    """将文件的大小转成带单位的形式"""
    units = ['B', 'KB', 'M', 'G', 'T']    
    for unit in units:
        if size < 1024:
            break
        size = size // 1024
    return '{} {}'.format(size, unit)

def humantime(seconds):
    """将秒数转成00:00:00的形式"""
    h = m = ''
    seconds = int(seconds)
    if seconds >= 3600:
        h = '{:02}:'.format(seconds // 3600)
        seconds = seconds % 3600
    if 1 or seconds >= 60:
        m = '{:02}:'.format(seconds // 60)
        seconds = seconds % 60
    return '{}{}{:02}'.format(h, m, seconds)
        
def processbar(pos, p2, fn, f_size, start):
    '''打印进度条'''
    percent = min(pos * 10000 // p2, 10000)
    done = '=' * (percent//1000)
    half = '-' if percent // 100 % 10 > 5 else ''
    tobe = ' ' * (10 - percent//1000 - len(half))
    tip = '{}{}, '.format('\33[?25l', os.path.basename(fn))  #隐藏光标          
    past = time.time()-start
    remain = past/(percent+0.01)*(10000-percent)      
    print('\r{}{:.2f}% [{}{}{}] {:,}/{:,} [{}<{}]'.format(tip, 
            percent/100, done, half, tobe, 
            min(pos*int(f_size//p2+0.5), f_size), f_size, 
            humantime(past), humantime(remain)),
        end='')
    if percent == 10000:
        print('\33[?25h', end='')     # 显示光标  
