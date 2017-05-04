#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function, division, unicode_literals
from unittest import TestCase, main
from collections import Counter

from wordcounter_multiprocesses import WordCounter

class WordCounterMultiprocessesTest(TestCase):

    def test_result(self):
        f1 = 'tmp1.txt'
        f2 = 'tmp2.txt'
        words = ['你', '我', '它', '她', '他']
        amounts = [20000, 3000, 1, 50000, 6666]        
        c = Counter(dict(zip(words, amounts)))
        result = '\n'.join(['{}: {}'.format(i, j) for i, j in c.most_common()])
        
        s = '\n'.join(['{}\n'.format(i)*j for i,j in zip(words, amounts)])
        with open(f1, 'wb') as f:
            f.write(s.encode('utf-8'))   
        ws = [WordCounter(f1, f2, i)  for i in [0, 1, None]]          
        for w in ws:  
            w.run()    
            self.assertEqual(c, w.counter)
            self.assertEqual(result, w.result)

if __name__ == '__main__':
    main()
