# linuxtools
some minitools create in linux os that are program with python
------------

## 1. wordcounter: 多进程分段读取大文件，并统计词频
默认为10M以下文件，直接单进程读取；10M以上，分128个进程来读取（在本机上试验发现128个进程数的速度是相对较快的，test里面的wordcounter_multiprocesses可以对比不同进程数的读取速度）

采用chardet自动判断文件编码

How to use:

`pip install chardet`

`python wordcounter file1 file2` 其中file1为要分析的文件名，file2是分析结果要写入的文件名。
