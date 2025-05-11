# NCBI SRA大数据下载
# 准备工作
参考：https://www.jianshu.com/p/b195e8f8f49d
https://www.ncbi.nlm.nih.gov/sra?term=SRX17918111
https://www.ncbi.nlm.nih.gov/sra?term=SRX17918113
直接去数据库网站https://www.ebi.ac.uk/ena/browser/home搜SRX17918111和SRX17918113
在下方页面[Read Files]找到[show column files]，把[fastq_aspera]和[fastq_ftp]以及[fastq_md5]之类的都记得勾上，下载到本地以后excel打开挑选信息重命名成[下载样本列表.xlsx]文件
# 快速开始
挑好需要下载的文件以后按照顺序在终端 python 1.download_FTP_curl.py，linux使用download_FTP_linux.py
之后按照顺序运行脚本即可
## FTP
windows直接用download_FTP.py或者 download_FTP_curl.py实现自动化断点传输下载
linux使用download_FTP_linux.py实现自动化断点传输下载
## aspera
参考https://www.jianshu.com/p/7eb4776429b9
速度快但是需要私钥公钥秘钥等，建议自己下载以后本地部署Aspera Connect 4.1.3以前的版本到本地以后使用download_ascp.py实现自动化高速下载

# MD5 checksum
## 运行md5check.py就好，参数自己改吧，自动多线程并行
HDD使用_HDD版本的md5check
如果出现未通过的文件使用2.1.md5check_loop_fix.py循环下载检测并行修复
## 样本文件组装
3.data_organize.py
运行以后会被根据样本重命名文件夹并将同一个样本来源的数据放入，方便后续cellranger之类的，参考4.cellranger的脚本，这个项目的功能到此为止，就是做数据下载的