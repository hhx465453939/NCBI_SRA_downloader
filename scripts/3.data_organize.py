# D:\NCBI_ascp\data
## 数据合并
import os
import shutil
from pathlib import Path
import re

# 配置参数
DATA_DIR = r"D:\NCBI_ascp\data"
OUTPUT_DIR = r"D:\NCBI_ascp\organized_data"  # 整理后的输出目录

def organize_fastq_files():
    # 创建输出目录
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    
    # 获取所有SRR开头的fastq文件
    fastq_files = [f for f in os.listdir(DATA_DIR) 
                  if f.startswith('SRR') and f.endswith('.fastq.gz')]
    
    # 提取唯一的SRR编号
    srr_ids = set()
    for file in fastq_files:
        match = re.match(r'(SRR\d+)_[12]\.fastq\.gz', file)
        if match:
            srr_ids.add(match.group(1))
    
    # 为每个SRR创建文件夹并移动文件
    for srr_id in sorted(srr_ids):
        srr_dir = os.path.join(OUTPUT_DIR, srr_id)
        os.makedirs(srr_dir, exist_ok=True)
        
        # 查找配对的fastq文件
        r1 = f"{srr_id}_1.fastq.gz"
        r2 = f"{srr_id}_2.fastq.gz"
        
        # 移动文件
        for f in [r1, r2]:
            src = os.path.join(DATA_DIR, f)
            dst = os.path.join(srr_dir, f)
            
            if os.path.exists(src):
                shutil.move(src, dst)
                print(f"已移动: {f} -> {srr_dir}")
            else:
                print(f"警告: 文件不存在 {f}")
    
    print(f"\n整理完成! 所有文件已组织到: {OUTPUT_DIR}")

if __name__ == '__main__':
    organize_fastq_files()
