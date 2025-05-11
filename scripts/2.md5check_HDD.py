import os
import sys
import hashlib
import pandas as pd
from multiprocessing import Pool, cpu_count
from pathlib import Path
import time
import argparse
from functools import partial

# 配置参数
DEFAULT_EXCEL_PATH = r"D:\NCBI_ascp\data_report\下载样本列表.xlsx"
DOWNLOAD_DIR = r"D:\NCBI_ascp\data"
CHUNK_SIZE = 8192 * 1024  # 增大块大小减少I/O操作 (8MB)
MAX_QUEUE_SIZE = 2  # 控制内存中的待处理文件数量

def calculate_md5(file_path):
    """计算文件的MD5值 (优化I/O的版本)"""
    md5 = hashlib.md5()
    with open(file_path, 'rb', buffering=CHUNK_SIZE) as f:
        while chunk := f.read(CHUNK_SIZE):
            md5.update(chunk)
    return md5.hexdigest()

def process_single_file(file_info, md5_map):
    """处理单个文件的MD5校验 (适配多进程的版本)"""
    file_name, expected_md5 = file_info
    file_path = os.path.join(DOWNLOAD_DIR, file_name)
    
    result = {
        'file_name': file_name,
        'expected_md5': expected_md5,
        'actual_md5': None,
        'is_valid': False,
        'error': None
    }
    
    try:
        if not os.path.exists(file_path):
            result['error'] = "File not found"
            return result
            
        actual_md5 = calculate_md5(file_path)
        result.update({
            'actual_md5': actual_md5,
            'is_valid': (actual_md5 == expected_md5)
        })
    except Exception as e:
        result['error'] = str(e)
    
    return result

def build_md5_map(excel_path):
    """从Excel构建文件名到MD5的映射字典 (优化版本)"""
    try:
        df = pd.read_excel(excel_path, engine='openpyxl')
        md5_map = {}
        
        for _, row in df.iterrows():
            if pd.notna(row.get('fastq_ftp')) and pd.notna(row.get('fastq_md5')):
                files = row['fastq_ftp'].split(';')
                md5s = row['fastq_md5'].split(';')
                
                for file, md5 in zip(files, md5s):
                    file_name = file.split('/')[-1]
                    md5_map[file_name] = md5.strip()
        
        return md5_map
    except Exception as e:
        print(f"Error reading Excel: {str(e)}")
        sys.exit(1)

def process_files_sequentially(md5_map):
    """顺序处理文件但使用多核计算的流水线架构"""
    file_queue = list(md5_map.items())
    results = []
    processed = 0
    total_files = len(file_queue)
    
    # 创建进程池
    with Pool(processes=cpu_count()) as pool:
        # 使用partial固定md5_map参数
        worker = partial(process_single_file, md5_map=md5_map)
        
        # 分批处理避免内存爆炸
        batch_size = MAX_QUEUE_SIZE * cpu_count()
        for i in range(0, total_files, batch_size):
            batch = file_queue[i:i + batch_size]
            
            # 处理当前批次
            for result in pool.imap_unordered(worker, batch):
                results.append(result)
                processed += 1
                
                # 进度显示
                progress = processed / total_files * 100
                sys.stdout.write(f"\rProcessing: {processed}/{total_files} ({progress:.1f}%)")
                sys.stdout.flush()
    
    return results

def main():
    parser = argparse.ArgumentParser(description='MD5校验工具 (优化版)')
    parser.add_argument('-e', '--excel', type=str, default=DEFAULT_EXCEL_PATH,
                      help=f'Excel文件路径 (默认: {DEFAULT_EXCEL_PATH})')
    args = parser.parse_args()
    
    print("启动MD5校验 (优化流水线架构)...")
    print(f"工作目录: {DOWNLOAD_DIR}")
    print(f"CPU核心数: {cpu_count()}")
    start_time = time.time()
    
    # 构建MD5映射表
    md5_map = build_md5_map(args.excel)
    print(f"找到 {len(md5_map)} 个需要校验的文件")
    
    # 处理文件
    results = process_files_sequentially(md5_map)
    
    # 分析结果
    valid_count = sum(1 for r in results if r['is_valid'])
    invalid_count = len(results) - valid_count
    
    print(f"\n\n校验完成! 耗时: {time.time()-start_time:.2f}秒")
    print(f"总计: {len(results)} 个文件 | 有效: {valid_count} | 无效: {invalid_count}")
    
    # 输出无效文件
    if invalid_count > 0:
        print("\n无效文件列表:")
        invalid_files = [r for r in results if not r['is_valid']]
        for file in invalid_files:
            print(f"\n文件名: {file['file_name']}")
            print(f"预期MD5: {file['expected_md5']}")
            print(f"实际MD5: {file['actual_md5']}")
            if file['error']:
                print(f"错误: {file['error']}")
    
    # 保存结果
    result_csv = os.path.join(DOWNLOAD_DIR, "md5_verification_results.csv")
    pd.DataFrame(results).to_csv(result_csv, index=False)
    print(f"\n完整结果已保存到: {result_csv}")

if __name__ == '__main__':
    main()
