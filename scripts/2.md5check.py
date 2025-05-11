import os
import sys
import hashlib
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
import time
import argparse

# 配置参数
DEFAULT_EXCEL_PATH = r"D:\NCBI_ascp\data_report\下载样本列表.xlsx"
DOWNLOAD_DIR = r"D:\NCBI_ascp\data"
MAX_WORKERS = os.cpu_count()  # 使用所有可用CPU核心
CHUNK_SIZE = 8192  # 读取文件的块大小

def calculate_md5(file_path):
    """计算文件的MD5值 (使用内存高效的方式)"""
    md5 = hashlib.md5()
    with open(file_path, 'rb') as f:
        while chunk := f.read(CHUNK_SIZE):
            md5.update(chunk)
    return md5.hexdigest()

def process_file(file_path, expected_md5):
    """处理单个文件的MD5校验"""
    file_name = os.path.basename(file_path)
    try:
        actual_md5 = calculate_md5(file_path)
        is_valid = (actual_md5 == expected_md5)
        return {
            'file_name': file_name,
            'expected_md5': expected_md5,
            'actual_md5': actual_md5,
            'is_valid': is_valid,
            'error': None
        }
    except Exception as e:
        return {
            'file_name': file_name,
            'expected_md5': expected_md5,
            'actual_md5': None,
            'is_valid': False,
            'error': str(e)
        }

def build_md5_map(excel_path):
    """从Excel构建文件名到MD5的映射字典"""
    try:
        df = pd.read_excel(excel_path)
    except FileNotFoundError:
        print(f"错误: 找不到Excel文件 {excel_path}")
        sys.exit(1)
    except Exception as e:
        print(f"读取Excel文件失败: {str(e)}")
        sys.exit(1)
        
    md5_map = {}
    
    for _, row in df.iterrows():
        if pd.notna(row['fastq_ftp']) and pd.notna(row['fastq_md5']):
            files = row['fastq_ftp'].split(';')
            md5s = row['fastq_md5'].split(';')
            
            for file, md5 in zip(files, md5s):
                file_name = file.split('/')[-1]
                md5_map[file_name] = md5.strip()
    
    return md5_map

def main():
    parser = argparse.ArgumentParser(description='MD5校验工具')
    parser.add_argument('-e', '--excel', type=str, default=DEFAULT_EXCEL_PATH,
                       help=f'Excel文件路径 (默认: {DEFAULT_EXCEL_PATH})')
    args = parser.parse_args()
    
    print(f"开始MD5校验 (使用 {MAX_WORKERS} 个并行进程)...")
    start_time = time.time()
    
    # 构建MD5映射表
    md5_map = build_md5_map(args.excel)
    
    # 准备任务列表
    tasks = []
    for file_name, expected_md5 in md5_map.items():
        file_path = os.path.join(DOWNLOAD_DIR, file_name)
        if os.path.exists(file_path):
            tasks.append((file_path, expected_md5))
        else:
            print(f"警告: 文件未找到 {file_name}")
    
    # 并行处理
    results = []
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_file, *task) for task in tasks]
        
        for future in as_completed(futures):
            results.append(future.result())
            print(".", end="", flush=True)  # 进度指示
    
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
    
    # 保存完整结果到CSV
    result_df = pd.DataFrame(results)
    result_csv = os.path.join(DOWNLOAD_DIR, "md5_verification_results.csv")
    result_df.to_csv(result_csv, index=False)
    print(f"\n完整结果已保存到: {result_csv}")

if __name__ == '__main__':
    main()
