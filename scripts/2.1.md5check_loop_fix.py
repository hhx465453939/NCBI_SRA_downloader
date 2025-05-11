import os
import pandas as pd
import hashlib
import subprocess
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# 配置参数
EXCEL_PATH = r"D:\NCBI_ascp\data_report\下载样本列表.xlsx"
DOWNLOAD_DIR = r"D:\NCBI_ascp\data"
MD5_RESULT_FILE = r"D:\NCBI_ascp\data\md5_verification_results.csv"
MAX_RETRIES = 3
MAX_WORKERS = 4
CHUNK_SIZE = 1024 * 1024  # 1MB

def load_failed_files(md5_file):
    """从MD5校验结果中加载失败的文件"""
    try:
        df = pd.read_csv(md5_file)
        return df[df['is_valid'] == False]['file_name'].tolist()
    except Exception as e:
        print(f"Error loading MD5 result file: {e}")
        return []

def get_download_info(excel_path, file_list):
    """从Excel中获取需要下载的文件信息"""
    try:
        df = pd.read_excel(excel_path)
        download_info = []
        
        for file_name in file_list:
            # 从文件名提取SRR编号
            srr_id = file_name.split('_')[0]
            # 查找Excel中对应的行
            row = df[df['run_accession'].str.startswith(srr_id)]
            if not row.empty:
                # 获取下载链接和MD5值
                urls = row['fastq_ftp'].values[0].split(';')
                md5s = row['fastq_md5'].values[0].split(';')
                
                # 确定是1还是2
                if '_1.fastq.gz' in file_name:
                    url = urls[0]
                    md5 = md5s[0]
                else:
                    url = urls[1]
                    md5 = md5s[1]
                
                download_info.append({
                    'file_name': file_name,
                    'url': f"ftp://{url}",  # 修改为ftp协议
                    'md5': md5
                })
        return download_info
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return []

def download_with_curl(url, dest_path, md5):
    """使用curl下载文件并校验MD5"""
    retry = 0
    while retry < MAX_RETRIES:
        try:
            print(f"Downloading {url} to {dest_path} (attempt {retry+1})")
            
            # 使用curl下载文件（支持断点续传）
            cmd = f'curl -L -C - -o "{dest_path}" "{url}"'
            subprocess.run(cmd, shell=True, check=True)
            
            # 校验MD5
            if verify_md5(dest_path, md5):
                print(f"Download and verification successful for {dest_path}")
                return True
            else:
                print(f"MD5 verification failed for {dest_path}")
                os.remove(dest_path)
                retry += 1
        except Exception as e:
            print(f"Error downloading {url}: {e}")
            retry += 1
            if os.path.exists(dest_path):
                os.remove(dest_path)
    return False

def verify_md5(file_path, expected_md5):
    """验证文件的MD5值"""
    if not os.path.exists(file_path):
        return False
    
    md5_hash = hashlib.md5()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(CHUNK_SIZE), b''):
            md5_hash.update(chunk)
    return md5_hash.hexdigest() == expected_md5

def process_download(download_info):
    """处理单个下载任务"""
    file_name = download_info['file_name']
    dest_path = os.path.join(DOWNLOAD_DIR, file_name)
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    
    success = download_with_curl(download_info['url'], dest_path, download_info['md5'])
    return {
        'file_name': file_name,
        'success': success,
        'url': download_info['url']
    }

def main():
    # 1. 加载校验失败的文件
    failed_files = load_failed_files(MD5_RESULT_FILE)
    if not failed_files:
        print("No failed files found. All files are valid.")
        return
    
    print(f"Found {len(failed_files)} files to redownload:")
    for f in failed_files:
        print(f" - {f}")
    
    # 2. 获取下载信息
    download_info = get_download_info(EXCEL_PATH, failed_files)
    if not download_info:
        print("No download information found for failed files.")
        return
    
    # 3. 并发下载
    print(f"Starting download with {MAX_WORKERS} workers...")
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_download, info) for info in download_info]
        
        for future in as_completed(futures):
            result = future.result()
            if result['success']:
                print(f"Successfully downloaded {result['file_name']}")
            else:
                print(f"Failed to download {result['file_name']}")
    
    print(f"Download completed in {time.time() - start_time:.2f} seconds")
    
    # 4. 重新校验所有文件
    print("\nVerifying downloaded files...")
    verification_results = []
    for info in download_info:
        file_path = os.path.join(DOWNLOAD_DIR, info['file_name'])
        is_valid = verify_md5(file_path, info['md5'])
        verification_results.append({
            'file_name': info['file_name'],
            'expected_md5': info['md5'],
            'actual_md5': hashlib.md5(open(file_path, 'rb').read()).hexdigest() if os.path.exists(file_path) else None,
            'is_valid': is_valid
        })
    
    # 5. 保存校验结果
    result_df = pd.DataFrame(verification_results)
    result_df.to_csv(MD5_RESULT_FILE, index=False)
    print(f"Verification results saved to {MD5_RESULT_FILE}")
    
    # 6. 显示最终结果
    success_count = result_df['is_valid'].sum()
    print(f"\nFinal result: {success_count} files successfully downloaded and verified, {len(result_df) - success_count} files failed")

if __name__ == "__main__":
    main()
