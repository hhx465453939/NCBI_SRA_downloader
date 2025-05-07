import os
import subprocess
import pandas as pd
from pathlib import Path

# 输入文件路径（Excel）
excel_path = r"D:\NCBI_ascp\data_report\下载样本列表.xlsx"

# 输出目录（确保存在并有写入权限）
download_dir = r"D:\NCBI_ascp\data"

# 确保目录存在并有写入权限
Path(download_dir).mkdir(parents=True, exist_ok=True)
if not os.access(download_dir, os.W_OK):
    print(f"错误：没有写入权限 {download_dir}")
    exit(1)

# 读取Excel文件
df = pd.read_excel(excel_path)
ftp_links = df["fastq_ftp"].dropna().tolist()

def download_curl(link):
    """
    使用curl下载文件，支持断点续传
    :param link: FTP链接
    """
    # 提取文件名
    file_name = link.split("/")[-1]
    dest_path = os.path.join(download_dir, file_name)

    # 构建完整的FTP URL
    if not link.startswith("ftp://"):
        link = "ftp://" + link

    # 使用curl下载（支持断点续传）
    cmd = f'curl -L -C - -o "{dest_path}" "{link}"'
    print(f"正在下载: {file_name}")

    try:
        subprocess.run(cmd, shell=True, check=True)
        print(f"文件下载完成: {dest_path}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"下载失败 {link}: {e}")
        return False

# 处理所有链接
for link in ftp_links:
    if ";" in link:
        for sublink in link.split(";"):
            sublink = sublink.strip()
            if sublink:  # 确保不是空字符串
                download_curl(sublink)
    else:
        download_curl(link)

print("所有文件下载完成！")
