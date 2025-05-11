import os
import requests
from pathlib import Path
import pandas as pd

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

def download_file(url, dest_path):
    """
    下载文件，支持断点续传
    :param url: 文件下载链接
    :param dest_path: 文件保存路径
    """
    # 检查文件是否已部分下载
    file_size = 0
    if os.path.exists(dest_path):
        file_size = os.path.getsize(dest_path)

    # 设置请求头，支持断点续传
    headers = {"Range": f"bytes={file_size}-"} if file_size else {}

    try:
        # 发起请求
        response = requests.get(url, headers=headers, stream=True, timeout=30)
        response.raise_for_status()

        # 获取文件总大小
        total_size = int(response.headers.get("content-length", 0)) + file_size

        # 以追加模式写入文件
        with open(dest_path, "ab") as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # 过滤掉空的chunk
                    file.write(chunk)
                    file_size += len(chunk)
                    print(f"下载进度: {file_size}/{total_size} bytes", end="\r")

        print(f"\n文件下载完成: {dest_path}")
        return True

    except requests.exceptions.RequestException as e:
        print(f"下载失败 {url}: {e}")
        return False

def download_ftp(link):
    """
    处理FTP链接并下载文件
    :param link: FTP链接
    """
    # 提取文件名
    file_name = link.split("/")[-1]
    dest_path = os.path.join(download_dir, file_name)

    # 构建完整的HTTP URL（假设FTP服务器支持HTTP访问）
    if not link.startswith("ftp://"):
        link = "ftp://" + link
    http_link = link.replace("ftp://", "https://")

    print(f"正在下载: {file_name}")
    return download_file(http_link, dest_path)

# 处理所有链接
for link in ftp_links:
    if ";" in link:
        for sublink in link.split(";"):
            sublink = sublink.strip()
            if sublink:  # 确保不是空字符串
                download_ftp(sublink)
    else:
        download_ftp(link)

print("所有文件下载完成！")
