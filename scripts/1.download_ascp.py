import pandas as pd
import os
import subprocess

# 输入文件路径（Excel）
excel_path = "/mnt/d/NCBI_ascp/data_report/下载样本列表.xlsx"
# 输出目录（确保存在）
download_dir = "/mnt/d/NCBI_ascp/data"
os.makedirs(download_dir, exist_ok=True)

# 读取 Excel 文件
df = pd.read_excel(excel_path)
aspera_links = df["fastq_aspera"].dropna().tolist()

# Aspera 参数
ascp_cmd = "ascp"
aspera_key = "~/.aspera/connect/etc/asperaweb_id_dsa.openssh"
aspera_user = "anonftp"  # EBI 公共数据使用 anonftp
aspera_options = "-QT -l 1000m -k 1"  # 断点续传 (-k 1) + 限速 1000Mbps

# 下载函数（支持断点续传）
def download_aspera(link):
    file_name = link.split("/")[-1]
    dest_path = os.path.join(download_dir, file_name)
    cmd = f"{ascp_cmd} {aspera_options} -i {aspera_key} {aspera_user}@{link} {dest_path}"
    print(f"Downloading: {file_name}")
    try:
        subprocess.run(cmd, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error downloading {link}: {e}")

# 遍历所有链接并下载
for link in aspera_links:
    if ";" in link:  # 处理多个文件（如 _1.fastq.gz 和 _2.fastq.gz）
        for sublink in link.split(";"):
            download_aspera(sublink.strip())
    else:
        download_aspera(link)

print("所有文件下载完成！")
