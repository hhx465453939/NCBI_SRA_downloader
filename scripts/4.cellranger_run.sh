wsl
su 你的用户名
cellranger --version
# cellranger cellranger-7.2.0
#!/bin/bash
cd /home/你的用户名/cellranger_org
mkdir C57
cd C57

wget https://ftp.ensembl.org/pub/release-114/fasta/mus_musculus_c57bl6nj/dna/Mus_musculus_c57bl6nj.C57BL_6NJ_v3.dna.toplevel.fa.gz
wget https://ftp.ensembl.org/pub/release-114/gtf/mus_musculus_c57bl6nj/Mus_musculus_c57bl6nj.C57BL_6NJ_v3.114.gtf.gz

gunzip Mus_musculus_c57bl6nj.C57BL_6NJ_v3.114.gtf.gz
gunzip Mus_musculus_c57bl6nj.C57BL_6NJ_v3.dna.toplevel.fa.gz
cellranger mkref --genome=C57 --fasta=Mus_musculus_c57bl6nj.C57BL_6NJ_v3.dna.toplevel.fa --genes=Mus_musculus_c57bl6nj.C57BL_6NJ_v3.114.gtf --nthreads=8 --memgb=32 --localmem=64

# 配置参数
INPUT_DIR="/mnt/d/NCBI_ascp/organized_data"  # 输入目录（WSL路径）
OUTPUT_DIR="/mnt/d/NCBI_ascp/cellranger_results"  # 输出目录
REFERENCE="/home/你的用户名/cellranger_org/C57"  # 参考基因组路径
THREADS=4  # 每个样本使用的CPU核心数

# 创建输出目录
cd /mnt/d/NCBI_ascp/data
mkdir -p "$OUTPUT_DIR"

# 获取所有样本目录
for SAMPLE_DIR in "$INPUT_DIR"/SRR*; do
    if [ -d "$SAMPLE_DIR" ]; then
        # 提取样本ID（SRRxxxxx）
        SAMPLE_ID=$(basename "$SAMPLE_DIR")
        # 设置输出子目录名称（C57-1_analysis格式）
        OUTPUT_SUBDIR="C57-${SAMPLE_ID#SRR}_analysis"
        
        echo "Processing sample: $SAMPLE_ID"
        echo "----------------------------------------"
        
        # 运行cellranger count
        cellranger count \
            --id="$OUTPUT_SUBDIR" \
            --transcriptome="$REFERENCE" \
            --fastqs="$SAMPLE_DIR" \
            --sample="$SAMPLE_ID" \
            --localcores=$THREADS
        
        # 移动结果到输出目录
        mv "$OUTPUT_SUBDIR" "$OUTPUT_DIR/"
        
        echo "Finished processing $SAMPLE_ID"
        echo "Results saved to: $OUTPUT_DIR/$OUTPUT_SUBDIR"
        echo "----------------------------------------"
    fi
done

echo "All samples processed!"
