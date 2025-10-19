import os
import re
from PyPDF2 import PdfReader
from PyPDF2.errors import PdfReadError

def clean_filename(title):
    """
    清理标题，将非法字符、所有空格、以及用户指定的'-'替换为'_'。
    """
    if not title:
        return None
    
    # 将标题转为字符串
    title_str = str(title)
    
    # 定义所有需要被替换为'_'的字符：
    # 1. Windows/Linux/Mac的非法文件名字符： [\\/:*?"<>|]
    # 2. 所有的空白字符 (空格, tab, 换行等)： \s
    # 3. 用户额外指定的连字符： -
    #
    # 我们将这些字符合并到一个字符集[]中，并使用 '+' 来匹配一个或多个
    # 这样的连续字符，将它们统一替换为 *单个* '_'
    
    chars_to_replace_regex = r'[\\/:*?"<>|\s-]+'
    
    cleaned_title = re.sub(chars_to_replace_regex, '_', title_str)
    
    # 移除可能在开头或结尾产生的 '_'
    cleaned_title = cleaned_title.strip('_')
    
    # 避免文件名过长（大多数操作系统限制在255个字符）
    # 我们保守一点，限制在200个字符
    final_title = cleaned_title[:200]
    
    # 再次清理，防止截断时在末尾留下 '_'
    return final_title.strip('_')

def rename_pdfs_in_folder(root_folder):
    """
    递归遍历文件夹，重命名所有PDF文件。
    """
    print(f"开始扫描文件夹: {root_folder}\n")
    processed_count = 0
    renamed_count = 0
    failed_count = 0

    # os.walk 会递归遍历所有子文件夹
    for dirpath, dirnames, filenames in os.walk(root_folder):
        for filename in filenames:
            
            # 检查文件是否为PDF
            if filename.lower().endswith('.pdf'):
                processed_count += 1
                full_path = os.path.join(dirpath, filename)
                
                try:
                    # 打开PDF文件并读取元数据
                    with open(full_path, 'rb') as f:
                        reader = PdfReader(f)
                        meta = reader.metadata
                        
                        # 尝试获取 /Title 字段
                        paper_title = meta.get('/Title')
                    
                    cleaned_title = clean_filename(paper_title)
                    
                    if not cleaned_title:
                        print(f"[跳过] {filename} (未找到有效的Title元数据)")
                        continue

                    # --- 准备新文件名 ---
                    new_filename = cleaned_title + ".pdf"
                    new_full_path = os.path.join(dirpath, new_filename)
                    
                    # 检查新文件名是否与旧文件名相同
                    if full_path == new_full_path:
                        print(f"[跳过] {filename} (已是正确名称)")
                        continue
                        
                    # 检查新文件名是否已存在（避免覆盖）
                    counter = 1
                    base_new_name = cleaned_title
                    while os.path.exists(new_full_path):
                        print(f"[警告] {new_filename} 已存在。尝试添加后缀...")
                        new_filename = f"{base_new_name}_({counter}).pdf" # 后缀也用_
                        new_full_path = os.path.join(dirpath, new_filename)
                        counter += 1
                        # 安全退出，防止无限循环
                        if counter > 50:
                            print(f"[失败] 无法为 {filename} 找到一个不冲突的名称")
                            break
                    
                    if os.path.exists(new_full_path):
                         failed_count += 1
                         continue

                    # --- 执行重命名 ---
                    os.rename(full_path, new_full_path)
                    print(f"[成功] {filename} -> {new_filename}")
                    renamed_count += 1

                except PdfReadError:
                    print(f"[失败] {filename} (文件损坏或无法读取)")
                    failed_count += 1
                except Exception as e:
                    print(f"[失败] {filename} (发生未知错误: {e})")
                    failed_count += 1
                    
    print("\n--- 处理完毕 ---")
    print(f"总共扫描PDF: {processed_count}")
    print(f"成功重命名: {renamed_count}")
    print(f"失败/跳过: {processed_count - renamed_count}")


# --- 脚本主入口 ---
if __name__ == "__main__":
    # 1. 将这里的路径修改为你的PDF论文根文件夹
    # ！！！重要！！！
    # Windows 路径示例: r"C:\Users\YourName\Documents\Papers"
    # (注意前面的 r)
    #
    # macOS/Linux 路径示例: "/Users/YourName/Documents/Papers"
    
    folder_path = r"D:\xupt\paper"
    
    # 2. 检查路径是否设置

    if not os.path.isdir(folder_path):
        print(f"错误：路径 '{folder_path}' 不存在或不是一个文件夹。")
    else:
        # 3. 运行脚本
        rename_pdfs_in_folder(folder_path)