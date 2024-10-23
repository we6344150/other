import os
import ebooklib
from ebooklib import epub
 
def convert_mobi_to_txt(mobi_file_path, txt_file_path):
    # 确保输出目录存在
    os.makedirs(os.path.dirname(txt_file_path), exist_ok=True)
 
    # 使用 ebooklib 打开mobi文件
    book = epub.open_book(mobi_file_path)
 
    # 创建一个文本文件用于保存转换后的内容
    with open(txt_file_path, 'w', encoding='utf-8') as txt_file:
        for (section_index, section) in enumerate(book.sections):
            # 将每个部分的内容写入文本文件
            txt_file.write(f'Section {section_index + 1}\n')
            txt_file.write(section.content)
            txt_file.write('\n\n')
 
    # 关闭书籍
    book.close()
 
# 使用示例
convert_mobi_to_txt('C:\py\自控力 - 美 - 凯利·麦格尼格尔.mobi', 'C:\py\自控力 - 美 - 凯利·麦格尼格尔.txt')