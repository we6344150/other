import os
os.add_dll_directory(r"C:\\Program Files\\GTK3-Runtime Win64\\bin")
import weasyprint

html = weasyprint.HTML('C:/Users/sesa748256/Downloads/template 1.html')
pdf = html.write_pdf()
# 或者直接保存到文件
html.write_pdf('C:/TEMP/output.pdf')
