import requests
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.responses import FileResponse,HTMLResponse,StreamingResponse
from fastapi.middleware.gzip import GZipMiddleware

app = FastAPI()

@app.get("/req/readAndUpload")
def readAndUpload():
    # 访问preupload接口拿url和key
    url = "http://dspdev.cn.schneider-electric.com/api/master-data/file/pre-upload"
    res=[]
    for i in range(27,31):
        filename='small_file_'+str(i)+'.csv'
        params = {'fileName': filename}  # 参数字典
        response = requests.get(url, params=params) 
        res.append(response.json().get('body'))
    #print(res[:4])
    print("res[0]",res[0])
    #print("body",res[0].get('body'))
    # print("url",res[0].body.url)
    # print("key",res[0].body.key)
    keys=[]
    # 读本地文件并上传
    for index, dictionary in enumerate(res):
        print("index",index)
        i=index+27
        print("i",i)
        filename='small_file_'+str(i)+'.csv'
        # 1. 打开本地文件并读取二进制内容
        with open(filename, 'rb') as file:
            file_content = file.read()

        # 2. 准备headers（例如Content-Type可能需要设置为application/octet-stream或其他合适的MIME类型）
        headers = {'Content-Type': 'application/octet-stream'}

        # 3. 发送PUT请求，数据是文件的二进制内容
        response = requests.put(
            dictionary.get('url'),
            headers=headers,
            data=file_content
        )
        print("key",dictionary.get('key'))
        keys.append(dictionary.get('key'))
        # 4. 检查响应状态码和内容
        if response.status_code == 200:
            print("成功上传")
        else:
            print(f"请求失败，状态码：{response.status_code}")
        response.close()
    print(keys)