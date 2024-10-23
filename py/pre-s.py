import ctypes
import time

# 定义虚拟键码
VK_SPACE = 0x20

# 定义输入结构体
class KEYBDINPUT(ctypes.Structure):
    _fields_ = [
        ("wVk", ctypes.c_ushort),
        ("wScan", ctypes.c_ushort),
        ("dwFlags", ctypes.c_ulong),
        ("time", ctypes.c_ulong),
        ("dwExtraInfo", ctypes.c_ulong)
    ]

class INPUT(ctypes.Structure):
    class _INPUT(ctypes.Union):
        _fields_ = [("ki", KEYBDINPUT)]
    _anonymous_ = ("_input",)
    _fields_ = [("type", ctypes.c_ulong), ("_input", _INPUT)]

# 加载用户32位动态链接库
user32 = ctypes.windll.user32

# 发送键盘事件
def press_key(vk_code):
    # 按下键
    user32.keybd_event(vk_code, 0, 0, 0)
    # 释放键
    user32.keybd_event(vk_code, 0, 2, 0)

def press_space():
    press_key(VK_SPACE)

while True:
    press_space()
    time.sleep(10)
