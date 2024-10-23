from playwright.sync_api import Playwright, sync_playwright, Expect,Locator
import playwright as p
import re
from decimal import Decimal, getcontext

# 设置全局精度
getcontext().prec = 9  # 设置精度为9位


class FormValidator:
    def __init__(self, page: p.sync_api.Page):
        self.page = page

    def get_element_by_tag(self, tag, type):
            """
            目前只支持 CSS选择器 和 标签名称获取元素
            tag: 标签CSS选择器或者标签名称
            type: 标签类型
            """
            try:
                if type == 'CSS':
                    element =self.page.locator(tag)
                else:
                    element =self.page.get_by_placeholder(tag)
            except:
                print(f"不支持的标签类型：{tag}")
            return element
    def validate_max_length(self,element: Locator,expected_length: int):
        """
        最大长度校验
        expected_length: 预期长度
        """
        max_length = element.get_attribute('maxlength')
        if max_length is None:
            print(f"标签'{element.evaluate('(element) => element.tagName')}'没有设置最大长度属性")
        else:
            print(f"当前标签'{element.evaluate('(element) => element.tagName')}'的最大长度是： {max_length} ")
        assert max_length == str(expected_length), f"预期长度是{expected_length}, 但是实际设置的最大长度是 {max_length}"

    def validate_number(self,element: Locator):
        """
        数字类型校验
        """
        try:
            float(element.input_value())
        except ValueError:
            raise ValueError("输入的不是有效的数字")
        print("数字校验通过")
    def validate_email(self,element: Locator):
        """
        邮箱类型校验
        """
        if not re.match(r"[^@]+@[^@]+\.[^@]+", element.input_value()):
            raise ValueError("输入的不是有效的邮箱地址")
        print("邮箱校验通过")

    def validate_phone_number(self,element: Locator):
        """
        手机号类型校验
        """
        if not re.match(r"^\d{11}$", element.input_value()):  # 假设手机号为11位数字
            raise ValueError("输入的不是有效的手机号码")
        print("手机号校验通过")
    def validate_digit(self,element: Locator,integer_digits: int,decimal_digits: int):
        """
        整数小数位数校验
        integer_digits: 整数位数
        decimal_digits: 小数位数
        """
          # 尝试将字符串转换为浮点数
        try:
            value_float = float(element.input_value())
        except ValueError:
            print("输入的值不是一个有效的数字。")
            return
        
        input_val=Decimal(value_float)
       # 获取整数部分和小数部分
        integer_part = int(input_val)
        decimal_part = str(value_float).split('.')[1].rstrip('0')
        print(f"整数部分的字符串: {str(integer_part)}")
        print(f"小数部分的字符串: {str(decimal_part)}")
        # 计算整数位的个数
        integer_len = len(str(integer_part)) if integer_part != 0 else 1  # 处理0的情况
        
        # 计算小数位的个数
        # 将小数部分转换为字符串，并去掉前导的0和小数点
        decimal_len = len(decimal_part)
        
        print(f"整数位的个数: {integer_len}")
        print(f"小数位的个数: {decimal_len}")
        assert integer_len == integer_digits, f"预期整数位的个数{integer_digits}, 但是实际整数位的个数是 {integer_len}"
        assert decimal_len == decimal_digits, f"预期小数位的个数是{decimal_digits}, 但是实际小数位的个数是 {decimal_len}"

        print("日期校验通过")
    def validate_empty(self,element: Locator):
        """
        必填校验
        """
        if not element.input_value().strip():  
            raise ValueError("输入框是空的或仅包含空白字符")
        print("必填校验通过")

# if __name__ == "__main__":
#     with sync_playwright() as playwright:
#         browser = playwright.chromium.launch(headless=False)
#         page = browser.new_page()
#         page.goto("https://dsp.schneider-electric.cn")
#         page.get_by_role("link", name="密码登录").click()
#         page.get_by_label("Username").fill("joycebai")
#         page.get_by_label("Password").click()
#         page.get_by_label("Password").fill("welcome")
#         page.get_by_label("Password").press("Enter")
        
#         page.get_by_text("CN01 SEC施耐德电气（中国）有限公司").click()
#         page.goto("https://dsp.schneider-electric.cn/web/contract/list")
#         page.get_by_role("button", name="签署文件上传").click()
#         #实例化校验类
#         validator = FormValidator(page)
#         #校验方法测试
#         element= validator.get_element_by_tag('请输入','text')
#         #validator.validate_max_length(element, 50)
#         element.fill('1222@126.com')
#         # validator.validate_email(element)
#         # element.fill('12345678901.99999')
#         # validator.validate_digit(element, 11, 5)
#         validator.validate_empty(element)
#         browser.close()