from playwright.sync_api import Playwright, sync_playwright, expect
import db_cnn
from validate_from import FormValidator


def run(playwright: Playwright) -> None:
    #查看是否有指定授权牌，如果有进行删除
    res=db_cnn.execute_query("select * from T_AUTHORIZATION_CARD where id = 4923998041335341056")
    print(res)
    if res is not None:
        db_cnn.execute_delete("delete from T_AUTHORIZATION_CARD where id = 4923998041335341056")
    browser = playwright.chromium.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()
    page.goto("https://dsp.schneider-electric.cn/keycloak/auth/realms/dsp/protocol/openid-connect/auth?response_type=code&client_id=pc&redirect_uri=https%3A%2F%2Fdsp.schneider-electric.cn%2Fweb%2Fauth%2Fcallback%2F&state=5c565eeb80ad8c27&skip_keycloak=0&logined_rediect=%2Fweb%2F")
    page.get_by_role("link", name="密码登录").click()
    page.get_by_label("Username").fill("joycebai")
    page.get_by_label("Password").click()
    page.get_by_label("Password").fill("welcome")
    page.get_by_label("Password").press("Enter")
    page.get_by_text("CN01 SEC施耐德电气（中国）有限公司").click()
    page.goto("https://dsp.schneider-electric.cn/web/contract/authorize-brand-mgt")

    #实例化校验类
    validator = FormValidator(page)
    auth_add(page, validator)
    #判断是否年度模版已存在授权牌，如果存在删除该授权牌
    error=page.get_by_text("Error: 该年度该模板已存在授权牌")
    if error.text_content() == "Error: 该年度该模板已存在授权牌":
        # db_cnn.execute_delete("delete from T_AUTHORIZATION_CARD WHERE TEMPLATE_ID=(SELECT id FROM T_AUTHORIZATION_CARD_TEMPLATE WHERE TEMPLATE_NAME='默认模板11')")
        page.wait_for_timeout(3*1000)
        page.get_by_role("button", name="取消").click()
        page.get_by_label("授权牌名称 :").fill("授权牌00112")
        page.get_by_role("button", name="查询").click()
        page.locator("a").filter(has_text="维护客户范围").click()
        page.locator("label").filter(has_text="按客户分组").locator("span").nth(1).click()
        page.get_by_role("button", name="维护组范围").click()
        page.locator("label").filter(has_text="vue3测试新建分组").locator("span").nth(1).click()
        page.get_by_label("维护组范围").get_by_role("button").nth(2).click()
        page.get_by_role("button", name="确认").click()
        page.get_by_role("button", name="提交").click()   
    else:
        auth_add(page, validator)
     
    # ---------------------
    context.close()
    browser.close()

def auth_add(page, validator):
    page.get_by_role("button", name="新增").click()
    element=page.get_by_label("授权牌名称:")
    validator.validate_max_length(element,30)
    page.get_by_label("授权牌模板:").click()
    page.locator("li").filter(has_text="默认模板11").click()
    page.get_by_label("授权牌名称:").click()
    page.get_by_label("授权牌名称:").fill("授权牌00112")
    page.get_by_label("客户代码:").click()
    page.get_by_label("客户代码:").fill("136101,136100")
    page.get_by_role("button", name="添加").click()
    page.get_by_role("button", name="提交").click()


with sync_playwright() as playwright:
    run(playwright)
