from flask import Flask, request, json

app = Flask(__name__)

@app.route('/get_page_source', methods=["POST"])
def get_page_source():
    from selenium import webdriver

    data = json.loads(request.data)
    url = data["dou_url"]
    driver = webdriver.Chrome()
    driver.get(url)
    result = driver.page_source
    driver.quit()

    return result

if __name__ == '__main__':
    app.run(host= '0.0.0.0')