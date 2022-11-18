import requests

# test with request library
data = requests.get('https://google.com')
print(data.content)