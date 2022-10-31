# zootest

Программа собирает данные о товарах и сохраняет их в csv файл. Настраивается через config.json.
 Программа тестировалась на **Python3.9**

 Зависимости: bs4 requests

 Выполните команды ниже перед запуском
 ```
python3.9 -m venv venv
source venv/bin/activate
pip install -r requrements.txt
python3.9 parse.py
```

Файл cprofile_output.png - результат работы профайлера cProfile,
схема показывает узкие места в программе. 
В данном случае узкое место это синхронный requests.
