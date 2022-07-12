import requests
import re
from bs4 import BeautifulSoup

import pandas as pd

base_url = 'https://ru.wikipedia.org'
url = 'https://ru.wikipedia.org/wiki/%D0%93%D0%BE%D1%80%D0%BE%D0%B4%D1%81%D0%BA%D0%B8%D0%B5_%D0%BD%D0%B0%D1%81%D0%B5%D0%BB%D1%91%D0%BD%D0%BD%D1%8B%D0%B5_%D0%BF%D1%83%D0%BD%D0%BA%D1%82%D1%8B_%D0%9C%D0%BE%D1%81%D0%BA%D0%BE%D0%B2%D1%81%D0%BA%D0%BE%D0%B9_%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D0%B8'


def population_to_int(population):  #
    m = re.search('^(↘|↗|→)(.*)(\[)', population)  # очищаем от лишних символов число населения
    str_population = m.group(2)  #
    population = int(''.join(filter(str.isdigit, str_population)))  # избавляемся от пробелов между групп цифр

    return population


def parse_towns(url):
    resp = requests.get(url)
    soup = BeautifulSoup(resp.content, 'html.parser')
    tables = soup.find_all('table', class_=['standard', 'sortable', 'jquery-tablesorter'])  # Собираем обе таблицы: города + ПГТ
    df = pd.DataFrame()
    for table in tables:
        for row in table.find_all('tr'):
            tds = row.find_all('td')
            if tds:
                name = tds[1].text  # название города
                population = population_to_int(tds[4].text)  # население
                link = base_url + tds[1].find('a')['href']  # ссылка на страницы города
                df_i = pd.DataFrame([[name, population, link]], columns=['name', 'population', 'link'])
                df = pd.concat([df, df_i], ignore_index=True)  # складываем в один датафрейм

    return df





