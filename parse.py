"""
Цель -получение информации о товарах:цены, артикулы, штрихкоды, параметры; Получение списка доступных категорий товаров.
Получение списка категорий товаров. У каждой категории необходимо получить уникальный id и по возможности id
родительских либо дочерних категорий для понимания иерархии дерева категорий. Парсер должен конфигурироваться для
сбора данных как по полному каталогу, так и по выбранным категориям.

Перечень столбцов выходного CSV-файла с пояснениями по формированию:
price_datetime - Дата и время сбора данных по товару.
price - Регулярная цена. 
price_promo - Акционная цена.
sku_status - 1/0 - наличие товара.
sku_barcode - Штрихкод товара. Если их несколько, дублируем строку для каждого из них. 
sku_article - Артикул товара. Не записывать в результат парсинга товары, у которых совпадает и артикул, и штрихкод
с уже найденными товарами.
sku_name - Наименование товара. 
sku_category - Название категории товара в каталоге. Сбор данных должен происходить по иерархии сверху вниз, от
родительских категорий к дочерним. Например: ”Колбасные изделия|Сосиски, сардельки, шпикачки|Сосиски”.
sku_country - страна. 
sku_weight_min - вес.
sku_volume_min - объем. 
sku_quantity_min - количество единиц, например, 100 салфеток в пачке. 
sku_link - ссылка на товар в интернет-магазине. Собираем без лишних параметров, в максимально лаконичном виде.
sku_images - Прямые ссылки на каждую фотографию товара.
"""
import datetime
import logging
import pathlib
import random
import time
import requests
import json
import re
import csv

import bs4

GLOBAL_SETTINGS: dict = {}


class Utils:
    @classmethod
    def export_to_csv(cls, parser_obj):
        """Экспортирует найденные товары в файл"""
        output_directory = pathlib.Path(GLOBAL_SETTINGS['output_directory'])
        if not output_directory.exists():
            output_directory.mkdir()

        with open(output_directory / pathlib.Path(GLOBAL_SETTINGS['filename_output']), 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file, delimiter=';', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            csv_writer.writerow([
                'Дата и время',
                'Регулярная цена',
                'Акционная цена',
                'Наличие товара',
                'Штрихкод товара',
                'Артикул товара',
                'Наименование товара',
                'Название категории',
                'Страна',
                'Вес',
                'Обьем',
                'Количество единиц',
                'Cсылка на товар',
                'Прямые ссылки фотографии',
            ])
            for product in parser_obj.Products:
                for sku_article, variant_product in product.products_info.items():
                    csv_writer.writerow([
                        product.price_datetime,
                        variant_product['price'],
                        variant_product['price_promo'],
                        variant_product['sku_status'],
                        variant_product['sku_barcode'],
                        sku_article,
                        product.sku_name,
                        product.sku_category,
                        product.sku_country,
                        variant_product['sku_weight_min'],
                        variant_product['sku_volume_min'],
                        variant_product['sku_quantity_min'],
                        product.sku_link,
                        product.sku_images, ])

    @classmethod
    def configure_logger(cls):
        logdir = pathlib.Path(GLOBAL_SETTINGS['logs_dir'])
        if not logdir.exists():
            logdir.mkdir()

        logging.basicConfig(handlers=(logging.FileHandler(logdir / pathlib.Path(GLOBAL_SETTINGS['logs_filename'])),
                                      logging.StreamHandler()),
                            format='[%(asctime)s | %(levelname)s]: %(message)s',
                            datefmt='%m.%d.%Y %H:%M:%S',
                            level=logging.INFO)


class Caller:
    def __init__(self):
        self.session: requests.Session = requests.session()

    def get_html(self, url: str, headers: str = "") -> str:
        """Получает html страницы. Перезапускает подключение если сервер не доступен"""
        if GLOBAL_SETTINGS['delay_range_s']:
            time.sleep(random.randint(*tuple(GLOBAL_SETTINGS['delay_range_s'])))
        for _ in range(GLOBAL_SETTINGS['restart']['restart_count']):
            try:
                for _ in range(GLOBAL_SETTINGS['max_retries']):
                    r = self.session.get(url, headers=headers)
                    if r.status_code == 200:
                        return r.text
                    time.sleep(GLOBAL_SETTINGS['restart']['interval_m'])
                    continue
            except (requests.exceptions.ConnectionError, requests.exceptions.ConnectTimeout):
                time.sleep(GLOBAL_SETTINGS['restart']['interval_m'])
                continue
        raise ConnectionError(
            f'Сеть не доступна. Проведено {GLOBAL_SETTINGS["restart"]["restart_count"]} безуспешных попыток перезапуска.')

    def get_tags(self, url, tag, attr, pattern):
        page = self.get_html(url)
        bs = bs4.BeautifulSoup(page, 'html.parser')
        return bs.find(tag, attrs={attr: re.compile(pattern)})


class ParserProduct:
    """Содержит функции выборки данных."""

    def __init__(self, url: str, category: str):
        self.price_datetime: str = datetime.datetime.today().strftime('%d.%m.%Y %H:%M:%S')
        self.sku_name: str = ''
        self.sku_category = category
        self.sku_country: str = ''
        self.sku_link: str = url
        self.sku_images: list = []
        self.products_info: dict[str:dict] = {}
        self.catalog_detail_tags: bs4.Tag = GLOBAL_SETTINGS['caller'].get_tags(self.sku_link, 'div', 'class',
                                                                               'catalog-detail')

    def __repr__(self):
        return self.sku_name

    def __str__(self):
        return self.__repr__()

    def __call__(self):
        self.get_name()
        self.get_sku_country()
        self.get_sku_images()
        self.get_product_info()

    def get_product_info(self):
        """Перебирает товары разной фасовки, штрихкода, цены и т.п."""
        table_product = self.catalog_detail_tags.find('table', attrs={'class': 'tg22 b-catalog-element-offers-table'})
        table_product_tr = table_product.find_all('tr', attrs={'class': 'b-catalog-element-offer'})
        for tr in table_product_tr:
            sku_article = self.get_sku_article(tr)
            sku_barcode = self.get_barcode(tr)
            # Если нет штрихкода и артикула, идентифицирвоать товар не получится, пропускаем
            if not sku_barcode and not sku_barcode:
                continue
            price_and_promo = self.get_price(tr)
            sku_weight,sku_volume,sku_quantity = self.get_sku_weight_volume_quantity(tr)
            self.products_info[sku_article] = {
                'price': price_and_promo[0],
                'price_promo': price_and_promo[1] if price_and_promo[1] else '',
                'sku_status': self.get_sku_status(tr),
                'sku_barcode': sku_barcode,
                'sku_quantity_min': sku_weight,
                'sku_volume_min': sku_volume,
                'sku_weight_min': sku_quantity,
            }

    def get_name(self):
        """ Получает имя товара"""
        try:
            self.sku_name = self.catalog_detail_tags.find('div', attrs={'class': 'catalog-element-right'}).h1.text
        except AttributeError:
            self.sku_name = 'Нет наименования'
            logging.warning(f'Нет наименования url{GLOBAL_SETTINGS["url"]}{self.sku_link}')

    @staticmethod
    def get_price(start_point: bs4.Tag) -> tuple[str, str]:
        """ Получает цену товара, включая аукционную."""
        blocks = start_point.find('b', text='Цена:')
        try:
            if blocks:
                parent = blocks.parent
                if parent.s:
                    return parent.s.text, parent.span.text
                return parent.span.text, ''
            return '', ''
        except AttributeError:
            return '', ''

    @staticmethod
    def get_sku_status(start_point: bs4.Tag):
        """ Определяет доступность товара к заказу"""
        return 1 if start_point.find('span', text='В корзину') else 0

    @staticmethod
    def get_barcode(start_point: bs4.Tag):
        """ Получает штрихкод, если штрихкода нет возвращает пустую строку"""
        blocks = start_point.find('b', text='Штрихкод:')
        try:
            tags_b = blocks.parent.find_all('b')
            return tags_b[1].text if len(tags_b) > 1 else ''
        except AttributeError:
            return ''

    @staticmethod
    def get_sku_article(start_point: bs4.Tag):
        """Получает артикул, если его нет возвращает пустую строку"""
        blocks = start_point.find('b', text='Артикул:')
        try:
            parent = blocks.parent
            tags_b = parent.find_all('b')
            return tags_b[1].text if len(tags_b) > 1 else ''
        except AttributeError:
            return ''

    def get_sku_country(self):
        """ Получает страну происводителя товара"""
        try:
            self.sku_country = self.catalog_detail_tags.find('div', attrs={
                'class': 'catalog-element-offer-left'}).p.text.replace('Страна производства: ', '')
        except AttributeError:
            self.sku_country = ''

    @staticmethod
    def get_sku_weight_volume_quantity(start_point: bs4.Tag) -> tuple[str, str, str]:
        """ Определяет тип фасовки товара, возвращает количество штук | обьем | вес товара"""
        blocks = start_point.find('b', text='Фасовка:')
        try:
            parent = blocks.parent
            tags_b = parent.find_all('b')
            if len(tags_b) > 1:
                value = tags_b[1].text
                if "шт" in value:
                    return tuple((value, '', ''))  # (sku_quantity_min,sku_volume_min,sku_weight_min)
                if "мл" in value:
                    return tuple(('', value, ''))
                if "г" in value:
                    return tuple(('', '', value))
        except AttributeError:
            return tuple(('', '', ''))
        return tuple(('', '', ''))

    def get_sku_images(self):
        """ Получает список url изображений к товару, убирает дубли"""
        images = self.catalog_detail_tags.find_all('a', attrs={'rel': 'groupimg'})
        for img in images:
            img_url = f'{GLOBAL_SETTINGS["url"]}{img["href"]}'
            if img_url not in self.sku_images:
                self.sku_images.append(img_url)


class Category:
    """ Хранит список категорий товаров, хранит url только конечных категорий,
     учитывает родительские категории"""

    def __init__(self, name: str, main_category: tuple[str, int]):
        self.name: str = name
        self.parents: list[tuple[str, int]] = [main_category]

    def __repr__(self):
        return f"Category({' - '.join([cat[0] for cat in sorted(self.parents, key=lambda x: x[1])])} - {self.name})"

    def __str__(self):
        return f"{' - '.join([cat[0] for cat in sorted(self.parents, key=lambda x: x[1])])} - {self.name}"


class ZoParser:
    def __init__(self, settings):
        self.settings = settings
        self.Categoryes: dict[str:Category] = {}
        self.Products = []

    def get_categoryes(self):
        """ Получает список главных категорий, учитывает ограничение в config.json """
        main_menu = GLOBAL_SETTINGS['caller'].get_tags(self.settings['url'],'div','id','catalog-menu')
        main_category_tags = main_menu.find_all('a', attrs={'class': 'catalog-menu-icon'})
        for main_cat in main_category_tags:

            if self.settings['categories']:
                if main_cat['title'] in self.settings['categories']:
                    self.parse_categoryes(main_cat['title'], f'{self.settings["url"]}{main_cat["href"]}')
                continue

    def parse_categoryes(self, main_category_name: str, main_category_url: str):
        """Парсит подкатегории, сохраняет их в Categoryes"""
        side_menu = GLOBAL_SETTINGS['caller'].get_tags(main_category_url, 'div', 'class', 'catalog-menu-left')
        sub_categoryes = side_menu.find_all('a', attrs={'class': re.compile(self.settings['PATTERN_SUB_MENU'])})
        for cat in sub_categoryes:
            self.Categoryes[cat['href']] = (Category(cat.text, (main_category_name, 0)))

    def normilize_category(self):
        """
        Cоотносит родительские категории с дочерними по url, переносит родительские категории в конечные дочерние.
        """
        del_items_category: list = []
        for cat_url, category1 in self.Categoryes.items():
            for cat_url2, category2 in self.Categoryes.items():
                if cat_url == cat_url2:
                    continue
                if cat_url in cat_url2:
                    position = cat_url2.find(cat_url) + len(cat_url)
                    category2.parents.append((category1.name, position))
                    if cat_url not in del_items_category:
                        del_items_category.append(cat_url)

        for del_cat in del_items_category:
            del self.Categoryes[del_cat]

    def get_link_product(self, url):
        """Берем ссылки на товары, включая все страницы(пагинацию)"""
        section = GLOBAL_SETTINGS['caller'].get_tags(url, 'div', 'class', 'catalog-section')
        product_tags = section.find_all("a", attrs={'class': 'name'})
        result = []
        for product in product_tags:
            result.append(product['href'])
        if navigation_div := section.find('div', attrs={'class', 'navigation'}):
            current_page_number = int(navigation_div.span.text)
            for pagination_item in navigation_div.find_all('a'):
                if current_page_number + 1 == int(pagination_item.text):
                    result.extend(self.get_link_product(f'{self.settings["url"]}{pagination_item["href"]}'))
        return result

    def walk(self):
        """Проходит по всем ссылкам категорий, ищет ссылки на товары, парсит и добавляет товары"""
        for category_url, category_obj in self.Categoryes.items():
            url_products = self.get_link_product(f'{self.settings["url"]}{category_url}?pc=60')
            logging.info(f'Категория:{category_obj}: Количество товаров:{len(url_products)}')
            if url_products:
                for url in url_products:
                    product = ParserProduct(f'{self.settings["url"]}{url}', category_obj.__str__())
                    self.Products.append(product)
                    product()



if __name__ == "__main__":
    with open('config.json', 'r') as f:
        GLOBAL_SETTINGS = json.load(f)
    Utils.configure_logger()
    GLOBAL_SETTINGS['caller'] = Caller()
    parser = ZoParser(GLOBAL_SETTINGS)
    parser.get_categoryes()
    parser.normilize_category()
    parser.walk()
    Utils.export_to_csv(parser)
