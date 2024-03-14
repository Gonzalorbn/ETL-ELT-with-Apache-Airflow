# script para consultar api de mercado libre y guardar la información relevante en un archivo tsv
import requests
import json
from datetime import date

DATE=str(date.today()).replace('-','')

def get_most_relevant_items_for_category(category):
    """
    Recibimos los items mas relevantes por categoria.La variable category
    se le deberá asignar la categoría que queramos usar.
    La variable response termina transformandose en un json

    """
    url = (f'https://api.mercadolibre.com/sites/MLA/search?category={category}#json')
    response = requests.get(url).text
    response = json.loads(response)
    data = response["results"]
    
    with open('/opt/airflow/plugins/tmp/file.tsv', 'w', encoding='utf-8') as file:
       
        for item in data:
            _id = getKeyFromItem(item,'id')
            site_id = getKeyFromItem(item,'site_id')
            title = getKeyFromItem(item,'title')
            price = getKeyFromItem(item,'price')
            sold_quantity = getKeyFromItem(item,'sold_quantity')
            thumbnail = getKeyFromItem(item,'thumbnail')
            
            file.write(f'{_id}\t{site_id}\t{title}\t{price}\t{sold_quantity}\t{thumbnail}\t{DATE}\n')


def getKeyFromItem(item, key):
    """
    Recibo la key que necesito sacar del diccionario almacenado en item
    """
    return str(item[key]).replace('','').strip() if item.get(key) else "null"


def main():
    CATEGORY = "MLA1577"
    get_most_relevant_items_for_category(CATEGORY)

main()
