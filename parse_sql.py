import re
import json
from collections import defaultdict
from tqdm import tqdm
from html import unescape
import logging
import os


def process_record(record_xml):
    try:
        date_match = re.search(r"<dc:date>(.+?)</dc:date>", record_xml)
        if date_match:
            date = date_match.group(1)
        else:
            return None
        
        year_match = re.search(r"\d{4}", date)
        if year_match:
            year = year_match.group()
        else:
            return None
        
        setspec_match = re.search(r"<setSpec>(.+?)</setSpec>", record_xml)
        if setspec_match:
            setspec = setspec_match.group(1)
        else:
            return None
        
        identifier_match = re.findall(r"<dc:identifier.*?>(.+?)</dc:identifier>", record_xml)
        creator_match = re.findall(r"<dc:creator.*?>(.+?)</dc:creator>", record_xml)
        publisher_match = re.findall(r"<dc:publisher.*?>(.+?)</dc:publisher>", record_xml)
        title_match = re.findall(r"<dc:title.*?>(.+?)</dc:title>", record_xml)
        description_match = re.findall(r"<dc:description.*?>(.+?)</dc:description>", record_xml)
        source_match = re.findall(r"<dc:source.*?>(.+?)</dc:source>", record_xml)
        relation_match = re.findall(r"<dc:relation.*?>(.+?)</dc:relation>", record_xml)
        language_match = re.findall(r"<dc:language.*?>(.+?)</dc:language>", record_xml)
        rights_match = re.findall(r"<dc:rights.*?>(.+?)</dc:rights>", record_xml)

        article = {
            "identifier": tuple(unescape(e) for e in identifier_match),
            "date": tuple([unescape(date)]),
            "creator": tuple(unescape(e) for e in creator_match),
            "publisher": tuple(unescape(e) for e in publisher_match),
            "title": tuple(unescape(e) for e in title_match),
            "description": tuple(unescape(e) for e in description_match),
            "source": tuple(unescape(e) for e in source_match),
            "relation": tuple(unescape(e) for e in relation_match),
            "language": tuple(unescape(e) for e in language_match),
            "rights": tuple(unescape(e) for e in rights_match),
            }

        return year, setspec, article
    except Exception as e:
        logging.error(f"Error parsing record: {e}")
        return None

def parse_sql():
    data_dir = "data"
    output_dir = os.path.join(data_dir, "json")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    sql_file = os.path.join(data_dir, "database.sql")
    record_pattern = re.compile(r"<record.+?</record>", re.DOTALL | re.MULTILINE)
    data = defaultdict(lambda: defaultdict(set))
    count = 0

    with open(sql_file, "r") as file:
        for line in tqdm(file, desc="Processing records"):
            records = record_pattern.findall(line)
            for rec in records:
                result = process_record(rec)
                if result:
                    year, setspec, article = result
                    data[year][setspec].add(tuple(article.items()))
                    count += 1

    print(f"Done!\nCount: {count} unique articles")

    for year, setspecs in data.items():
        output_file = os.path.join(output_dir, f"{year}.json")
        json_data = {setspec: [dict(articles) for articles in setspecs[setspec]] for setspec in setspecs}
        n_articles = sum(len(articles) for articles in json_data.values())
        print(f"{year}: {n_articles} articles")
        with open(output_file, "w") as file:
            json.dump(json_data, file, indent=2)

if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)
    parse_sql()