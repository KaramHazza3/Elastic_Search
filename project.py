from elasticsearch import Elasticsearch
from dateutil import parser
from dateutil.parser import ParserError
import spacy
from geopy.geocoders import Nominatim
from bs4 import BeautifulSoup
import os
from elasticsearch import helpers
import re


nlp = spacy.load("en_core_web_sm")

geolocator = Nominatim(user_agent="my_application")
es = Elasticsearch(
        hosts=["https://localhost:9200"],
        basic_auth=("elastic", "oSbIdu8bNkXs+BNN_XkM"),
        verify_certs=False
    )

index_name = 'my_index'

configurations = {
  "mappings": {
    "properties": {
      "title": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        },
        "analyzer": "autocomplete",
        "search_analyzer": "standard"
      },
      "content": {
        "type": "text",
        "analyzer": "content_analyzer"
      },
      "authors": {
        "type": "nested",
        "properties": {
          "first_name": { "type": "text" },
          "last_name": { "type": "text" }
        }
      },
      "date": {
        "type": "date"
      },
      "geopoint": {
        "type": "geo_point"
      },
      "temporalExpressions": {
        "type": "text"
      },
      "georeferences": {
        "type": "text"
      }
    }
  },
  "settings": {
    "analysis": {
      "analyzer": {
        "autocomplete": {
          "tokenizer": "autocomplete",
          "filter": [
            "lowercase"
          ]
        },
        "content_analyzer": {
          "tokenizer": "standard",
          "char_filter": ["html_strip"],
          "filter": ["lowercase", "stop", "length", "stemmer"]
        }
      },
      "filter": {
        "length": {
          "type": "length",
          "min": 3
        },
        "stemmer": {
          "type": "stemmer",
          "language": "english"
        }
      },
      "tokenizer": {
        "autocomplete": {
          "type": "edge_ngram",
          "min_gram": 1,
          "max_gram": 10,
          "token_chars": [
            "letter"
          ]
        }
      }
    }
  }
}


if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)

es.indices.create(index=index_name, body=configurations)

extracted_dir = 'C:\\Users\\karam\\OneDrive\\Desktop\\IR Project\\archive'

def extract_georeferences(text):
  doc = nlp(text)
  georeferences = [ent.text for ent in doc.ents if ent.label_ == "GPE"]
  return georeferences

def get_coordinates(georeference):
    location = geolocator.geocode(georeference)
    if location:
        return location.latitude, location.longitude
    else:
        return 0.0, 0.0

for root, _, files in os.walk(extracted_dir):
    for file in files:
        docs = []
        if file.endswith(".sgm"):
            sgm_file_path = os.path.join(root, file)

            with open(sgm_file_path, 'r', encoding='iso-8859-1') as sgm_file:
                print(sgm_file_path)
                soup = BeautifulSoup(sgm_file, 'html.parser')
                reuters_tags = soup.find_all('reuters')

                for reuter in reuters_tags:
                    
                    if reuter.title:
                        title = reuter.title.text.strip()
                    else:
                        title = "No title found"

                    content = reuter.get_text().strip()
                    content = re.sub(r'[^a-zA-Z0-9\s]', '', content)
                    content = re.sub(r'\s+', ' ', content)
                    print(content)
                    date = None
                    if reuter.date:
                        date_string = reuter.date.text.strip()
                        date_string = re.sub('[♣♠♥♦]', '', date_string)
                        try:
                          date = parser.parse(date_string)
                        except ParserError:
                          print(f"Failed to parse date: {date_string}")
                          date = None
                    
                  
                    authors = []
                    if reuter.author:
                        author = reuter.author.text.strip()
                        first_name = author.split(' ')[1].strip()
                        last_name = author.split(' ')[2].strip()
                        author_obj = {"first_name": first_name, "last_name": last_name}
                        authors.append(author_obj)
                    else:
                      authors.append({"first_name": "Unknown" , "last_name": "Unknown"})
                  
                    geo_points = {}
                    if reuter.places:
                        place = reuter.places.text.strip()
                        lat, lon = get_coordinates(place)
                        geo_points = {"lat": lat, "lon": lon}
                    
                            
                    temporal_doc = nlp(content)
                    temporal_expressions = []
                    for ent in temporal_doc.ents:
                      if ent.label_ == "DATE":
                         if "YEAR" in ent.text:
                            year_part = ent.text.split()[-1]
                            date_string = f"January 1, {year_part}" 
                         else:
                             date_string = ent.text

                    georeferences = extract_georeferences(content)

                    if date is None and temporal_expressions:
                        date = parser.parse(temporal_expressions[0])

                    if not geo_points and georeferences:
                        lat, lon = get_coordinates(georeferences[0])
                        geo_points = {"lat": lat, "lon": lon}
                        

                    document = {
                        "title": title,
                        "content": content,
                        "authors": authors,
                        "date": date,
                        "geopoint": geo_points,
                        "temporalExpressions": temporal_expressions,
                        "georeferences": georeferences
                    }
                    docs.append(document)
        actions = [
            {
                "_index": index_name,
                "_source": doc
            }
            for doc in docs
        ]
        try:
          helpers.bulk(es, actions)
        except helpers.BulkIndexError as e:
          for err in e.errors:
            print(err)

