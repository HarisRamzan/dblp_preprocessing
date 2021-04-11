from lxml import etree
from datetime import datetime
import csv
import codecs
#import ujson
import re
import pyodbc 
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=LAPTOP-9ENBVPRF\SQLEXPRESS;'
                      'Database=DPLP;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

all_elements = {"article", "inproceedings", "proceedings", "book", "incollection", "phdthesis", "mastersthesis", "www"}
# all of the feature types in dblp
all_features = {"address", "author", "booktitle", "cdrom", "chapter", "cite", "crossref", "editor", "ee", "isbn",
                "journal", "month", "note", "number", "pages", "publisher", "school", "series", "title", "url",
                "volume", "year"}

def log_msg(message):
    """Produce a log with current time"""
    print(message)
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), message)

def parse_article(dblp_path, save_path, save_to_csv=False, include_key=False):
    type_name = ['article']
    features = ['title', 'journal', 'year','volume','month','url','note','cdrom','publisher','number', 'author','ee']
    info = parse_entity(dblp_path, save_path, type_name, features, save_to_csv=save_to_csv, include_key=include_key)
    log_msg('Total articles found: {}, articles contain all features: {}, articles contain part of features: {}'
            .format(info[0] + info[1], info[0], info[1]))
    log_msg("Features information: {}".format(str(info[2])))

def context_iter(dblp_path):
    print(dblp_path)
    """Create a dblp data iterator of (event, element) pairs for processing"""
    return etree.iterparse(source=dblp_path, dtd_validation=True, load_dtd=True)  # required dtd

def extract_feature(elem, features, include_key=False):
    """Extract the value of each feature"""
    if include_key:
        attribs = {'key': [elem.attrib['key']]}
    else:
        attribs = {}
    for feature in features:
        attribs[feature] = []
    for sub in elem:
        if sub.tag not in features:
            continue
        text = re.sub("<.*?>", "", etree.tostring(sub).decode('utf-8')) if sub.text is None else sub.text
            #text = re.sub("<.*?>", "", etree.tostring(sub).decode('utf-8')) if sub.text is None else sub.text
#        elif sub.tag == 'pages':
#            #text = count_pages(sub.text)
#        #else:
#            #text = sub.text
        if text is not None and len(text) > 0:
               attribs[sub.tag] = attribs.get(sub.tag) + [text]
               
    return attribs

def clear_element(element):
    """Free up memory for temporary element tree after processing the element"""
    element.clear()
    while element.getprevious() is not None:
        del element.getparent()[0]
        
def Save_Artical(results):
      for record in results:
            print(record)
            cursor.execute('''
                           INSERT INTO dbo.Publications (Title)
                           VALUES (?)
                           ''',record['title'][0])
            conn.commit()
            results=[]
            cursor.execute('''SELECT @@IDENTITY AS 'Identity' ''')
            id=0
            for row in cursor:
                id= row[0]
            Journal, Year, Volume, Month, Url, Note, Rom, Publish, Number = None, None, None , None , None , None , None , None , None 
            if record['journal']:
                Journal=record['journal'][0]
            if record['year']:
                Year=record['year'][0]
            if record['volume']:
                Volume=record['volume'][0]
            if record['month']:
                Month=record['month'][0]
            if record['url']:
                Url=record['url'][0]
            if record['note']:
                Note=record['note'][0]
            if record['cdrom']:
                Rom=record['cdrom'][0]
            if record['publisher']:
                Publish=record['publisher'][0]
            if record['number']:
                Number=record['number'][0]
            cursor.execute('''
                           INSERT INTO dbo.Article(journal, year,volume,month,url,note,cdrom,publisher,number,PublicationID)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                           ''', (Journal, Year, Volume, Month, Url, Note, Rom, Publish, Number, id))
            conn.commit()
            if record['author']:
                for item in record['author']:
                    cursor.execute('''
                           INSERT INTO dbo.Authors(Author, PublicationID)
                           VALUES (?, ?)
                           ''', (item, id))
                    conn.commit()
            if record['ee']:
                for item in record['ee']:
                    cursor.execute('''
                           INSERT INTO dbo.EE(ElectronicEdition, PublicationID)
                           VALUES (?, ?)
                           ''', (item, id))
                    conn.commit()       
def Save_book(results):
      for record in results:
            print(record)
            cursor.execute('''
                           INSERT INTO dbo.Publications (Title)
                           VALUES (?)
                           ''',record['title'][0])
            conn.commit()
            results=[]
            cursor.execute('''SELECT @@IDENTITY AS 'Identity' ''')
            id=0
            for row in cursor:
                id= row[0]
            ISBN, Year, Volume, Url, Note, BookTitle, Publish, Series = None, None, None , None , None , None , None , None 
            if record['isbn']:
                ISBN=record['isbn'][0]
            if record['year']:
                Year=record['year'][0]
            if record['volume']:
                Volume=record['volume'][0]
            if record['url']:
                Url=record['url'][0]
            if record['note']:
                Note=record['note'][0]
            if record['booktitle']:
                BookTitle =record['booktitle'][0]
            if record['publisher']:
                Publish=record['publisher'][0]
            if record['series']:
                Series=record['series'][0]
            cursor.execute('''
                           INSERT INTO dbo.Books(ISBN, Year,Publisher,BookTitle,Url,Note,Series,Volume,PublicationID)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                           ''', (ISBN, Year, Publish, BookTitle, Url, Note, Series, Volume, id))
            conn.commit()
            if record['author']:
                for item in record['author']:
                    cursor.execute('''
                           INSERT INTO dbo.Authors(Author, PublicationID)
                           VALUES (?, ?)
                           ''', (item, id))
                    conn.commit()
            if record['ee']:
                for item in record['ee']:
                    cursor.execute('''
                           INSERT INTO dbo.EE(ElectronicEdition, PublicationID)
                           VALUES (?, ?)
                           ''', (item, id))
                    conn.commit() 
            if record['editor']:
                for item in record['editor']:
                    cursor.execute('''
                           INSERT INTO dbo.Editors(Editor, PublicationID)
                           VALUES (?, ?)
                           ''', (item, id))
                    conn.commit()

def Save_inproceedings(results):
      for record in results:
            print(record)
            cursor.execute('''
                           INSERT INTO dbo.Publications (Title)
                           VALUES (?)
                           ''',record['title'][0])
            conn.commit()
            results=[]
            cursor.execute('''SELECT @@IDENTITY AS 'Identity' ''')
            id=0
            for row in cursor:
                id= row[0]
            Rom, Year, Month, Url, Note, BookTitle, Pages,Refcross = None, None, None , None , None , None , None , None
            if record['month']:
                Month=record['month'][0]
            if record['crossref']:
                Refcross=record['crossref'][0]
            if record['year']:
                Year=record['year'][0]
            if record['url']:
                Url=record['url'][0]
            if record['note']:
                Note=record['note'][0]
            if record['booktitle']:
                BookTitle =record['booktitle'][0]
            if record['cdrom']:
                Rom=record['cdrom'][0]
            if record['pages']:
                Pages=record['pages'][0]
            cursor.execute('''
                           INSERT INTO dbo.Inproceedings(BookTitle,Year,Month,Url,Note,cdrom,Pages,crossref,PublicationID)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                           ''', (BookTitle, Year, Month, Url, Note, Rom, Pages, Refcross, id))
            conn.commit()
            if record['author']:
                for item in record['author']:
                    cursor.execute('''
                           INSERT INTO dbo.Authors(Author, PublicationID)
                           VALUES (?, ?)
                           ''', (item, id))
                    conn.commit()
            if record['ee']:
                for item in record['ee']:
                    cursor.execute('''
                           INSERT INTO dbo.EE(ElectronicEdition, PublicationID)
                           VALUES (?, ?)
                           ''', (item, id))
                    conn.commit()   
                    
def Save_proceedings(results):
      for record in results:
            print(record)
            cursor.execute('''
                           INSERT INTO dbo.Publications (Title)
                           VALUES (?)
                           ''',record['title'][0])
            conn.commit()
            results=[]
            cursor.execute('''SELECT @@IDENTITY AS 'Identity' ''')
            id=0
            for row in cursor:
                id= row[0]
            ISBN, Year, Volume, Url, BookTitle, Publish,Series = None, None, None , None , None , None , None 
            if record['isbn']:
                ISBN=record['isbn'][0]
            if record['series']:
                Series=record['series'][0]
            if record['year']:
                Year=record['year'][0]
            if record['url']:
                Url=record['url'][0]
            if record['booktitle']:
                BookTitle =record['booktitle'][0]
            if record['publisher']:
                Publish=record['publisher'][0]
            if record['volume']:
                Volume=record['volume'][0]
            cursor.execute('''
                           INSERT INTO dbo.Proceedings(BookTitle,Year,Url,Series,ISBN,Volume,Publisher,PublicationID)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                           ''', (BookTitle, Year, Url, Series, ISBN, Volume, Publish, id))
            conn.commit()
            if record['author']:
                for item in record['author']:
                    cursor.execute('''
                           INSERT INTO dbo.Authors(Author, PublicationID)
                           VALUES (?, ?)
                           ''', (item, id))
                    conn.commit()
            if record['ee']:
                for item in record['ee']:
                    cursor.execute('''
                           INSERT INTO dbo.EE(ElectronicEdition, PublicationID)
                           VALUES (?, ?)
                           ''', (item, id))
                    conn.commit() 
            if record['editor']:
                for item in record['editor']:
                    cursor.execute('''
                           INSERT INTO dbo.Editors(Editor, PublicationID)
                           VALUES (?, ?)
                           ''', (item, id))
                    conn.commit()
                    
def Save_incollections(results):
      for record in results:
            print(record)
            cursor.execute('''
                           INSERT INTO dbo.Publications (Title)
                           VALUES (?)
                           ''',record['title'][0])
            conn.commit()
            results=[]
            cursor.execute('''SELECT @@IDENTITY AS 'Identity' ''')
            id=0
            for row in cursor:
                id= row[0]
            Year, Url, BookTitle, Pages, Cref = None, None, None , None , None  
 
            if record['crossref']:
                Cref=record['crossref'][0]
            if record['year']:
                Year=record['year'][0]
            if record['url']:
                Url=record['url'][0]
            if record['booktitle']:
                BookTitle =record['booktitle'][0]
            if record['pages']:
                Pages=record['pages'][0]
            cursor.execute('''
                           INSERT INTO dbo.Incollections(BookTitle,Year,Url,Pages,crossref,PublicationID)
                           VALUES (?, ?, ?, ?, ?, ?)
                           ''', (BookTitle, Year, Url, Pages, Cref, id))
            conn.commit()
            if record['author']:
                for item in record['author']:
                    cursor.execute('''
                           INSERT INTO dbo.Authors(Author, PublicationID)
                           VALUES (?, ?)
                           ''', (item, id))
                    conn.commit()
            if record['ee']:
                for item in record['ee']:
                    cursor.execute('''
                           INSERT INTO dbo.EE(ElectronicEdition, PublicationID)
                           VALUES (?, ?)
                           ''', (item, id))
                    conn.commit()
                    
def Save_phdthesis(results):
      for record in results:
            print(record)
            cursor.execute('''
                           INSERT INTO dbo.Publications (Title)
                           VALUES (?)
                           ''',record['title'][0])
            conn.commit()
            results=[]
            cursor.execute('''SELECT @@IDENTITY AS 'Identity' ''')
            id=0
            for row in cursor:
                id= row[0]
            Year, Pages, School, Series, Volume = None, None, None , None , None  
 
            if record['school']:
                School=record['school'][0]
            if record['year']:
                Year=record['year'][0]
            if record['series']:
                Series=record['series'][0]
            if record['volume']:
                Volume =record['volume'][0]
            if record['pages']:
                Pages=record['pages'][0]
            cursor.execute('''
                           INSERT INTO dbo.Phdthesis(Pages,School,Year,Series,Volume,PublicationID)
                           VALUES (?, ?, ?, ?, ?, ?)
                           ''', (Pages, School, Year, Series, Volume, id))
            conn.commit()
            if record['author']:
                for item in record['author']:
                    cursor.execute('''
                           INSERT INTO dbo.Authors(Author, PublicationID)
                           VALUES (?, ?)
                           ''', (item, id))
                    conn.commit()
            if record['ee']:
                for item in record['ee']:
                    cursor.execute('''
                           INSERT INTO dbo.EE(ElectronicEdition, PublicationID)
                           VALUES (?, ?)
                           ''', (item, id))
                    conn.commit()
                    
def Save_mastersthesis(results):
      for record in results:
            print(record)
            cursor.execute('''
                           INSERT INTO dbo.Publications (Title)
                           VALUES (?)
                           ''',record['title'][0])
            conn.commit()
            results=[]
            cursor.execute('''SELECT @@IDENTITY AS 'Identity' ''')
            id=0
            for row in cursor:
                id= row[0]
            Year, Pages, School, Note= None, None, None , None   
 
            if record['school']:
                School=record['school'][0]
            if record['year']:
                Year=record['year'][0]
            if record['note']:
                Note=record['note'][0]
            if record['pages']:
                Pages=record['pages'][0]
            cursor.execute('''
                           INSERT INTO dbo.Masterthesis(Year,School,Note,Pages,PublicationID)
                           VALUES (?, ?, ?, ?, ?)
                           ''', (Year, School, Note, Pages, id))
            conn.commit()
            if record['author']:
                for item in record['author']:
                    cursor.execute('''
                           INSERT INTO dbo.Authors(Author, PublicationID)
                           VALUES (?, ?)
                           ''', (item, id))
                    conn.commit()
            if record['ee']:
                for item in record['ee']:
                    cursor.execute('''
                           INSERT INTO dbo.EE(ElectronicEdition, PublicationID)
                           VALUES (?, ?)
                           ''', (item, id))
                    conn.commit()

def Save_WWW(results):
      for record in results:
            print(record)
            cursor.execute('''
                           INSERT INTO dbo.Publications (Title)
                           VALUES (?)
                           ''',record['title'][0])
            conn.commit()
            results=[]
            cursor.execute('''SELECT @@IDENTITY AS 'Identity' ''')
            id=0
            for row in cursor:
                id= row[0]
            Url, Note= None, None   
 
            if record['url']:
                Url=record['url'][0]
            if record['note']:
                Note=record['note'][0]
        
            cursor.execute('''
                           INSERT INTO dbo.WWW(Note,Url,PublicationID)
                           VALUES (?, ?, ?)
                           ''', (Note, Url, id))
            conn.commit()
            if record['author']:
                for item in record['author']:
                    cursor.execute('''
                           INSERT INTO dbo.Authors(Author, PublicationID)
                           VALUES (?, ?)
                           ''', (item, id))
                    conn.commit()
                    
def parse_entity(dblp_path, save_path, type_name, features=None, save_to_csv=False, include_key=False):
    """Parse specific elements according to the given type name and features"""
    log_msg("PROCESS: Start parsing for {}...".format(str(type_name)))
    assert features is not None, "features must be assigned before parsing the dblp dataset"
    results = []
    attrib_count, full_entity, part_entity = {}, 0, 0
    for _, elem in context_iter(dblp_path):
        if elem.tag in all_elements:
            if elem.tag == 'article':
               features = ['title', 'journal', 'year','volume','month','url','note','cdrom','publisher','number', 'author','ee']
               attrib_values = extract_feature(elem, features, include_key)  # extract required features
               results.append(attrib_values);  # add record to results array
               Save_Artical(results);
               results=[];
            if elem.tag == 'book':
               features = ['title', 'isbn', 'year','publisher','booktitle','url','note','series','volume', 'author','ee','editor']
               attrib_values = extract_feature(elem, features, include_key)  # extract required features
               results.append(attrib_values);  # add record to results array
               Save_book(results)
               results=[]
            if elem.tag == 'inproceedings':
               features = ['title', 'booktitle', 'year','month','url','note','cdrom','pages', 'crossref','author','ee']
               attrib_values = extract_feature(elem, features, include_key)  # extract required features
               results.append(attrib_values);  # add record to results array
               Save_inproceedings(results)
               results =[]
            if elem.tag == 'proceedings':
               features = ['title', 'booktitle', 'year','url','series','isbn','volume','publisher','author','ee','editor']
               attrib_values = extract_feature(elem, features, include_key)  # extract required features
               results.append(attrib_values);
               Save_proceedings(results)
               results =[]# add record to results array
            if elem.tag == 'incollection':
               features = ['title', 'booktitle', 'year','url','pages', 'crossref','author','ee','editor']
               attrib_values = extract_feature(elem, features, include_key)  # extract required features
               results.append(attrib_values);  # add record to results array
               Save_incollections(results)
               results =[]# add record to results array
            if elem.tag == 'phdthesis':
               features = ['title', 'pages', 'school','year','series', 'volume','author','ee']
               attrib_values = extract_feature(elem, features, include_key)  # extract required features
               results.append(attrib_values);  # add record to results array
               Save_phdthesis(results)
               results =[]# add record to results array
            if elem.tag == 'mastersthesis':
               features = ['title', 'year', 'school','note','pages', 'volume','author','ee']
               attrib_values = extract_feature(elem, features, include_key)  # extract required features
               results.append(attrib_values);  # add record to results array
               Save_mastersthesis(results)
               results =[]# add record to results array
            if elem.tag == 'www':
               features = ['title', 'note','url','author']
               attrib_values = extract_feature(elem, features, include_key)  # extract required features
               results.append(attrib_values);  # add record to results array
               Save_WWW(results)
               results=[]
                
            
        elif elem.tag not in all_elements:
            continue
        clear_element(elem)
        
                    

            # some features contain multiple values (e.g.: author), concatenate with `::`
            #  ,if record['year']: record['year'][0],if record['volume']: record['volume'][0],if record['month']: record['month'][0],if record['url']: record['url'],if record['note']: record['note'][0],if record['cdrom']: record['cdrom'][0],if record['publisher']: record['publisher'][0],if record['number']: record['number'][0],id
           # row = ['::'.join(v) for v in list(record.values())]
            #print(row)
#            ujson.dump(results, f)
    return full_entity, part_entity, attrib_count
        

def main():
    dblp_path = 'dblp.xml'
    save_path = 'article.json'
    try:
        context_iter(dblp_path)
        log_msg("LOG: Successfully loaded \"{}\".".format(dblp_path))
    except IOError:
        log_msg("ERROR: Failed to load file \"{}\". Please check your XML and DTD files.".format(dblp_path))
        exit()
    parse_article(dblp_path, save_path, save_to_csv=False)


if __name__ == '__main__':
    main()