from bs4 import BeautifulSoup
from cassandra.cqlengine import columns
from cassandra.cqlengine import connection
from cassandra.cqlengine.query import DoesNotExist
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.models import Model
import requests
import sys

#Defining base url for the website(QUORA)
base_url = 'https://www.quora.com/'
secret_sauce = '?share=1'
scrape_ques = []

#cassandra model
class QuestionModel(Model):
    question_url = columns.Text(primary_key=True)
    question_que = columns.Text(required=True)
    question_body = columns.Text(required=True)

#actual scraping of the answers 
def scrape_que_and_ans(qs):
    scrape_ques = [] 
    if len(qs) == 0:
        return
    while len(qs) > 0:
        q = qs.pop(0)
        url = base_url + q.get('href') + secret_sauce
        
        #trying with custom URL 
        #url = 'https://www.quora.com/How-is-NIIT-University-regarding-placements?share=1'
        
        print('The URL to the question is : '+url)
        try:
            QuestionModel.get(question_url=url)
            continue
        except DoesNotExist:
            r = requests.get(url)
            soup = BeautifulSoup(r.text, 'html.parser')
            #print(soup)
            #soup = BeautifulSoup(r.text, "lxml")
            try:
            
                #trying to scrape the question from the website
                #question = soup.select('div.question_text_edit span.rendered_qtext')[0].text
                #question = 'What is coding?'
                
                question = q.get('href')
                print('Scraping Answers !')
                #bodyp  = soup.select('div.Answer span.rendered_qtext p')
                bodyp  = soup.select('div.ui_qtext_expanded span.ui_qtext_rendered_qtext p.ui_qtext_para')
                bodyli = soup.select('div.Answer span.rendered_qtext li')
                bodys  = soup.select('div.Answer span.rendered_qtext')
                body = bodyp + bodyli + bodys
                #print(body)
                bodyt = ' '
                
                for b in body:
                    bodyt = bodyt + '. ' + b.text
                if bodyt != ' ':
                    #print('Testing Insertion !')
                    print('Inserting: {}'.format(question.encode('utf-8'))+' into database !')
                    QuestionModel.create(
                            question_url=url,
                            question_que=question,
                            question_body=bodyt
                            )
                            
                #scrape related questions
                #scrape_ques += soup.select('li.related_question a')
                if len(scrape_ques) > 10000:
                    break 
            except IndexError:
                continue
    #scrape_que_and_ans(scrape_ques)

#program starts here
if __name__ == '__main__':
    connection.setup(['127.0.0.1'], 'cqlengine')
    print('Connected to the Database !')
    sync_table(QuestionModel)
    scrape_que_and_ans([{ 'href': sys.argv[1] }])
