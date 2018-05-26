from cassandra.cqlengine import columns
from cassandra.cqlengine import connection
from cassandra.cqlengine.models import Model
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import plotly

#cassandra table for the question and answers
class QuestionModel(Model):
    question_url  = columns.Text(primary_key=True)
    question_que  = columns.Text(required=True)
    question_body = columns.Text(required=True)

if __name__ == '__main__':
    count = 0
    neg = 0.0
    neu = 0.0
    pos = 0.0
    
    #Connecting to the database !
    connection.setup(['127.0.0.1'], 'cqlengine')
    
    #Loading the data(Answers) from database
    qs = QuestionModel.objects.all()
    
    #Invoking the sentiment Analyzer
    sid = SentimentIntensityAnalyzer()
    
    #Calculating the scores for +ve, -ve, and neutral
    for q in qs:
        body = q.question_body
        ss = sid.polarity_scores(body)
        neg = neg + ss['neg']
        neu = neg + ss['neu']
        pos = neg + ss['pos']
        count = count + 1
        print('Processed: {}. {}'.format(count, q.question_que.encode('utf-8')))
    
    #ploting the result through a HTML file using plotly !
    plotly.offline.plot({
        'data': [{
            'labels': ['Negative', 'Neutral', 'Postive', 'WTF'],
            'values': [neg, neu, pos, (1 - (neg+neu+pos))],
            'type': 'pie'
            }],
        })
