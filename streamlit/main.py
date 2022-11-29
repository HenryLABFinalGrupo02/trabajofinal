import streamlit as st
import pandas as pd 
from st_on_hover_tabs import on_hover_tabs

st.set_page_config(
   page_title="Project Henry",
   page_icon="👋",  
   layout="wide"
)

st.title('Welcome to Vocado Admin Center')

##################
## IMPORT DATA ###
##################

business = pd.read_csv(r'business_1000.csv')
checkin = pd.read_csv(r'checkin_1000.csv')
review = pd.read_csv(r'review_1000.csv')
#tip = pd.read_csv(r'C:\Users\USER\Documents\SOYHENRY\LABS\TRABAJO_GRUPAL\trabajofinal\Airflow-Spark\data\tip_1000.csv')
#user = pd.read_csv(r'C:\Users\USER\Documents\SOYHENRY\LABS\TRABAJO_GRUPAL\trabajofinal\Airflow-Spark\data\user_1000.csv')

# st.sidebar.title('Menu')
# paginas = st.sidebar.selectbox("Select :",['pagina1','pagina2'])

with st.sidebar:
        tabs = on_hover_tabs(tabName=['Dashboard', 'Money', 'Economy'], 
                             iconName=['dashboard', 'money', 'economy'],
                             styles = {'navtab': {'background-color':'transparent',
                                                  'color': '#818181',
                                                  'font-size': '18px',
                                                  'transition': '.3s',
                                                  'white-space': 'nowrap',
                                                  'text-transform': 'uppercase'},
                                       'tabOptionsStyle': {':hover :hover': {'color': 'red',
                                                                      'cursor': 'pointer'}},
                                       'iconStyle':{'position':'fixed',
                                                    'left':'7.5px',
                                                    'text-align': 'left'},
                                       'tabStyle' : {'list-style-type': 'none',
                                                     'margin-bottom': '30px',
                                                     'padding-left': '30px'}},
                             key="0")

##################
#### FUNTIONS ####
##################

def metricas(): 
   review_stars = business['stars'].mean()
   review['positive_score'] = review['pos_reviews'] / ( review['neg_reviews'] + review['pos_reviews'])
   Positive_sentiment = review['positive_score'].mean()
   review_total = review.shape[0]
   number_visits = checkin['number_visits'].sum()

   st.markdown("### Oportunities")
   oportunity = st.columns(3)
   oportunity[0].metric('Business Line', '98,7%', delta=None, delta_color="normal")
   oportunity[1].metric('Location', '93,5%', delta=None, delta_color="normal")
   oportunity[2].metric('Services', '90,7%', delta=None, delta_color="normal")

   st.markdown("### Account Summary")
   metrics = st.columns(6)
   metrics[0].metric('Review Total', review_total, delta=None, delta_color="normal")
   metrics[1].metric('Review stars', round(review_stars, 2), delta=None, delta_color="normal")
   metrics[2].metric('Positive sentiment', f'{round(Positive_sentiment, 2)*100}%', delta=None, delta_color="normal")
   metrics[3].metric('Influencer Score', '98,7%', delta=None, delta_color="normal")
   metrics[4].metric('Top Hour', '18:00', delta=None, delta_color="normal")
   metrics[5].metric('Number_visits', number_visits)


metricas()  


