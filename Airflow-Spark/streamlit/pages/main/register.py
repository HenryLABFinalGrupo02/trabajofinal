import authenticator_edit as stauth
import yaml
import streamlit as st



with open(r'config.yaml') as file:
   users = yaml.load(file, Loader=yaml.FullLoader)

authenticator = stauth.Authenticate(
   users['credentials'],
   users['cookie']['name'],
   users['cookie']['key'],
   users['cookie']['expiry_days'],
   users['preauthorized']
)

try:
   if authenticator.register_user('Register user', preauthorization=True):
      st.success('User registered successfully')
except Exception as e:
   st.error(e)

with open('config.yaml', 'w') as file:
   yaml.dump(users, file, default_flow_style=False)
   