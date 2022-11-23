####################################
######## LIBRARIES IMPORT ##########
####################################
from transformers import pipeline
from datasets import load_dataset
from datetime import datetime


####################################
######## PATH SETTING ##########
####################################
now = datetime.now().strftime("%f")
path_to_save = f"./data/sentiment/new{now}.csv"


####################################
######## DATA PREPARING ##########
####################################
dataset = load_dataset("json", data_files="./data/review.json")
dataset = dataset['train'].remove_columns(["user_id", "business_id", "date", "useful", "funny", "cool", "stars"])


####################################
######## MODEL LOADING ##########
####################################
classifier = pipeline("sentiment-analysis", model="textattack/albert-base-v2-yelp-polarity", device=0)


####################################
######## COMMON FUNCTIONS ##########
####################################
def add_predictions(records):
    predictions = classifier([record for record in records['text']], truncation=True)
    if isinstance(predictions, list):
        return {"labels": [pred["label"] for pred in predictions], "scores": [pred["score"] for pred in predictions]}
    else:
        return {"labels": predictions["label"], "scores": predictions["score"]}


####################################
######## MAKE PREDICTIONS ##########
####################################
dataset = dataset.map(add_predictions, batched=True, batch_size=64)
dataset = dataset.remove_columns(["text"])

####################################
######## SAVING DATA ##########
####################################
dataset.to_csv(path_to_save)