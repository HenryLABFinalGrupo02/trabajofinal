# import libraries
import spacy_streamlit
import typer

def main(models: str, default_text: str):
    """
    > The function takes in a string of models and a string of default text. It then splits the models
    string into a list of models and passes that list of models and the default text to the visualize
    function in the spacy_streamlit library

    Arguments: 
    --------------------------------
    :param models: The models you want to visualiz
    :param default_text: The text that will be displayed in the text box when the app is first loaded

    Types: 
    --------------------------------
    :type models: str
    :type default_text: str
    """
    models = [name.strip() for name in models.split(",")]
    spacy_streamlit.visualize(models, default_text, visualizers=["textcat"])


if __name__ == "__main__":
    try:
        typer.run(main)
    except SystemExit:
        pass