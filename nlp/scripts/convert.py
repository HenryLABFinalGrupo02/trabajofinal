"""Convert textcat annotation from JSONL to spaCy v3 .spacy format."""
import srsly
import typer
import warnings
from pathlib import Path

import spacy
from spacy.tokens import DocBin


def convert(lang: str, input_path: Path, output_path: Path):
    """
    > It takes a JSONL file, loads it into a DocBin, and saves it to disk

    Arguments: 
    --------------------------------    
    :param lang: The language of the text
    :param input_path: The path to the JSONL file you want to convert
    :param output_path: The path to the output file

    Types: 
    --------------------------------
    :type lang: str
    :type input_path: Path
    :type output_path: Path
    """
    nlp = spacy.blank(lang)
    db = DocBin()
    for line in srsly.read_jsonl(input_path):
        doc = nlp.make_doc(line["text"])
        doc.cats = line["cats"]
        db.add(doc)
    db.to_disk(output_path)


if __name__ == "__main__":
    typer.run(convert)
