from sentence_transformers import SentenceTransformer

_model = None

def get_model():
    global _model
    if _model is None:
        _model = SentenceTransformer('paraphrase-MiniLM-L6-v2')
        _model.tokenizer.clean_up_tokenization_spaces = False  # Explicitly set this
    return _model
