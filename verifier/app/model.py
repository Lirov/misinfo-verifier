import os
from typing import List, Dict
import numpy as np
import onnxruntime as ort
from transformers import AutoTokenizer
from huggingface_hub import hf_hub_download

# Default to a compact MNLI ONNX model; can be overridden via env
NLI_ONNX = os.getenv("NLI_ONNX", "hf-internal-testing/tiny-random-bert-for-sequence-classification")
NLI_MODEL = os.getenv("NLI_MODEL", NLI_ONNX)

# Map model logits to human labels (assume MNLI label order)
ID2LABEL = {0: "contradiction", 1: "neutral", 2: "entailment"}
LABEL2ID = {v: k for k, v in ID2LABEL.items()}

_tokenizer = None
_session: ort.InferenceSession | None = None

def get_tokenizer():
    global _tokenizer
    if _tokenizer is None:
        _tokenizer = AutoTokenizer.from_pretrained(NLI_MODEL)
    return _tokenizer

def get_session():
    global _session
    if _session is None:
        providers = ["CPUExecutionProvider"]
        onnx_path = hf_hub_download(repo_id=NLI_ONNX, filename="model.onnx")
        _session = ort.InferenceSession(onnx_path, providers=providers)
    return _session

def softmax(x: np.ndarray) -> np.ndarray:
    e = np.exp(x - np.max(x, axis=-1, keepdims=True))
    return e / np.sum(e, axis=-1, keepdims=True)

def nli_batch(premises: List[str], hypotheses: List[str]) -> List[Dict]:
    tok = get_tokenizer()
    sess = get_session()
    enc = tok(premises, hypotheses, padding=True, truncation=True, max_length=256, return_tensors="np")
    inputs = {k: v for k, v in enc.items() if k in {i.name for i in sess.get_inputs()}}
    outputs = sess.run(None, inputs)
    logits = outputs[0]
    probs = softmax(logits)
    out: List[Dict] = []
    for p in probs:
        scores = {ID2LABEL[i]: float(p[i]) for i in range(min(3, p.shape[-1]))}
        label_id = int(np.argmax(p[:3])) if p.shape[-1] >= 3 else int(np.argmax(p))
        out.append({"label": ID2LABEL.get(label_id, "neutral"), "scores": scores})
    return out
