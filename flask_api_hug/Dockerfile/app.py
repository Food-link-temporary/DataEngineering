from flask import Flask, request, jsonify
from typing import Dict
from urllib.parse import urlparse
from transformers import PreTrainedTokenizerFast, GPT2LMHeadModel
import re

# SEPERATOR = "<SEP>"
RECIPE_PATTERN = r"Recipe Title:(.*?)Ingredients:(.*?)Instructions:(.*?)$"

# load the pre-trained KoGPT tokenizer
TOKENIZER = PreTrainedTokenizerFast.from_pretrained('skt/kogpt2-base-v2')

# load the pre-trained KoGPT model
MODEL = GPT2LMHeadModel.from_pretrained('skt/kogpt2-base-v2')

app = Flask(__name__)


@app.route('/api', methods=['GET'])
def process_text():
    
    return request.url

    params = urlparse(request.url).params

    print("params 체크:",params)
    params = {key:value for key, value in [param.split('=') for param in params.split('&')]}

    print(params)
    # 데이터 처리 및 결과 얻기
    # result = process_data(params["ingredients"])

    return jsonify(params)


def generate_text(input_text: str) -> str:
    input_ids = TOKENIZER.encode(input_text, return_tensors='pt')
    output = MODEL.generate(input_ids, max_length=150, do_sample=True, temperature=0.3)
    generated_text = TOKENIZER.decode(output[0], skip_special_tokens=True)
    return generated_text


def process_data(data: str) -> Dict[str,str]:
    generated_text = generate_text(data)
    try:
        matches = re.search(RECIPE_PATTERN, generated_text, re.DOTALL)
        return {
            "foodName": matches.group(1).strip(),
            "ingredients": matches.group(2).strip(),
            "recipe": matches.group(3).strip()
        }
    except: return {"title":str(), "ingredients":str(), "recipe":str()}


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
