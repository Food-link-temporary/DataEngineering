from flask import Flask, request, jsonify, Request
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

    params = urlparse(request.url).query

    params = {key:value for key, value in [param.split('=') for param in params.split('&')]}

    # 데이터 처리 및 결과 얻기
    result = process_data(params["ingredients"])

    return jsonify(result)


def generate_text(input_text: str) -> str:
    input_text = "다음 식재료를 사용하는 레시피명과 조리과정을 알려주세요.\n"+input_text
    input_ids = TOKENIZER.encode(input_text, return_tensors='pt')
    output = MODEL.generate(input_ids, max_length=150, do_sample=True, temperature=0.3)
    generated_text = TOKENIZER.decode(output[0], skip_special_tokens=True)
    return generated_text


def process_data(data: str) -> Dict[str,str]:
    generated_text = generate_text(data)

    try:
        # matches = re.search(RECIPE_PATTERN, generated_text, re.DOTALL)
        return {
            "foodName": "food name", # matches.group(1).strip(),
            "ingredients": data, # matches.group(2).strip(),
            "recipe": generated_text.encode("utf-8").decode("utf-8") # matches.group(3).strip()
        }
    except: return {"title":str(), "ingredients":str(), "recipe":str()}


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
