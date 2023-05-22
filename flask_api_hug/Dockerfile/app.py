from flask import Flask, request, jsonify
from typing import Dict, List
from urllib.parse import urlparse
import argparse
import json
import requests

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
# from transformers import PreTrainedTokenizerFast, GPT2LMHeadModel
import numpy as np
import pandas as pd

QUESTION_HEADER = "다음 식재료를 사용하는 레시피명, 재료, 조리과정을 JSON 형태로 반환해주세요.\n"
RECIPE_KEYS = ["foodName", "ingredients", "recipe"]
DB_KEYS = ["name", "recipeIngredient", "recipeInstructions"]
NUM_SIMILAR_RECIPES = 3

# load the pre-trained KoGPT tokenizer
# TOKENIZER = PreTrainedTokenizerFast.from_pretrained('skt/kogpt2-base-v2')

# load the pre-trained KoGPT model
# MODEL = GPT2LMHeadModel.from_pretrained('skt/kogpt2-base-v2')

parser = argparse.ArgumentParser()
# parser.add_argument("--k", "--apiKey", dest="menu", type=str, help='Please set Google Bard API key')
parser.add_argument("--k", "--apiKey", dest="menu", type=str, help='Please set Google Bard API key',default=str()) # 임시 테스트
args = parser.parse_args()

app = Flask(__name__)


@app.route('/api', methods=['GET'])
def process_text():
    params = urlparse(request.url).query
    params = {key:value for key, value in [param.split('=') for param in params.split('&')]}
    result = process_data(params["ingredients"], args.apiKey)
    return jsonify(result)


def process_data(query: str, apiKey: str) -> List[Dict]:
    try:
        generated_recipe = generate_recipe(query, apiKey)
        data = {key: value for key, value in zip(RECIPE_KEYS, generated_recipe.values())}
        similar = get_similar_recipes(data, RECIPES, NUM_SIMILAR_RECIPES).to_dict("records")
    except:
        data = {"title":str(), "ingredients":query.split(','), "recipe":str()}
        similar = get_similar_recipes(data, RECIPES, NUM_SIMILAR_RECIPES).to_dict("records")
    return [data]+similar


# def generate_recipe(ingredients: str, apiKey: str) -> Dict[str,str]:
#     question = QUESTION_HEADER+ingredients
#     headers = {"Authorization":"Bearer "+apiKey, "Content-Type":"text/plain"}
#     data = {"input": question}
#     response = requests.post("https://api.bardapi.dev/chat", headers=headers, json=data)
#     return json.loads(response.json()["output"])

#     # input_ids = TOKENIZER.encode(input_text, return_tensors='pt')
#     # output = MODEL.generate(input_ids, max_length=150, do_sample=True, temperature=0.3)
#     # generated_text = TOKENIZER.decode(output[0], skip_special_tokens=True)
#     # return generated_text



def generate_recipe(ingredients: str, apiKey: str) -> Dict[str,str]:


    dummy_recipe =    {
        "foodName": "감자 김치 계란 볶음밥",
        "ingredients": [
            "감자 1개",
            "당근 1/2개",
            "양파 1/2개",
            "계란 2개",
            "김치 1/2컵",
            "밥 1공기",
            "식용유 1큰술",
            "간장 2큰술",
            "고춧가루 1큰술",
            "설탕 1큰술",
            "참기름 1큰술",
            "깨 약간"
        ],
        "recipe": [
            "1. 감자, 당근, 양파는 깨끗이 씻어 먹기 좋은 크기로 썰어줍니다.",
            "2. 계란은 풀어줍니다.",
            "3. 팬에 식용유를 두르고 중불로 가열합니다.",
            "4. 감자를 넣고 노릇노릇해질 때까지 볶습니다.",
            "5. 당근, 양파, 김치를 넣고 함께 볶습니다.",
            "6. 계란을 넣고 스크램블 에그를 만들어줍니다.",
            "7. 밥을 넣고 볶습니다.",
            "8. 간장, 고춧가루, 설탕, 참기름을 넣고 간을 맞춥니다.",
            "9. 깨를 뿌려주면 완성입니다."
        ]
    }

    return dummy_recipe 

    # input_ids = TOKENIZER.encode(input_text, return_tensors='pt')
    # output = MODEL.generate(input_ids, max_length=150, do_sample=True, temperature=0.3)
    # generated_text = TOKENIZER.decode(output[0], skip_special_tokens=True)
    # return generated_text


def literal_evals(x):
    from ast import literal_eval

    import re


    print(x)
    return literal_eval(str(x))

def load_recipes() -> pd.DataFrame:


    from sqlalchemy import create_engine
    import pandas as pd

    engine = create_engine('postgresql://root:root@192.168.219.123:5432/recipe')
    engine.connect()

    query = """
    SELECT * FROM _10000_recipe;
    """

    ## 레시피 데이터 프레임생성
    df = pd.read_sql(query, con=engine)

    ## 속성명 변환
    df.rename(columns={'recipe_ingredient':'recipeIngredient', 'recipe_instruction':'recipeInstructions'}, inplace=True)
    


    # df["recipeIngredient"] = df["recipeIngredient"].apply(literal_evals)
    # df["recipeInstructions"] = df["recipeInstructions"].apply(literal_evals)

    return df.drop_duplicates("name").reset_index(drop=True)



def get_cosine_similarity(df: pd.DataFrame, column: str) -> np.ndarray:
    """
    특정 열에 대한 코사인 유사도를 반환하는 함수
    """

    tokenized_data = df[column].fillna(str()).apply(lambda x: x if isinstance(x, str) else ', '.join(x))
    vectorizer = TfidfVectorizer()
    array = vectorizer.fit_transform(tokenized_data).todense()
    return cosine_similarity(array, array)


def make_similar_index(df: pd.DataFrame, title=True, ingredients=True) -> np.ndarray:
    """
    코사인 유사도 합을 반환하는 함수
    """

    if not (title and ingredients): ingredients = True
    title_similarity = get_cosine_similarity(df, "name") if title else 0
    ingredients_similarity = get_cosine_similarity(df, "recipeIngredient") if ingredients else 0
    similarity = title_similarity + ingredients_similarity
    return similarity.argsort()[:, ::-1]


def get_similar_recipes(recipe: Dict[str,str], df: pd.DataFrame, num_display=3) -> pd.DataFrame:
    """
    코사인 유사도에 기반하여 특정 조건을 만족하는 행과 유사한 데이터프레임을 반환하는 함수
    """

    recipes = insert_recipe(recipe, df)
    max_index = min(num_display, len(recipes))

    similar_index = make_similar_index(recipes, title=(len(recipe["foodName"])>0))
    return recipes.loc[similar_index[0,1:max_index+1]]


def insert_recipe(recipe: Dict[str,str], df: pd.DataFrame) -> pd.DataFrame:
    """
    주어진 레시피 딕셔너리를 전체 레시피 테이블의 최상단에 삽입하는 함수
    """

    recipes = df.copy()
    recipe_info = [recipe[key] for key in RECIPE_KEYS]
    recipes.loc[-1] = recipe_info
    recipes.index = recipes.index+1
    return recipes.sort_index()


RECIPES = load_recipes()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
