from flask import Flask, request, jsonify
from transformers import PreTrainedTokenizerFast, GPT2LMHeadModel



app = Flask(__name__)

@app.route('/api', methods=['POST'])
def process_text():
    text = request.json['text']

    # 전달 받은 텍스트를 Spark DataFrame으로 변환


    return jsonify(result)


def process_data(df):
 

    # load the pre-trained KoGPT tokenizer
    tokenizer = PreTrainedTokenizerFast.from_pretrained('skt/kogpt2-base-v2')

    # load the pre-trained KoGPT model
    model = GPT2LMHeadModel.from_pretrained('skt/kogpt2-base-v2')

    input_text = """
    '코코넛설탕', '토막난 닭', '말린 가지', '멸치다시마육수'
    위 재료를 사용하는 요리명과 레시피는 다음과 같습니다.
    """

    # tokenize text
    input_ids = tokenizer.encode(input_text, return_tensors='pt')

    # generate text using the model
    output = model.generate(input_ids, max_length=150, do_sample=True, temperature=0.3)

    # decode generated text
    generated_text = tokenizer.decode(output[0], skip_special_tokens=True)

    return result_df.toJSON().collect()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)



