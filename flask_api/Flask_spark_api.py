from flask import Flask, request, jsonify
from pyspark.sql import SparkSession

app = Flask(__name__)

# SparkSession 생성
spark = SparkSession.builder.appName("SparkStreamingApp").getOrCreate()

# Spark Streaming 애플리케이션 생성
ssc = None  # Spark StreamingContext 객체

@app.route('/process-text', methods=['POST'])
def process_text():
    text = request.json['text']

    # 전달 받은 텍스트를 Spark DataFrame으로 변환
    data = [(text,)]
    df = spark.createDataFrame(data, ['text'])

    # 데이터 처리 및 결과 얻기
    result = process_data(df)

    return jsonify(result)

def process_data(df):
    # 데이터 처리 작업 수행
    # 예시로 단어 세기 작업을 수행하도록 하겠습니다.
    word_counts = df.selectExpr("split(text, ' ') as words") \
                    .selectExpr("explode(words) as word") \
                    .groupBy("word").count()

    # 결과를 DataFrame으로 변환하여 반환
    result_df = word_counts.select("word", "count")

    return result_df.toJSON().collect()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
