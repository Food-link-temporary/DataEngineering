from ast import literal_eval
from bs4 import BeautifulSoup, Tag
from typing import Any, Callable, Dict, Iterable, List, Optional, TypeVar, Union
from urllib.parse import urlparse
import json
import pandas as pd
import re
import requests


HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
    "sec-ch-ua": '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
#     "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
    "User-Agent": "Mozilla/5.0"
}


parse_path = lambda url: re.sub(urlparse(url).path+'$','',url)

def get_headers(authority=str(), referer=str(), cookies=str(), host=str(),
                origin: Optional[Union[bool,str]]=False, **kwargs) -> Dict:
    headers = HEADERS.copy()
    if authority: headers["Authority"] = urlparse(authority).hostname
    if referer: headers["referer"] = referer
    if host: headers["Host"] = urlparse(host).hostname
    if origin: headers["Origin"] = parse_path(origin if isinstance(origin, str) else (authority if authority else host))
    if cookies: headers["Cookie"] = cookies
    return dict(headers, **kwargs)

session = requests.Session()



_KT = TypeVar("_KT")
_VT = TypeVar("_VT")

def cast_float(__object, default: Optional[float]=0., strict=False, **kwargs) -> float:
    try:
        return float(__object) if strict else float(re.sub("[^\d.]",'',str(__object)))
    except (ValueError, TypeError):
        return default

def cast_int(__object, default: Optional[int]=0, strict=False, **kwargs) -> int:
    try:
        return int(float(__object)) if strict else int(cast_float(__object, None))
    except (ValueError, TypeError):
        return default

def apply_func(value, func: Callable, default=None, **kwargs) -> Any:
    try: return func(value, **kwargs)
    except: return default

def hier_get(__m: Dict, __path: Iterable[_KT], default=None, apply: Optional[Callable]=None,
            instance: Optional[type]=None, empty=True, **kwargs) -> _VT:
    try:
        for key in __path:
            __m = __m[key]
        value = apply_func(__m, apply, default, **kwargs) if apply else __m
        value = default if instance and not isinstance(value, instance) else value
        return value if value or empty else default
    except: return default

def re_get(pattern: str, string: str, default=str(), groups=False, **kwargs) -> str:
    if not re.search(pattern, string): return default
    catch = re.search(pattern, string).groups()
    return catch[0] if catch and not groups else catch

def select_text(source: Tag, selector: str, pattern='\n', sub=' ', many=False, **kwargs) -> Union[str,List[str]]:
    try:
        if many: return [re.sub(pattern, sub, select.text).strip() for select in source.select(selector)]
        else: return re.sub(pattern, sub, source.select_one(selector).text).strip()
    except (AttributeError, IndexError, TypeError):
        return list() if many else str()




ORDER_MAP = {"정확순":"accuracy", "최신순":"date", "추천순":"reco"}
get_params = lambda **kwargs: {k:v for k,v in kwargs.items() if v}
uri = "https://www.10000recipe.com/recipe/"

def fetch(session: requests.Session, query=str(), sortType="추천순", page=1,
        cat1=str(), cat2=str(), cat3=str(), cat4=str(), **kwargs) -> List[str]:
    url = uri+"list.html"
    params = get_params(q=query, order=ORDER_MAP[sortType], page=page,
                        cat1=cat1, cat2=cat2, cat3=cat3, cat4=cat4)
    headers = get_headers(url, referer=url)
    headers["upgrade-insecure-requests"] = '1'
    response = session.get(url, params=params, headers=headers)
    return parse(response.text, **kwargs)

def parse(response: str, **kwargs) -> List[str]:
    source = BeautifulSoup(response, 'html.parser')
    uris = source.select("a.common_sp_link")
    ids = [uri.attrs["href"].split('/')[-1] for uri in uris if "href" in uri.attrs]
    return ids





# ---- Start recipe info fatch ----

uri = "https://www.10000recipe.com/recipe/"

def recipe_info_fetch(session: requests.Session, recipeId: str, **kwargs) -> Dict:
    url = uri+recipeId # https://www.10000recipe.com/recipe/6997297
    headers = get_headers(url, referer=uri+"list.html")
    headers["upgrade-insecure-requests"] = '1'
    response = session.get(url, headers=headers)
    return recipe_info_parse(response.text, recipeId, **kwargs)

def recipe_info_parse(response: str, recipeId: str, **kwargs) -> Dict:
    source = BeautifulSoup(response, 'html.parser')
    raw_json = source.select_one("script[type=\"application/ld+json\"]").text
    try: data = json.loads(raw_json)
    except: data = literal_eval(raw_json)
    return map_recipe(data, recipeId, source, **kwargs)

def map_recipe(data: Dict, recipeId: str, source=None, **kwargs) -> Dict:
    recipe_info = {"recipeId": recipeId}
    recipe_info["name"] = data.get("name")
    recipe_info["author"] = hier_get(data, ["author","name"])
    recipe_info["ratingValue"] = cast_int(hier_get(data, ["aggregateRating","ratingValue"]))
    recipe_info["reviewCount"] = cast_int(hier_get(data, ["aggregateRating","reviewCount"]))
    recipe_info["totalTime"] = data.get("totalTime")
    recipe_info["recipeYield"] = data.get("recipeYield")
    try: recipe_info["recipeIngredient"] = ','.join(data["recipeIngredient"])
    except: recipe_info["recipeIngredient"] = extract_ingredient(source, **kwargs)
    recipe_info["recipeInstructions"] = '\n'.join(
        [step.get("text",str()) for step in data.get("recipeInstructions",list())
            if isinstance(step, dict)])
    recipe_info["createDate"] = data.get("datePublished")
    return recipe_info

def extract_ingredient(source: Tag, **kwargs) -> str:
    cont_ingre = source.select_one("div.cont_ingre")
    if cont_ingre:
        return [ingre.split() for ingre in cont_ingre.select_one("dd").text.split(',')]
    else: return str()



# ---- End recipe info fatch ----


# ---- Start review fatch ----

GENDER = {"info_name_m":"M", "info_name_f":"F"}
uri = "https://www.10000recipe.com/recipe/"
rid_ptn = "replyReviewDiv_(\d+)"
uid_ptn = "/profile/review.html\?uid=([\d\w]+)"
date_ptn = "(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"

def review_fetch(session: requests.Session, recipeId: str, **kwargs) -> List[Dict]:
    url = uri+recipeId
    headers = get_headers(url, referer=uri+"list.html")
    headers["upgrade-insecure-requests"] = '1'
    response = session.get(url, headers=headers)
    return review_parse(response.text, recipeId, **kwargs)

def review_parse(response: str, recipeId: str, **kwargs) -> List[Dict]:
    source = BeautifulSoup(response, 'html.parser')
    reply_divs = source.select("div.view_reply")
    review_div = [div for div in reply_divs if div.select_one("div.reply_tit").text.strip().startswith("요리 후기")]
    if review_div:
        review_list = review_div[0].select("div.reply_list")
        return [map_review(review, recipeId, **kwargs) for review in review_list]
    else: return list()

def map_review(data: Tag, recipeId: str, **kwargs) -> Dict:
    review_info = dict()
    review_info["reviewId"] = re_get(rid_ptn, data.select("div")[-1].attrs.get("id"))
    review_info["recipeId"] = recipeId
    review_info["userId"] = re_get(uid_ptn, data.select_one("a").attrs.get("href"))
    review_info["contents"] = select_text(data, "p.reply_list_cont")
    detail = data.select_one("h4.media-heading")
    if detail:
        review_info["userName"] = select_text(detail, "b")
        gender = detail.select_one("b").attrs.get("class")
        review_info["userGender"] = GENDER.get(gender[0]) if gender else None
        review_info["createDate"] = re_get(date_ptn, detail.text)
    return review_info

# ---- End review fatch ----


# ---- Start comment fatch ----

GENDER = {"info_name_m":"M", "info_name_f":"F"}
uri = "https://www.10000recipe.com/recipe/"
cid_ptn = "replyCommentDiv_(\d+)"
uid_ptn = "/profile/recipe_comment.html\?uid=([\d\w]+)"
date_ptn = "(\d{4}-\d{2}-\d{2} \d{2}:\d{2})"

def comment_fetch(session: requests.Session, recipeId: str, page=1, **kwargs) -> List[Dict]:
    url = uri+"ajax.html"
    params = dict(q_mode="getListComment", seq=recipeId, page=page)
    headers = get_headers(url, referer=uri+recipeId)
    headers["upgrade-insecure-requests"] = '1'
    response = session.get(url, params=params, headers=headers)
    return comment_parse(response.text, recipeId, **kwargs)

def comment_parse(response: str, recipeId: str, **kwargs) -> List[Dict]:
    source = BeautifulSoup(response, 'html.parser')
    comment_list = source.select("div.reply_list")
    return [map_comment(comment, recipeId, **kwargs) for comment in comment_list]

def map_comment(data: Tag, recipeId: str, **kwargs) -> Dict:
    comment_info = dict()
    comment_info["commentId"] = re_get(cid_ptn, data.select("div")[-1].attrs.get("id"))
    comment_info["recipeId"] = recipeId
    comment_info["userId"] = re_get(uid_ptn, data.select_one("a").attrs.get("href"))
    comment_info["contents"] = select_text(data, "div.media-body").split('|')[-1]
    detail = data.select_one("h4.media-heading")
    if detail:
        comment_info["userName"] = select_text(detail, "b")
        gender = detail.select_one("b").attrs.get("class")
        comment_info["userGender"] = GENDER.get(gender[0]) if gender else None
        comment_info["createDate"] = re_get(date_ptn, detail.text)
    return comment_info





import pyarrow as pa
import pyarrow.fs as fs
import io
import requests
from pyarrow import csv

# Connect to HDFS
hdfs = pa.HadoopFileSystem(host='192.168.219.121', port=9000)


# ---- End comment fatch ----
checkFile = hdfs.exists('/usr/recipe/data/count.txt')
if(checkFile == True):
  count = hdfs.cat("/usr/recipe/data/count.txt")
  count = int(count)
else:
  count = 0


# Read the contents of the file
path = '/usr/data/test_merge_distict_all.csv/part-00000-6a575808-4644-4c80-85d5-04a9b11f73b7-c000.csv'
with hdfs.open(path) as f:
    csv_file = csv.read_csv(f)
    dfRecipeId = csv_file.to_pandas()
    
    
batchSize = 10
start_num = count
end_num = start_num + batchSize
    
recipe_data = {}

for i in range(start_num, end_num) :
    recipeId = str(dfRecipeId._c2[i])
    try:
        recipe_info = recipe_info_fetch(session, recipeId)
        review = review_fetch(session, recipeId)
        comment = comment_fetch(session, recipeId)
    except (json.decoder.JSONDecodeError, SyntaxError):
        with hdfs.open("/usr/recipe/data/errorId.txt", "ab") as f:
            f.write(str(recipeId).encode()+"\n")
            f.close()
    else:
        recipe_data[recipeId] = {"recipeInfo" : recipe_info, "review" : review, "comment" : comment}

# count.txt 파일을 생성 후 새로 업데이트 된 count 값 입력
# hdfs에 count.txt 파일을 업로드 하여 다음 크론잡 실행시 count 값 반영

count = end_num
with open("count.txt", "w") as f:
  f.write(str(count))
  f.close()
with open("count.txt", 'rb') as f:
  hdfs.upload("/usr/recipe/data/count.txt", f)
  f.close()




with open('data.json','w') as f:
  json.dump(recipe_data, f, ensure_ascii=False, indent=4)
  f.close()

with open('data.json', 'rb') as f:
  hdfs.upload('/usr/recipe/data/'+"recipe"+str(count)+".json", f)
  f.close()
hdfs.close()



