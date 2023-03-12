import pyarrow as pa
import pyarrow.fs as fs
import io
import requests

# Connect to HDFS

hdfs = pa.HadoopFileSystem(host='192.168.219.121', port=9000)

# List the contents of the root directory
files = hdfs.ls('/')
for file in files:
    print(file)



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


source = BeautifulSoup("""<div class="cate_list">
                <a href="javascript:goSearchRecipe('cat4','')">전체</a><a href="javascript:goSearchRecipe('cat4','63')">밑반찬</a><a href="javascript:goSearchRecipe('cat4','56')" class="active">메인반찬</a><a href="javascript:goSearchRecipe('cat4','54')">국/탕</a><a href="javascript:goSearchRecipe('cat4','55')">찌개</a><a href="javascript:goSearchRecipe('cat4','60')">디저트</a><a href="javascript:goSearchRecipe('cat4','53')">면/만두</a><a href="javascript:goSearchRecipe('cat4','52')">밥/죽/떡</a><a href="javascript:goSearchRecipe('cat4','61')">퓨전</a><a href="javascript:goSearchRecipe('cat4','57')">김치/젓갈/장류</a><a href="javascript:goSearchRecipe('cat4','58')">양념/소스/잼</a><a href="javascript:goSearchRecipe('cat4','65')">양식</a><a href="javascript:goSearchRecipe('cat4','64')">샐러드</a><a href="javascript:goSearchRecipe('cat4','68')">스프</a><a href="javascript:goSearchRecipe('cat4','66')">빵</a><a href="javascript:goSearchRecipe('cat4','69')">과자</a><a href="javascript:goSearchRecipe('cat4','59')">차/음료/술</a><a href="javascript:goSearchRecipe('cat4','62')">기타</a>            </div>
                        <div class="cate_list">
                <a href="javascript:goSearchRecipe('cat2','')">전체</a><a href="javascript:goSearchRecipe('cat2','12')" class="active">일상</a><a href="javascript:goSearchRecipe('cat2','18')">초스피드</a><a href="javascript:goSearchRecipe('cat2','13')">손님접대</a><a href="javascript:goSearchRecipe('cat2','19')">술안주</a><a href="javascript:goSearchRecipe('cat2','21')">다이어트</a><a href="javascript:goSearchRecipe('cat2','15')">도시락</a><a href="javascript:goSearchRecipe('cat2','43')">영양식</a><a href="javascript:goSearchRecipe('cat2','17')">간식</a><a href="javascript:goSearchRecipe('cat2','45')">야식</a><a href="javascript:goSearchRecipe('cat2','20')">푸드스타일링</a><a href="javascript:goSearchRecipe('cat2','46')">해장</a><a href="javascript:goSearchRecipe('cat2','44')">명절</a><a href="javascript:goSearchRecipe('cat2','14')">이유식</a><a href="javascript:goSearchRecipe('cat2','22')">기타</a>            </div>
                        <div class="cate_list">
                <a href="javascript:goSearchRecipe('cat3','')" class="active">전체</a><a href="javascript:goSearchRecipe('cat3','70')">소고기</a><a href="javascript:goSearchRecipe('cat3','71')">돼지고기</a><a href="javascript:goSearchRecipe('cat3','72')">닭고기</a><a href="javascript:goSearchRecipe('cat3','23')">육류</a><a href="javascript:goSearchRecipe('cat3','28')">채소류</a><a href="javascript:goSearchRecipe('cat3','24')">해물류</a><a href="javascript:goSearchRecipe('cat3','50')">달걀/유제품</a><a href="javascript:goSearchRecipe('cat3','33')">가공식품류</a><a href="javascript:goSearchRecipe('cat3','47')">쌀</a><a href="javascript:goSearchRecipe('cat3','32')">밀가루</a><a href="javascript:goSearchRecipe('cat3','25')">건어물류</a><a href="javascript:goSearchRecipe('cat3','31')">버섯류</a><a href="javascript:goSearchRecipe('cat3','48')">과일류</a><a href="javascript:goSearchRecipe('cat3','27')">콩/견과류</a><a href="javascript:goSearchRecipe('cat3','26')">곡류</a><a href="javascript:goSearchRecipe('cat3','34')">기타</a>            </div>
                        <div class="cate_list">
                <a href="javascript:goSearchRecipe('cat1','')" class="active">전체</a><a href="javascript:goSearchRecipe('cat1','6')">볶음</a><a href="javascript:goSearchRecipe('cat1','1')">끓이기</a><a href="javascript:goSearchRecipe('cat1','7')">부침</a><a href="javascript:goSearchRecipe('cat1','36')">조림</a><a href="javascript:goSearchRecipe('cat1','41')">무침</a><a href="javascript:goSearchRecipe('cat1','42')">비빔</a><a href="javascript:goSearchRecipe('cat1','8')">찜</a><a href="javascript:goSearchRecipe('cat1','10')">절임</a><a href="javascript:goSearchRecipe('cat1','9')">튀김</a><a href="javascript:goSearchRecipe('cat1','38')">삶기</a><a href="javascript:goSearchRecipe('cat1','67')">굽기</a><a href="javascript:goSearchRecipe('cat1','39')">데치기</a><a href="javascript:goSearchRecipe('cat1','37')">회</a><a href="javascript:goSearchRecipe('cat1','11')">기타</a>            </div>
                            </div>""", 'html.parser')
pattern = "javascript:goSearchRecipe([\d\w()',]+)"
raw_cat = [(re_get(pattern, cat.attrs["href"]),cat.text) for cat in source.select("a") if "href" in cat.attrs]
cat_map = lambda catType, catId, catName: {"categoryId":catId, "categoryType":catType, "categoryName":catName}
categories = [cat_map(*literal_eval(data), name) for data, name in raw_cat]
categories = pd.DataFrame(categories)
categories = categories[categories["categoryId"]!='']


# ... 으로 중간 생략되는 데이터를 모두 표시하기 위해
# 최대 행 수 설정
# pd.set_option('display.max_rows', 500)
# 최대 열 수 설정
# pd.set_option('display.max_columns', 500)


# categories[categories["categoryId"]=='63']

catId = categories.categoryId
catType = categories.categoryType

# categories
for pair1 in zip(catId, catType):
    print(pair1[0], pair1[1])

categories.to_csv("catType.csv" ,index=False, header=False)



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



# for i in range(100):
#     ids = fetch(session, page=i)




temp_all = []
## 카테고리 ID와 TYPE 쌍을 페어로 전달
## 각 카테고리 ex) cat4 , cat3.. 에 맞는 카테고리 ID를 입력하여 레시피 ID 추출
## 각 아이디, 타입별 페이지 10개씩만 추출

#num_page = 2
# iter = 3
checkFile = hdfs.exists('/usr/data/count.txt')
if(checkFile == True):
  count = hdfs.cat("/usr/data/count.txt")
  count = int(count)
else:
  count = 1

start_page = count
end_page = start_page + 1



# while count != iter:
temp_cat = []

for pair in zip(catId,catType):
  if(pair[1]=='cat4'):
    for i in range(start_page, end_page):
      tempIds = fetch(session, cat4=pair[0], page=i)
      if len(tempIds) == 0: break
      for id in tempIds:
        temp_cat.append([pair[0],pair[1], id])
  elif(pair[1]=='cat3'):
    for i in range(start_page, end_page):
      tempIds = fetch(session, cat3=pair[0], page=i)
      if len(tempIds) == 0: break
      for id in tempIds:
        temp_cat.append([pair[0],pair[1], id])
  elif(pair[1]=='cat2'):
    for i in range(start_page, end_page):
      tempIds = fetch(session, cat2=pair[0], page=i)
      if len(tempIds) == 0: break
      for id in tempIds:
        temp_cat.append([pair[0],pair[1], id])
  elif(pair[1]=='cat1'):
    for i in range(start_page, end_page):
      tempIds = fetch(session, cat1=pair[0], page=i)
      if len(tempIds) == 0: break
      for id in tempIds:
        temp_cat.append([pair[0],pair[1], id])
    
count = count + 1

temp_cat = pd.DataFrame(temp_cat)
temp_cat.to_csv("category"+str(count)+".csv", index=False)
#     temp_cat.to_csv("test"+str(count)+".csv", index=False)

with open("count.txt", "w") as f:
  f.write(str(count))
  f.close()
with open("count.txt", 'rb') as f:
  hdfs.upload("/usr/data/count.txt", f)
  f.close()


#hdfs = pa.HadoopFileSystem(host='192.168.219.121', port=9000)

# List the contents of the root directory
#files = hdfs.ls('/')
#for file in files:
#    print(file)



# for i in range(1, 5):
#     tempIds = fetch(session, page=i)
#     for id in tempIds:
#         temp_all.append(id)


# Define the local file path and HDFS path
#local_file_path = '/root/hadoop_test/testload.txt'
#hdfs_path = '/usr/test/file2.txt'
'''
# Upload the file to HDFS
with open(local_file_path, 'rb') as f:
    hdfs.upload(hdfs_path, f)
'''


## upload 2
"""
response = requests.get('https://blog.naver.com/kihyun1998/223033574553')
data = response.text

with open(data, 'rb') as f:
    hdfs.upload(hdfs_path, f)
"""
