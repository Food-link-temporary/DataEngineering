import pyarrow as pa
import pyarrow.fs as fs
import io
import requests

# Connect to HDFS



# List the contents of the root directory
# files = hdfs.ls('/')
# for file in files:
#     print(file)



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
                <a href="javascript:goSearchRecipe('cat4','')">??????</a><a href="javascript:goSearchRecipe('cat4','63')">?????????</a><a href="javascript:goSearchRecipe('cat4','56')" class="active">????????????</a><a href="javascript:goSearchRecipe('cat4','54')">???/???</a><a href="javascript:goSearchRecipe('cat4','55')">??????</a><a href="javascript:goSearchRecipe('cat4','60')">?????????</a><a href="javascript:goSearchRecipe('cat4','53')">???/??????</a><a href="javascript:goSearchRecipe('cat4','52')">???/???/???</a><a href="javascript:goSearchRecipe('cat4','61')">??????</a><a href="javascript:goSearchRecipe('cat4','57')">??????/??????/??????</a><a href="javascript:goSearchRecipe('cat4','58')">??????/??????/???</a><a href="javascript:goSearchRecipe('cat4','65')">??????</a><a href="javascript:goSearchRecipe('cat4','64')">?????????</a><a href="javascript:goSearchRecipe('cat4','68')">??????</a><a href="javascript:goSearchRecipe('cat4','66')">???</a><a href="javascript:goSearchRecipe('cat4','69')">??????</a><a href="javascript:goSearchRecipe('cat4','59')">???/??????/???</a><a href="javascript:goSearchRecipe('cat4','62')">??????</a>            </div>
                        <div class="cate_list">
                <a href="javascript:goSearchRecipe('cat2','')">??????</a><a href="javascript:goSearchRecipe('cat2','12')" class="active">??????</a><a href="javascript:goSearchRecipe('cat2','18')">????????????</a><a href="javascript:goSearchRecipe('cat2','13')">????????????</a><a href="javascript:goSearchRecipe('cat2','19')">?????????</a><a href="javascript:goSearchRecipe('cat2','21')">????????????</a><a href="javascript:goSearchRecipe('cat2','15')">?????????</a><a href="javascript:goSearchRecipe('cat2','43')">?????????</a><a href="javascript:goSearchRecipe('cat2','17')">??????</a><a href="javascript:goSearchRecipe('cat2','45')">??????</a><a href="javascript:goSearchRecipe('cat2','20')">??????????????????</a><a href="javascript:goSearchRecipe('cat2','46')">??????</a><a href="javascript:goSearchRecipe('cat2','44')">??????</a><a href="javascript:goSearchRecipe('cat2','14')">?????????</a><a href="javascript:goSearchRecipe('cat2','22')">??????</a>            </div>
                        <div class="cate_list">
                <a href="javascript:goSearchRecipe('cat3','')" class="active">??????</a><a href="javascript:goSearchRecipe('cat3','70')">?????????</a><a href="javascript:goSearchRecipe('cat3','71')">????????????</a><a href="javascript:goSearchRecipe('cat3','72')">?????????</a><a href="javascript:goSearchRecipe('cat3','23')">??????</a><a href="javascript:goSearchRecipe('cat3','28')">?????????</a><a href="javascript:goSearchRecipe('cat3','24')">?????????</a><a href="javascript:goSearchRecipe('cat3','50')">??????/?????????</a><a href="javascript:goSearchRecipe('cat3','33')">???????????????</a><a href="javascript:goSearchRecipe('cat3','47')">???</a><a href="javascript:goSearchRecipe('cat3','32')">?????????</a><a href="javascript:goSearchRecipe('cat3','25')">????????????</a><a href="javascript:goSearchRecipe('cat3','31')">?????????</a><a href="javascript:goSearchRecipe('cat3','48')">?????????</a><a href="javascript:goSearchRecipe('cat3','27')">???/?????????</a><a href="javascript:goSearchRecipe('cat3','26')">??????</a><a href="javascript:goSearchRecipe('cat3','34')">??????</a>            </div>
                        <div class="cate_list">
                <a href="javascript:goSearchRecipe('cat1','')" class="active">??????</a><a href="javascript:goSearchRecipe('cat1','6')">??????</a><a href="javascript:goSearchRecipe('cat1','1')">?????????</a><a href="javascript:goSearchRecipe('cat1','7')">??????</a><a href="javascript:goSearchRecipe('cat1','36')">??????</a><a href="javascript:goSearchRecipe('cat1','41')">??????</a><a href="javascript:goSearchRecipe('cat1','42')">??????</a><a href="javascript:goSearchRecipe('cat1','8')">???</a><a href="javascript:goSearchRecipe('cat1','10')">??????</a><a href="javascript:goSearchRecipe('cat1','9')">??????</a><a href="javascript:goSearchRecipe('cat1','38')">??????</a><a href="javascript:goSearchRecipe('cat1','67')">??????</a><a href="javascript:goSearchRecipe('cat1','39')">?????????</a><a href="javascript:goSearchRecipe('cat1','37')">???</a><a href="javascript:goSearchRecipe('cat1','11')">??????</a>            </div>
                            </div>""", 'html.parser')
pattern = "javascript:goSearchRecipe([\d\w()',]+)"
raw_cat = [(re_get(pattern, cat.attrs["href"]),cat.text) for cat in source.select("a") if "href" in cat.attrs]
cat_map = lambda catType, catId, catName: {"categoryId":catId, "categoryType":catType, "categoryName":catName}
categories = [cat_map(*literal_eval(data), name) for data, name in raw_cat]
categories = pd.DataFrame(categories)
categories = categories[categories["categoryId"]!='']






catId = categories.categoryId
catType = categories.categoryType

# categories
for pair1 in zip(catId, catType):
    print(pair1[0], pair1[1])

categories.to_csv("catType.csv" ,index=False, header=False)



ORDER_MAP = {"?????????":"accuracy", "?????????":"date", "?????????":"reco"}
get_params = lambda **kwargs: {k:v for k,v in kwargs.items() if v}
uri = "https://www.10000recipe.com/recipe/"

def fetch(session: requests.Session, query=str(), sortType="?????????", page=1,
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




## ????????????????????? ?????????

hdfs = pa.HadoopFileSystem(host='192.168.219.121', port=9000)

temp_all = []
## ???????????? ID??? TYPE ?????? ????????? ??????
## ??? ???????????? ex) cat4 , cat3.. ??? ?????? ???????????? ID??? ???????????? ????????? ID ??????
## ??? ?????????, ????????? ????????? 10????????? ??????

## hdfs??? count.txt ????????? ????????? ??? ?????? ???????????? ????????? ????????? ????????? ??????
## ?????? ????????? ????????? 1??????????????? ??????
checkFile = hdfs.exists('/usr/data/count.txt')
if(checkFile == True):
  count = hdfs.cat("/usr/data/count.txt")
  count = int(count)
else:
  count = 1

start_page = count
end_page = start_page + 1


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

# csv ????????? ?????? ??????
temp_cat = pd.DataFrame(temp_cat)
temp_cat.to_csv("category"+str(count)+".csv", index=False)

# count.txt ????????? ?????? ??? ?????? ???????????? ??? count ??? ??????
# hdfs??? count.txt ????????? ????????? ?????? ?????? ????????? ????????? count ??? ??????
with open("count.txt", "w") as f:
  f.write(str(count))
  f.close()
with open("count.txt", 'rb') as f:
  hdfs.upload("/usr/data/count.txt", f)
  f.close()

# ????????? csv ????????? ???????????? hdfs??? ?????????
with open("category"+str(count)+".csv", 'rb') as f:
  hdfs.upload('/usr/data/'+"category"+str(count)+".csv", f)
  f.close()

hdfs.close()


