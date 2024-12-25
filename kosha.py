import datetime
import json
import os
import random
import re
import time
from bs4 import BeautifulSoup
import requests
import boto3
from dotenv import load_dotenv
import os
import glob
import schedule
import psycopg2
from psycopg2 import sql

def createFolder(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print('Error: Creating directory. ' + directory)
def GetSearch():
    
    dataList=[]
    guideList = ["A","B","C",'D','E','F','G',"H","K",'M',"O",'P','T','W','X']
    for guide in guideList:
      pageCount=1
      while True:
          cookies = {
          'WMONID': 'M3MLkwgq9cp',
          'kosha_visited': '20241210230453596001',
          'english_visited': '20241210230519893001',
          'JSESSIONID': 'Cdal7zRZnh1MRAEkNXSqIiv1p9lARp4igqPp2H6LUU5npMxQvGMiXONSLZdhKIst.amV1c19kb21haW4va29zaGFjbXMy',
          'org.springframework.web.servlet.i18n.CookieLocaleResolver.LOCALE': 'ko',
          'develop_preview_mode': 'N',
        }

          headers = {
              'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
              'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
              'Cache-Control': 'max-age=0',
              'Connection': 'keep-alive',
              'Content-Type': 'application/x-www-form-urlencoded',
              # 'Cookie': 'WMONID=M3MLkwgq9cp; kosha_visited=20241210230453596001; english_visited=20241210230519893001; JSESSIONID=Cdal7zRZnh1MRAEkNXSqIiv1p9lARp4igqPp2H6LUU5npMxQvGMiXONSLZdhKIst.amV1c19kb21haW4va29zaGFjbXMy; org.springframework.web.servlet.i18n.CookieLocaleResolver.LOCALE=ko; develop_preview_mode=N',
              'Origin': 'https://www.kosha.or.kr',
              'Referer': 'https://www.kosha.or.kr/kosha/data/guidanceA.do',
              'Sec-Fetch-Dest': 'document',
              'Sec-Fetch-Mode': 'navigate',
              'Sec-Fetch-Site': 'same-origin',
              'Sec-Fetch-User': '?1',
              'Upgrade-Insecure-Requests': '1',
              'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
              'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
              'sec-ch-ua-mobile': '?0',
              'sec-ch-ua-platform': '"Windows"',
          }
          data = {
              'pageIndex': pageCount,
              'mode': 'list',
              'sortCondition': 'B',
              'searchCondition': 'A',
              'searchKeyword': '',
          }
          response = requests.post('https://www.kosha.or.kr/kosha/data/guidance{}.do'.format(guide), cookies=cookies, headers=headers, data=data)
          soup=BeautifulSoup(response.text,'html.parser')
          with open('kosha_search.html','w',encoding='utf-8') as file:
            file.write(soup.prettify())
          table=soup.find('tbody')
          items=soup.find_all('tr')
          print("아이템수:",len(items))
          if len(items)<=2:
              break
          for index,item in enumerate(items):
              if index==0:
                continue
              # print('item:',item)
              itemTitle=item.find('td',class_='board-list-title').get_text().replace('\n','').replace('\t','').replace('\r','').strip()
              itemNo=item.find('td',attrs={'headers':'guidelineNo'}).get_text().replace('\n','').replace('\t','').replace('\r','').strip()
              pubDate=item.find('td',attrs={'headers':'pubDate'}).get_text().replace('\n','').replace('\t','').replace('\r','').strip()
              attachment=item.find('td',attrs={'headers':'board_file'}).find('a')['href']
              boardNo=item.find('td',attrs={'headers':'board_num'}).get_text().replace('\n','').replace('\t','').replace('\r','').strip()
              data={
              'itemTitle':itemTitle,
              'itemNo':itemNo,
              'pubDate':pubDate,
              'attachment':"https://www.kosha.or.kr"+attachment,
              'boardNo':boardNo,
              'category':guide,
              'pageCount':pageCount
              }
              dataList.append(data)

          with open('kosha_search.json','w',encoding='utf-8') as file:
              json.dump(dataList,file,ensure_ascii=False,indent=4)
          print("페이지수:",pageCount,'카테고리:',guide)
          pageCount+=1
          time.sleep(1)
def GetDetail(inputData,infobase):
  cookies = {
    'WMONID': 'M3MLkwgq9cp',
    'kosha_visited': '20241210230453596001',
    'english_visited': '20241210230519893001',
    'JSESSIONID': 'Cdal7zRZnh1MRAEkNXSqIiv1p9lARp4igqPp2H6LUU5npMxQvGMiXONSLZdhKIst.amV1c19kb21haW4va29zaGFjbXMy',
    'org.springframework.web.servlet.i18n.CookieLocaleResolver.LOCALE': 'ko',
    'develop_preview_mode': 'N',
  }

  headers = {
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
      'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
      'Cache-Control': 'max-age=0',
      'Connection': 'keep-alive',
      'Content-Type': 'application/x-www-form-urlencoded',
      # 'Cookie': 'WMONID=M3MLkwgq9cp; kosha_visited=20241210230453596001; english_visited=20241210230519893001; JSESSIONID=Cdal7zRZnh1MRAEkNXSqIiv1p9lARp4igqPp2H6LUU5npMxQvGMiXONSLZdhKIst.amV1c19kb21haW4va29zaGFjbXMy; org.springframework.web.servlet.i18n.CookieLocaleResolver.LOCALE=ko; develop_preview_mode=N',
      'Origin': 'https://www.kosha.or.kr',
      'Referer': 'https://www.kosha.or.kr/kosha/data/guidanceA.do',
      'Sec-Fetch-Dest': 'document',
      'Sec-Fetch-Mode': 'navigate',
      'Sec-Fetch-Site': 'same-origin',
      'Sec-Fetch-User': '?1',
      'Upgrade-Insecure-Requests': '1',
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
      'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
      'sec-ch-ua-mobile': '?0',
      'sec-ch-ua-platform': '"Windows"',
  }

  data = {
      'sfhlhTchnlgyManualNo': inputData['itemNo'],
      'pageUri': '/kosha/data/guidanceA.do',
      'pageMenuCd': '13987',
  }

  response = requests.post('https://www.kosha.or.kr/kosha/data/guidanceDetail.do', cookies=cookies, headers=headers, data=data)
  soup=BeautifulSoup(response.text,'html.parser')
  with open('kosha_detail.html','w',encoding='utf-8') as file:
    file.write(soup.prettify())
    
  timeNowMonth=datetime.datetime.now().strftime("%Y%m")
  infobase['KOSHA-GUIDE'][0]['metadata']['CreationDate']=inputData['pubDate'] + " 00:00:00"
  infobase['KOSHA-GUIDE'][0]['metadata']['ModDate']=inputData['pubDate'] + " 00:00:00"
  # Sanitize the itemTitle to remove invalid filename characters
  sanitized_title = re.sub(r'[<>:"/\\|?*]', '', inputData['itemTitle'])
  infobase['KOSHA-GUIDE'][0]['metadata']['FileName'] = (
      inputData['itemNo'] + "_" + sanitized_title + "_" + inputData['pubDate'].replace("-", "")
  )
  
  infobase['KOSHA-GUIDE'][0]['data']['id']=inputData['boardNo']
  infobase['KOSHA-GUIDE'][0]['data']['title']=inputData['itemTitle']
  infobase['KOSHA-GUIDE'][0]['data']['content']['guideNumber']=inputData['itemNo']
  infobase['KOSHA-GUIDE'][0]['data']['content']['publishDate']=inputData['pubDate']
  try:
    contents = [tag.get_text().strip() for tag in soup.find_all('tr', class_='view-body')[-1].find_all(['p', 'div'])]
  except Exception as e:
    contents=[]
  infobase['KOSHA-GUIDE'][0]['data']['content']['contents'] = [text for text in contents if text]
  infobase['KOSHA-GUIDE'][0]['data']['attachments']['fileName']=inputData['itemNo']+" "+inputData['itemTitle']+"_"+inputData['pubDate'].replace("-","")+".pdf"
  infobase['KOSHA-GUIDE'][0]['data']['attachments']['fileUrl']="\\collection\\kosha-guide\\attachments\\{}".format(timeNowMonth)
  first_letter = inputData['itemNo'].split('-')[0][0]
  if first_letter in ['X', 'Z']:
      category = 'KOSHAGuideXZ'
  else:
      category = f'KOSHAGuide{first_letter}'
  infobase['KOSHA-GUIDE'][0]['metadata']['Category'] = category
  
  with open('result/'+infobase['KOSHA-GUIDE'][0]['metadata']['FileName'],'w',encoding='utf-8') as file:
    json.dump(infobase,file,ensure_ascii=False,indent=4)
  return infobase
def MakeBucket():
    aws_access_key_id = os.getenv('aws_access_key_id')
    aws_secret_access_key = os.getenv('aws_secret_access_key')
    region_name = os.getenv('region_name')
    bucket_name = 'kosha-guide'
    print(aws_access_key_id)
    print(aws_secret_access_key)
    print(region_name)
    print(bucket_name)
    s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
    )
    try:
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': region_name}
        )
        print(f"버킷 {bucket_name} 생성 성공!")
    except Exception as e:
        print(f"버킷 생성 실패: {str(e)}")

def download_file(url, save_path,fileName):
    cookies = {
        'WMONID': 'm300laClJYh',
        'cyberJSESSIONID': 'urGMsORUy_HTDY3oTDpZmrYAGVvmvfOvZ5f4E9esLWoGVRfrRI5M!313933250!835582491',
    }

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        # 'Cookie': 'WMONID=m300laClJYh; cyberJSESSIONID=urGMsORUy_HTDY3oTDpZmrYAGVvmvfOvZ5f4E9esLWoGVRfrRI5M!313933250!835582491',
        'Referer': 'https://cyber.kgs.or.kr/kgscode.codeSearch.listV2.ex.do?pubEng2=N&pageIndex=1&pblcRlmCd=&pblcMdclCd=&pblcNm=&pblcCd=&stDayY=2008&stDayM=01&etDayY=2024&etDayM=12&orderKey=stDay&pubEnd01=F',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }
    try:
        # 저장할 디렉토리 경로 생성
        save_dir = os.path.dirname(save_path)
        if save_dir:
            os.makedirs(save_dir, exist_ok=True)
            
        response = requests.get(url, cookies=cookies, headers=headers, stream=True)
        response.raise_for_status()
        
        # URL에서 파일명 추출
        
        full_path = os.path.join(save_path, fileName)
        
        with open(full_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)
        print(f"파일 다운로드 성공: {full_path}")
        return True
    except Exception as e:
        print(f"파일 다운로드 실패: {str(e)}")
        return False
def PrintS3FileNames():
    aws_access_key_id = os.getenv('aws_access_key_id')
    aws_secret_access_key = os.getenv('aws_secret_access_key')
    region_name = os.getenv('region_name')
    bucket_name = 'kosha-guide'
    prefix=""
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )

    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' in response:
            for obj in response['Contents']:
                print(obj['Key'])
        else:
            print("버킷에 파일이 없습니다.")
    except Exception as e:
        print("파일 목록을 가져오는 데 실패했습니다:", str(e))   

def UploadImageToS3(inputData):
  # AWS 계정의 액세스 키와 시크릿 키를 설정합니다.
  timeNow=datetime.datetime.now().strftime("%Y%m")
  aws_access_key_id=os.getenv('aws_access_key_id')
  aws_secret_access_key=os.getenv('aws_secret_access_key')
  region_name=os.getenv('region_name')
  bucket_name='htc-ai-datalake'

  print('aws_access_key_id:',aws_access_key_id)
  print('aws_secret_access_key:',aws_secret_access_key)
  print('region_name:',region_name)
  print('bucket_name:',bucket_name)
  
  # S3 클라이언트를 생성합니다.
  s3_client = boto3.client(
      's3',
      aws_access_key_id=aws_access_key_id,
      aws_secret_access_key=aws_secret_access_key,
      region_name=region_name
  )
  
  
  try:
      response = s3_client.upload_file(
          Filename="result/{}".format(inputData['KOSHA-GUIDE'][0]['metadata']['FileName']),
          Bucket=bucket_name,
          Key=f"collection/kosha-guide/{timeNow}/{inputData['KOSHA-GUIDE'][0]['metadata']['FileName']}"  # timeNow(예: 202412) 폴더 안에 파일 저장
      )
      print("JSON파일 업로드 성공!")
  except Exception as e:
      print("파일 업로드 실패:", str(e))
      return None          
  try:
      response = s3_client.upload_file(
          Filename="result/{}".format(inputData['KOSHA-GUIDE'][0]['data']['attachments']['fileName']),
          Bucket=bucket_name,
          Key=f"collection/kosha-guide/attachments/{timeNow}/{inputData['KOSHA-GUIDE'][0]['data']['attachments']['fileName']}"  # timeNow(예: 202412) 폴더 안에 파일 저장
      )
      print("PDF파일 업로드 성공!")
  except Exception as e:
      print("파일 업로드 실패:", str(e))
      return None          
  
def insert_dummy_data(inputData):
  # 데이터베이스 연결 정보
    initial_db_params = {
        'dbname': 'htc-aikr-prod',
        'user': 'postgres',
        'password': 'ddiMaster1!',
        'host': '127.0.0.1',
        'port': '5432'
    }
    try:
        # 데이터베이스에 연결
        connection = psycopg2.connect(**initial_db_params)
        cursor = connection.cursor()
        
        # 더미 데이터 삽입
        insert_query = """
            INSERT INTO "COLLECTION_DATA" ("NAME", "FILE_NAME", "FILE_PATH", "METHOD", "COLLECTION_ID")
            VALUES (%s, %s, %s, %s, %s)
        """
        # dummy_data = [
        #     ('name1', 'file1', '/path/to/file1', 'AUTO', 78),
        # ]
        
        datas=[]
        timeNow=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        timeNowMonth=datetime.datetime.now().strftime("%Y%m")
        koreanTitle=inputData['KOSHA-GUIDE'][0]['data']['title']
        attachmentFileName=inputData['KOSHA-GUIDE'][0]['data']['attachments']['fileName']
        attachmentFileUrl=inputData['KOSHA-GUIDE'][0]['data']['attachments']['fileUrl'].lstrip('\\')
        datas.append((koreanTitle,attachmentFileName, attachmentFileUrl, 'AUTO', "106"))
        
        for data in datas:
            cursor.execute(insert_query, data)
        
        # 변경사항 커밋
        connection.commit()
        print("Dummy data inserted successfully.")
    
    except Exception as error:
        print(f"Error: {error}")
    
    finally:
        # 연결 닫기
        if connection:
            cursor.close()
            connection.close()  
  
  
def job():
  #========게시글 가져오기        
  GetSearch()


  # #==========상세정보 가져오기
  load_dotenv()
  infobase={
      "KOSHA-GUIDE": [
          {
              "metadata": {
                  "Type": "Kosha-guide",
                  "Source": "https://www.kosha.or.kr/kosha/data/guidanceA.do",
                  "Author": "산업안전보건공단",
                  "CreationDate": "",
                  "ModDate": "",
                  "Category": "",
                  "FileName": ""
              },
              "data": {
                  "id": "",
                  "title": "",
                  "content": {
                      "guideNumber": "",
                      "publishDate": "",
                      "contents": [
                      ]
                  },
                  "attachments": {
                      "fileName": "",
                      "fileUrl": ""
                  }
              }
          }
      ]
  }

  createFolder('result')

  with open('kosha_search.json','r',encoding='utf-8') as file:
    dataList=json.load(file)
  #===========필요한것만 필터링
  # filteredDataList = [data for data in dataList if data['boardNo'] == "450"]
  # dataList = filteredDataList

  for index,data in enumerate(dataList):
    print("{}/{}번째 데이터 처리중".format(index+1,len(dataList)))
    print(data['itemNo'],data['itemTitle'])
    result=GetDetail(data,infobase)
    time.sleep(random.randint(3,6))
    if result['KOSHA-GUIDE'][0]['data']['attachments']['fileUrl']:
      print("다운로드")
      try:
        download_file(data['attachment'],'result',result['KOSHA-GUIDE'][0]['data']['attachments']['fileName'])
      except Exception as e:
        print(f"파일 다운로드 실패: {str(e)}")
    UploadImageToS3(result)
    with open('result.json','w',encoding='utf-8') as file:
      json.dump(result,file,ensure_ascii=False,indent=4)
    insert_dummy_data(result)
    # if index>=10:
    #   break

  files = glob.glob('result/*')
  for f in files:
      try:
          os.remove(f)
          print(f"{f} 삭제 성공")
      except Exception as e:
          print(f"{f} 삭제 실패: {str(e)}")




def run_job():
    job()

schedule.every().monday.at("09:00").do(run_job)
print("최초1회실행")
job()

while True:
    timeNow=datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    print("현재시간은:",timeNow)
    schedule.run_pending()
    time.sleep(10)

load_dotenv()
# PrintS3FileNames()
