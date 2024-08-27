import requests
import re
import time
import csv
import json
import logging
from datetime import datetime

# 设置日志
logging.basicConfig(filename='bilibili_crawler.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

HEADERS = {
    'authority': 'api.bilibili.com',
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
    'cookie': "buvid4=B89D88A0-2128-3022-824E-841AA53F952987934-023060722-PdJr0jKE6N4t%2BlUDwV4k7hBEKj%2B%2FFX1hjQcsYJLJCnt9UVNUDe95oA%3D%3D; buvid_fp_plain=undefined; DedeUserID=279325657; DedeUserID__ckMd5=5bd56e8ddd33929c; hit-new-style-dyn=1; CURRENT_QUALITY=120; enable_web_push=DISABLE; share_source_origin=QQ; FEED_LIVE_VERSION=V_HEADER_LIVE_NO_POP; CURRENT_FNVAL=4048; bsource_origin=bing_ogv; home_feed_column=5; buvid3=5C45911B-EDC3-454F-B3A8-57E2CE2FBD3186193infoc; b_nut=1717771686; _uuid=DBCA1FD9-1026D-9F510-F2CA-11CD8C47DF9B86447infoc; header_theme_version=CLOSE; rpdid=|(J|~u)uRu|k0J'u~ku|luRR); fingerprint=6217d2f9f098d1c086a49121bdf56054; buvid_fp=6217d2f9f098d1c086a49121bdf56054; bsource=search_google; SESSDATA=2b4e76cc%2C1740206668%2Cda16d%2A82CjAIInt1GQijqCHIa_bxydvr1mDMIeu9hMofbzBrXjCSJvw6Ebxvk9KDCHr1oOdBW3QSVnlhdmk4X0tpaXZVcUlKNlJqNk1xb2hvNFo5T0sySzBpd093eFZlbHU4YTQtckVqa3hRZDJWX2hDTU5waGNWWE9kWG85VE1jUTc2NGR2TkF1MkZjZmhRIIEC; bili_jct=3c2147997da34a3ed72f87a4a3fc66ae; sid=8lip3z2l; bili_ticket=eyJhbGciOiJIUzI1NiIsImtpZCI6InMwMyIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3MjQ5MTM5MDgsImlhdCI6MTcyNDY1NDY0OCwicGx0IjotMX0.61Dlfd0H8jzocQiXhsQXXiQJnsD3u8_arCqndZA3wTQ; bili_ticket_expires=1724913848; browser_resolution=1718-1270; bp_t_offset_279325657=970266741805416453; PVID=5; b_lsid=69698A64_191942643FF",  # 请替换为你的cookie
    'origin': 'https://www.bilibili.com',
    'referer': 'https://www.bilibili.com/',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36 Edg/106.0.1370.47'
}

class CookieError(Exception):
    pass
https://www.bilibili.com/video/BV1Ti421a7dv/
class BilibiliCommentFetcher:
    def __init__(self, video_url: str, start_page: int = 1):
        self.video_url = video_url
        self.bv_number = self.extract_bv_number(video_url)
        self.aid = self.get_video_id()
        self.current_page = start_page
        self.comments = []

    def extract_bv_number(self, url: str) -> str:
        match = re.search(r'BV\w+', url)
        return match.group(0) if match else None

    def get_video_id(self):
        response = requests.get(self.video_url, headers=HEADERS)
        response.encoding = 'utf-8'
        content = response.text
        aid_regex = '"aid":(\d+),"bvid":"{}"'.format(self.bv_number)
        video_aid = re.findall(aid_regex, content)[0]
        return video_aid

    def fetch_comment_replies(self, comment_id, parent_user_name, max_pages=1000):
        replies = []
        for page in range(1, max_pages + 1):
            url = f'https://api.bilibili.com/x/v2/reply/wbi/main?oid={self.aid}&type=1&root={comment_id}&ps=10&pn={page}'
            try:
                response = requests.get(url, headers=HEADERS, timeout=10)
                response.raise_for_status()
                data = response.json()
                if data.get('code') != 0:
                    logging.warning(f"API返回非零代码: {data.get('code')}, 消息: {data.get('message')}")
                    raise CookieError('Cookie可能已失效，请重新配置')
                if data and 'data' in data and 'replies' in data['data']:
                    for reply in data['data']['replies']:
                        reply_info = {
                            '用户昵称': reply['member']['uname'],
                            '用户性别': reply['member']['sex'],
                            '评论内容': reply['content']['message'],
                            '被回复用户': parent_user_name,
                            '点赞数': reply['like'],
                            '评论层级': '二级评论',
                            '用户当前等级': reply['member']['level_info']['current_level'],
                            '回复时间': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(reply['ctime'])),
                            'IP属地': reply['reply_control'].get('location', 'Unknown')
                        }
                        replies.append(reply_info)
                    if len(data['data']['replies']) < 10:  # If less than 10 replies, we've reached the end
                        break
                else:
                    break
            except requests.RequestException as e:
                logging.error(f"请求出错: {e}")
                break
            time.sleep(1)  # 每次请求后暂停1秒
        return replies

    def fetch_comments(self, max_pages=1000):
        for page in range(self.current_page, max_pages + 1):
            url = f'https://api.bilibili.com/x/v2/reply?pn={page}&type=1&oid={self.aid}&sort=2'
            try:
                response = requests.get(url, headers=HEADERS, timeout=10)
                response.raise_for_status()
                data = response.json()
                if data.get('code') != 0:
                    logging.warning(f"API返回非零代码: {data.get('code')}, 消息: {data.get('message')}")
                    raise CookieError('Cookie可能已失效，请重新配置')
                if data and 'data' in data and 'replies' in data['data']:
                    for comment in data['data']['replies']:
                        comment_info = {
                            '用户昵称': comment['member']['uname'],
                            '用户性别': comment['member']['sex'],
                            '评论内容': comment['content']['message'],
                            '被回复用户': '',
                            '评论层级': '一级评论',
                            '用户当前等级': comment['member']['level_info']['current_level'],
                            '点赞数': comment['like'],
                            '回复时间': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(comment['ctime'])),
                            'IP属地': comment['reply_control'].get('location', 'Unknown')
                        }
                        self.comments.append(comment_info)
                        replies = self.fetch_comment_replies(comment['rpid'], comment['member']['uname'])
                        self.comments.extend(replies)
                    if len(data['data']['replies']) < 20:  # If less than 20 comments, we've reached the end
                        break
                else:
                    logging.info(f"第 {page} 页没有评论数据")
                    break
            except CookieError as e:
                logging.error(f"Cookie错误: {e}")
                self.save_state()
                return False
            except requests.RequestException as e:
                logging.error(f"请求出错: {e}")
                self.save_state()
                return False
            except KeyError as e:
                logging.error(f"数据结构错误: {e}")
                logging.debug(f"返回的数据: {data}")
                self.save_state()
                return False
            
            logging.info(f"已爬取 {page} 页评论")
            self.current_page = page + 1
            
            if page % 10 == 0:
                logging.info(f"已爬取 {page} 页，保存临时文件并暂停15秒...")
                self.save_comments_to_csv(self.comments, f'{self.bv_number}_comments_temp.csv')
                time.sleep(15)  # 每10页暂停15秒
            else:
                time.sleep(5)  # 其他情况暂停5秒
        
        return True

    def save_comments_to_csv(self, comments, filename):
        with open(filename, mode='w', encoding='utf-8-sig', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=['用户昵称', '用户性别', '评论内容', '被回复用户', '评论层级', 
                                                      '用户当前等级', '点赞数', '回复时间', 'IP属地'])
            writer.writeheader()
            for comment in comments:
                writer.writerow(comment)

    def save_state(self):
        state = {
            'video_url': self.video_url,
            'bv_number': self.bv_number,
            'aid': self.aid,
            'current_page': self.current_page,
        }
        with open(f'{self.bv_number}_state.json', 'w') as f:
            json.dump(state, f)
        logging.info(f"当前状态已保存到 {self.bv_number}_state.json")

    def load_state(self):
        try:
            with open(f'{self.bv_number}_state.json', 'r') as f:
                state = json.load(f)
            self.video_url = state['video_url']
            self.bv_number = state['bv_number']
            self.aid = state['aid']
            self.current_page = state['current_page']
            logging.info(f"已从 {self.bv_number}_state.json 加载状态")
            return True
        except FileNotFoundError:
            logging.info("未找到保存的状态文件")
            return False

def update_cookie():
    new_cookie = input("请输入新的cookie: ")
    HEADERS['cookie'] = new_cookie
    logging.info("Cookie已更新")

def main():
    video_url = input("请输入视频URL: ")
    start_page_input = input("请输入起始页码（如果从头开始，直接按回车）: ")
    
    start_page = 1
    if start_page_input.strip():
        try:
            start_page = int(start_page_input)
        except ValueError:
            print("无效的页码，将从第1页开始爬取。")
    
    fetcher = BilibiliCommentFetcher(video_url, start_page)
    logging.info(f"正在获取 BV号为 {fetcher.bv_number} 的视频评论...")
    
    if start_page == 1 and fetcher.load_state():
        logging.info(f"从第 {fetcher.current_page} 页继续爬取")
    else:
        logging.info(f"从第 {start_page} 页开始爬取")
    
    while True:
        if fetcher.fetch_comments():
            break
        else:
            print("Cookie可能已失效，请更新。")
            update_cookie()
    
    filename = f'{fetcher.bv_number}_comments.csv'
    fetcher.save_comments_to_csv(fetcher.comments, filename)
    logging.info(f"所有评论已保存到 {filename}")

if __name__ == '__main__':
    main()