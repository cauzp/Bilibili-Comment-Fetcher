import hashlib
import os
import re
import time
import urllib.parse
import csv

import pandas as pd
import requests
from lxml import etree

# NOTE: Please configure your Cookie here.
HEADERS = {
    'cookie': "buvid4=B89D88A0-2128-3022-824E-841AA53F952987934-023060722-PdJr0jKE6N4t%2BlUDwV4k7hBEKj%2B%2FFX1hjQcsYJLJCnt9UVNUDe95oA%3D%3D; buvid_fp_plain=undefined; DedeUserID=279325657; DedeUserID__ckMd5=5bd56e8ddd33929c; hit-new-style-dyn=1; CURRENT_QUALITY=120; enable_web_push=DISABLE; share_source_origin=QQ; FEED_LIVE_VERSION=V_HEADER_LIVE_NO_POP; CURRENT_FNVAL=4048; bsource_origin=bing_ogv; home_feed_column=5; buvid3=5C45911B-EDC3-454F-B3A8-57E2CE2FBD3186193infoc; b_nut=1717771686; _uuid=DBCA1FD9-1026D-9F510-F2CA-11CD8C47DF9B86447infoc; header_theme_version=CLOSE; rpdid=|(J|~u)uRu|k0J'u~ku|luRR); fingerprint=6217d2f9f098d1c086a49121bdf56054; buvid_fp=6217d2f9f098d1c086a49121bdf56054; bsource=search_google; SESSDATA=2b4e76cc%2C1740206668%2Cda16d%2A82CjAIInt1GQijqCHIa_bxydvr1mDMIeu9hMofbzBrXjCSJvw6Ebxvk9KDCHr1oOdBW3QSVnlhdmk4X0tpaXZVcUlKNlJqNk1xb2hvNFo5T0sySzBpd093eFZlbHU4YTQtckVqa3hRZDJWX2hDTU5waGNWWE9kWG85VE1jUTc2NGR2TkF1MkZjZmhRIIEC; bili_jct=3c2147997da34a3ed72f87a4a3fc66ae; sid=8lip3z2l; bili_ticket=eyJhbGciOiJIUzI1NiIsImtpZCI6InMwMyIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3MjQ5MTM5MDgsImlhdCI6MTcyNDY1NDY0OCwicGx0IjotMX0.61Dlfd0H8jzocQiXhsQXXiQJnsD3u8_arCqndZA3wTQ; bili_ticket_expires=1724913848; browser_resolution=1718-1270; bp_t_offset_279325657=970266741805416453; b_lsid=2106F826E_19193D2F3F3; PVID=5",
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 Edg/127.0.0.0',
}

# 指定的视频URL
VIDEO_URL = 'https://www.bilibili.com/video/BV1SQ4y1V7do/'

class BilibiliCommentFetcher:
    """Fetches and stores Bilibili comments using the Bilibili API."""

    comment_api = 'https://api.bilibili.com/x/v2/reply/wbi/main'
    a = 'ea1db124af3c7062474693fa704f4ff8'

    def __init__(self, video_url: str) -> None:
        self.video_url = video_url
        self.bv_number = self.extract_bv_number(video_url)
        self.title = self.get_title()

    def extract_bv_number(self, url: str) -> str:
        """Extracts the BV number from the video URL."""
        match = re.search(r'BV\w+', url)
        return match.group(0) if match else None

    def get_title(self) -> str:
        """Gets the title of the video."""
        response = requests.get(self.video_url, headers=HEADERS)
        tree = etree.HTML(response.text)
        xpath = '//title[@data-vue-meta="true"]/text()'
        title = tree.xpath(xpath)[0].split('_', maxsplit=1)[0]
        return title

    def get_oid(self) -> str:
        """Gets the oid of the video."""
        response = requests.get(self.video_url, headers=HEADERS)
        pat = re.compile(r'&oid=(\d+)')
        try:
            oid = pat.search(response.text).group(1)
        except AttributeError:
            raise CookieError('Cookie is invalid or expired, please reconfigure it.')
        return oid

    def get_w_rid(self, oid: str, pagination_str: str = '{"offset":""}') -> str:
        """Gets the w_rid of the video."""
        if pagination_str == '{"offset":""}':
            pagination_str = urllib.parse.quote(pagination_str)
            l = [
                'mode=3',
                f'oid={oid}',
                f'pagination_str={pagination_str}',
                'plat=1',
                'seek_rpid=',
                'type=1',
                'web_location=1315875',
                f'wts={time.time():.0f}',
            ]
        else:
            pagination_str = urllib.parse.quote(pagination_str)
            l = [
                'mode=3',
                f'oid={oid}',
                f'pagination_str={pagination_str}',
                'plat=1',
                'type=1',
                'web_location=1315875',
                f'wts={time.time():.0f}',
            ]

        y = '&'.join(l)
        data = y + self.a

        md5 = hashlib.md5()
        md5.update(data.encode('utf-8'))
        w_rid = md5.hexdigest()
        return w_rid

    def get_next_offset_and_comments_in_page_1(
        self, oid: str, w_rid: str
    ) -> tuple[str, list[dict[str, list[str]]]]:
        """Gets the next offset and comments in page 1."""
        params = {
            'oid': f'{oid}',
            'type': '1',
            'mode': '3',
            'pagination_str': '{"offset":""}',
            'plat': '1',
            'seek_rpid': '',
            'web_location': '1315875',
            'w_rid': f'{w_rid}',
            'wts': f'{time.time():.0f}',
        }
        response = requests.get(self.comment_api, params=params, headers=HEADERS)

        data = response.json()
        next_offset = data['data']['cursor']['pagination_reply']['next_offset']

        comments = [
            {
                (
                    data['data']['replies'][i]['member']['uname'],
                    data['data']['replies'][i]['member']['sex'],
                    data['data']['replies'][i]['content']['message'],
                    data['data']['replies'][i]['like'],
                ): [
                    data['data']['replies'][i]['replies'][j]['content']['message']
                    for j in range(len(data['data']['replies'][i]['replies']))
                ]
            }
            for i in range(len(data['data']['replies']))
        ]

        return next_offset, comments

    def fetch_comments(
        self, oid: str, w_rid: str, pagination_str: str
    ) -> list[dict[str, list[str]]]:
        """Fetches comments of the page(for pages after page 1)."""
        params = {
            'oid': f'{oid}',
            'type': '1',
            'mode': '3',
            'pagination_str': pagination_str,
            'plat': '1',
            'web_location': '1315875',
            'w_rid': f'{w_rid}',
            'wts': f'{time.time():.0f}',
        }
        response = requests.get(self.comment_api, params=params, headers=HEADERS)

        data = response.json()
        comments = [
            {
                (
                    data['data']['replies'][i]['member']['uname'],
                    data['data']['replies'][i]['member']['sex'],
                    data['data']['replies'][i]['content']['message'],
                    data['data']['replies'][i]['like'],
                ): [
                    data['data']['replies'][i]['replies'][j]['content']['message']
                    for j in range(len(data['data']['replies'][i]['replies']))
                ]
            }
            for i in range(len(data['data']['replies']))
        ]
        return comments


class CookieError(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)


def save_to_csv(comments, filename):
    """Saves comments to a CSV file."""
    df = pd.concat(map(pd.Series, comments), axis=0)
    df = df.explode().rename_axis(['User Name', 'Sex', 'Comments', 'Likes']).rename('Replies')
    df.to_csv(filename, encoding='utf-8-sig')


def main():
    path = os.path.dirname(__file__)
    os.chdir(path)

    fetcher = BilibiliCommentFetcher(VIDEO_URL)
    print(f'Video found: {fetcher.title}.')
    print(f'BV number: {fetcher.bv_number}')

    oid = fetcher.get_oid()

    # NOTE: Page 1.
    w_rid = fetcher.get_w_rid(oid=oid)

    next_offset, comments_page_1 = fetcher.get_next_offset_and_comments_in_page_1(
        oid=oid, w_rid=w_rid
    )
    total_comments = comments_page_1
    print(f'Page 1: {len(total_comments)} comments fetched.')

    # NOTE: Pages after page 1.
    next_offset = next_offset.replace('"', r'\"')
    pagination_str = f'{{"offset":"{next_offset}"}}'

    page = 2
    comments_ = None
    while True:
        w_rid = fetcher.get_w_rid(oid=oid, pagination_str=pagination_str)
        comments = fetcher.fetch_comments(
            oid=oid, w_rid=w_rid, pagination_str=pagination_str
        )
        if len(comments) == 0:
            break
        elif comments == comments_:
            raise CookieError('Cookie is invalid or expired, please reconfigure it.')
        else:
            total_comments.extend(comments)
            print(f'Page {page}: {len(comments)} comments fetched.')
            
            # 每爬取一页评论，停止5秒
            time.sleep(2)
            
            # 每爬取10页，停止15秒，并将内容保存到CSV文件中
            if page % 10 == 0:
                print(f"Saving comments after page {page}...")
                save_to_csv(total_comments, f'{fetcher.bv_number}_comments_temp.csv')
                time.sleep(15)
            
            page += 1

        comments_ = comments

    # 最后保存所有评论
    print("Saving all comments...")
    save_to_csv(total_comments, f'{fetcher.bv_number}_comments.csv')
    print(f"All comments saved to {fetcher.bv_number}_comments.csv")


if __name__ == '__main__':
    main()