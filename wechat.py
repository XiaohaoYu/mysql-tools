#!/bin/env python
#coding:utf-8
#Author: Hogan
#Descript : 微信公众号发送告警脚本


import urllib.request,json
import sys
from optparse import OptionParser


# 更改为自己公众号参数
corpid = 'wxd984fbfbeef48ab8'
secret = 'rKH-m_rwTwDG2OkVPSmAVZRbhM9_2PoHZAbxdStEgp0'
toparty = '1'
agentid = '2'


class WeChat(object):
        __token_id = ''
        # init attribute
        def __init__(self,url):
                self.__url = url.rstrip('/')
                self.__corpid = corpid
                self.__secret = secret

        # Get TokenID
        def authID(self):
                params = {'corpid':self.__corpid, 'corpsecret':self.__secret}
                data = urllib.parse.urlencode(params)
                content = self.getToken(data)

                try:
                        self.__token_id = content['access_token']
                        # print content['access_token']
                except KeyError:
                        raise KeyError

        # Establish a connection
        def getToken(self,data,url_prefix='/'):
                url = self.__url + url_prefix + 'gettoken?'
                try:
                        response = urllib.request.Request(url + data)
                except KeyError:
                        raise KeyError
                result = urllib.request.urlopen(response)
                content = json.loads(result.read())
                return content

        # Get sendmessage url
        def postData(self,data,url_prefix='/'):
                url = self.__url + url_prefix + 'message/send?access_token=%s' % self.__token_id
                request = urllib.request.Request(url,data.encode('UTF-8'))
                try:
                        result = urllib.request.urlopen(request)
                except urllib.request.HTTPError as e:
                        if hasattr(e,'reason'):
                                print ('reason',e.reason)
                        elif hasattr(e,'code'):
                                print ('code',e.code)
                        return 0
                else:
                        content = json.loads(result.read())
                        result.close()
                return content

        # send message
        def sendMessage(self,title,content):

                self.authID()

                data = json.dumps({
                        'touser':"@all",
                        'toparty':toparty,
                        'msgtype':"text",
                        'agentid':agentid,
                        'text':{
                                "content": "Title:  {0}\n Content:  {1}".format(title, content)
                        },
                        'safe':"0"
                },ensure_ascii=False)

                response = self.postData(data)
                print (response)

if __name__ == '__main__':
        a = WeChat('https://qyapi.weixin.qq.com/cgi-bin')
        title, content = sys.argv[1], sys.argv[2]
        parser = OptionParser()
        parser.add_option("-t", "--title", dest="title", default=title, )
        parser.add_option("-c", "--content", dest="content", default=content, )
        (options, args) = parser.parse_args()

        a.sendMessage(options.title,options.content)
