import time
import json

import requests
from requests import Response

# Kapacitor官方文档：https://docs.influxdata.com/kapacitor/v1.6/working

MASTER_KAPACITOR_1="192.168.114.247:9092"
MASTER_KAPACITOR_2="192.168.114.249:9092"
CLUSTER_KAPACITOR="192.168.114.248:9092"
INFLUXDB="192.168.114.247:8086"
DICT = {
    MASTER_KAPACITOR_1:{
        "DATA":{}
    },
    MASTER_KAPACITOR_2:{
        "DATA":{}
    },
    CLUSTER_KAPACITOR:{
        "DATA":{}
    }
}
# 主服务器list用于check
MASTER_KAPACITOR_LIST = [MASTER_KAPACITOR_1, MASTER_KAPACITOR_2]

#判断kapacitor是否挂掉
# param@host kapacitor链接
# return 返回值r.status_code 正常为2XX
def kapacitorAliveCheck(host):
    r = Response()
    try:
        url="http://"+host+"/kapacitor/v1/ping"
        r = requests.get(url)
    except Exception as e:
        print(e)
        r.status_code=404
    finally:
        return r.status_code

# 将查询服务器的task列表和状态记录到一个目录中
# param@host kapacitor链接
# return DICT 分服务器的任务列表状态字典
def kapacitorTaskStatusRecord(host):
    try:
        dire = {}
        url="http://"+host+"/kapacitor/v1/tasks"
        r = requests.get(url)
        tasksJson = json.loads(r.text)
        tasklen=len(tasksJson['tasks'])
        for i in range(tasklen):
            dire[tasksJson['tasks'][i]['id']]=tasksJson['tasks'][i]['status']
        DICT[host]["DATA"] = dire
        return
    except Exception:
        return 404

# 根据status列表更新kapacitor的任务状态
# param@status task列表和状态
# param@deadhost 死掉的主服务器地址（或者起死回生的
# param@deadflag 主服务器死亡状态
# return 返回值r.status_code 正常为2XX
def updateKapacitor(host,deadhost,deadflag):
    data={"status": "disabled"}
    dire=DICT[deadhost]["DATA"]
    r = Response()
    if dire==404:
        return 404
    for k,v in dire.items():
        if deadflag:
            data["status"]=v
        url = "http://{}/kapacitor/v1/tasks/{}".format(host,k)
        r= requests.patch(url,data=json.dumps(data))
        print(f"  结果：{r.status_code}  更新内容：{data}")
    return r.status_code


# 判断kapacitor是否挂掉
# param@host fluxdb链接
# return 返回值r.status_code 正常为2XX
def influxdbAliveCheck(host):
    url="http://"+host+"/ping"
    r = requests.get(url)
    return r.status_code


r = []
r2={}
flag=0
temp=True
deadKpacitor = ''

while True:
    try:
        for kapacitorServer in MASTER_KAPACITOR_LIST:
            if flag==0 and kapacitorAliveCheck(kapacitorServer)==204 and influxdbAliveCheck(INFLUXDB)==204:
                kapacitorTaskStatusRecord(kapacitorServer)
                print(f"{kapacitorServer}当前任务状态已记录。")
            elif flag!=0 and kapacitorAliveCheck(kapacitorServer) == 204 and influxdbAliveCheck(INFLUXDB) == 204:
                if kapacitorServer == deadKpacitor:
                    updateKapacitor(kapacitorServer, deadKpacitor, True)
                    print("切换回主服务器成功！")
                    r = kapacitorTaskStatusRecord(kapacitorServer)
                    updateResult =updateKapacitor(CLUSTER_KAPACITOR, deadKpacitor, False)
                    deadKpacitor = ''
                    flag = 0
            else:
                if deadKpacitor != kapacitorServer:
                    deadKpacitor = kapacitorServer
                    print(f"!!!{kapacitorServer}状态异常，正在切换{CLUSTER_KAPACITOR}!!!")
                    updateKapacitor(CLUSTER_KAPACITOR, deadKpacitor, True)
                    print("备用服务器切换成功！")
                    flag = 1
                kapacitorTaskStatusRecord(CLUSTER_KAPACITOR)

        time.sleep(10)
    except OSError:
         pass
