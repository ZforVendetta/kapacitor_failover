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

# 将查询服务器的task列表和状态记录到字典中
# param@host kapacitor链接
# return 空
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
        # python3.6 and newer
        #print(f"  result:{r.status_code}  data:{data}")
        # else
        print("result:"+r.status_code+"  data:"+data)
    return r.status_code


# 判断kapacitor是否挂掉
# param@host fluxdb链接
# return 返回值r.status_code 正常为2XX
def influxdbAliveCheck(host):
    url="http://"+host+"/ping"
    r = requests.get(url)
    return r.status_code


deadKapacitor1 = False
deadKapacitor2 = False

while True:
    try:
        for kapacitorServer in MASTER_KAPACITOR_LIST:
            # 判断当前服务器是否正常工作
            if kapacitorAliveCheck(kapacitorServer)==204 and influxdbAliveCheck(INFLUXDB)==204:
                # 如果是主服务器1
                if kapacitorServer == MASTER_KAPACITOR_1:
                    # 判断之前是否已经挂掉
                    if not deadKapacitor1:
                        # 之前没挂就更新状态
                        kapacitorTaskStatusRecord(kapacitorServer)
                        # python3.6 and newer
                        #print(f"{kapacitorServer} task list recorded.")
                        # else
                        print(kapacitorServer+"task list recorded.")
                    else:
                        # 挂掉的服务器恢复了。切回来
                        # 在恢复的服务器里把挂掉前的状态恢复过来
                        updateKapacitor(kapacitorServer, kapacitorServer, True)
                        deadKapacitor1 = False
                        print("turn into Main server")
                        # 重新记录一下状态
                        kapacitorTaskStatusRecord(kapacitorServer)
                        # 关闭备用服务器中，与当前主服务器相同的任务
                        updateResult =updateKapacitor(CLUSTER_KAPACITOR, kapacitorServer, False)
                else:
                    # 服务器2的分支
                    if not deadKapacitor2:
                        kapacitorTaskStatusRecord(kapacitorServer)
                        # python3.6 and newer
                        #print(f"{kapacitorServer} task list recorded.")
                        # else
                        print(kapacitorServer+"task list recorded.")
                    else:
                        updateKapacitor(kapacitorServer, kapacitorServer, True)
                        deadKapacitor2 = False
                        print("trun into Main server")
                        kapacitorTaskStatusRecord(kapacitorServer)
                        updateResult =updateKapacitor(CLUSTER_KAPACITOR, kapacitorServer, False)
            else:
                # 当前主服务器或者influxdb挂掉的情况
                # 主服务器1的分支
                if kapacitorServer == MASTER_KAPACITOR_1 and not deadKapacitor1:
                    deadKapacitor1 = True
                    # python3.6 and newer
                    #print(f"!!!{kapacitorServer}ERROR!Changing server{CLUSTER_KAPACITOR}!!!")
                    # else
                    print(kapacitorServer+"ERROR.Changing server"+CLUSTER_KAPACITOR)
                    updateKapacitor(CLUSTER_KAPACITOR, kapacitorServer, True)
                    print("Changed")
                # 主服务器2的分支
                if kapacitorServer == MASTER_KAPACITOR_2 and not deadKapacitor2:
                    deadKapacitor2 = True
                    # python3.6 and newer
                    #print(f"!!!{kapacitorServer}ERROR!Changing server{CLUSTER_KAPACITOR}!!!")
                    # else
                    print(kapacitorServer+"ERROR.Changing server"+CLUSTER_KAPACITOR)
                    updateKapacitor(CLUSTER_KAPACITOR, kapacitorServer, True)
                    print("Changed")
                kapacitorTaskStatusRecord(CLUSTER_KAPACITOR)
        # 120秒检测一次
        time.sleep(120)
    except OSError:
         pass
