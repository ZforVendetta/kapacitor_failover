import time
import json

import requests
from requests import Response

# Kapacitor:https://docs.influxdata.com/kapacitor/v1.6/working

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



def influxdbAliveCheck(host):
    url="http://"+host+"/ping"
    r = requests.get(url)
    return r.status_code


r = []
r2={}
temp=True
deadKapacitor1 = False
deadKapacitor2 = False

while True:
    try:
        for kapacitorServer in MASTER_KAPACITOR_LIST:
            if kapacitorAliveCheck(kapacitorServer)==204 and influxdbAliveCheck(INFLUXDB)==204:
                if kapacitorServer == MASTER_KAPACITOR_1:
                    if not deadKapacitor1:
                        kapacitorTaskStatusRecord(kapacitorServer)
                        # python3.6 and newer
                        #print(f"{kapacitorServer} task list recorded.")
                        # else
                        print(kapacitorServer+"task list recorded.")
                    else:
                        updateKapacitor(kapacitorServer, kapacitorServer, True)
                        deadKapacitor1 = False
                        print("turn into Main server")
                        r = kapacitorTaskStatusRecord(kapacitorServer)
                        updateResult =updateKapacitor(CLUSTER_KAPACITOR, kapacitorServer, False)
                else:
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
                        r = kapacitorTaskStatusRecord(kapacitorServer)
                        updateResult =updateKapacitor(CLUSTER_KAPACITOR, kapacitorServer, False)
            else:
                if kapacitorServer == MASTER_KAPACITOR_1 and not deadKapacitor1:
                    deadKapacitor1 = True
                    # python3.6 and newer
                    #print(f"!!!{kapacitorServer}ERROR!Changing server{CLUSTER_KAPACITOR}!!!")
                    # else
                    print(kapacitorServer+"ERROR.Changing server"+CLUSTER_KAPACITOR)
                    updateKapacitor(CLUSTER_KAPACITOR, kapacitorServer, True)
                    print("Changed")
                kapacitorTaskStatusRecord(CLUSTER_KAPACITOR)
                if kapacitorServer == MASTER_KAPACITOR_2 and not deadKapacitor2:
                    deadKapacitor2 = True
                    # python3.6 and newer
                    #print(f"!!!{kapacitorServer}ERROR!Changing server{CLUSTER_KAPACITOR}!!!")
                    # else
                    print(kapacitorServer+"ERROR.Changing server"+CLUSTER_KAPACITOR)
                    updateKapacitor(CLUSTER_KAPACITOR, kapacitorServer, True)
                    print("Changed")
                kapacitorTaskStatusRecord(CLUSTER_KAPACITOR)

        time.sleep(3)
    except OSError:
         pass
