import http.client
import json
import time

def get_export_url():

    def get_export_id():
        conn = http.client.HTTPSConnection("app.salsify.com")
        payload = json.dumps({
            "configuration": {
                "entity_type": "product",
                "format": "csv",
                "filter": "=list:default",
                "include_all_columns": False,
                "properties":"'UPC','Product','Ready to publish','Color','Website publish','Workflow Status','USE','Shape','General Size','Recommended Style','MSRP','Wholesale Price','Designer Price','Stocking Dealer Price','Traffic recommendation','Shedding','Created Date','Construction','Material','BORDER','General Size - Computed','Features','Pile description','Construction Technique','Masterpiece','UPDATED Flat image','Filter Primary Rug Classification','Filter Style','Filter Patterns','Filter Material','Filter Color 1','Product Patterns','Product Colors','Product Styles','Non-slip back','Reversible','Latex free','MACHINE WASHABLE?','All-natural','Recycled','JPG Export - Flat image','Fill Material','Back Material','Cover Material'"}

        })
        headers = {
            'Content-Type': 'application/json',
            'Authorization': ''
        }
        conn.request("POST", "/api/orgs/123456789/export_runs", payload, headers)
        res = conn.getresponse()
        data = res.read()
        res_data = json.loads(data.decode("utf-8"))
        url_id = res_data['id']
        # print(url_id)
        return url_id


    conn = http.client.HTTPSConnection("app.salsify.com")
    payload_2 = ''
    header_2 = {
        'Authorization': '',
        'Connection': "keep-alive"
    }
    export_id=get_export_id()
    t = 0
    while True:
        try:
            # print("start- sleep", t)
            time.sleep(30)
            conn.request("GET", "/api/orgs/123456789/export_runs/{}".format(export_id),
                         payload_2, header_2)
            res = conn.getresponse()
            # print(res.status)
            data = res.read()
            res_data = json.loads(data.decode("utf-8"))
            export_url = res_data['url']
            if export_url != '':
                return export_url
            else:
                t += 30
                # print("t is ", t)
            if t > 3600:
                print("Couldn't get the export_url within one hour time limit!")
                break
        except ConnectionError as e:
            print('Could not establish connection:', str(e))
            raise e
    return export_url
