from flask import Flask, jsonify
import redis
import time


app = Flask(__name__)

# @app.route('/')
# def index():
#     return 'Hello World'


index_name = 'id'
redis_server = '127.0.0.1'
redis_port = '6379'
r = redis.Redis(
    host=redis_server,
    port=redis_port)

# To calculate total time to get required result.
t = time.process_time()

AD_ID = 'adId'
allClickedAds = 'allClickedAds'

response = []
@app.route('/add-record', methods=['POST'])
def create_new_mssisdn_and_adID(mssisdn, adID):
    if (r.hexists(mssisdn, adID)):
        return 'This mssisdn with the Ad_id already exists'
    else:
        r.hset(mssisdn, adID,1)
        if(r.hexists(allClickedAds,mssisdn)):
            r.incrby(allClickedAds,mssisdn,1)
            return jsonify({'Created':{
                'mssisdn':mssisdn,
                'adId':adID,
                'count':1,
                'total count':r.hget(allClickedAds,mssisdn)
            }})
        else:
            r.hset(allClickedAds,mssisdn,1)
            return jsonify({'Created': {
                'mssisdn': mssisdn,
                'adId': adID,
                'count': 1,
                'total count':1
            }})


@app.route('/fetch-msisdn-ad-count', methods=['GET'])
def getAllmssisdnCount(mssisdn):
    if(r.hexists(allClickedAds,mssisdn)):
        numberOfClicks = r.hget(allClickedAds,mssisdn)
        return jsonify({'All Ads Clicked': {
                'mssisdn': mssisdn,
                'total count':numberOfClicks
            }})
    else:
        return 'This mssisdn does not exist'

@app.route('/fetch-msisdn-ad-id-record',methods=['GET'])
def getmssisdnPerAd(mssisdn, adId):
    if(r.hexists(mssisdn,adId)):
        numberOfClicks = r.hget(mssisdn,adId)
        return jsonify({'Created': {
            'mssisdn': mssisdn,
            'adId': adId,
            'count': numberOfClicks
        }})
    else:
        return "This mssisdn hasn't clicked on the adId"
