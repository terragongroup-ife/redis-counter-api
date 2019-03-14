from flask import Flask, jsonify, make_response, request
import redis
import time
import csv
import logging

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

# To use logging module to log Info/Debug message properly
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(name)s %(levelname)s %(message)s')
logger = logging.getLogger('redis_setup')

# To calculate total time to get required result.
t = time.process_time()

AD_ID = 'adId'
allClickedAds = 'allClickedAds'

response = []


#
# def run_once(f):
#     def wrapper(*args, **kwargs):
#         if not wrapper.has_run:
#             wrapper.has_run = True
#             return f(*args, **kwargs)
#     wrapper.has_run = False
#     return wrapper
#
#
# @run_once
def populate_redis():

    with open('redis_data.csv') as data:
        readCSV = csv.reader(data, delimiter=',')
        line_count = 0
        for row in readCSV:
            if (line_count == 0):
                logger.info(f'Column names are {", ".join(row)}')
                line_count += 1
            else:
                if r.hexists(row[5], row[4]):
                    r.hincrby(row[5], row[4], 1)


                else:
                    r.hset(row[5], row[4], 1)

                if r.hexists(row[5],allClickedAds):
                    r.incrby(allClickedAds, 1)
                else:
                    r.hset(row[5], allClickedAds,1)

                line_count += 1


@app.route('/add-record', methods=['POST'])
def create_new_mssisdn_and_adID(mssisdn, adID):
    mssisdn = request.data
    if (r.hexists(mssisdn, adID)):
        return 'This mssisdn with the Ad_id already exists'
    else:
        r.hset(mssisdn, adID, 1)
        if (r.hexists(mssisdn, allClickedAds)):
            r.incrby(allClickedAds, 1)

            return jsonify({'Created': {
                'mssisdn': mssisdn,
                'adId': adID,
                'count': 1,
                'total count': r.hget(allClickedAds, mssisdn)
            }})
        else:
            r.hset(mssisdn, allClickedAds, 1)
            return jsonify({'Created': {
                'mssisdn': mssisdn,
                'adId': adID,
                'count': 1,
                'total count': 1
            }})


@app.route('/fetch-msisdn-ad-count', methods=['GET'])
def getAllmssisdnCount(mssisdn):
    if r.hexists( mssisdn, allClickedAds):
        numberOfClicks = r.hget(mssisdn, allClickedAds)
        return jsonify({'All Ads Clicked': {
            'mssisdn': mssisdn,
            'total count': numberOfClicks
        }})
    else:
        return 'This mssisdn does not exist'


@app.route('/fetch-msisdn-ad-id-record', methods=['GET'])
def getmssisdnPerAd(mssisdn, adId):
    if (r.hexists(mssisdn, adId)):
        numberOfClicks = r.hget(mssisdn, adId)
        return jsonify({'Created': {
            'mssisdn': mssisdn,
            'adId': adId,
            'count': numberOfClicks
        }})
    else:
        return "This mssisdn hasn't clicked on the adId"


@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)


if __name__ == '__main__':
    populate_redis()
    app.run(debug=True)
