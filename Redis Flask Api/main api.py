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

                    # count = r.hincrby(row[5], row[4], 1)

                    #logger.debug("userID: " + str(row[5]) + ' adId: ' + str(row[4]) + ' exists count: ' + str(r.hget(row[5], row[4])))


                else:
                    r.hset(row[5], row[4], 1)

                if r.hexists(row[5], allClickedAds+str(row[5])):
                    r.hincrby( row[5] , allClickedAds+str(row[5]), 1)

                    logger.debug(str(row[5]) + " exists new Count: " + str(r.hget(row[5], allClickedAds+str(row[5]))))
                else:
                    r.hset(row[5], allClickedAds+row[5], 1)

                line_count += 1


@app.route('/add-record', methods=['POST'])
def create_new_msisdn_and_adID():
    content = request.get_json(force= True)
    msisdn = content['msisdn']
    adID = content['adId']
    if (r.hexists(msisdn, adID)):
        return 'This msisdn with the Ad_id already exists'
    else:
        r.hset(msisdn, adID, 1)
        if (r.hexists(msisdn, allClickedAds)):
            r.incrby(allClickedAds+str(msisdn), 1)

            return jsonify({'Created': {
                'msisdn': msisdn,
                'adId': adID,
                'count': 1,
                'total count': r.hget(msisdn, allClickedAds+str(msisdn))
            }})
        else:
            r.hset(msisdn, allClickedAds+str(msisdn), 1)
            return jsonify({'Created': {
                'msisdn': msisdn,
                'adId': adID,
                'count': 1,
                'total count': 1
            }})


@app.route('/fetch-msisdn-ad-count', methods=['GET'])
def getAllmsisdnCount():
    content = request.get_json(force=True)
    msisdn = content['msisdn']
    if r.hexists( msisdn, allClickedAds+str(msisdn)):
        numberOfClicks = r.hget(msisdn, allClickedAds+str(msisdn))
        return jsonify({'All Ads Clicked': {
            'msisdn': str(msisdn),
            'total count': str(numberOfClicks)
        }})
    else:
        return 'This msisdn does not exist'



@app.route('/fetch-msisdn-ad-id-record', methods=['GET'])
def getmsisdnPerAd():
    content = request.get_json(force=True)

    msisdn = content['msisdn']

    adId = content['adId']

    if (r.hexists(msisdn, adId)):
        numberOfClicks = r.hget(msisdn, adId)
        return jsonify({'Get msisdn per ID': {
            'msisdn': msisdn,
            'adId': adId,
            'count': str(numberOfClicks)
        }})
    else:
        return jsonify({"Error" : "This msisdn hasn't clicked on the adId"})


# @app.route('/get-all', methods=['GET'])
# def getAll():
#     id = request.args.get('id')
#     return str(r.hgetall(str(id)))


@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)


if __name__ == '__main__':
    r.flushall()
    populate_redis()
    app.run(debug=True)
