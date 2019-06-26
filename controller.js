const { write_schedule } = require("./helper");
const { flexsearch, pool:worker_pool, connection } = require("./handler");
const index_map = {};
let worker_index = 0;
var Hashids = require('hashids');
var hashids = new Hashids('h6ALinjI');

var redis = require("redis"),
    client = redis.createClient({host: 'sg-geocode2.hktaxiapp.com', db: 1, password: '95c0aa18e14cdeba5029c9c64cae42e73abc79a347b24a21990f1d9d428acd3e'});

client.on("error", function (err) {
    console.log("Error " + err);
});

const {promisify} = require('util');
const hgetAsync = promisify(client.hget).bind(client);

module.exports = {

    index: function(req, res, next){

        try{

            if(worker_pool.length){

                const uid = (Math.random() * 999999999) >> 0;

                connection[uid] = res;

                for(let i = 1; i < worker_pool.length; i++){

                    worker_pool[i].send({

                        job: "info",
                        task: uid
                    });
                }
            }
            else{

                res.json(flexsearch.info());
            }
        }
        catch(err){

            next(err);
        }
    },

    add: function(req, res, next){

        const id = req.params.id;
        const content = req.params.content;

        if(id || (id === 0)){

            try{

                if(content){

                    register_task("add", id, content, write_schedule);
                }

                res.sendStatus(200);
            }
            catch(err){

                next(err);
            }
        }
        else{

            res.sendStatus(422);
        }
    },

    add_bulk: function(req, res, next){

        let json = req.body;

        if(json){

            try{

                if(json.constructor !== Array){

                    json = [json];
                }

                for(let i = 0, len = json.length; i < len; i++){

                    const query = json[i];
                    const id = query.id;
                    const content = query.content;

                    if(id || (id === 0)){

                        if(content){

                            register_task("add", id, content, (worker_pool.length || (i === len - 1)) && write_schedule);
                        }
                    }
                }

                res.sendStatus(200);
            }
            catch(err){

                next(err);
            }
        }
        else{

            res.sendStatus(422);
        }
    },

    update: function(req, res, next){

        const id = req.params.id;
        const content = req.params.content;

        if(id || (id === 0)){

            try{

                if(content){

                    register_task("update", id, content, write_schedule);
                }

                res.sendStatus(200);
            }
            catch(err){

                next(err);
            }
        }
        else{

            res.sendStatus(404);
        }
    },

    update_bulk: function(req, res, next){

        let json = req.body;

        if(json){

            try{

                if(json.constructor !== Array){

                    json = [json];
                }

                for(let i = 0, len = json.length; i < len; i++){

                    const query = json[i];
                    const id = query.id;
                    const content = query.content;

                    if(id || (id === 0)){

                        if(content){

                            register_task("update", id, content, (worker_pool.length || (i === len - 1)) && write_schedule);
                        }
                    }
                }

                res.sendStatus(200);
            }
            catch(err){

                next(err);
            }
        }
        else{

            res.sendStatus(422);
        }
    },

    search: async function(req, res, next){

        const query = req.params.query || req.body;

        if(query){

            try{

                if(worker_pool.length){

                    const uid = (Math.random() * 999999999) >> 0;

                    connection[uid] = res;

                    for(let i = 1; i < worker_pool.length; i++){

                        worker_pool[i].send({

                            job: "search",
                            query: query,
                            task: uid
                        });
                    }
                }
                else{
                    var search_results = await flexsearch.search(query, 5);
                    var ret = {"status": "OK", "predictions": []};

                    for (let item of search_results) {
                      console.log(item);
                      var numbers = hashids.decode(item);
                      console.log(numbers);
                      if (numbers.length) {
                        if (numbers[1]) {
                          var chinesename = await hgetAsync('ibg1000:' + numbers[1], 'chinesename');
                          var c_locality = await hgetAsync('ibg1000:' + numbers[1], 'c_locality');
                          ret['predictions'].push({
                            'description': '香港' + chinesename,
                            'place_id': item + '=',
                            'terms': [{
                              'offset': 2,
                              'value': c_locality
                            }]
                          });
                        } else if (numbers[2]) {
                          var chinesename = await hgetAsync('isg1000:' + numbers[2], 'chinesename');
                          var c_locality = await hgetAsync('isg1000:' + numbers[2], 'c_locality');
                          ret['predictions'].push({
                            'description': '香港' + chinesename,
                            'place_id': item + '=',
                            'terms': [{
                              'offset': 2,
                              'value': c_locality
                            }]
                          });
                        } else if (numbers[3]) {
                        }
                      }
                    }

                    res.json(ret);
                }
            }
            catch(err){

                next(err);
            }
        }
        else{

            res.json([]);
        }
    },

    search2: async function(req, res, next){

        const query = req.query.input;
        const language = req.query.language;
        const isEn = language && language.startsWith('en');

        if(query){

            try{

                if(worker_pool.length){

                    const uid = (Math.random() * 999999999) >> 0;

                    connection[uid] = res;

                    for(let i = 1; i < worker_pool.length; i++){

                        worker_pool[i].send({

                            job: "search",
                            query: query,
                            task: uid
                        });
                    }
                }
                else{
                    var search_results = await flexsearch.search({
                      query: query,
                      // threshold: 1,
                      // depth: 4,
                      limit: 5,
                    });
                    var ret = {"status": "OK", "predictions": []};

                    for (let item of search_results) {
                      console.log(item);
                      var numbers = hashids.decode(item);
                      console.log(numbers);
                      if (numbers.length) {
                        if (numbers[1]) {
                          if (isEn) {
                            var building = await hgetAsync('ibg1000:' + numbers[1], 'e_address');
                            var locality = await hgetAsync('ibg1000:' + numbers[1], 'e_locality');
                          } else {
                            var building = '香港' + await hgetAsync('ibg1000:' + numbers[1], 'c_address');
                            var locality = await hgetAsync('ibg1000:' + numbers[1], 'c_locality');
                          }
                          ret['predictions'].push({
                            'description': building,
                            'place_id': item + '=',
                            'terms': [{
                              'offset': 2,
                              'value': locality
                            }]
                          });
                        } else if (numbers[2]) {
                          if (isEn) {
                            var site = await hgetAsync('isg1000:' + numbers[2], 'e_address');
                            var locality = await hgetAsync('isg1000:' + numbers[2], 'e_locality');
                          } else {
                            var site = '香港' + await hgetAsync('isg1000:' + numbers[2], 'c_address');
                            var locality = await hgetAsync('isg1000:' + numbers[2], 'c_locality');
                          }
                          ret['predictions'].push({
                            'description': site,
                            'place_id': item + '=',
                            'terms': [{
                              'offset': 2,
                              'value': locality
                            }]
                          });
                        } else if (numbers[3]) {
                          if (isEn) {
                            var street = await hgetAsync('irg1000:' + numbers[3], 'e_street');
                            var locality = await hgetAsync('irg1000:' + numbers[3], 'e_locality');
                          } else {
                            var street = '香港' + await hgetAsync('irg1000:' + numbers[3], 'c_street');
                            var locality = await hgetAsync('irg1000:' + numbers[3], 'c_locality');
                          }
                          ret['predictions'].push({
                            'description': street,
                            'place_id': item + '=',
                            'terms': [{
                              'offset': 2,
                              'value': locality
                            }]
                          });
                        }
                      }
                    }

                    res.json(ret);
                }
            }
            catch(err){

                next(err);
            }
        }
        else{

            // res.json([]);
            res.json({"status": "OK", "predictions": []});
        }
    },

    remove: function(req, res, next){

        const id = req.params.id;

        if(id || (id === 0)){

            try{

                register_task("remove", id, write_schedule);

                delete index_map["@" + id];

                res.sendStatus(200);
            }
            catch(err){

                next(err);
            }
        }
        else{

            res.sendStatus(422);
        }
    },

    remove_bulk: function(req, res, next){

        const json = req.body;

        if(json){

            try{

                for(let i = 0, len = json.length; i < len; i++){

                    const id = json[i];

                    if(id || (id === 0)){

                        register_task("remove", id, (worker_pool.length || (i === len - 1)) && write_schedule);

                        delete index_map["@" + id];
                    }
                }

                res.sendStatus(200);
            }
            catch(err){

                next(err);
            }
        }
        else{

            res.sendStatus(422);
        }
    }
};

function register_task(job, id, content, write){

    if(worker_pool.length){

        let current_index;

        if(!(current_index = index_map["@" + id])){

            if(++worker_index >= worker_pool.length){

                worker_index = 1;
            }

            index_map["@" + id] = current_index = worker_index;
        }

        worker_pool[current_index].send({

            job: job,
            id: id,
            content: content,
            write: (job === "remove" ? content : write) && true
        });
    }
    else{

        flexsearch[job](id, content, write);
    }
}
