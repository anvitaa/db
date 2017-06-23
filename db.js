const _ = require('lodash')
const bunyan = require('bunyan')
const log = bunyan.createLogger({
  name: "yd-internal-api"
})
const request = require('request');
const cheerio = require('cheerio')
const promise_limit = require('promise-limit')
const lda = require('lda')
const article_extractor = require('article-extractor')
const fetch = require('node-fetch')
const rake = require('node-rake')


log.info('connecting to db host:', process.env.DB_HOST)

const knex = require('knex')({
  client: 'mysql',
  connection: {
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASS,
    database: process.env.DB_NAME
  }
})


// log.info([1,2,3])


var comments = getBoardCommentText()


function* lookupUserByToken(token) {
  return knex.select()
    .from('auth_tokens')
    .where('token', token)
}

function* getUserById(userId) {
  return knex.select()
    .from('users')
    .where('id', userId)
}

function* getBoardsFollowedByUser(userId) {
  return knex.select('title', 'board_id')
    .from('board_followers')
    .join('boards', 'boards.id', '=', 'board_followers.board_id')
    .where('board_followers.user_id', userId)
    .where('boards.deleted', 0)
}

function* getBoardStatsOwnedBy(userId) {
  let boards = yield getBoardsOwnedByUser(userId)
  let withStats = yield boards.map(b => {
    return addBoardStats(b, userId)
  })
  return withStats
}

// *note, this is the same query used for the board dropdown 
// a bit confusing b/c of the inner join
function* getBoardsOwnedOrFollowed(userId, orgId) {
  return knex.raw(`
    SELECT DISTINCT(b.id) as board_id,
    b.title, b.user_id as board_owner_id
    FROM boards b
    INNER JOIN organization_categories cat on b.organization_category_id = cat.id AND cat.deleted = 0
    LEFT JOIN board_followers f on b.id = f.board_id
    WHERE (b.user_id = ${userId} OR ( f.user_id = ${userId} AND f.status = 0))
    AND b.deleted = 0
    AND b.title != 'Private Bookmarks ${userId}'
    AND b.status in (1,2)
    AND b.organization_id = ${orgId}
    GROUP BY b.id
    ORDER by b.title`)
}

function* getBoardStatsOwnedOrAdminBy(userId, orgId) {
  let data = yield getBoardsOwnedOrFollowed(userId, orgId)
  data = data[0]
  let withStats = yield data.map(b => {
    return addBoardStats(b, userId)
  })
  return withStats
}

function* getBoardStatsFollowedBy(userId) {
  let boards = yield getBoardsFollowedByUser(userId)
  let withStats = yield boards.map(b => {
    return addBoardStats(b, userId)
  })
  return withStats
}

function* getBoardAdmins(boardId) {
  return knex.select('user_id')
    .from('board_admins')
    .where('board_id', boardId)
    .where('status', 1)
}

function* addBoardStats(board, userId) {

  let points = yield getAllPointsOnBoard(board.board_id)

  let avg = _.sum(points.map(x => x.total)) / points.length

  let usersPoints = points.filter(x => x.user_id == userId)

  let admins = yield getBoardAdmins(board.board_id)

  if (!usersPoints.length) {
    return _.merge(board, {
      board_admins: admins,
      users_points: 0,
      followers: points.length,
      percentile: 0,
      avg: Math.floor(avg)
    })
  }

  let position = points.map(x => x.user_id).indexOf(userId)
  let percentile = 100 - (position * 100 / points.length)

  return _.merge(board, {
    board_admins: admins,
    users_points: usersPoints[0].total,
    followers: points.length,
    percentile: Math.floor(percentile),
    avg: Math.floor(avg),

  })

}

function* getAllPointsOnBoard(boardId) {
  return knex('users_points')
    .distinct('user_id')
    .sum('points as total')
    .join('points_types', 'users_points.points_type_id', 'points_types.id')
    .where('board_id', boardId)
    .where('users_points.deleted', 0)
    .where('points_types.deleted', 0)
    .groupBy('user_id')
    .orderBy('total')
}

function* getBoardsOwnedByUser(userId) {
  return knex.select('title', 'id as board_id')
    .from('boards')
    .where('user_id', userId)
    .where('deleted', 0)
}

function* getBoardsAdminByUser(userId) {
  return knex.select('board_id')
    .from('board_admins')
    .orWhere('user_id', userId)
}

function* isBoardOwner(userId, boardId) {
  return knex.select()
    .from('boards')
    .where('user_id', userId)
    .where('id', boardId)
    .where('deleted', 0)
}

function* getBoardFollowers(boardId) {

  let subquery = knex.sum('points')
    .from('users_points')
    .where('users_points.user_id', knex.column('u.id'))
    .where('users_points.deleted', 0)
    .where('users_points.board_id', boardId)
    .as('points')

  var fields = [
    'u.id',
    'u.first_name',
    'u.last_name',
    'email',
    subquery
  ]

  return knex.select(fields)
    .from('users AS u')
    .join('board_followers', 'u.id', 'board_followers.user_id')
    .leftJoin('user_emails', 'user_emails.user_id', 'u.id')
    .where('board_followers.board_id', boardId)
    .where('primary', 1)
}

function* getPinIdsOnBoard(boardId) {
  return yield knex
    .select('id', 'user_id', 'created_at')
    .from('pins')
    .where('board_id', boardId)
    .where('deleted', 0)
    .map(x => {
      x.type = 'pin'
      return x
    })
}

function* getPinIdsOnBoardByUser(boardId, userId) {
  return yield knex
    .select('id', 'user_id', 'created_at')
    .from('pins')
    .where('board_id', boardId)
    .where('user_id', userId)
    .where('deleted', 0)
    .map(x => {
      x.type = 'pin'
      return x
    })
}


// *note 
// comments table doesnt' track board_id directly 
// but rather indirectly via pin_id
// thus why we pass in array of pins
function* getCommentIdsOnPins(pinIdsOnBoard) {
  return yield knex
    .select('id', 'user_id', 'pin_id', 'created_at')
    .from('comments')
    .where('deleted', 0)
    .whereIn('pin_id', pinIdsOnBoard.map(x => x.id))
    .map(x => {
      x.type = 'comment'
      return x
    })
}

function* getCommentIdsOnPinsByUser(pinIdsOnBoard, userId) {
  return yield knex
    .select('id', 'user_id', 'pin_id', 'created_at')
    .from('comments')
    .where('deleted', 0)
    .where('user_id', userId)
    .whereIn('pin_id', pinIdsOnBoard.map(x => x.id))
    .map(x => {
      x.type = 'comment'
      return x
    })
}

function* getPinVotesOnPins(pinIdsOnBoard) {
  return yield knex
    .select('id', 'user_id', 'pin_id')
    .from('pin_votes')
    .whereIn('pin_id', pinIdsOnBoard.map(x => x.id))
    .map(x => {
      x.type = 'vote'
      x.subtype = 'pin_vote'
      return x
    })
}

function* getCommentVotesOnPins(pinIdsOnBoard) {
  return yield knex
    .select('id', 'user_id', 'comment_id')
    .from('comment_votes')
    // TODO how are votes marked as deleted
    // .where('deleted', 0)
    .whereIn('pin_id', pinIdsOnBoard.map(x => x.id))
    .map(x => {
      x.type = 'vote'
      x.subtype = 'comment_vote'
      return x
    })
}

function* getInteractionsOnBoard(boardId) {
  let pinIdsOnBoard = yield getPinIdsOnBoard(boardId)
  let collections = yield [
    getCommentIdsOnPins(pinIdsOnBoard),
    getPinVotesOnPins(pinIdsOnBoard),
    getCommentVotesOnPins(pinIdsOnBoard),
    getBoardCommentText(pinIdsOnBoard)
  ]
  let together = Array.prototype.concat.apply([], collections.concat(pinIdsOnBoard))
  return together
}

function* getBoardCommentText(boardId) {

  let pinIdsOnBoard = yield getPinIdsOnBoard(boardId)
  let raw_comments = {}
  for (var i = 0; i < pinIdsOnBoard.length; i++) {
    var pin_id = pinIdsOnBoard[i].id
    let pin_comments = yield knex
      .select('body')
      .from('comments')
      .where('deleted', 0)
      .whereIn('pin_id', pin_id)

    raw_comments[pin_id] = {}
    if (pin_comments.length > 0) {
      raw_comments[pin_id].comments = _.map(pin_comments, 'body')
      raw_comments[pin_id].comments = _.map(raw_comments[pin_id].comments, (html_comment) => {
        return cheerio.load(html_comment).text()
      })
      var to_analyse = _.join(raw_comments[pin_id].comments, ',')
      raw_comments[pin_id].lda_analysis = lda(to_analyse.match(/[^\.!\?]+[\.!\?]+/g), 2, 5)
      try {
        raw_comments[pin_id].rake_analysis = rake.generate(to_analyse)
      } catch (err) {
        console.log(pin_id + " " + err)
      }

    }
  }
  return raw_comments

}



function* test() {

  // var x = new Promise(function(resolve, reject) {
  //   article_extractor.extractData("https://www.nytimes.com/2017/06/13/us/politics/jeff-sessions-testimony.html",
  //   function(err, data) {
  //     console.log(data)
  //     return data
  //   })

  // })  
  // return x
  var wordmap = {}
  var comparr = new Set()
    // var artisticarr = []
    // var edarr = []
    // var govarr =[]
    // var instarr =[]
    // var intllarr = []
    // var militaryarr = []
    // var poliarr = []
    // var publicinst = []
    // var relarr = []
    // var sports = []
    // var stock = []
    // var terrarr = []

  // var location = []
  // var product = []

  //  iterate through entity list
  for (var i = 0; i < anvita.length; i++) {
    var pin = anvita[i]
    pin.time = yield knex
      .select('created_at')
      .from('pins')
      .where('deleted', 0)
      .where('id', pin.pin_id)
    time = pin.time[0].created_at
    for (var j = 0; j < pin.analy.entity_list.length; j++) {
      var entity = pin.analy.entity_list[j]
      var wordstoadd = []
      wordstoadd.push(entity.form)
      var typewords = _.split(entity.sementity.type, '>')
      for (var k = 0; k < typewords.length; k++) {
        wordstoadd.push(typewords[k])
      }
      for (var l = 0; l < wordstoadd.length; l++) {
        if (wordmap[wordstoadd[l]]) {
          if (wordmap[wordstoadd[l]][time]) {
            wordmap[wordstoadd[l]][time] = wordmap[wordstoadd[l]][time] + parseInt(entity.relevance)
          } else {
            wordmap[wordstoadd[l]][time] = parseInt(entity.relevance)
          }
        } else {
          wordmap[wordstoadd[l]] = {}
          wordmap[wordstoadd[l]][time] = parseInt(entity.relevance)
        }


      }

      if (_.startsWith(entity.sementity.type, 'Top>Organization>Company')) {
        comparr.add(entity.form)
      }

    }

  }
  return [wordmap, comparr]

}


//only get relevant text - parse html stuff out
function parse(board_content) {

  try {
    console.log('try to parse')
    board_content.article_content = board_content.article_content + board_content.title + " " + board_content.user_note + " " + board_content.body + " " + cheerio.load(board_content.raw_html).text()
  } catch (err) {
    console.log(err)
  }
  // console.log('parse' + board_content.pin_id)
  return board_content
}

function text_anal(board_content) {
  board_content.lda_analysis = lda(board_content.article_content.match(/[^\.!\?]+[\.!\?]+/g), 1, 2)
  try {
    board_content.rake_analysis = rake.generate(board_content.article_content).slice(0, 10)
  } catch (err) {
    console.log(board_content.pin_id + " " + err)
  }
  return board_content

}



function meaning_cloud(pinstuff) {

  var type = pinstuff[0]
  var pin = pinstuff[1]

  return new Promise((resolve, reject) => {

    var options = {
      method: 'POST',
      url: 'http://api.meaningcloud.com/topics-2.0',
      headers: {
        'content-type': 'application/x-www-form-urlencoded'
      },
      form: {
        key: '71ce4f9e09d44e8fffe65477852af009',
        lang: 'en',
        txt: (type == 'user') ? pin.user_content : (type== "url") ? pin.url_content: null,
        tt: 'ec'
      }
    }


    request(options, function(error, response, body) {
      if (error) reject(error)
      body = JSON.parse(body)
        // console.log(body)
      if (parseInt(body.status.code) == 0) {

        function parse_entity_concept(entity) {
          var x = {}
          x[entity.form] = entity.relevance
          var rel = entity.relevance / 1.1
          _.forEach(_.tail(_.split(entity.sementity.type, '>')), (sem_part) => {
            rel = rel * 1.1
            x[sem_part] = rel
          })
          return x
        }
        if (type == "user") {
          pin.user_content_anal = {
            entities: body.entity_list.map(parse_entity_concept),
            concepts: body.concept_list.map(parse_entity_concept)
          }
        }
        if (type == "url") {
          pin.url_content_anal = {
            entities: body.entity_list.map(parse_entity_concept),
            concepts: body.concept_list.map(parse_entity_concept)
          }
        }

      }

      resolve(pin)
    })
  })
}


function* getBoardContentText(boardId) {
  let pin_db = yield knex
    .select('id', 'title', 'user_note', 'body', 'url_web', 'created_at')
    .from('pins')
    .where('deleted', 0)
    .whereIn('board_id', boardId)
    .map((pin) => {
      pin.user_content = pin.title + " " + pin.user_note + " " + pin.body
      delete pin.user_note
      delete pin.title
      delete pin.body
      return pin
    })
  let pin_comments = yield knex
    .select('body', 'user_id', 'created_at')
    .from('comments')
    .where('deleted', 0)
    .whereIn('pin_id', pin_db.map((pin) => {
      return pin.id
    }))

  let url_scrape = (pins) => {
    var promise_array = []
    _.forEach(pins, (pin) => {

      if (pin.url_web) {
        var content = new Promise((resolve, reject) => {
          article_extractor.extractData(pin.url_web, (err, res) => {
            if (err) console.log(err)
            pin.url_content = res.content
            resolve(pin.url_content)
          })
        })
        promise_array.push(content)
      }

    })
    return Promise.all(promise_array).then((value) => {
      return pins
    })

  }
  let loops = (pins) => {
    // console.log(pins)
    return new Promise((resolve, reject) => {

      let results = [],
        proc
      var pin_content_q = []
      let wrapper = () => {
        if (!pins.length && !pin_content_q.length) {
          clearInterval(proc)
          resolve(results)
        }

        if (!pin_content_q.length && pins.length) {
          var pin = pins.shift()
          if(pin.url_web) {pin_content_q.push('url')}
          pin_content_q.push('user')
        }
        meaning_cloud([pin_content_q.shift(), pin]).then(res => {

          results.push(res)
        })
      }
      proc = setInterval(wrapper, 1000)

    })

  }
  return url_scrape(pin_db).then(loops)
    // Promise.all()


}


module.exports = {
  getBoardStatsOwnedOrAdminBy,
  getAllPointsOnBoard,
  getBoardFollowers,
  getBoardStatsFollowedBy,
  getBoardStatsOwnedBy,
  getBoardsFollowedByUser,
  getBoardsOwnedByUser,
  getCommentIdsOnPins,
  getCommentIdsOnPinsByUser,
  getCommentVotesOnPins,
  getInteractionsOnBoard,
  getPinIdsOnBoard,
  getPinIdsOnBoardByUser,
  getPinVotesOnPins,
  getUserById,
  isBoardOwner,
  knex,
  test,
  getBoardCommentText,
  getBoardContentText,
  lookupUserByToken
}
